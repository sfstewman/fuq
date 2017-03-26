package node

import (
	"context"
	"fmt"
	"github.com/sfstewman/fuq"
	"github.com/sfstewman/fuq/proto"
	"log"
	"sync"
)

const MaxUint16 = int(^uint16(0))

type channelQueuer struct {
	actCh chan WorkerAction
	updCh chan fuq.JobStatusUpdate
}

func newChannelQueuer() channelQueuer {
	return channelQueuer{
		actCh: make(chan WorkerAction),
		updCh: make(chan fuq.JobStatusUpdate),
	}
}

func (cq channelQueuer) RequestAction(ctx context.Context, nproc int) (WorkerAction, error) {
	select {
	case act, ok := <-cq.actCh:
		if !ok {
			goto stop
		}
		return act, nil
	case <-ctx.Done():
		goto stop
	}

stop:
	return StopAction{All: false}, nil
}

func (cq channelQueuer) UpdateAndRequestAction(ctx context.Context, status fuq.JobStatusUpdate, nproc int) (WorkerAction, error) {
	done := ctx.Done()
	select {
	case cq.updCh <- status:
		return cq.RequestAction(ctx, nproc)
	case <-done:
		return StopAction{All: false}, nil
	}
}

type DispatchConfig struct {
	DefaultLogDir string
	Logger        *log.Logger
	Messenger     proto.Messenger
}

/* Dispatch maintains a pool of workers, and communicates with the
 * Foreman to distribute work to these workers.
 *
 */
type Dispatch struct {
	mu sync.Mutex

	workers []*Worker
	tasks   []fuq.Task

	stopSignal chan struct{}

	Logger        *log.Logger
	Queuer        channelQueuer
	Messenger     proto.Messenger // proto.WebsocketMessenger
	M             *proto.Conn
	DefaultLogDir string
}

func NewDispatch(cfg DispatchConfig) *Dispatch {
	d := Dispatch{
		Logger:        cfg.Logger,
		Messenger:     cfg.Messenger,
		Queuer:        newChannelQueuer(),
		DefaultLogDir: cfg.DefaultLogDir,
	}

	mopts := proto.Opts{
		Messenger: cfg.Messenger,
		Flusher:   proto.NopFlusher{},
		Worker:    true,
	}

	d.M = proto.NewConn(mopts)

	d.M.OnMessageFunc(proto.MTypeJob, d.onJob)
	d.M.OnMessageFunc(proto.MTypeStop, d.onStop)
	// TODO: handle RESET ?

	return &d
}

func (d *Dispatch) runWorker(ctx context.Context, worker *Worker) {
	worker.Loop(ctx)

	d.mu.Lock()
	defer d.mu.Unlock()

	ind := -1
	for i, w := range d.workers {
		if w == worker {
			ind = i
			break
		}
	}

	log.Printf("worker %p (%d) done", worker, ind)
	if ind >= 0 {
		n := len(d.workers)
		copy(d.workers[ind:n-1], d.workers[ind+1:])
		d.workers = d.workers[:n-1]
	}
}

func (d *Dispatch) StartWorkers(ctx context.Context, n int, r Runner) {
	d.mu.Lock()
	defer d.mu.Unlock()

	workers := d.workers
	if len(workers) > 0 {
		// this is largely to avoid having two sets of workers
		// with a different workerCtx.
		//
		// XXX - allow groups of workers to have different
		// worker contexts
		panic("StartWorkers cannot be called when Dispatch already has workers")
	}

	workerCtx := WorkerContext(ctx)

	off := len(workers)
	for i := 0; i < n; i++ {
		w := &Worker{
			Seq:           off + i,
			Logger:        d.Logger,
			Runner:        r,
			Queuer:        d.Queuer,
			DefaultLogDir: d.DefaultLogDir,
		}

		workers = append(workers, w)

		go d.runWorker(workerCtx, w)
	}

	d.workers = workers
}

func (d *Dispatch) enqueueAction(act WorkerAction) error {
	nproc, _ := d.numProcs()

	if nproc == 0 {
		return fmt.Errorf("nproc == 0 with actions to enqueue")
	}

	actCh := d.Queuer.actCh

	// XXX - journal: task enqueued
	select {
	case actCh <- act:
		return nil
	default:
		return fmt.Errorf("enqueuing more tasks than workers")
	}
}

func (d *Dispatch) Enqueue(tasks []fuq.Task) (nproc, nrun uint16, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// actCh := d.Queuer.actCh
	np, nr := d.numProcs()

	nproc = uint16(np)
	nrun = uint16(nr)

	for _, newTask := range tasks {
		action := RunAction(newTask)
		if err = d.enqueueAction(action); err != nil {
			return
		}

		d.tasks = append(d.tasks, newTask)
		nproc--
		nrun++
	}

	// sanity check
	np, nr = d.numProcs()
	if uint16(np) != nproc || uint16(nr) != nrun {
		panic(fmt.Sprintf("expected (nproc|nrun) = (%d|%d), actual value is (%d|%d)",
			nproc, nrun, np, nr))
	}

	err = nil
	return
}

func (d *Dispatch) numProcs() (nproc, nrun int) {
	// assumes that d is locked
	nw := len(d.workers)
	nrun = len(d.tasks)
	nproc = nw - nrun

	if nproc < 0 {
		panic(fmt.Sprintf("invalid nproc: nproc=%d, nproc < 0", nproc))
	}

	if nproc > MaxUint16 {
		panic(fmt.Sprintf("invalid nproc: nproc=%d, max is 16 bits %d", nproc, MaxUint16))
	}

	if nrun > MaxUint16 {
		panic(fmt.Sprintf("invalid nrun: nrun=%d, max is 16 bits %d", nrun, MaxUint16))
	}

	return
}

func (d *Dispatch) NumProcs() (nproc, nrun int) {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.numProcs()
}

func checkOK(m proto.Message, nproc, nrun uint16) error {
	switch m.Type {
	case proto.MTypeOK:
		/* okay */
	case proto.MTypeError:
		ecode, arg0 := m.AsError()
		return fmt.Errorf("error response: code=%d, arg=%d", ecode, arg0)
	default:
		return fmt.Errorf("expected OK response, but received %s", m.Type)
	}

	mnp, mnr := m.AsOkay()

	if mnp != nproc || mnr != nrun {
		return fmt.Errorf("expected OK(%d|%d) but received OK(%d|%d)",
			nproc, nrun, mnp, mnr)
	}

	return nil
}

func (d *Dispatch) sendStop(isImmed bool, waitCh chan struct{}) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	act := StopAction{All: isImmed, WaitChan: waitCh}
	return d.enqueueAction(act)
}

func (d *Dispatch) Stop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopSignal == nil {
		return
	}

	close(d.stopSignal)
	d.stopSignal = nil
}

func (d *Dispatch) onStop(msg proto.Message) proto.Message {
	var (
		nproc, nrun int
	)

	nproc, nrun = d.NumProcs()

	np := msg.AsStop()
	isImmed := false
	if np == 0 {
		np = 1
	}

	if np == proto.StopImmed {
		isImmed = true
		np = 1
	}

	if np > uint32(nproc) {
		np = uint32(nproc)
	}

	log.Printf("stopping %d workers", np)

	for i := uint32(0); i < np; i++ {
		waitCh := make(chan struct{})
		if err := d.sendStop(isImmed, waitCh); err != nil {
			nproc, nrun := d.NumProcs()
			log.Printf("error enqueuing %d stop requests: %v", np, err)
			return proto.ErrorMessage(proto.MErrNoProcs,
				proto.NProcsToU32(uint16(nproc), uint16(nrun)),
				msg.Seq)
		}
		<-waitCh
	}

	if isImmed {
		d.Stop()
	}

	nproc, nrun = d.NumProcs()
	return proto.OkayMessage(uint16(nproc), uint16(nrun), msg.Seq)
}

func (d *Dispatch) onJob(msg proto.Message) proto.Message {
	taskPtr := msg.Data.(*[]fuq.Task)
	tasks := *taskPtr
	log.Printf("received %d tasks: %v", len(tasks), tasks)

	nproc, nrun, err := d.Enqueue(tasks)
	if err != nil {
		log.Printf("error enqueuing %d tasks: %v", len(tasks), err)
		return proto.ErrorMessage(proto.MErrNoProcs,
			proto.NProcsToU32(uint16(nproc), uint16(nrun)),
			msg.Seq)
	}

	return proto.OkayMessage(uint16(nproc), uint16(nrun), msg.Seq)
}

func findTask(tasks []fuq.Task, job fuq.JobId, taskNum int) int {
	for i, t := range tasks {
		if t.JobId == job && t.Task == taskNum {
			return i
		}
	}

	return -1
}

func (d *Dispatch) recordJobFinished(upd fuq.JobStatusUpdate) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// fine for small scans... maybe replace with a hash if we ever
	// queue on machines with lots of cores
	ind := findTask(d.tasks, upd.JobId, upd.Task)

	np, nr := d.numProcs()
	if ind < 0 {
		return fmt.Errorf("confused state?  cannot find job %d, task %d",
			upd.JobId, upd.Task)
	}

	n := len(d.tasks)
	if ind < n-1 {
		copy(d.tasks[ind:n-1], d.tasks[ind+1:])
		// d.tasks[ind] = d.tasks[n-1]
	}
	d.tasks = d.tasks[:n-1]

	nproc, nrun := d.numProcs()
	if nproc != (np+1) || nrun != (nr-1) {
		panic(fmt.Sprintf(
			"expected (nproc|nrun) = (%d|%d) after recording job finished, but found (%d|%d)",
			np+1, nr-1, nproc, nrun))
	}

	return nil
}

func (d *Dispatch) sendUpdate(ctx context.Context, upd fuq.JobStatusUpdate) error {
	err := d.recordJobFinished(upd)
	if err != nil {
		return err
	}

	resp, err := d.M.SendUpdate(ctx, upd)
	if err != nil {
		return err
	}

	if resp.Type != proto.MTypeOK {
		return fmt.Errorf("expected OK response, but received: %v", resp)
	}

	return nil
}

func (d *Dispatch) sendHello(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	nproc, nrun := d.numProcs()
	resp, err := d.M.SendHello(ctx, proto.HelloData{
		NumProcs: nproc,
		Running:  d.tasks,
	})

	if err != nil {
		return fmt.Errorf("error during HELLO: %v", err)
	}

	if err := checkOK(resp, uint16(nproc), uint16(nrun)); err != nil {
		return fmt.Errorf("error in HELLO reply: %v", err)
	}

	return nil
}

func (d *Dispatch) QueueLoop(ctx context.Context) error {
	stopSignal := make(chan struct{})

	d.mu.Lock()
	if d.stopSignal != nil {
		d.mu.Unlock()
		panic("queue loop already started")
	}

	d.stopSignal = stopSignal
	d.mu.Unlock()

	errCh := make(chan error)

	if err := d.Messenger.Dial(); err != nil {
		return err
	}

	go func() {
		if err := d.M.ConversationLoop(ctx); err != nil {
			errCh <- err
		}
	}()

	if err := d.sendHello(ctx); err != nil {
		return err
	}

	for {
		updCh := d.Queuer.updCh

		// Check for: job update, cancellation
		select {
		case err := <-errCh:
			log.Printf("error in conversation loop: %v", err)
			return err

		case upd := <-updCh:
			if err := d.sendUpdate(ctx, upd); err != nil {
				log.Printf("error sending update for job %d, task %d (status=%s): %v",
					upd.JobId, upd.Task, upd.Status, err)
			}

		case <-stopSignal:
			log.Print("Dispatch: STOP signaled")
			return nil

		case <-ctx.Done():
			log.Print("Dispatch: context canceled")
			return nil
		}
	}
}

/*
func (d *Dispatch) Dial() (*websocket.Conn, *http.Response, error) {
	cookie := d.Config.Cookie()

	tlsConfig, err := fuq.SetupTLSRootCA(d.E.Config)
	if err != nil {
		return nil, nil, fmt.Errorf("error setting up tls config: %v", err)
	}

	dialer := websocket.Dialer{
		TLSClientConfig:   tlsConfig,
		HandshakeTimeout:  30 * time.Second,
		ReadBufferSize:    1024,
		WriteBufferSize:   1024,
		EnableCompression: false,
	}

	url := d.E.Config.EndpointURL("node/converse")
	headers := http.Header{}
	headers.Add("Cookie", string(cookie))
	return dialer.Dial(url, headers)
}
*/

/*
func makeEndpoint(c Config, configHTTP2 bool) (*Endpoint, error) {
	tlsConfig, err := fuq.SetupTLSRootCA(c)
	if err != nil {
		return nil, fmt.Errorf("error setting up tls config: %v", err)
	}

	transport := &http.Transport{
		TLSClientConfig:    tlsConfig,
		DisableCompression: true,
	}

	// enable http/2 support
	if configHTTP2 {
		if err := http2.ConfigureTransport(transport); err != nil {
			return nil, fmt.Errorf("error adding http/2 support: %v", err)
		}
	}

	client := &http.Client{
		Transport: transport,
	}

	return &Endpoint{
		Config: c,
		Client: client,
	}, nil
}
*/
