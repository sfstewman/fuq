package node

import (
	"context"
	"errors"
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

var errSendHello = errors.New("foreman requested HELLO")

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

	nstop int

	signals struct {
		workerFinished chan struct{}
		stop           chan struct{}
	}

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
	d.M.OnMessageFunc(proto.MTypeCancel, d.onCancel)
	// TODO: handle RESET ?

	return &d
}

func (d *Dispatch) runWorker(ctx context.Context, worker *Worker) {
	worker.Loop(ctx)

	d.mu.Lock()
	defer d.mu.Unlock()

	d.signalWorkerFinished()

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

func (d *Dispatch) signalWorkerFinished() {
	// d.mu LOCK MUST BE HELD BY CALLER

	signal := d.signals.workerFinished
	d.signals.workerFinished = nil

	if signal == nil {
		return
	}

	log.Printf("node.Dispatch(%p): closing workerFinished (%v)",
		d, signal)
	close(signal)
}

func (d *Dispatch) runningState() (nproc, nrun, nstop int, signal chan struct{}) {
	d.mu.Lock()
	defer d.mu.Unlock()

	nproc, nrun = d.numProcs()
	nstop = d.nstop

	signal = make(chan struct{})
	d.signals.workerFinished = signal
	log.Printf("node.Dispatch(%p): created new workerFinished signal (%v)",
		d, signal)

	return nproc, nrun, nstop, signal
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

func (d *Dispatch) NumProcs() (nproc, nrun, nstop int) {
	d.mu.Lock()
	defer d.mu.Unlock()

	nproc, nrun = d.numProcs()
	nstop = d.nstop
	return nproc, nrun, nstop
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
	act := StopAction{All: isImmed, WaitChan: waitCh}
	return d.enqueueAction(act)
}

func (d *Dispatch) sendStopIfAnyToStop() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.nstop == 0 {
		return nil
	}

	log.Printf("node.Dispatch(%p): nstop=%d, sending STOP", d, d.nstop)

	waitCh := make(chan struct{})
	if err := d.sendStop(false, waitCh); err != nil {
		return err
	}

	<-waitCh
	d.nstop--
	log.Printf("node.Dispatch(%p): STOP received, nstop=", d, d.nstop)
	return nil
}

func (d *Dispatch) Stop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stop()
}

func (d *Dispatch) stop() {
	// LOCK MUST BE HELD BY CALLER
	if d.signals.stop == nil {
		return
	}

	close(d.signals.stop)
	d.signals.stop = nil
}

func (d *Dispatch) onStop(msg proto.Message) proto.Message {
	var (
		nproc, nrun int
	)

	d.mu.Lock()
	defer d.mu.Unlock()

	nproc, nrun = d.numProcs()
	nstop := msg.AsStop()
	if nstop == 0 {
		nstop = 1
	}

	isImmed := false
	if nstop == proto.StopImmed {
		isImmed = true
		nstop = 1
	}

	np := nstop
	if np > uint32(nproc) {
		np = uint32(nproc)
	}

	log.Printf("node.Dispatch(%p): STOP(%#v) received, stopping %d workers", d, msg, np)

	for i := uint32(0); i < np; i++ {
		waitCh := make(chan struct{})
		if err := d.sendStop(isImmed, waitCh); err != nil {
			nproc, nrun := d.numProcs()
			log.Printf("error enqueuing %d stop requests: %v", np, err)
			return proto.ErrorMessage(proto.MErrNoProcs,
				proto.NProcsToU32(uint16(nproc), uint16(nrun)),
				msg.Seq)
		}
		<-waitCh
		nstop--
		nproc--
	}

	// remaining left to stop
	d.nstop = int(nstop)

	if isImmed {
		d.stop()
	}

	return proto.OkayMessage(uint16(nproc), uint16(nrun), msg.Seq)
}

func (d *Dispatch) onJob(msg proto.Message) proto.Message {
	tasks := msg.Data.([]fuq.Task)
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

func (d *Dispatch) cancelTasks(pairs []fuq.TaskPair) int {
	// XXX - this prevents d.workers from changing, but doesn't
	// directly prevent jobs from being enqueued or updates from
	// being sent back.  I'm not sure if we need to do this, though.
	// Two reasons:
	//
	// 1. This function is (currently) meant to be called from the
	//    onCancel function, which is called from within the
	//    goroutine that runs the event dispatch loop.
	//
	//    This means that new tasks cannot be enqueued before
	//    this function returns, because JOB messages are blocked
	//    until the event dispatch loop can proceed.
	//
	// 2. Each worker is processed once, and their current action is
	//    locked during the processing.  This prevents a worker from
	//    updating at the same time that it's being processed.
	//
	d.mu.Lock()
	defer d.mu.Unlock()

	ncanceled := 0

	// This is a double linear scan, which isn't particularly
	// efficient, but it should be fine for a small number of
	// workers and cancel requests.  If either becomes large and
	// cancel requests become common, we should revisit how we do
	// this.
	//
	// Iterate over the workers first so we process each worker in
	// turn.
	for _, w := range d.workers {
		err := w.WithCurrent(func(w *Worker, act WorkerAction, cancel context.CancelFunc) error {
			for _, pair := range pairs {
				if act == nil {
					continue
				}

				runAct, ok := act.(RunAction)
				if !ok {
					continue
				}

				if runAct.JobId != pair.JobId {
					continue
				}

				if pair.Task < 0 || runAct.Task == pair.Task {
					log.Printf("node.Dispatch(%p): canceling job %d, task %d on worker %p",
						d, pair.JobId, pair.Task, w)
					cancel()
					ncanceled++
				}
			}

			return nil
		})
		if err != nil {
			panic(fmt.Sprintf("unexpected error while canceling tasks: %v", err))
		}
	}

	return ncanceled
}

func (d *Dispatch) onCancel(msg proto.Message) proto.Message {
	pairs := msg.Data.([]fuq.TaskPair)
	log.Printf("node.Dispatch(%p): cancel request %#v", d, pairs)

	ncanceled := d.cancelTasks(pairs)

	// OK message is (ncanceled|0)
	return proto.OkayMessage(uint16(ncanceled), 0, msg.Seq)
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
	resp, err := d.M.SendUpdate(ctx, upd)
	if err != nil {
		return err
	}

	if err := d.recordJobFinished(upd); err != nil {
		return err
	}

	if resp.Type != proto.MTypeOK {
		return fmt.Errorf("expected OK response, but received: %v", resp)
	}

	np, nr := resp.AsOkay()
	if np == ^uint16(0) && nr == ^uint16(0) {
		log.Printf("node.Dispatch(%p): Foreman requested HELLO", d)
		return errSendHello
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

	// BEGIN CRITICAL REGION
	d.mu.Lock()
	if d.signals.stop != nil {
		d.mu.Unlock()
		panic("queue loop already started")
	}
	d.signals.stop = stopSignal
	d.mu.Unlock()
	// END CRITICAL REGION

	errCh := make(chan error)

	go func() {
		defer close(errCh)
		if err := d.M.ConversationLoop(ctx); err != nil {
			errCh <- err
		}
	}()

	sendHello := true

	for {
		if sendHello {
			sendHello = false
			if err := d.sendHello(ctx); err != nil {
				return err
			}
		}

		updCh := d.Queuer.updCh
		np, nr, ns, wsig := d.runningState()
		log.Printf("node.Dispatch(%p): np=%d, nr=%d, ns=%d", d, np, nr, ns)

		if np == 0 && nr == 0 {
			log.Printf("node.Dispatch(%p): no remaining workers, stopping", d)
			return nil
		}

		// Check for: job update, cancellation
		select {
		case err := <-errCh:
			if err != nil {
				log.Printf("node.Dispatch(%p): error in conversation loop: %v", d, err)
			} else {
				log.Printf("node.Dispatch(%p): conversation loop ended", d)
			}
			return err

		case upd := <-updCh:
			log.Printf("node.Dispatch(%p): UPDATE for job %d, task %d (status=%s)",
				d, upd.JobId, upd.Task, upd.Status)

			err := d.sendUpdate(ctx, upd)
			switch err {
			case nil:
				/* nop */
			case errSendHello:
				sendHello = true

			default: /* err != nil */
				log.Printf("node.Dispatch(%p): error sending update for job %d, task %d (status=%s): %v",
					d, upd.JobId, upd.Task, upd.Status, err)
			}

			if err := d.sendStopIfAnyToStop(); err != nil {
				log.Printf("node.Dispatch(%p): error sending STOP: %v", d, err)
			}

		case <-wsig:
			log.Printf("node.Dispatch(%p): worker finished", d)

		case <-stopSignal:
			log.Print("Dispatch: STOP signaled")
			return nil

		case <-ctx.Done():
			log.Print("Dispatch: context canceled")
			return nil
		}
	}
}
