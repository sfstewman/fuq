package node

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/sfstewman/fuq"
	"github.com/sfstewman/fuq/fuqtest"
	"github.com/sfstewman/fuq/proto"
	"github.com/sfstewman/fuq/websocket"
)

type okayRunner struct {
	mu sync.Mutex
	T  []fuq.Task
	E  error
}

func (r *okayRunner) Run(ctx context.Context, t fuq.Task, w *Worker) (fuq.JobStatusUpdate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.T = append(r.T, t)
	status := fuq.JobStatusUpdate{
		JobId:   t.JobId,
		Task:    t.Task,
		Success: r.E == nil,
		Status:  "",
	}

	log.Printf("task: %v.  update: %v", t, status)

	return status, r.E
}

type dispatcherTestState struct {
	tmpDir     string
	ctx        context.Context
	cancelFunc context.CancelFunc

	socketPair *fuqtest.WSPair
	messenger  *websocket.Messenger

	runner     Runner
	dispatcher *Dispatch

	serverSignal, dispatchSignal chan struct{}
}

func newDispatcherTestState(nw int, runner Runner) *dispatcherTestState {
	dts := dispatcherTestState{}
	defer func() {
		if exc := recover(); exc != nil {
			dts.Close()
			panic(exc)
		}
	}()

	tmpDir, err := ioutil.TempDir(os.TempDir(), "dispatch_test")
	if err != nil {
		panic(err)
	}

	dts.tmpDir = tmpDir
	dts.ctx, dts.cancelFunc = context.WithCancel(context.Background())
	dts.socketPair = fuqtest.NewWSPair(tmpDir)
	dts.messenger = &websocket.Messenger{
		C:       dts.socketPair.CConn,
		Timeout: 1 * time.Second,
	}

	dts.runner = runner
	dts.dispatcher = NewDispatch(DispatchConfig{
		Messenger: dts.messenger,
	})

	if nw > 0 {
		dts.dispatcher.StartWorkers(dts.ctx, nw, dts.runner)
	}

	return &dts
}

func (dts *dispatcherTestState) Close() {
	if dts.socketPair != nil {
		dts.socketPair.Close()
	}

	if dts.cancelFunc != nil {
		dts.cancelFunc()
	}
}

func (dts *dispatcherTestState) startServer() (*proto.Conn, chan proto.Message, chan struct{}) {
	d, ctx := dts.dispatcher, dts.ctx

	server := proto.NewConn(proto.Opts{
		Messenger: &websocket.Messenger{
			C:       dts.socketPair.SConn,
			Timeout: 1 * time.Second,
		},
		Worker: false,
	})

	msgCh := make(chan proto.Message)
	syncCh := make(chan struct{})
	server.OnMessageFunc(proto.MTypeHello, func(m proto.Message) proto.Message {
		msgCh <- m
		data := m.Data.(proto.HelloData)
		np := uint16(data.NumProcs)
		nr := uint16(len(data.Running))
		return proto.OkayMessage(np, nr, m.Seq)
	})

	server.OnMessageFunc(proto.MTypeUpdate, func(m proto.Message) proto.Message {
		msgCh <- m

		// this cheats, but we're not testing whether the
		// foreman can keep track of this here
		np, nr, ns := d.NumProcs()
		log.Printf("UPDATE np=%d, nr=%d, ns=%d", np, nr, ns)
		return proto.OkayMessage(uint16(np+1), uint16(nr-1), m.Seq)
	})

	dts.serverSignal = make(chan struct{})
	dts.dispatchSignal = make(chan struct{})

	go func(syncCh, signal chan struct{}) {
		syncCh <- struct{}{}
		if err := server.ConversationLoop(ctx); err != nil {
			panic(err)
		}
		close(signal)
	}(syncCh, dts.serverSignal)
	<-syncCh

	go func(syncCh, signal chan struct{}) {
		syncCh <- struct{}{}
		if err := d.QueueLoop(ctx); err != nil {
			if err == proto.ErrClosed {
				return
			}

			panic(err)
		}
		close(signal)
	}(syncCh, dts.dispatchSignal)
	<-syncCh

	return server, msgCh, syncCh
}

func TestDispatcherJobs(t *testing.T) {
	runner := &okayRunner{}

	dts := newDispatcherTestState(2, runner)
	defer dts.Close()

	ctx := dts.ctx
	server, msgCh, _ := dts.startServer()

	hello := <-msgCh
	hdata := hello.Data.(proto.HelloData)
	if hdata.NumProcs != 2 {
		t.Errorf("invalid HELLO: expected NumProcs = %d, found %d",
			2, hdata.NumProcs)
	}

	if len(hdata.Running) != 0 {
		t.Errorf("invalid HELLO: expected #Running = %d, found %d",
			0, len(hdata.Running))
	}

	tasks := []fuq.Task{
		fuq.Task{
			Task: 1,
			JobDescription: fuq.JobDescription{
				JobId:      fuq.JobId(7),
				Name:       "this_job",
				NumTasks:   3,
				WorkingDir: ".",
				LoggingDir: ".",
				Command:    "/bin/true", // XXX - make more portable!
			},
		},
		fuq.Task{
			Task: 2,
			JobDescription: fuq.JobDescription{
				JobId:      fuq.JobId(7),
				Name:       "this_job",
				NumTasks:   3,
				WorkingDir: ".",
				LoggingDir: ".",
				Command:    "/bin/true", // XXX - make more portable!
			},
		},
	}

	resp, err := server.SendJob(ctx, tasks)

	if err != nil {
		t.Fatalf("error sending tasks: %v", err)
	}

	switch resp.Type {
	case proto.MTypeOK:
		/* okay */
	case proto.MTypeError:
		errcode, arg0 := resp.AsError()
		t.Fatalf("error response: code = %d, arg = %d", errcode, arg0)
	default:
		t.Fatalf("unknown respone type: %s", resp.Type)
	}

	var msg proto.Message
	msg = <-msgCh
	if msg.Type != proto.MTypeUpdate {
		t.Fatalf("expected to receive UPDATE message, but received %s", msg.Type)
	}

	<-msgCh
	if msg.Type != proto.MTypeUpdate {
		t.Fatalf("expected to receive UPDATE message, but received %s", msg.Type)
	}

	if len(runner.T) != 2 {
		t.Errorf("expected runner to see two jobs")
	}

	sort.Slice(runner.T, func(i, j int) bool { return runner.T[i].Task < runner.T[j].Task })

	if !reflect.DeepEqual(runner.T, tasks) {
		t.Errorf("runner.T should have a copy of the originally submitted tasks")
	}

	dts.dispatcher.Stop()
	<-dts.dispatchSignal
}

func TestDispatcherStop(t *testing.T) {
	runner := &okayRunner{}

	dts := newDispatcherTestState(4, runner)
	defer dts.Close()

	ctx := dts.ctx
	server, msgCh, _ := dts.startServer()

	hello := <-msgCh
	hdata := hello.Data.(proto.HelloData)
	if hdata.NumProcs != 4 {
		t.Errorf("invalid HELLO: expected NumProcs = %d, found %d",
			4, hdata.NumProcs)
	}

	if len(hdata.Running) != 0 {
		t.Errorf("invalid HELLO: expected #Running = %d, found %d",
			0, len(hdata.Running))
	}

	resp, err := server.SendStop(ctx, 1)
	if err != nil {
		t.Fatalf("error sending tasks: %v", err)
	}

	np, nr := resp.AsOkay()
	if np != 3 || nr != 0 {
		t.Errorf("expected OK(%d|%d), but received OK(%d|%d)",
			3, 0, np, nr)
	}
}

// node.Runner that blocks until the context is canceled
type blockingRunner struct {
	T *testing.T

	mu sync.Mutex

	running map[fuq.TaskPair]chan struct{}
	// nrunning int64

	started chan int
	ended   chan int

	finishAll chan struct{}

	wg sync.WaitGroup
}

func (br *blockingRunner) NumRunning() int {
	br.mu.Lock()
	defer br.mu.Unlock()

	return len(br.running)
}

func (br *blockingRunner) addTask(pair fuq.TaskPair) (finish chan struct{}) {
	finish = make(chan struct{})

	br.mu.Lock()
	defer br.mu.Unlock()

	br.running[pair] = finish
	return finish
}

func (br *blockingRunner) delTask(pair fuq.TaskPair) {
	br.mu.Lock()
	defer br.mu.Unlock()
	delete(br.running, pair)
}

func (br *blockingRunner) finishTask(pair fuq.TaskPair) {
	br.mu.Lock()
	finish := br.running[pair]
	br.running[pair] = nil
	br.mu.Unlock()

	if finish != nil {
		close(finish)
	}
}

func (br *blockingRunner) FinishAll() {
	close(br.finishAll)
}

func (br *blockingRunner) Run(ctx context.Context, t fuq.Task, w *Worker) (fuq.JobStatusUpdate, error) {
	pair := fuq.TaskPair{JobId: t.JobId, Task: t.Task}
	finish := br.addTask(pair)
	finishAll := br.finishAll
	defer br.delTask(pair)

	br.wg.Add(1)
	defer br.wg.Done()

	// signal that we've started
	select {
	case br.started <- t.Task:
		/* nop */
		br.T.Logf("Runner for task %d sent STARTED event", t.Task)
	case <-finishAll:
		/* nop */
	case <-ctx.Done():
		br.T.Logf("Runner for task %d received CANCEL event", t.Task)
		return fuq.JobStatusUpdate{}, ctx.Err()
	}

	// block until a finish signal or the context is canceled
	select {
	case <-ctx.Done():
	case <-finish:
		br.T.Logf("Task %d received FINISH-ALL signal", t.Task)
	case <-finishAll:
		br.T.Logf("Task %d Received FINISH-ALL signal", t.Task)
	}

	// signal that we're ending
	br.ended <- t.Task
	br.T.Logf("Runner for %d sent ENDED event", t.Task)

	// return success to ensure that the dispatcher sets Success to
	// false and the status to the error
	return fuq.JobStatusUpdate{
		JobId:   t.JobId,
		Task:    t.Task,
		Success: true,
		Status:  "done",
	}, ctx.Err()
}

func newBlockingRunner(t *testing.T) *blockingRunner {
	return &blockingRunner{
		T:         t,
		started:   make(chan int),
		ended:     make(chan int),
		running:   make(map[fuq.TaskPair]chan struct{}),
		finishAll: make(chan struct{}),
	}
}

func TestDispatcherCancel(t *testing.T) {
	runner := newBlockingRunner(t)

	dts := newDispatcherTestState(2, runner)
	defer dts.Close()

	ctx := dts.ctx
	server, msgCh, _ := dts.startServer()

	log.Printf("T %s: runner       = %p", t.Name(), runner)
	log.Printf("T %s: dispatcher   = %p", t.Name(), dts.dispatcher)
	log.Printf("T %s: C proto.Conn = %p\n", t.Name(), dts.dispatcher.M)
	log.Printf("T %s: S proto.Conn = %p\n", t.Name(), server)
	log.Printf("T %s: messenger    = %p\n", t.Name(), dts.dispatcher.Messenger)

	hello := <-msgCh
	hdata := hello.Data.(proto.HelloData)
	if hdata.NumProcs != 2 {
		t.Errorf("invalid HELLO: expected NumProcs = %d, found %d",
			2, hdata.NumProcs)
	}

	if len(hdata.Running) != 0 {
		t.Errorf("invalid HELLO: expected #Running = %d, found %d",
			0, len(hdata.Running))
	}

	tasks := []fuq.Task{
		fuq.Task{
			Task: 3,
			JobDescription: fuq.JobDescription{
				JobId:      fuq.JobId(7),
				Name:       "this_job",
				NumTasks:   3,
				WorkingDir: ".",
				LoggingDir: ".",
				Command:    "/bin/true", // XXX - make more portable!
			},
		},
		fuq.Task{
			Task: 9,
			JobDescription: fuq.JobDescription{
				JobId:      fuq.JobId(7),
				Name:       "this_job",
				NumTasks:   3,
				WorkingDir: ".",
				LoggingDir: ".",
				Command:    "/bin/true", // XXX - make more portable!
			},
		},
	}

	resp, err := server.SendJob(ctx, tasks)

	// make sure both jobs haved started
	<-runner.started
	<-runner.started

	if err != nil {
		t.Fatalf("error sending tasks: %v", err)
	}

	switch resp.Type {
	case proto.MTypeOK:
		/* okay */
	case proto.MTypeError:
		errcode, arg0 := resp.AsError()
		t.Fatalf("error response: code = %d, arg = %d", errcode, arg0)
	default:
		t.Fatalf("unknown respone type: %s", resp.Type)
	}

	if nr := runner.NumRunning(); nr != 2 {
		t.Fatalf("NumRunning = %d, but expected 2", nr)
	}

	cancelMesg := []fuq.TaskPair{{7, 3}}
	resp, err = server.SendCancel(ctx, cancelMesg)

	nc1, nc2 := resp.AsOkay()
	if nc1 != 1 || nc2 != 0 {
		t.Errorf("expected OK(1|0) reply, but received OK(%d|%d)", nc1, nc2)
	}

	taskNum := <-runner.ended
	if taskNum != 3 {
		t.Fatalf("expected task 2 to end, but task %d ended", taskNum)
	}

	// dispatcher should send an UPDATE
	updMsg := <-msgCh
	t.Logf("received UPDATE: %v", updMsg)
	upd := updMsg.Data.(fuq.JobStatusUpdate)
	if upd.JobId != 7 || upd.Task != 3 {
		t.Errorf("update for job %d, task %d, expected update for job 7, task 3",
			upd.JobId, upd.Task)
	}

	if upd.Success {
		t.Error("expected update to indicate failure, but indicated success")
	}

	if upd.Status == "done" {
		t.Error("update status should indicate error, but is \"done\"")
	}

	// send CANCELs that don't match any running jobs
	resp, err = server.SendCancel(ctx, []fuq.TaskPair{{9, -1}})
	nc1, nc2 = resp.AsOkay()
	if nc1 != 0 || nc2 != 0 {
		t.Errorf("expected OK(0|0) to CANCEL that matches no running jobs, but found OK(%d|%d)",
			nc1, nc2)
	}

	resp, err = server.SendCancel(ctx, []fuq.TaskPair{{7, 1}})
	nc1, nc2 = resp.AsOkay()
	if nc1 != 0 || nc2 != 0 {
		t.Errorf("expected OK(0|0) to CANCEL that matches no running jobs, but found OK(%d|%d)",
			nc1, nc2)
	}

	resp, err = server.SendCancel(ctx, []fuq.TaskPair{{10, -1}})
	nc1, nc2 = resp.AsOkay()
	if nc1 != 0 || nc2 != 0 {
		t.Errorf("expected OK(0|0) to CANCEL that matches no running jobs, but found OK(%d|%d)",
			nc1, nc2)
	}

	// cancel all tasks of job 7
	resp, err = server.SendCancel(ctx, []fuq.TaskPair{{7, -1}})
	nc1, nc2 = resp.AsOkay()
	if nc1 != 1 || nc2 != 0 {
		t.Errorf("expected OK(1|0) to CANCEL(7,-1), but found OK(%d|%d)",
			nc1, nc2)
	}

	taskNum = <-runner.ended
	if taskNum != 9 {
		t.Fatalf("expected task 1 to end, but task %d ended", taskNum)
	}

	// dispatcher should send an UPDATE
	updMsg = <-msgCh
	t.Logf("received UPDATE: %v", updMsg)
	upd = updMsg.Data.(fuq.JobStatusUpdate)
	if upd.JobId != 7 || upd.Task != 9 {
		t.Errorf("update for job %d, task %d, expected update for job 7, task 9",
			upd.JobId, upd.Task)
	}

	if upd.Success {
		t.Error("expected update to indicate failure, but indicated success")
	}

	if upd.Status == "done" {
		t.Error("update status should indicate error, but is \"done\"")
	}

	// wait
	runner.wg.Wait()

	// stop dispatcher
	dts.dispatcher.Stop()
	<-dts.dispatchSignal
}

func TestDispatcherStopFinishesWhenJobsFinish(t *testing.T) {
	runner := newBlockingRunner(t)

	dts := newDispatcherTestState(2, runner)
	defer dts.Close()

	ctx := dts.ctx
	server, msgCh, _ := dts.startServer()

	log.Printf("T %s: runner       = %p", t.Name(), runner)
	log.Printf("T %s: dispatcher   = %p", t.Name(), dts.dispatcher)
	log.Printf("T %s: C proto.Conn = %p\n", t.Name(), dts.dispatcher.M)
	log.Printf("T %s: S proto.Conn = %p\n", t.Name(), server)
	log.Printf("T %s: messenger    = %p\n", t.Name(), dts.dispatcher.Messenger)

	hello := <-msgCh
	hdata := hello.Data.(proto.HelloData)
	if hdata.NumProcs != 2 {
		t.Errorf("invalid HELLO: expected NumProcs = %d, found %d",
			2, hdata.NumProcs)
	}

	if len(hdata.Running) != 0 {
		t.Errorf("invalid HELLO: expected #Running = %d, found %d",
			0, len(hdata.Running))
	}

	tasks := []fuq.Task{
		fuq.Task{
			Task: 3,
			JobDescription: fuq.JobDescription{
				JobId:      fuq.JobId(7),
				Name:       "this_job",
				NumTasks:   3,
				WorkingDir: ".",
				LoggingDir: ".",
				Command:    "/bin/true", // XXX - make more portable!
			},
		},
		fuq.Task{
			Task: 9,
			JobDescription: fuq.JobDescription{
				JobId:      fuq.JobId(7),
				Name:       "this_job",
				NumTasks:   3,
				WorkingDir: ".",
				LoggingDir: ".",
				Command:    "/bin/true", // XXX - make more portable!
			},
		},
	}

	resp, err := server.SendJob(ctx, tasks)

	// make sure both jobs haved started
	<-runner.started
	<-runner.started

	if err != nil {
		t.Fatalf("error sending tasks: %v", err)
	}

	switch resp.Type {
	case proto.MTypeOK:
		// okay
	case proto.MTypeError:
		errcode, arg0 := resp.AsError()
		t.Fatalf("error response: code = %d, arg = %d", errcode, arg0)
	default:
		t.Fatalf("unknown respone type: %s", resp.Type)
	}

	if nr := runner.NumRunning(); nr != 2 {
		t.Fatalf("NumRunning = %d, but expected 2", nr)
	}

	// Send STOP

	resp, err = server.SendStop(ctx, 2)
	if err != nil {
		t.Fatalf("error sending tasks: %v", err)
	}

	np, nr := resp.AsOkay()
	if np != 0 || nr != 2 {
		t.Errorf("expected OK(%d|%d), but received OK(%d|%d)",
			0, 2, np, nr)
	}

	runner.finishTask(fuq.TaskPair{7, 3})
	if tnum := <-runner.ended; tnum != 3 {
		t.Fatalf("expected ending task to be 3, but found %d", tnum)
	}
	upd := <-msgCh
	expected := proto.Message{
		Type: proto.MTypeUpdate,
		Data: fuq.JobStatusUpdate{
			JobId:   7,
			Task:    3,
			Success: true,
			Status:  "done",
		},
		Seq: upd.Seq,
	}

	if !reflect.DeepEqual(upd, expected) {
		t.Fatalf("expected UPDATE to be '%v', but found '%v'",
			expected, upd)
	}

	runner.finishTask(fuq.TaskPair{7, 9})
	if tnum := <-runner.ended; tnum != 9 {
		t.Fatalf("expected ending task to be 9, but found %d", tnum)
	}
	upd = <-msgCh
	expected = proto.Message{
		Type: proto.MTypeUpdate,
		Data: fuq.JobStatusUpdate{
			JobId:   7,
			Task:    9,
			Success: true,
			Status:  "done",
		},
		Seq: upd.Seq,
	}

	if !reflect.DeepEqual(upd, expected) {
		t.Fatalf("expected UPDATE to be '%v', but found '%v'",
			expected, upd)
	}

	// now wait for dispatch to stop
	<-dts.dispatchSignal
}

func TestDispatcherStopImmed(t *testing.T) {
	runner := &okayRunner{}

	dts := newDispatcherTestState(4, runner)
	defer dts.Close()

	ctx := dts.ctx
	server, msgCh, _ := dts.startServer()

	hello := <-msgCh
	hdata := hello.Data.(proto.HelloData)
	if hdata.NumProcs != 4 {
		t.Errorf("invalid HELLO: expected NumProcs = %d, found %d",
			4, hdata.NumProcs)
	}

	if len(hdata.Running) != 0 {
		t.Errorf("invalid HELLO: expected #Running = %d, found %d",
			0, len(hdata.Running))
	}

	err := server.SendStopImmed(ctx)
	if err != nil {
		t.Fatalf("error sending tasks: %v", err)
	}

	<-dts.dispatchSignal
}

func TestDispatcherSendsHelloOnSignal(t *testing.T) {
	runner := newBlockingRunner(t)

	dts := newDispatcherTestState(2, runner)
	defer dts.Close()

	ctx := dts.ctx
	server, msgCh, _ := dts.startServer()

	hello := <-msgCh
	hdata := hello.Data.(proto.HelloData)
	if hdata.NumProcs != 2 {
		t.Errorf("invalid HELLO: expected NumProcs = %d, found %d",
			2, hdata.NumProcs)
	}

	if len(hdata.Running) != 0 {
		t.Errorf("invalid HELLO: expected #Running = %d, found %d",
			0, len(hdata.Running))
	}

	tasks := []fuq.Task{
		fuq.Task{
			Task: 1,
			JobDescription: fuq.JobDescription{
				JobId:      fuq.JobId(7),
				Name:       "this_job",
				NumTasks:   3,
				WorkingDir: ".",
				LoggingDir: ".",
				Command:    "/bin/true", // XXX - make more portable!
			},
		},
		fuq.Task{
			Task: 2,
			JobDescription: fuq.JobDescription{
				JobId:      fuq.JobId(7),
				Name:       "this_job",
				NumTasks:   3,
				WorkingDir: ".",
				LoggingDir: ".",
				Command:    "/bin/true", // XXX - make more portable!
			},
		},
	}

	resp, err := server.SendJob(ctx, tasks)
	if err != nil {
		t.Fatalf("error sending tasks: %v", err)
	}

	if expected := proto.OkayMessage(0, 2, resp.Seq); !reflect.DeepEqual(resp, expected) {
		t.Fatalf("expected '%v' but found '%v'", expected, resp)
	}

	// make sure both jobs haved started
	<-runner.started
	<-runner.started

	np, nr, ns := dts.dispatcher.NumProcs()
	if np != 0 || nr != 2 || ns != 0 {
		t.Fatalf("expected np=0, nr=2, ns=0, but found np=%d, nr=%d, ns=%d",
			np, nr, ns)
	}

	dts.dispatcher.ResendHello()
	hello2 := <-msgCh
	hdata2 := hello2.Data.(proto.HelloData)
	expectedHello := proto.HelloData{NumProcs: 0, Running: tasks}
	if !reflect.DeepEqual(expectedHello, hdata2) {
		t.Fatalf("expected HELLO to be '%#v'\nbut found '%#v'",
			expectedHello, hdata2)
	}

	runner.FinishAll()
	<-runner.ended
	<-msgCh

	<-runner.ended
	<-msgCh

	// Wait for server to end...
	dts.dispatcher.Stop()
	<-dts.dispatchSignal
}
