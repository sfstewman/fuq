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
		data := m.Data.(*proto.HelloData)
		np := uint16(data.NumProcs)
		nr := uint16(len(data.Running))
		return proto.OkayMessage(np, nr, m.Seq)
	})

	server.OnMessageFunc(proto.MTypeUpdate, func(m proto.Message) proto.Message {
		msgCh <- m

		// this cheats, but we're not testing whether the
		// foreman can keep track of this here
		np, nr := d.NumProcs()
		return proto.OkayMessage(uint16(np), uint16(nr), m.Seq)
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
	hdata := hello.Data.(*proto.HelloData)
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
}

func TestDispatcherStop(t *testing.T) {
	runner := &okayRunner{}

	dts := newDispatcherTestState(4, runner)
	defer dts.Close()

	ctx := dts.ctx
	server, msgCh, _ := dts.startServer()

	hello := <-msgCh
	hdata := hello.Data.(*proto.HelloData)
	if hdata.NumProcs != 4 {
		t.Errorf("invalid HELLO: expected NumProcs = %d, found %d",
			2, hdata.NumProcs)
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

func TestDispatcherStopImmed(t *testing.T) {
	runner := &okayRunner{}

	dts := newDispatcherTestState(4, runner)
	defer dts.Close()

	ctx := dts.ctx
	server, msgCh, _ := dts.startServer()

	hello := <-msgCh
	hdata := hello.Data.(*proto.HelloData)
	if hdata.NumProcs != 4 {
		t.Errorf("invalid HELLO: expected NumProcs = %d, found %d",
			2, hdata.NumProcs)
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
