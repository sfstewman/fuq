package node

import (
	"context"
	"github.com/sfstewman/fuq"
	"testing"
	"time"
)

type ChanQueuer struct {
	Q chan WorkerAction
	S chan struct{}
}

func (cq ChanQueuer) RequestAction(ctx context.Context, nproc int) (WorkerAction, error) {
	select {
	case act, ok := <-cq.Q:
		if !ok {
			// !ok means cq.Q is closed: send a stop action
			goto stop
		}

		return act, nil

	case <-cq.S:
		goto stop

	case <-ctx.Done():
		goto stop
	}

stop:
	return StopAction{All: false}, nil
}

func (cq ChanQueuer) UpdateAndRequestAction(ctx context.Context, status fuq.JobStatusUpdate, nproc int) (WorkerAction, error) {
	return cq.RequestAction(ctx, nproc)
}

type testingWorker struct {
	Worker
	stopCh   chan struct{}
	actionCh chan WorkerAction
	doneCh   chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
}

func makeTestingWorker() *testingWorker {
	var w testingWorker

	w.stopCh = make(chan struct{})
	w.actionCh = make(chan WorkerAction)
	w.doneCh = make(chan struct{})

	w.ctx, w.cancel = context.WithCancel(context.Background())

	w.Worker = Worker{
		Queuer: ChanQueuer{w.actionCh, w.stopCh},
	}

	return &w
}

func TestLoopStop(t *testing.T) {
	w := makeTestingWorker()

	go func() {
		w.Loop(w.ctx)
		close(w.doneCh)
	}()

	w.actionCh <- StopAction{All: false}
	<-w.doneCh
}

func TestLoopWait(t *testing.T) {
	w := makeTestingWorker()

	go func() {
		w.Loop(w.ctx)
		close(w.doneCh)
	}()

	waits := []time.Duration{
		10 * time.Millisecond,
		50 * time.Millisecond,
		100 * time.Millisecond,
	}

	for _, wait := range waits {
		buffer := wait / 5 // allow 20% buffer
		if buffer < 3*time.Millisecond {
			buffer = 3 * time.Millisecond
		}

		t0 := time.Now()
		w.actionCh <- WaitAction{Interval: wait}

		// blocks until after the wait
		w.actionCh <- NopAction{}

		dt := time.Since(t0)
		if dt < wait || dt > wait+buffer {
			t.Errorf("asked for wait of %s (buffer %s), actual wait was %s",
				wait, buffer, dt)
		}
	}

	w.cancel()
	// make sure we actually stop!
	<-w.doneCh
}

type testRunner struct {
	R int
	T fuq.Task
	S fuq.JobStatusUpdate
	E error
}

func (r *testRunner) Run(ctx context.Context, t fuq.Task, w *Worker) (fuq.JobStatusUpdate, error) {
	r.T = t
	r.R++
	return r.S, r.E
}

func TestLoopRun(t *testing.T) {
	w := makeTestingWorker()

	task := fuq.Task{
		Task: 85,
		JobDescription: fuq.JobDescription{
			JobId:      fuq.JobId(5),
			Name:       "fooTask",
			NumTasks:   103,
			WorkingDir: "/tmp", // XXX - better
			LoggingDir: "/tmp",
			Command:    "testCmd",
		},
	}

	r := &testRunner{
		S: fuq.JobStatusUpdate{
			JobId:   task.JobDescription.JobId,
			Task:    task.Task,
			Success: true,
		},
	}
	w.Worker.Runner = r

	go func() {
		w.Loop(w.ctx)
		close(w.doneCh)
	}()

	w.actionCh <- RunAction(task)

	// blocks until run is complete
	w.actionCh <- NopAction{}

	if r.R != 1 {
		t.Fatalf("runner was not run (R=%d, expected 1)", r.R)
	}

	if r.T != task {
		t.Errorf("task does not agree")
	}

	w.actionCh <- StopAction{All: false}
	<-w.doneCh
}
