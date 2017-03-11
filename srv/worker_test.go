package srv

import (
	"github.com/sfstewman/fuq"
	"testing"
	"time"
)

type ChanQueuer struct {
	Q chan interface{}
	S chan struct{}
}

func (cq ChanQueuer) RequestAction(nproc int) (interface{}, error) {
	var act interface{}
	var ok bool

	select {
	case act, ok = <-cq.Q:
		if !ok {
			goto stop
		}
		return act, nil

	case <-cq.S:
		goto stop
	}

stop:
	return StopAction{false}, nil
}

func (cq ChanQueuer) UpdateAndRequestAction(status fuq.JobStatusUpdate, nproc int) (interface{}, error) {
	return cq.RequestAction(nproc)
}

type SimpleStopper struct {
	S chan struct{}
}

func (s SimpleStopper) Stop() {
	if s.S != nil {
		close(s.S)
	}
}

func (s SimpleStopper) IsStopped() bool {
	select {
	case <-s.S:
		return true
	default:
		return s.S == nil
	}
}

type testingWorker struct {
	Worker
	stopper  SimpleStopper
	stopCh   chan struct{}
	actionCh chan interface{}
	doneCh   chan struct{}
}

func makeTestingWorker() *testingWorker {
	var w testingWorker

	w.stopCh = make(chan struct{})
	w.actionCh = make(chan interface{})
	w.doneCh = make(chan struct{})

	w.stopper = SimpleStopper{w.stopCh}
	w.Worker = Worker{
		Stopper: w.stopper,
		Queuer:  ChanQueuer{w.actionCh, w.stopCh},
	}

	return &w
}

func TestLoopStop(t *testing.T) {
	w := makeTestingWorker()

	go func() {
		w.Loop()
		close(w.doneCh)
	}()

	w.actionCh <- StopAction{All: false}
	<-w.doneCh
}

func TestLoopWait(t *testing.T) {
	w := makeTestingWorker()

	go func() {
		w.Loop()
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

	w.stopper.Stop()
	// make sure we actually stop!
	<-w.doneCh
}

type testRunner struct {
	R int
	T fuq.Task
	S fuq.JobStatusUpdate
	E error
}

func (r *testRunner) Run(t fuq.Task, w *Worker) (fuq.JobStatusUpdate, error) {
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
		w.Loop()
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
