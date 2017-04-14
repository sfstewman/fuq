package node

import (
	"context"
	"errors"
	"github.com/sfstewman/fuq"
	"log"
	"sync"
)

const (
	MaxLogRetry = 10000
)

var ErrStopCond = errors.New("stop condition")

type Queuer interface {
	RequestAction(ctx context.Context, nproc int) (WorkerAction, error)
	UpdateAndRequestAction(ctx context.Context, status fuq.JobStatusUpdate, nproc int) (WorkerAction, error)
}

type Stopper interface {
	IsStopped() bool
	Stop()
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

var (
	_ Stopper = SimpleStopper{}
)

type stopAllKey struct{}

func WorkerContext(parent context.Context) context.Context {
	stopCtx, stopFunc := context.WithCancel(parent)
	return context.WithValue(stopCtx, stopAllKey{}, stopFunc)
}

type Worker struct {
	Seq           int
	Logger        *log.Logger
	Runner        Runner
	Queuer        Queuer
	DefaultLogDir string
	NumWaits      int
	Stop          bool

	current struct {
		sync.Mutex
		action WorkerAction
		cancel context.CancelFunc
	}
}

type Runner interface {
	Run(context.Context, fuq.Task, *Worker) (fuq.JobStatusUpdate, error)
}

type RunnerFunc func(context.Context, fuq.Task, *Worker) (fuq.JobStatusUpdate, error)

func (f RunnerFunc) Run(ctx context.Context, t fuq.Task, w *Worker) (fuq.JobStatusUpdate, error) {
	return f(ctx, t, w)
}

func (w *Worker) Log(format string, args ...interface{}) {
	if w.Logger != nil {
		w.Logger.Printf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

func (w *Worker) setCurrent(ctx context.Context, action WorkerAction) context.Context {
	w.current.Lock()
	defer w.current.Unlock()

	if w.current.action != nil || w.current.cancel != nil {
		panic("setCurrent called while current != nil")
	}

	actCtx, actCancel := context.WithCancel(ctx)
	w.current.action = action
	w.current.cancel = actCancel

	return actCtx
}

func (w *Worker) ClearCurrent() {
	w.current.Lock()
	defer w.current.Unlock()

	w.current.action = nil
	w.current.cancel = nil
}

func (w *Worker) Current() WorkerAction {
	w.current.Lock()
	defer w.current.Unlock()

	return w.current.action
}

func (w *Worker) CancelCurrent() {
	w.current.Lock()
	defer w.current.Unlock()

	if w.current.cancel != nil {
		w.current.cancel()
	}
}

func (w *Worker) RunJob(ctx context.Context, t fuq.Task) (fuq.JobStatusUpdate, error) {
	runner := w.Runner
	if runner == nil {
		return defaultRunner(ctx, t, w)
	}

	return runner.Run(ctx, t, w)
}

func (w *Worker) CurrentAction() (WorkerAction, context.CancelFunc) {
	w.current.Lock()
	defer w.current.Unlock()

	return w.current.action, w.current.cancel
}

func (w *Worker) WithCurrent(fn func(*Worker, WorkerAction, context.CancelFunc) error) error {
	w.current.Lock()
	defer w.current.Unlock()

	return fn(w, w.current.action, w.current.cancel)
}

func (w *Worker) actOnRequest(ctx context.Context, req WorkerAction) (*fuq.JobStatusUpdate, error) {
	reqCtx := w.setCurrent(ctx, req)
	defer w.ClearCurrent()

	return req.Act(reqCtx, w)
}

func (w *Worker) Loop(ctx context.Context) {
	var (
		req WorkerAction
		err error
	)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			/* nop */
		}

		if req == nil {
			// request a job from the foreman
			req, err = w.Queuer.RequestAction(ctx, 1)
		}

		if err != nil {
			w.Log("error requesting job: %v", err)
			req = WaitAction{}
		}

		upd, err := w.actOnRequest(ctx, req)
		if err == ErrStopCond {
			return
		}

		if upd == nil {
			req = nil
			continue
		}

		if err != nil {
			w.Log("%s: %v", "error encountered while running job", err)
			upd.Success = false
			upd.Status = err.Error()
		}

		req, err = w.Queuer.UpdateAndRequestAction(ctx, *upd, 1)
	}
}

func (w *Worker) Close() {
	w.current.Lock()
	defer w.current.Unlock()

	if w.current.cancel != nil {
		w.current.cancel()
	}
}
