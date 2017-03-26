package node

import (
	"context"
	"errors"
	"fmt"
	"github.com/sfstewman/fuq"
	"log"
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
}

type Runner interface {
	Run(context.Context, fuq.Task, *Worker) (fuq.JobStatusUpdate, error)
}

type RunnerFunc func(context.Context, fuq.Task, *Worker) (fuq.JobStatusUpdate, error)

func (f RunnerFunc) Run(ctx context.Context, t fuq.Task, w *Worker) (fuq.JobStatusUpdate, error) {
	return f(ctx, t, w)
}

func (w *Worker) LogIfError(err error, pfxFmt string, args ...interface{}) {
	if err == nil {
		return
	}

	pfx := fmt.Sprintf(pfxFmt, args...)
	w.Log("%s: %v", pfx, err)
}

func (w *Worker) Log(format string, args ...interface{}) {
	if w.Logger != nil {
		w.Logger.Printf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

func (w *Worker) RunJob(ctx context.Context, t fuq.Task) (fuq.JobStatusUpdate, error) {
	runner := w.Runner
	if runner == nil {
		return defaultRunner(ctx, t, w)
	}

	return runner.Run(ctx, t, w)
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

		upd, err := req.Act(ctx, w)
		if err == ErrStopCond {
			return
		}

		w.LogIfError(err, "error encountered while running job")

		if upd == nil {
			req = nil
			continue
		}

		req, err = w.Queuer.UpdateAndRequestAction(ctx, *upd, 1)
	}
}

func (w *Worker) Close() {
	/* nop for now */
}
