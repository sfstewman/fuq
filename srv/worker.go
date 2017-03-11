package srv

import (
	"errors"
	"fmt"
	"github.com/sfstewman/fuq"
	"log"
)

const (
	MaxLogRetry = 10000
)

var ErrStopCond = errors.New("stop condition")

type Stopper interface {
	IsStopped() bool
	Stop()
}

type Worker struct {
	Seq           int
	Logger        *log.Logger
	Runner        Runner
	Stopper       Stopper
	Stop          bool
	Queuer        Queuer
	DefaultLogDir string
	NumWaits      int
}

type Runner interface {
	Run(fuq.Task, *Worker) (fuq.JobStatusUpdate, error)
}

type RunnerFunc func(fuq.Task, *Worker) (fuq.JobStatusUpdate, error)

func (f RunnerFunc) Run(t fuq.Task, w *Worker) (fuq.JobStatusUpdate, error) {
	return f(t, w)
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

func (w *Worker) RunJob(t fuq.Task) (fuq.JobStatusUpdate, error) {
	runner := w.Runner
	if runner == nil {
		return defaultRunner(t, w)
	}

	return runner.Run(t, w)
}

func (w *Worker) Loop() {
	var (
		req WorkerAction
		err error
	)

	for !w.Stopper.IsStopped() {
		if req == nil {
			// request a job from the foreman
			req, err = w.Queuer.RequestAction(1)
		}

		if err != nil {
			w.Log("error requesting job: %v", err)
			req = WaitAction{}
		}

		upd, err := req.Act(w)
		if err == ErrStopCond {
			return
		}

		w.LogIfError(err, "error encountered while running job")

		if upd == nil {
			req = nil
			continue
		}

		req, err = w.Queuer.UpdateAndRequestAction(*upd, 1)
	}
}

func (w *Worker) Close() {
	/* nop for now */
}
