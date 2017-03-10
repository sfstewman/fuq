package srv

import (
	"errors"
	"fmt"
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
	Stopper       Stopper
	Stop          bool
	Queuer        Queuer
	DefaultLogDir string
	NumWaits      int
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

func (w *Worker) Loop() {
	numWaits := 0

run_loop:
	for !w.Stopper.IsStopped() {
		// request a job from the foreman
		req, err := w.Queuer.RequestAction(1)
		if err != nil {
			w.Log("error requesting job: %v", err)
			req = WaitAction{}
		}

	req_switch:
		switch r := req.(type) {
		case NopAction:
			// nop, mostly useful for testing

		case WaitAction:
			// add -1.5 to 1.5 second variability so not all
			// clients contact at once...
			r.Wait()
			numWaits++
			continue run_loop

		case StopAction:
			if r.All {
				w.Stopper.Stop()
			}
			break run_loop

		case Runner:
			numWaits = 0 // reset wait counter

			status, err := r.Run(w)
			w.LogIfError(err, "error encountered while running job")

			if status.Success {
				w.Log("job %d:%d completed successfully", status.JobId, status.Task)
			} else {
				w.Log("job %d:%d encountered error: %s",
					status.JobId, status.Task, status.Status)
			}

			req, err = w.Queuer.UpdateAndRequestAction(status, 1)
			if err != nil {
				w.Log("error requesting job: %v", err)
				req = WaitAction{}
			}
			goto req_switch

		default:
			w.Log("unexpected result when requesting job: %v", req)
		}
	}
}

func (w *Worker) Close() {
	/* nop for now */
}
