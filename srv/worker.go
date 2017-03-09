package srv

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

const (
	DefaultInterval  = 5 * time.Second
	IntervalIncrease = 500 * time.Millisecond
	MaxInterval      = 60 * time.Second

	MaxLogRetry = 10000
)

type Worker struct {
	Seq    int
	Logger *log.Logger
	// Name          string
	Config        *WorkerConfig
	Queuer        Queuer
	DefaultLogDir string
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
	for !w.Config.IsAllStop() {
		// request a job from the foreman
		req, err := w.Queuer.RequestAction(1)
		if err != nil {
			w.Log("error requesting job: %v", err)
			req = WaitAction{}
		}

	req_switch:
		switch r := req.(type) {
		case WaitAction:
			// add -1.5 to 1.5 second variability so not all
			// clients contact at once...
			randDelay := time.Duration(rand.Intn(300)-150) * 10 * time.Millisecond
			r.Interval = DefaultInterval + time.Duration(numWaits)*IntervalIncrease + randDelay
			if r.Interval > MaxInterval {
				r.Interval = MaxInterval
			}
			r.Wait()
			numWaits++
			continue run_loop

		case StopAction:
			if r.All {
				w.Config.AllStop()
			}
			break run_loop

		case RunAction:
			numWaits = 0 // reset wait counter
			if r.LoggingDir == "" {
				r.LoggingDir = w.DefaultLogDir
			}

			status, err := r.Run(w.Logger)
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
