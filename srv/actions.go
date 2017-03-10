package srv

import (
	"fmt"
	"github.com/sfstewman/fuq"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"time"
)

type RefreshCookieAction struct{}

const (
	WaitMean   = 5 * time.Second
	WaitSpread = 1500 * time.Millisecond
	WaitDelta  = 500 * time.Millisecond
	MaxWait    = 60 * time.Second
)

// XXX - needs a new name
type Actioner interface {
	Act(*Worker) error
}

// NopAction is an action that does nothing (a nop).  Useful for
// testing.
type NopAction struct{}

func (NopAction) Act(*Worker) error {
	return nil
}

type WaitAction struct {
	Interval time.Duration
}

func (w WaitAction) Act(worker *Worker) error {
	w.Wait()
	worker.NumWaits++
	return nil
}

func (w WaitAction) Wait() time.Duration {
	delay := w.PickWaitTime()
	if delay > 0 {
		time.Sleep(delay)
	}

	if delay > MaxWait {
		delay = MaxWait
	}

	return delay
}

func (w WaitAction) PickWaitTime() time.Duration {
	if w.Interval > 0 {
		return w.Interval
	}

	spread := int(WaitSpread)
	randDelay := int(WaitMean) + rand.Intn(2*spread) - spread
	if randDelay < 0 {
		return time.Duration(0)
	}

	return time.Duration(randDelay)
}

type StopAction struct {
	All bool
}

func (s StopAction) Act(w *Worker) error {
	if s.All && w.Stopper != nil {
		w.Stopper.Stop()
	}
	return ErrStopCond
}

type Runner interface {
	Run(w *Worker) (fuq.JobStatusUpdate, error)
}

type RunAction fuq.Task

func (r RunAction) Run(w *Worker) (fuq.JobStatusUpdate, error) {
	status := fuq.JobStatusUpdate{
		JobId: r.JobId,
		Task:  r.Task,
	}

	if r.LoggingDir == "" {
		r.LoggingDir = w.DefaultLogDir
	}

	runner := exec.Command(r.Command, strconv.Itoa(r.Task))
	runner.Dir = r.WorkingDir

	/* XXX add logging directory option */
	runner.Stdin = nil

	// XXX make path handling less Unix-y
	logName := fmt.Sprintf("%s/%s_%d.log", r.LoggingDir, r.Name, r.Task)
	logFile, err := os.OpenFile(logName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil && os.IsExist(err) {
		for i := 1; i <= MaxLogRetry && os.IsExist(err); i++ {
			logName = fmt.Sprintf("%s/%s_%d-%d.log", r.LoggingDir, r.Name, r.Task, i)
			logFile, err = os.OpenFile(logName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
		}
	}

	if err != nil {
		status.Success = false
		status.Status = fmt.Sprintf("error opening log: %v", err)
		return status, nil
	}
	defer logFile.Close()

	runner.Stdout = logFile
	runner.Stderr = logFile

	w.Log("running: wd='%s', cmd='%s', args='%v', log='%s'",
		runner.Dir, runner.Path, runner.Args, logName)

	if err := runner.Run(); err != nil {
		status.Success = false
		status.Status = fmt.Sprintf("error encountered: %v", err)
	} else {
		status.Success = true
		status.Status = "done"
	}

	return status, nil
}
