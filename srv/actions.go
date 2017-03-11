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

// Action is something that the worker does.
//
// Not an -er name, but couldn't find a better name for this.
type WorkerAction interface {
	Act(*Worker) (*fuq.JobStatusUpdate, error)
}

// NopAction is an action that does nothing (a nop).  Useful for
// testing.
type NopAction struct{}

func (NopAction) Act(*Worker) (*fuq.JobStatusUpdate, error) {
	return nil, nil
}

type WaitAction struct {
	Interval time.Duration
}

func (w WaitAction) Act(worker *Worker) (*fuq.JobStatusUpdate, error) {
	w.Wait()
	worker.NumWaits++
	return nil, nil
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

func (s StopAction) Act(w *Worker) (*fuq.JobStatusUpdate, error) {
	if s.All && w.Stopper != nil {
		w.Stopper.Stop()
	}
	return nil, ErrStopCond
}

type RunAction fuq.Task

func (r RunAction) Run(w *Worker) (fuq.JobStatusUpdate, error) {
	return w.RunJob(fuq.Task(r))
}

func (r RunAction) Act(w *Worker) (*fuq.JobStatusUpdate, error) {
	upd, err := r.Run(w)
	return &upd, err
}

func defaultRunner(t fuq.Task, w *Worker) (fuq.JobStatusUpdate, error) {
	status := fuq.JobStatusUpdate{
		JobId: t.JobId,
		Task:  t.Task,
	}

	if t.LoggingDir == "" {
		t.LoggingDir = w.DefaultLogDir
	}

	runner := exec.Command(t.Command, strconv.Itoa(t.Task))
	runner.Dir = t.WorkingDir

	/* XXX add logging directory option */
	runner.Stdin = nil

	// XXX make path handling less Unix-y
	logName := fmt.Sprintf("%s/%s_%d.log", t.LoggingDir, t.Name, t.Task)
	logFile, err := os.OpenFile(logName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil && os.IsExist(err) {
		for i := 1; i <= MaxLogRetry && os.IsExist(err); i++ {
			logName = fmt.Sprintf("%s/%s_%d-%d.log", t.LoggingDir, t.Name, t.Task, i)
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
