package srv

import (
	"context"
	"fmt"
	"github.com/sfstewman/fuq"
	"log"
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
	Act(context.Context, *Worker) (*fuq.JobStatusUpdate, error)
}

// NopAction is an action that does nothing (a nop).  Useful for
// testing.
type NopAction struct{}

func (NopAction) Act(context.Context, *Worker) (*fuq.JobStatusUpdate, error) {
	return nil, nil
}

type WaitAction struct {
	Interval time.Duration
}

func (w WaitAction) Act(ctx context.Context, worker *Worker) (*fuq.JobStatusUpdate, error) {
	waiter := w.Waiter()
	if waiter == nil {
		return nil, nil
	}

	select {
	case <-waiter:
		worker.NumWaits++
		return nil, nil

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (w WaitAction) Waiter() <-chan time.Time {
	delay := w.PickWaitTime()
	if delay > MaxWait {
		delay = MaxWait
	}

	if delay <= 0 {
		return nil
	}

	return time.After(delay)
}

func (w WaitAction) Wait() {
	waiter := w.Waiter()
	if waiter != nil {
		<-waiter
	}
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
	All      bool
	WaitChan chan<- struct{}
}

func (s StopAction) Act(ctx context.Context, w *Worker) (*fuq.JobStatusUpdate, error) {
	defer func(ch chan<- struct{}) {
		if ch != nil {
			log.Print("closing wait channel")
			close(ch)
		}
	}(s.WaitChan)

	if !s.All {
		log.Print("STOP 1")
		return nil, ErrStopCond
	}

	// stop all
	log.Print("STOP ALL")
	v := ctx.Value(stopAllKey{})
	if v == nil {
		log.Print("CANNOT STOP ALL: no stop key")
		return nil, ErrStopCond
	}

	if stopFunc, ok := v.(context.CancelFunc); ok {
		log.Print("STOP ALL: calling STOP func")
		stopFunc()
	}

	return nil, ErrStopCond
}

type RunAction fuq.Task

func (r RunAction) Run(ctx context.Context, w *Worker) (fuq.JobStatusUpdate, error) {
	return w.RunJob(ctx, fuq.Task(r))
}

func (r RunAction) Act(ctx context.Context, w *Worker) (*fuq.JobStatusUpdate, error) {
	upd, err := r.Run(ctx, w)
	return &upd, err
}

func defaultRunner(ctx context.Context, t fuq.Task, w *Worker) (fuq.JobStatusUpdate, error) {
	status := fuq.JobStatusUpdate{
		JobId: t.JobId,
		Task:  t.Task,
	}

	if t.LoggingDir == "" {
		t.LoggingDir = w.DefaultLogDir
	}

	runner := exec.CommandContext(ctx, t.Command, strconv.Itoa(t.Task))
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
