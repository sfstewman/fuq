package srv

import (
	"context"
	"github.com/sfstewman/fuq"
	"log"
	"sync"
)

type JobQueuer interface {
	Close() error

	ClearJobs() error
	EachJob(func(fuq.JobDescription) error) error
	FetchJobId(fuq.JobId) (fuq.JobDescription, error)
	ChangeJobState(jobId fuq.JobId, newState fuq.JobStatus) (fuq.JobStatus, error)
	AddJob(job fuq.JobDescription) (fuq.JobId, error)

	UpdateTaskStatus(update fuq.JobStatusUpdate) error
	FetchJobTaskStatus(jobId fuq.JobId) (fuq.JobTaskStatus, error)
	FetchPendingTasks(nproc int) ([]fuq.Task, error)
}

/* FetchJobs queries a JobQueuer for jobs that match either a name or a
 * status or both.
 *
 * If both name and status are empty, FetchJobs returns all jobs that
 * are not in the Cancelled or Finished state.
 *
 * If name is not empty, FetchJobs returns all jobs whose names exactly
 * match name.
 *
 * If status is not empty, FetchJobs returns all jobs whose status
 * exactly matches status.
 *
 * Thus, if name and status are both not empty, FetchJobs will return
 * jobs matching both.
 *
 * Note that this is currently done via a full table scan over all jobs,
 * but may be optimized in the future.
 */
func FetchJobs(q JobQueuer, name, status string) ([]fuq.JobDescription, error) {
	var jobs []fuq.JobDescription

	err := q.EachJob(func(desc fuq.JobDescription) error {
		if name != "" && desc.Name != name {
			return nil
		}

		if status != "" && desc.Status.String() != status {
			return nil
		}

		if status == "" {
			if desc.Status == fuq.Cancelled {
				return nil
			}
			if desc.Status == fuq.Finished {
				return nil
			}
		}

		jobs = append(jobs, desc)
		return nil
	})

	return jobs, err
}

func AllJobs(q JobQueuer) ([]fuq.JobDescription, error) {
	return FetchJobs(q, "", "")
}

type connectionSet struct {
	sync.Mutex
	Set map[*persistentConn]struct{}
}

func newConnectionSet() connectionSet {
	return connectionSet{
		Set: make(map[*persistentConn]struct{}),
	}
}

func (cm *connectionSet) AddConn(pc *persistentConn) {
	cm.Lock()
	defer cm.Unlock()

	cm.Set[pc] = struct{}{}
}

func (cm *connectionSet) HasConn(pc *persistentConn) bool {
	cm.Lock()
	defer cm.Unlock()

	_, ok := cm.Set[pc]
	return ok
}

func (cm *connectionSet) DelConn(pc *persistentConn) {
	cm.Lock()
	defer cm.Unlock()

	delete(cm.Set, pc)
}

func (cm *connectionSet) EachConn(fn func(*persistentConn) error) error {
	cm.Lock()
	defer cm.Unlock()

	for pc := range cm.Set {
		if err := fn(pc); err != nil {
			return err
		}
	}

	return nil
}

type Foreman struct {
	JobQueuer
	TaskLogger TaskLogger

	Done chan<- struct{}

	// XXX: worth replacing with something that can scale?
	// we need to lock the database with every request; should
	// we just use that lock?
	shutdownReq struct {
		mu           sync.RWMutex
		shutdownHost map[string]struct{}
	}

	jobsSignal struct {
		mu    sync.Mutex
		ready chan struct{}
	}

	connections connectionSet
}

func NewForeman(q JobQueuer, done chan<- struct{}) (*Foreman, error) {
	f := Foreman{
		JobQueuer: q,
		Done:      done,
	}

	f.shutdownReq.shutdownHost = make(map[string]struct{})
	f.connections = newConnectionSet()

	return &f, nil
}

func (f *Foreman) Close() error {
	return nil
}

func (f *Foreman) ready(lock bool) <-chan struct{} {
	if lock {
		f.jobsSignal.mu.Lock()
		defer f.jobsSignal.mu.Unlock()
	}

	if f.jobsSignal.ready == nil {
		f.jobsSignal.ready = make(chan struct{})
	}
	return f.jobsSignal.ready
}

func (f *Foreman) AllShutdownNodes() []string {
	f.shutdownReq.mu.RLock()
	defer f.shutdownReq.mu.RUnlock()

	if len(f.shutdownReq.shutdownHost) == 0 {
		return nil
	}

	nodes := make([]string, len(f.shutdownReq.shutdownHost))
	count := 0
	for h, _ := range f.shutdownReq.shutdownHost {
		nodes[count] = h
		count++
	}

	return nodes[:count]
}

func (f *Foreman) IsNodeShutdown(name string) bool {
	f.shutdownReq.mu.RLock()
	defer f.shutdownReq.mu.RUnlock()

	_, ok := f.shutdownReq.shutdownHost[name]
	log.Printf("checking if node %s has a shutdown request: %v",
		name, ok)
	return ok
}

func (f *Foreman) ShutdownNodes(names []string) {
	f.shutdownReq.mu.Lock()
	defer f.shutdownReq.mu.Unlock()

	for _, n := range names {
		f.shutdownReq.shutdownHost[n] = struct{}{}
	}
}

func (f *Foreman) fetchNextTasks(ctx context.Context, nproc int, ni fuq.NodeInfo) ([]fuq.Task, error) {
	for {
		f.jobsSignal.mu.Lock()

		// request jobs
		tasks, err := f.FetchPendingTasks(nproc)
		if err != nil || len(tasks) > 0 {
			f.jobsSignal.mu.Unlock()
			return tasks, err
		}

		jobsAvail := f.ready(false)
		f.jobsSignal.mu.Unlock()

		if f.IsNodeShutdown(ni.UniqName) {
			return nil, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-jobsAvail:
			continue
		}
	}
}

func isValidJob(job fuq.JobDescription) bool {
	return true
}

func (f *Foreman) WakeupListeners() {
	f.jobsSignal.mu.Lock()
	defer f.jobsSignal.mu.Unlock()

	signal := f.jobsSignal.ready
	f.jobsSignal.ready = make(chan struct{})
	if signal != nil {
		close(signal)
	}
}
