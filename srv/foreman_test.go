package srv

import (
	"fmt"
	"github.com/sfstewman/fuq"
	"github.com/sfstewman/fuq/proto"
	"sort"
	"sync"
	"testing"
)

type simpleQueuer struct {
	mu     sync.Mutex
	jobs   []fuq.JobDescription
	status []fuq.JobTaskData
}

func (q *simpleQueuer) Close() error {
	return nil
}

func (q *simpleQueuer) ClearJobs() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.jobs = nil
	return nil
}

func (q *simpleQueuer) EachJob(fn func(fuq.JobDescription) error) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, job := range q.jobs {
		if err := fn(job); err != nil {
			return err
		}
	}

	return nil
}

func (q *simpleQueuer) FetchJobId(id fuq.JobId) (fuq.JobDescription, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	ind := int(id) - 1
	if ind < 0 || ind >= len(q.jobs) {
		return fuq.JobDescription{}, fmt.Errorf("invalid job id %d", id)
	}

	return q.jobs[ind], nil
}

func (q *simpleQueuer) ChangeJobState(jobId fuq.JobId, newState fuq.JobStatus) (fuq.JobStatus, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	ind := int(jobId) - 1
	if ind < 0 || ind >= len(q.jobs) {
		return fuq.JobStatus(0), fmt.Errorf("invalid job id %d", jobId)
	}

	prevStatus := q.jobs[ind].Status
	q.jobs[ind].Status = newState

	return prevStatus, nil
}

func (q *simpleQueuer) AddJob(job fuq.JobDescription) (fuq.JobId, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.jobs = append(q.jobs, job)
	q.status = append(q.status, fuq.JobTaskData{})

	n := len(q.jobs)
	if n != len(q.status) {
		panic("invalid simpleQueuer state")
	}

	jobId := fuq.JobId(n)
	q.jobs[n-1].JobId = jobId

	return jobId, nil
}

func (q *simpleQueuer) UpdateTaskStatus(update fuq.JobStatusUpdate) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	ind := int(update.JobId) - 1
	if ind < 0 || ind >= len(q.jobs) {
		return fmt.Errorf("invalid job id %d", update.JobId)
	}

	if err := q.status[ind].Update(update); err != nil {
		return err
	}

	if len(q.status[ind].Pending)+len(q.status[ind].Running) > 0 {
		return nil
	}

	if len(q.status[ind].Finished)+len(q.status[ind].Errors) == q.jobs[ind].NumTasks {
		q.jobs[ind].Status = fuq.Finished
	}

	return nil
}

func (q *simpleQueuer) FetchJobTaskStatus(jobId fuq.JobId) (fuq.JobTaskStatus, error) {
	var status fuq.JobTaskStatus

	q.mu.Lock()
	defer q.mu.Unlock()

	ind := int(jobId) - 1
	if ind < 0 || ind >= len(q.jobs) {
		return status, fmt.Errorf("invalid job id %d", jobId)
	}

	desc := q.jobs[ind]
	tasks := q.status[ind]
	status = fuq.MakeJobStatusUpdate(desc, tasks)
	return status, nil
}

func (q *simpleQueuer) FetchPendingTasks(nproc int) ([]fuq.Task, error) {
	tasks := make([]fuq.Task, 0, nproc)

	q.mu.Lock()
	defer q.mu.Unlock()

	if nproc <= 0 {
		return nil, nil
	}

	for ind, desc := range q.jobs {
		switch desc.Status {
		case fuq.Waiting:
			/* convert to a running task */
			nt := desc.NumTasks
			pending := make([]int, nt)
			for i := 0; i < nt; i++ {
				pending[i] = i + 1
			}

			q.jobs[ind].Status = fuq.Running

			q.status[ind] = fuq.JobTaskData{
				JobId:   desc.JobId,
				Pending: pending,
			}
			fallthrough

		case fuq.Running:
			status := q.status[ind]
			if len(status.Pending) == 0 {
				continue
			}

			n := nproc - len(tasks)
			pending := status.Pending
			if len(pending) < nproc {
				n = len(pending)
			}

			for _, task := range pending[:n] {
				tasks = append(tasks, fuq.Task{
					Task:           task,
					JobDescription: desc,
				})
				status.Running = append(status.Running, task)
			}

			status.Pending = status.Pending[n:]
			sort.Ints(status.Running)
			q.status[ind] = status

		default:
			/* nop */
		}

		if len(tasks) == nproc {
			break
		}
	}

	return tasks, nil
}

func newTestingForeman() *Foreman {
	queuer := &simpleQueuer{}
	done := make(chan struct{})
	f, err := NewForeman(queuer, done)

	if err != nil {
		panic(fmt.Sprintf("error making foreman: %v", err))
	}

	return f
}

func makeNodeInfo() fuq.NodeInfo {
	return fuq.NodeInfo{
		Node:    "voltron",
		Pid:     18,
		NumProc: 4,
	}
}

/* Calls the client target to add a new job */
func addJob(t *testing.T, f *Foreman, job fuq.JobDescription) fuq.JobId {
	jobId, err := f.AddJob(job)
	if err != nil {
		t.Fatalf("error adding job: %v", err)
	}

	if jobId == 0 {
		t.Fatalf("invalid job id (%d) but no error", jobId)
	}

	f.WakeupListeners()

	job.JobId = jobId
	job.Status = fuq.Waiting
	t.Logf("queued job %v", job)

	return jobId
}

type testClient struct {
	*proto.Conn
	NodeInfo fuq.NodeInfo
}
