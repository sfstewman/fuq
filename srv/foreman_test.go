package srv

import (
	"context"
	"fmt"
	"github.com/sfstewman/fuq"
	"github.com/sfstewman/fuq/proto"
	"reflect"
	"sort"
	"sync"
	"testing"
)

const (
	testingName = `voltron`
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
			desc = q.jobs[ind]

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

func makeNodeInfoWithName(node string) fuq.NodeInfo {
	return fuq.NodeInfo{
		Node:    node,
		Pid:     18,
		NumProc: 4,
	}
}

func makeNodeInfo() fuq.NodeInfo {
	return makeNodeInfoWithName(testingName)
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

func TestForemanReadyChanWaitsUntilWakeup(t *testing.T) {
	f := newTestingForeman()

	sendWake := make(chan struct{})
	recvWake := make(chan struct{})

	waitCh := f.ready(true)
	go func() {
		<-waitCh
		close(recvWake)
	}()

	go func() {
		sendWake <- struct{}{}
		f.WakeupListeners()
	}()

	select {
	case <-sendWake:
		/* okay */
		t.Logf("received sendWake first")
	case <-recvWake:
		t.Fatal("recvWake should be received after sendWake")
	}

	// make sure we receive recvWake
	<-recvWake
}

func TestForemanFetchNextTasksReturnsPending(t *testing.T) {
	var err error

	f := newTestingForeman()
	ni := makeNodeInfo()
	q := f.JobQueuer.(*simpleQueuer)

	desc1 := fuq.JobDescription{
		Name:       "job1",
		NumTasks:   8,
		WorkingDir: "/foo/bar",
		LoggingDir: "/foo/bar/logs",
		Command:    "/foo/foo_it.sh",
	}

	desc2 := fuq.JobDescription{
		Name:       "job2",
		NumTasks:   8,
		WorkingDir: "/foo/baz",
		LoggingDir: "/foo/baz/logs",
		Command:    "/foo/baz_it.sh",
	}

	/* add jobs */
	if desc1.JobId, err = q.AddJob(desc1); err != nil {
		t.Fatal(err)
	}

	if desc2.JobId, err = q.AddJob(desc2); err != nil {
		t.Fatal(err)
	}

	/* fetch first five, should all come from job1 */
	tasks, err := f.fetchNextTasks(context.TODO(), 5, ni)
	if err != nil {
		t.Fatal(err)
	}

	// jobs1 should now be Running
	desc1.Status = fuq.Running
	for i, task := range tasks {
		expected := fuq.Task{
			JobDescription: desc1,
			Task:           i + 1,
		}
		if task != expected {
			t.Fatalf("expected task %d to be '%v', but found '%v'",
				i, expected, task)
		}
	}

	jobs, err := AllJobs(q)
	if err != nil {
		t.Fatal(err)
	}

	if expected := []fuq.JobDescription{desc1, desc2}; !reflect.DeepEqual(jobs, expected) {
		t.Fatalf("expected AllJobs() to return '%v', but found '%v'",
			expected, jobs)
	}

	// fetch next five, first three should be from job1, next two
	// from job2
	tasks, err = f.fetchNextTasks(context.TODO(), 5, ni)
	if err != nil {
		t.Fatalf("error fetching tasks: %v", err)
	}

	if len(tasks) != 5 {
		t.Fatalf("expected five tasks, but received %d", len(tasks))
	}

	// job2 should now be Running
	desc2.Status = fuq.Running
	for i, task := range tasks[:3] {
		expected := fuq.Task{
			JobDescription: desc1,
			Task:           i + 6,
		}
		if task != expected {
			t.Fatalf("expected task %d to be '%v', but found '%v'",
				i, expected, task)
		}
	}

	for i, task := range tasks[3:] {
		expected := fuq.Task{
			JobDescription: desc2,
			Task:           i + 1,
		}
		if task != expected {
			t.Fatalf("expected task %d to be '%v', but found '%v'",
				i, expected, task)
		}
	}

	jobs, err = AllJobs(q)
	if err != nil {
		t.Fatal(err)
	}

	if expected := []fuq.JobDescription{desc1, desc2}; !reflect.DeepEqual(jobs, expected) {
		t.Fatalf("expected AllJobs() to return '%v', but found '%v'",
			expected, jobs)
	}

}

func TestForemanFetchNextTasksWaitsForTasks(t *testing.T) {
	var (
		fetchErr error

		f  = newTestingForeman()
		ni = makeNodeInfo()
		q  = f.JobQueuer.(*simpleQueuer)

		wait1  = make(chan struct{})
		wait2  = make(chan struct{})
		taskCh = make(chan []fuq.Task)
	)

	go func() {
		close(wait1)
		tasks, err := f.fetchNextTasks(context.TODO(), 5, ni)
		if err != nil {
			fetchErr = err
			close(taskCh)
			return
		}

		taskCh <- tasks
	}()

	desc := fuq.JobDescription{
		Name:       "job1",
		NumTasks:   8,
		WorkingDir: "/foo/bar",
		LoggingDir: "/foo/bar/logs",
		Command:    "/foo/foo_it.sh",
	}

	go func() {
		// signal twice.  first time to indicate that the
		// goroutine has started.  second time to indicate that
		// id is set.
		wait2 <- struct{}{}

		id, err := q.AddJob(desc)

		if err != nil {
			panic(err)
		}

		desc.JobId = id
		close(wait2)

		f.WakeupListeners()
	}()

	// make sure goroutine has started
	<-wait1

	// trigger f.WakeupListeners() to make sure that fetchNextTasks
	// will continue to wait after wakeup if there are no new tasks
	f.WakeupListeners()

	select {
	case <-wait2:
		/* okay */
		t.Logf("received wait2 first")

	case <-taskCh:
		t.Fatalf("received tasks before enqueuing them")
	}

	tasks, ok := <-taskCh
	if !ok {
		t.Fatalf("error fetching tasks: %v", fetchErr)
	}

	if len(tasks) != 5 {
		t.Fatalf("expected five tasks, but received %d", len(tasks))
	}

	// make sure desc.JobId is set
	<-wait2

	desc.Status = fuq.Running
	for i, theTask := range tasks {
		expected := fuq.Task{JobDescription: desc, Task: i + 1}
		if theTask != expected {
			t.Fatalf("expected task %d to be %v, but found %v",
				i+1, expected, theTask)
		}
	}

	// fetch additional tasks to drain the queue

	tasks, err := f.fetchNextTasks(context.TODO(), 5, ni)
	if err != nil {
		t.Fatalf("error fetching tasks: %v", err)
	}

	if len(tasks) != 3 {
		t.Fatalf("expected three tasks, found %d", len(tasks))
	}

	// test cancelation via the context

	wait1 = make(chan struct{})
	ctx, cancel := context.WithCancel(context.TODO())
	go func(ctx context.Context) {
		close(wait1)
		tasks, err := f.fetchNextTasks(ctx, 5, ni)
		if err != nil {
			fetchErr = err
			close(taskCh)
			return
		}

		taskCh <- tasks
	}(ctx)

	select {
	case <-wait1:
		/* okay */
		t.Logf("received wait1")
	case <-taskCh:
		t.Fatal("received tasks before context was canceled")
	}

	cancel()
	tasks, ok = <-taskCh
	if ok {
		t.Fatalf("expected a context error, but received tasks")
	}

	if fetchErr != context.Canceled {
		t.Fatalf("expected a context error, but error is %v", fetchErr)
	}
}

func TestForemanShutdownNodes(t *testing.T) {
	f := newTestingForeman()
	ni := makeNodeInfo()
	ni.UniqName = ni.Node + ":1"

	t.Logf("uniqName = %s", ni.UniqName)
	if f.IsNodeShutdown(ni.UniqName) {
		t.Fatalf("node %s should not be shutdown", ni.UniqName)
	}

	shutdownNodes := f.AllShutdownNodes()
	if len(shutdownNodes) > 0 {
		t.Fatalf("no nodes should be shutdown")
	}

	f.ShutdownNodes([]string{"voltron:1", "optimus:2", "robocop:1"})
	shutdownNodes = f.AllShutdownNodes()

	expectedNodes := []string{"optimus:2", "robocop:1", "voltron:1"}
	sort.Slice(shutdownNodes, func(i, j int) bool { return shutdownNodes[i] < shutdownNodes[j] })
	if !reflect.DeepEqual(shutdownNodes, expectedNodes) {
		t.Fatalf("expected shutdown nodes '%v', but found '%v'", expectedNodes, shutdownNodes)
	}

	for _, n := range expectedNodes {
		if !f.IsNodeShutdown(n) {
			t.Fatalf("expected IsShutdownNode(\"%s\") to be true, but it was false", n)
		}
	}
}

func TestForemanShutdownNodesCancelFetchNextTasks(t *testing.T) {
	var (
		fetchErr error

		f  = newTestingForeman()
		ni = makeNodeInfo()

		wait1  = make(chan struct{})
		wait2  = make(chan struct{})
		taskCh = make(chan []fuq.Task)
	)

	ni.UniqName = ni.Node + ":1"

	go func() {
		close(wait1)
		tasks, err := f.fetchNextTasks(context.TODO(), 5, ni)
		if err != nil {
			fetchErr = err
			close(taskCh)
			return
		}

		taskCh <- tasks
	}()

	go func() {
		// signal twice.  first time to indicate that the
		// goroutine has started.  second time to indicate that
		// id is set.
		wait2 <- struct{}{}

		f.ShutdownNodes([]string{ni.UniqName})
		close(wait2)

		f.WakeupListeners()
	}()

	// make sure goroutine has started
	<-wait1

	// trigger f.WakeupListeners() to make sure that fetchNextTasks
	// will continue to wait after wakeup if there are no new tasks
	f.WakeupListeners()

	select {
	case <-wait2:
		/* okay */
		t.Logf("received wait2 first")

	case <-taskCh:
		t.Fatalf("fetchNextTasks ended before shutdown sent")
	}

	tasks, ok := <-taskCh
	if !ok {
		t.Fatalf("error fetching tasks: %v", fetchErr)
	}

	if len(tasks) != 0 {
		t.Fatalf("expected zero tasks, received %d", len(tasks))
	}
}
