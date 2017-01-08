package srv

import (
	"github.com/sfstewman/fuq"
	"testing"
)

var testJobs []fuq.JobDescription = []fuq.JobDescription{
	fuq.JobDescription{
		Name: "foobar", NumTasks: 19,
		WorkingDir: "/path/to/foobar",
		LoggingDir: "/path/to/foobar/logs",
		Command:    "/bin/echo",
		Status:     fuq.Waiting,
	},

	fuq.JobDescription{
		Name: "foobaz", NumTasks: 23,
		WorkingDir: "/path/to/baz/foo",
		LoggingDir: "/path/to/baz_logs/foo",
		Command:    "/bin/true",
		Status:     fuq.Waiting,
	},

	fuq.JobDescription{
		Name: "quux", NumTasks: 29,
		WorkingDir: "/path/to/foobar/quux",
		LoggingDir: "/path/to/foobar/quux_logs",
		Command:    "/bin/false",
		Status:     fuq.Waiting,
	},
}

func compareJob(t *testing.T, actual, expected fuq.JobDescription) {
	if expected.JobId == 0 {
		expected.JobId = actual.JobId
	}
	if expected != actual {
		t.Errorf("initial description %#v is not equal to final description %#v",
			expected, actual)
	}
}

func addJobs(t *testing.T, jq JobQueuer, jobs []fuq.JobDescription) ([]fuq.JobDescription, map[fuq.JobId]int) {
	out := make([]fuq.JobDescription, len(jobs))
	revId := make(map[fuq.JobId]int)
	for i, j := range jobs {
		jobId, err := jq.AddJob(j)
		if err != nil {
			t.Fatalf("error adding job: %v", err)
		}

		j.JobId = jobId
		out[i] = j

		if _, ok := revId[jobId]; ok {
			t.Fatalf("duplicate job id %v", jobId)
		}
		revId[jobId] = i
	}

	return out, revId
}

func jqTestAddJob(t *testing.T, jq JobQueuer) {
	expectedJobs, revId := addJobs(t, jq, testJobs)
	for _, job := range expectedJobs {
		fetched, err := jq.FetchJobs(job.Name, "")
		if err != nil {
			t.Fatalf("error fetching job '%s': %v", job.Name, err)
		}

		if len(fetched) != 1 {
			t.Fatalf("expected one result for '%s', found %d",
				job.Name, len(fetched))
		}

		compareJob(t, fetched[0], job)
	}

	fetched, err := jq.FetchJobs("", "")
	if err != nil {
		t.Fatalf("error fetching jobs: %v", err)
	}

	if len(fetched) != len(expectedJobs) {
		t.Fatalf("expected %d results, found %d",
			len(testJobs), len(fetched))
	}

	for _, fj := range fetched {
		idx, ok := revId[fj.JobId]
		if !ok {
			t.Fatalf("unexpected job id %v", fj.JobId)
		}

		t.Logf("testing fetched job %v (index = %d)",
			fj.JobId, idx)
		compareJob(t, fj, expectedJobs[idx])
	}
}

func jqTestChangeJobState(t *testing.T, jq JobQueuer) {
	expectedJobs, revId := addJobs(t, jq, testJobs)

	oldState, err := jq.ChangeJobState(expectedJobs[0].JobId, fuq.Running)
	if err != nil {
		t.Fatalf("error changing job state: %v", err)
	}

	if oldState != fuq.Waiting {
		t.Errorf("invalid old job state, expected %v but found %v",
			fuq.Waiting, oldState)
	}

	fetched, err := jq.FetchJobs("", "")
	if err != nil {
		t.Fatalf("error fetching all jobs: %v", err)
	}

	hasExpected := false
	for _, j := range fetched {
		if j.JobId == expectedJobs[0].JobId {
			hasExpected = true

			if j.Status != fuq.Running {
				t.Errorf("job %v should have state %v but has state %v",
					j.JobId, fuq.Running, j.Status)
			}
		} else {
			if j.Status != fuq.Waiting {
				t.Errorf("job %v should have state %v but has state %v",
					j.JobId, fuq.Waiting, j.Status)
			}
		}
	}

	// manually update to new status
	expectedJobs[0].Status = fuq.Running

	if !hasExpected {
		t.Errorf("did not fetch expected job %v", expectedJobs[0].JobId)
	}

	runningJobs, err := jq.FetchJobs("", "running")
	if err != nil {
		t.Errorf("error fetching running jobs: %v", err)
	}

	waitingJobs, err := jq.FetchJobs("", "waiting")
	if err != nil {
		t.Errorf("error fetching waiting jobs: %v", err)
	}

	if len(runningJobs) != 1 {
		t.Fatalf("expected %d jobs to be running, found %d",
			1, len(runningJobs))
	}

	if len(waitingJobs) != len(expectedJobs)-1 {
		t.Fatalf("expected %d jobs to be waiting, found %d",
			len(expectedJobs)-1, len(waitingJobs))
	}

	if runningJobs[0] != expectedJobs[0] {
		t.Errorf("expected running job %v, but found %v",
			runningJobs[0], expectedJobs[0])
	}

	for _, j := range waitingJobs {
		ind, ok := revId[j.JobId]
		if !ok {
			t.Fatalf("unnknown job id %v when testing waiting jobs", j.JobId)
		}

		if j != expectedJobs[ind] {
			t.Errorf("expected waiting job %v but found %v",
				expectedJobs[ind], j)
		}
	}
}

func jqTestClearJobs(t *testing.T, jq JobQueuer) {
	expectedJobs, _ := addJobs(t, jq, testJobs)

	jobs, err := AllJobs(jq)
	if err != nil {
		t.Fatalf("error fetching jobs: %v", err)
	}

	if len(jobs) != len(expectedJobs) {
		t.Errorf("expected %d jobs before ClearJobs(), but found %d",
			len(expectedJobs), len(jobs))
	}

	if err := jq.ClearJobs(); err != nil {
		t.Fatalf("error clearing jobs: %v", err)
	}

	jobs, err = AllJobs(jq)
	if err != nil {
		t.Fatalf("error fetching jobs: %v", err)
	}

	if len(jobs) != 0 {
		t.Errorf("expected no jobs after ClearJobs(), but found %d",
			len(jobs))
	}
}

func jqTestFetchJobId(t *testing.T, jq JobQueuer) {
	expectedJobs, revId := addJobs(t, jq, testJobs)
	for id, ind := range revId {
		job, err := jq.FetchJobId(id)
		if err != nil {
			t.Errorf("error fetching job %d: %v", id, err)
		}

		compareJob(t, job, expectedJobs[ind])
	}
}

func jqTestFetchPendingTasks(t *testing.T, jq JobQueuer) {
	tasks, err := jq.FetchPendingTasks(2)
	if err != nil {
		t.Fatalf("error fetching pending tasks (no tasks): %v", err)
	}

	if len(tasks) != 0 {
		t.Fatalf("expected no tasks, but %d returned: %v", len(tasks), tasks)
	}

	expectedJobs, _ := addJobs(t, jq, testJobs)

	tasks, err = jq.FetchPendingTasks(2)
	if err != nil {
		t.Fatalf("error fetching pending tasks (no tasks): %v", err)
	}

	if len(tasks) != 2 {
		t.Fatalf("expected two tasks, but %d returned: %v", len(tasks), tasks)
	}

	runningJob := expectedJobs[0]
	runningJob.Status = fuq.Running
	for i := 0; i < 2; i++ {
		if tasks[i].JobDescription != runningJob {
			t.Errorf("returned task %d should be from job %v, but is %v",
				i, runningJob, tasks[i].JobDescription)
		}

		if tasks[i].Task != i+1 {
			t.Errorf("returned task %d should have Task=%d, but is %d",
				i, i+1, tasks[i].Task)
		}
	}

	// make sure tasks fetch across jobs
	tasks, err = jq.FetchPendingTasks(runningJob.NumTasks)
	if err != nil {
		t.Fatalf("error fetching pending tasks (no tasks): %v", err)
	}

	if len(tasks) != runningJob.NumTasks {
		t.Fatalf("expected two tasks, but %d returned: %v", len(tasks), tasks)
	}

	for i := 0; i < runningJob.NumTasks-2; i++ {
		if tasks[i].JobDescription != runningJob {
			t.Errorf("returned task %d should be from job %v, but is %v",
				i, runningJob, tasks[i].JobDescription)
		}

		if tasks[i].Task != i+3 {
			t.Errorf("returned task %d should have Task=%d, but is %d",
				i, i+1, tasks[i].Task)
		}
	}

	nextJob := expectedJobs[1]
	nextJob.Status = fuq.Running
	for i := 0; i < 2; i++ {
		ind := runningJob.NumTasks - 2 + i
		if tasks[ind].JobDescription != nextJob {
			t.Errorf("returned task %d should be from job %v, but is %v",
				ind, runningJob, tasks[i].JobDescription)
		}

		if tasks[ind].Task != i+1 {
			t.Errorf("returned task %d should have Task=%d, but is %d",
				ind, i+1, tasks[i].Task)
		}
	}
}

func jqTestFetchAndUpdatePendingTasks(t *testing.T, jq JobQueuer) {
	tasks, err := jq.FetchPendingTasks(2)
	if err != nil {
		t.Fatalf("error fetching pending tasks (no tasks): %v", err)
	}

	if len(tasks) != 0 {
		t.Fatalf("expected no tasks, but %d returned: %v", len(tasks), tasks)
	}

	expectedJobs, _ := addJobs(t, jq, testJobs)

	tasks, err = jq.FetchPendingTasks(2)
	if err != nil {
		t.Fatalf("error fetching pending tasks (no tasks): %v", err)
	}

	if len(tasks) != 2 {
		t.Fatalf("expected two tasks, but %d returned: %v", len(tasks), tasks)
	}

	expectedJobs[0].Status = fuq.Running

	id := expectedJobs[0].JobId
	status, err := jq.FetchJobTaskStatus(id)
	if err != nil {
		t.Fatalf("error fetching task status for job %d: %v", id, err)
	}

	if status.Description != expectedJobs[0] {
		t.Errorf("status of job %d should return %v, but %v",
			id, expectedJobs[0], status.Description)
	}

	if status.TasksFinished != 0 {
		t.Errorf("job %d should have no tasks finished, but found %d",
			id, status.TasksFinished)
	}

	ntasks := expectedJobs[0].NumTasks - 2
	if status.TasksPending != ntasks {
		t.Errorf("job %d should have %d tasks pending, but found %d",
			id, ntasks, status.TasksFinished)
	}

	if len(status.TasksRunning) != 2 {
		t.Errorf("job %d should have %d tasks running, but found %d",
			id, 2, len(status.TasksRunning))
	}

	if status.TasksRunning[0] != 1 || status.TasksRunning[1] != 2 {
		t.Errorf("job %d running tasks should be 1 and 2, but found %v",
			id, status.TasksRunning)
	}

	if len(status.TasksWithErrors) != 0 {
		t.Errorf("job %d should have no tasks with errors, but found %d",
			id, len(status.TasksRunning))
	}

	update := fuq.JobStatusUpdate{
		JobId:   id,
		Task:    tasks[0].Task,
		Success: true,
		Status:  "done",
	}

	if err := jq.UpdateTaskStatus(update); err != nil {
		t.Errorf("job %d update error: %v", id, err)
	}

	id = expectedJobs[0].JobId
	status, err = jq.FetchJobTaskStatus(id)
	if err != nil {
		t.Fatalf("error fetching task status for job %d: %v", id, err)
	}

	if status.Description != expectedJobs[0] {
		t.Errorf("status of job %d should return %v, but %v",
			id, expectedJobs[0], status.Description)
	}

	if status.TasksFinished != 1 {
		t.Errorf("job %d should have one task finished, but found %d",
			id, status.TasksFinished)
	}

	ntasks = expectedJobs[0].NumTasks - 2
	if status.TasksPending != ntasks {
		t.Errorf("job %d should have %d tasks finished, but found %d",
			id, ntasks, status.TasksFinished)
	}

	if len(status.TasksRunning) != 1 {
		t.Errorf("job %d should have one task running, but found %d",
			id, len(status.TasksRunning))
	}

	if status.TasksRunning[0] != 2 {
		t.Errorf("job %d running tasks should be 2, but found %v",
			id, status.TasksRunning)
	}

	if len(status.TasksWithErrors) != 0 {
		t.Errorf("job %d should have %d tasks finished, but found %d",
			id, 2, len(status.TasksRunning))
	}

}

type jqTest struct {
	Name string
	Test func(*testing.T, JobQueuer)
}

var jqTestTable []jqTest = []jqTest{
	{"jqTestAddJob", jqTestAddJob},
	{"jqTestChangeJobState", jqTestChangeJobState},
	{"jqTestClearJobs", jqTestClearJobs},
	{"jqTestFetchJobId", jqTestFetchJobId},
	{"jqTestFetchPendingTasks", jqTestFetchPendingTasks},
	{"jqTestFetchAndUpdatePendingTasks", jqTestFetchAndUpdatePendingTasks},
}
