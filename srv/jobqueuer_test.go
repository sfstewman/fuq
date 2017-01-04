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

type jqTest struct {
	Name string
	Test func(*testing.T, JobQueuer)
}

var jqTestTable []jqTest = []jqTest{
	{"jqTestAddJob", jqTestAddJob},
	{"jqTestChangeJobState", jqTestChangeJobState},
	{"jqTestClearJobs", jqTestClearJobs},
	{"jqTestFetchJobId", jqTestFetchJobId},
}
