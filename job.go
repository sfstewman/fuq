package fuq

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
)

type JobRequest struct {
	NumProc int `json:"nproc"`
}

type JobId uint64

type JobDescription struct {
	JobId      JobId     `json:"job_id"`
	Name       string    `json:"name"`
	NumTasks   int       `json:"num_tasks"`
	WorkingDir string    `json:"working_dir"`
	LoggingDir string    `json:"logging_dir"`
	Command    string    `json:"command"`
	Status     JobStatus `json:"status"`
}

func (jd *JobDescription) ExpandPaths(pv *PathVars) error {
	var err error
	if jd.WorkingDir, err = pv.ExpandPath(jd.WorkingDir); err != nil {
		return err
	}

	if jd.LoggingDir, err = pv.ExpandPath(jd.LoggingDir); err != nil {
		return err
	}

	return nil
}

func (jd *JobDescription) CheckPaths() error {
	if jd.WorkingDir == "" {
		return fmt.Errorf("invalid working directory '%s'", jd.WorkingDir)
	}

	if jd.LoggingDir == "" {
		return fmt.Errorf("invalid logging directory '%s'", jd.LoggingDir)
	}

	return nil
}

type JobTaskStatus struct {
	Description     JobDescription
	TasksFinished   int   // Number of tasks that are finished.
	TasksPending    int   // Number of tasks not yet running.
	TasksRunning    []int // Tasks currently currently running.
	TasksWithErrors []int
}

func ReadJobFile(in io.Reader) (JobDescription, error) {
	job := JobDescription{}
	err := ParseKVFile(in, func(key, value string) error {
		switch key {
		case "name":
			job.Name = value

		case "num_tasks":
			p, err := strconv.ParseInt(value, 10, 32)
			if err != nil || p <= 0 {
				return fmt.Errorf("invalid task count: %s", value)
			}
			job.NumTasks = int(p)

		case "working_dir":
			job.WorkingDir = value

		case "log_dir":
			job.LoggingDir = value

		case "command":
			job.Command = value

		default:
			return fmt.Errorf("unknown directive '%s'", key)
		}

		return nil
	})

	if err != nil {
		return JobDescription{}, fmt.Errorf("error reading job description: %v", err)
	}

	return job, nil
}

type NewJobResponse struct {
	JobId JobId `json:"job_id"`
}

type JobStateChangeResponse struct {
	JobId      JobId     `json:"job_id"`
	PrevStatus JobStatus `json:"prev_status"`
	NewStatus  JobStatus `json:"new_status"`
}

type ClientJobListReq struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

type ClientStateChangeReq struct {
	JobIds []JobId `json:"job_ids"`
	Action string
}

type JobStatus int

const (
	Waiting JobStatus = iota
	Running
	Finished
	Paused
	Cancelled
)

func (js JobStatus) MarshalJSON() ([]byte, error) {
	s := ""

	switch js {
	case Waiting:
		s = "waiting"
	case Running:
		s = "running"
	case Finished:
		s = "finished"
	case Paused:
		s = "paused"
	case Cancelled:
		s = "cancelled"
	default:
		return nil, fmt.Errorf("unknown status %d", int(js))
	}

	return json.Marshal(s)
}

func (js *JobStatus) UnmarshalJSON(b []byte) error {
	var s string

	n := len(b)
	if n >= 2 && b[0] == '"' && b[n-1] == '"' {
		s = string(b[1 : n-1])
	} else {
		s = string(b)
	}

	jsp, err := ParseJobStatus(s)
	if err != nil {
		return err
	}

	*js = jsp
	return nil
}

func ParseJobStatus(s string) (JobStatus, error) {
	switch s {
	case "waiting":
		return Waiting, nil
	case "running":
		return Running, nil
	case "finished":
		return Finished, nil
	case "paused":
		return Paused, nil
	case "cancelled":
		return Cancelled, nil
	default:
		return JobStatus(0), fmt.Errorf("unknown status '%s'", s)
	}
}

func (js JobStatus) String() string {
	switch js {
	case Waiting:
		return "waiting"
	case Running:
		return "running"
	case Finished:
		return "finished"
	case Paused:
		return "paused"
	case Cancelled:
		return "cancelled"
	default:
		return fmt.Sprintf("unknown_status_%d", int(js))
	}
}

type Task struct {
	Task int `json:"task"`
	JobDescription
}

type TaskPair struct {
	JobId JobId `json:"job_id"`
	Task  int   `json:"task"`
}

type JobTaskData struct {
	JobId
	Pending       []int
	Running       []int
	Finished      []int
	Errors        []int
	ErrorMessages []string
}

func (tasks *JobTaskData) Update(update JobStatusUpdate) error {
	ind := sort.SearchInts(tasks.Running, update.Task)
	if ind >= len(tasks.Running) || tasks.Running[ind] != update.Task {
		return fmt.Errorf("task %d is not in the running list", update.Task)
	}

	copy(tasks.Running[ind:], tasks.Running[ind+1:])
	tasks.Running = tasks.Running[:len(tasks.Running)-1]

	if update.Success {
		tasks.Finished = append(tasks.Finished, update.Task)
	} else {
		tasks.Errors = append(tasks.Errors, update.Task)
		tasks.ErrorMessages = append(tasks.ErrorMessages, update.Status)
	}

	return nil
}

type JobStatusUpdate struct {
	JobId   JobId       `json:"job_id"`
	Task    int         `json:"task"`
	Success bool        `json:"success"`
	Status  string      `json:"status"`
	NewJob  *JobRequest `json:"newjob"`
}

func MakeJobStatusUpdate(desc JobDescription, tasks JobTaskData) JobTaskStatus {
	status := JobTaskStatus{
		Description:     desc,
		TasksFinished:   len(tasks.Finished),
		TasksPending:    len(tasks.Pending),
		TasksRunning:    make([]int, len(tasks.Running)),
		TasksWithErrors: make([]int, len(tasks.Errors)),
	}

	copy(status.TasksRunning, tasks.Running)
	copy(status.TasksWithErrors, tasks.Errors)

	return status
}
