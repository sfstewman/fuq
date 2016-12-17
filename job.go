package fuq

import (
	"encoding/json"
	"fmt"
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
	s := string(b)
	switch s {
	case `waiting`, `"waiting"`:
		*js = Waiting
	case `running`, `"running"`:
		*js = Running
	case `finished`, `"finished"`:
		*js = Finished
	case `paused`, `"paused"`:
		*js = Paused
	case `cancelled`, `"cancelled"`:
		*js = Cancelled
	default:
		return fmt.Errorf("unknown status '%s'", s)
	}

	return nil
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

type JobTaskData struct {
	JobId
	Pending       []int
	Running       []int
	Finished      []int
	Errors        []int
	ErrorMessages []string
}

type JobStatusUpdate struct {
	JobId   JobId       `json:"job_id"`
	Task    int         `json:"task"`
	Success bool        `json:"success"`
	Status  string      `json:"status"`
	NewJob  *JobRequest `json:"newjob"`
}
