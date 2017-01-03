package srv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/sfstewman/fuq"
	"golang.org/x/crypto/bcrypt"
	"gopkg.in/vmihailenco/msgpack.v2"
	"log"
	"strconv"
	"strings"
)

const (
	jobInfoPrefix  = "job:info:"
	taskInfoPrefix = "task:info:"
	jobTasksPrefix = "job:tasks:"
)

/* Number of random bytes to append after node name when generating
 * cookie */
const (
	CookieSeqNumBytes = 16
	CookieCost        = bcrypt.DefaultCost + 2
)

var (
	namesBucket  []byte = []byte("names")
	cookieBucket []byte = []byte("cookies")
	jobBucket    []byte = []byte("jobs")

	jobIndexBucket []byte = []byte("index:primary")

	jobWaitingBucket   []byte = []byte("waiting")
	jobRunningBucket   []byte = []byte("running")
	jobFinishedBucket  []byte = []byte("finished")
	jobPausedBucket    []byte = []byte("paused")
	jobCancelledBucket []byte = []byte("cancelled")
)

var statusBuckets map[fuq.JobStatus][]byte = map[fuq.JobStatus][]byte{
	fuq.Waiting:   jobWaitingBucket,
	fuq.Running:   jobRunningBucket,
	fuq.Finished:  jobFinishedBucket,
	fuq.Paused:    jobPausedBucket,
	fuq.Cancelled: jobCancelledBucket,
}

type JobQueuer interface {
	Close() error
	ClearJobs() error
	AllJobs() ([]fuq.JobDescription, error)
	FetchJobs(name, status string) ([]fuq.JobDescription, error)
	ChangeJobState(jobId fuq.JobId, newState fuq.JobStatus) (fuq.JobStatus, error)
	AddJob(job fuq.JobDescription) (fuq.JobId, error)
	UpdateTaskStatus(update fuq.JobStatusUpdate) error
	FetchJobTaskStatus(jobId fuq.JobId) (fuq.JobTaskStatus, error)
	FetchPendingTasks(nproc int) ([]fuq.Task, error)
}

type JobStore struct {
	db *bolt.DB
}

var _ JobQueuer = (*JobStore)(nil)

func NewJobStore(dbpath string) (*JobStore, error) {
	db, err := bolt.Open(dbpath, 0666, nil)
	if err != nil {
		return nil, fmt.Errorf("error opening database '%s': %v",
			dbpath, err)
	}

	return newJobStore(db)
}

func newJobStore(db *bolt.DB) (*JobStore, error) {
	if err := db.Update(createJobsBucket); err != nil {
		return nil, fmt.Errorf("error creating database scheme: %v", err)
	}

	return &JobStore{db: db}, nil
}

func (d *JobStore) Close() error {
	return nil
}

func createJobsBucket(tx *bolt.Tx) error {
	var err error

	bkt := tx.Bucket(jobBucket)
	if bkt == nil {
		if bkt, err = tx.CreateBucket(jobBucket); err != nil {
			return fmt.Errorf("error creating '%s' bucket", jobBucket)
		}
	}

	if _, err = bkt.CreateBucketIfNotExists(jobIndexBucket); err != nil {
		return fmt.Errorf("error creating '%s' bucket",
			jobIndexBucket)
	}

	for _, b := range statusBuckets {
		if _, err = bkt.CreateBucketIfNotExists(b); err != nil {
			return fmt.Errorf("error recreating '%s/%s' bucket: %v",
				jobBucket, b, err)
		}
	}

	return nil
}

func (d *JobStore) ClearJobs() error {
	err := d.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(jobBucket); err != nil {
			return fmt.Errorf("error deleting '%s' bucket", jobBucket)
		}

		if err := createJobsBucket(tx); err != nil {
			return fmt.Errorf("error recreating jobs bucket: %v", err)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("error clearing jobs: %v", err)
	}

	return nil
}

func (d *JobStore) AllJobs() ([]fuq.JobDescription, error) {
	return d.FetchJobs("", "")
}

func validJobKeyPrefix(ks string) bool {
	return strings.HasPrefix(ks, jobInfoPrefix)
}

func jobKeyToId(ks string) fuq.JobId {
	ki := ks[len(jobInfoPrefix):]
	jobId, err := strconv.ParseUint(ki, 10, 64)

	if err != nil {
		return 0
	}

	return fuq.JobId(jobId)
}

func fetchJobFromTx(tx *bolt.Tx, jobId fuq.JobId) (fuq.JobDescription, error) {
	jobData := fuq.JobDescription{}

	jb, err := getJobBucket(tx)
	if err != nil {
		return jobData, err
	}

	js := getJobKey(jobId)
	raw := jb.Get(js)
	if raw == nil {
		return jobData, errors.New("job not found")
	}

	return bytesToJob(raw)
}

func fetchJob(jobBucket *bolt.Bucket, jobId fuq.JobId) (fuq.JobDescription, error) {
	jobData := fuq.JobDescription{}

	js := getJobKey(jobId)
	raw := jobBucket.Get(js)
	if raw == nil {
		return jobData, errors.New("job not found")
	}

	return bytesToJob(raw)
}

func (d *JobStore) FetchJobs(name, status string) ([]fuq.JobDescription, error) {
	var jobs []fuq.JobDescription

	err := d.db.View(func(tx *bolt.Tx) error {
		var jb *bolt.Bucket

		jb = tx.Bucket(jobBucket)
		if jb == nil {
			return nil
		}

		// FIXME: preallocate jobs
		cur := jb.Cursor()
		k, v := cur.Seek([]byte(jobInfoPrefix))
		for ; k != nil; k, v = cur.Next() {
			ks := string(k)
			// log.Printf("k = %s", ks)
			if !strings.HasPrefix(ks, jobInfoPrefix) {
				break
			}

			jobId := jobKeyToId(ks)
			if jobId == 0 {
				return fmt.Errorf("key '%s' has an invalid jobId", k)
			}

			jobData := fuq.JobDescription{}
			if err := msgpack.Unmarshal(v, &jobData); err != nil {
				return fmt.Errorf("error unmarshaling job data for key %s: %v",
					k, err)
			}

			if name != "" && jobData.Name != name {
				continue
			}

			if status != "" && jobData.Status.String() != status {
				continue
			}

			if status == "" {
				if jobData.Status == fuq.Cancelled {
					continue
				}
				if jobData.Status == fuq.Finished {
					continue
				}
			}

			if jobData.JobId == 0 {
				jobData.JobId = fuq.JobId(jobId)
			}

			jobs = append(jobs, jobData)
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error fetching jobs (name='%s',status='%s'): %v",
			name, status, err)
	}

	if jobs == nil {
		jobs = make([]fuq.JobDescription, 0, 0)
	}

	return jobs, nil
}

func (d *JobStore) ChangeJobState(jobId fuq.JobId, newState fuq.JobStatus) (fuq.JobStatus, error) {
	prevStatus := fuq.JobStatus(0)
	err := d.db.Update(func(tx *bolt.Tx) error {
		var err error
		prevStatus, err = d.setJobStatus(tx, jobId, newState)
		return err
	})
	return prevStatus, err
}

func (d *JobStore) HoldJob(jobId fuq.JobId) (fuq.JobStatus, error) {
	return d.ChangeJobState(jobId, fuq.Paused)
}

func (d *JobStore) ReleaseJob(jobId fuq.JobId) (fuq.JobStatus, error) {
	return d.ChangeJobState(jobId, fuq.Waiting)
}

func (d *JobStore) CancelJob(jobId fuq.JobId) (fuq.JobStatus, error) {
	return d.ChangeJobState(jobId, fuq.Cancelled)
}

func jobToBytes(jPtr *fuq.JobDescription) ([]byte, error) {
	return msgpack.Marshal(jPtr)
}

func bytesToJob(b []byte) (fuq.JobDescription, error) {
	job := fuq.JobDescription{}
	err := msgpack.Unmarshal(b, &job)
	return job, err
}

func jobIndexKey(job fuq.JobDescription) []byte {
	b := bytes.Buffer{}
	b.WriteString(job.Status.String())
	b.WriteByte(0)
	b.WriteString(job.Name)
	b.WriteByte(0)

	idBuf := [binary.MaxVarintLen64]byte{}
	nb := binary.PutUvarint(idBuf[:], uint64(job.JobId))
	b.Write(idBuf[0:nb])

	return b.Bytes()
}

func (d *JobStore) AddJob(job fuq.JobDescription) (fuq.JobId, error) {
	jobId := uint64(0)

	err := d.db.Update(func(tx *bolt.Tx) error {
		var jb *bolt.Bucket
		var jobData, jobKey []byte // , nextTaskKey []byte
		var err error

		jb = tx.Bucket(jobBucket)
		if jb == nil {
			return errors.New("cannot find job bucket")
		}

		indBkt := jb.Bucket(jobIndexBucket)
		if indBkt == nil {
			return errors.New("cannot find primary job index bucket")
		}

		wb, err := d.getStatusBucket(tx, fuq.Waiting)
		if err != nil {
			return fmt.Errorf("cannot find job/waiting bucket: %v", err)
		}

		id, err := jb.NextSequence()
		if err != nil {
			return err
		}
		job.JobId = fuq.JobId(id)

		jobKey = []byte(fmt.Sprintf("job:info:%d", job.JobId))
		// nextTaskKey = []byte(fmt.Sprintf("job:tasks:%d", job.JobId))

		jobData, err = msgpack.Marshal(&job)
		if err != nil {
			return fmt.Errorf("error marshaling job: %v", err)
			return err
		}

		if err = jb.Put(jobKey, jobData); err != nil {
			return fmt.Errorf("error adding job %d: %v",
				jobId, err)
		}

		/*
			tmp := [binary.MaxVarintLen64]byte{}
			nb := binary.PutUvarint(tmp[:], 0)
			if err = jb.Put(nextTaskKey, tmp[:nb]); err != nil {
				return fmt.Errorf("error adding next task entry for job %d: %v",
					jobId, err)
			}
		*/

		indKey := jobIndexKey(job)
		if err = indBkt.Put(indKey, []byte{}); err != nil {
			return fmt.Errorf("error updating indices: %v", err)
		}

		if err := wb.AddJob(job.JobId); err != nil {
			return fmt.Errorf("error updating indices: %v", err)
		}

		// update job waiting list
		/*
			nb = binary.PutUvarint(tmp[:], jobId)
			if err = wb.Put(tmp[:nb], []byte{}); err != nil {
				return fmt.Errorf("error updating waiting queue: %v", err)
			}
		*/

		return nil
	})

	return job.JobId, err
}

func getJobBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	jb := tx.Bucket(jobBucket)
	if jb == nil {
		return nil, errors.New("invalid schema: missing jobs bucket")
	}
	return jb, nil
}

func getJobKey(jobId fuq.JobId) []byte {
	return []byte(jobInfoPrefix + strconv.FormatUint(uint64(jobId), 10))
}

func getJobTaskKey(jobId fuq.JobId) []byte {
	return []byte(taskInfoPrefix + strconv.FormatUint(uint64(jobId), 10))
}

type statusBkt struct {
	status fuq.JobStatus
	*bolt.Bucket
	curs *bolt.Cursor
}

func (d *JobStore) getStatusBucket(tx *bolt.Tx, status fuq.JobStatus) (statusBkt, error) {
	jb, err := getJobBucket(tx)
	if err != nil {
		return statusBkt{}, err
	}

	bkt := jb.Bucket(statusBuckets[status])
	if bkt == nil {
		return statusBkt{}, fmt.Errorf("invalid schema: missing '%s' status bucket",
			status)
	}

	return statusBkt{status: status, Bucket: bkt, curs: nil}, nil
}

/* why big endian?  consistent sort order: smaller ids will be
 * sorted lexicographically before larger ids */
func (sb statusBkt) idToKey(jobId fuq.JobId) [8]byte {
	idBuf := [8]byte{}
	binary.BigEndian.PutUint64(idBuf[:], uint64(jobId))
	return idBuf
}

func (sb statusBkt) keyToId(k []byte) fuq.JobId {
	if k == nil {
		return fuq.JobId(0)
	}

	return fuq.JobId(binary.BigEndian.Uint64(k))
}

func (sb *statusBkt) AddJob(jobId fuq.JobId) error {
	sb.curs = nil // invalidate cursor on delete
	key := sb.idToKey(jobId)
	if err := sb.Put(key[:], []byte{}); err != nil {
		return fmt.Errorf("error adding job %d to '%s' status bucket: %v",
			uint64(jobId), sb.status, err)
	}

	return nil
}

func (sb *statusBkt) RemoveJob(jobId fuq.JobId) error {
	sb.curs = nil // invalidate cursor on delete
	key := sb.idToKey(jobId)
	if err := sb.Delete(key[:]); err != nil {
		return fmt.Errorf("error deleting job %d from '%s' status bucket: %v",
			uint64(jobId), sb.status, err)
	}

	return nil
}

func (sb *statusBkt) NextJob() fuq.JobId {
	var k []byte
	if sb.curs == nil {
		return sb.FirstJob()
	}

	k, _ = sb.curs.Next()
	if k == nil {
		return fuq.JobId(0)
	}
	log.Printf("next job in '%s' is %v", sb.status, k)
	return sb.keyToId(k)
}

func (sb *statusBkt) FirstJob() fuq.JobId {
	var k []byte
	if sb.curs == nil {
		sb.curs = sb.Cursor()
	}

	k, _ = sb.curs.First()
	if k == nil {
		return fuq.JobId(0)
	}
	return sb.keyToId(k)
}

func (d *JobStore) updateJobInTx(tx *bolt.Tx,
	jobId fuq.JobId, fxn func(*fuq.JobDescription) error) error {

	jb, err := getJobBucket(tx)
	if err != nil {
		return err
	}

	js := getJobKey(jobId)
	jobData, err := fetchJob(jb, jobId)
	if err != nil {
		return fmt.Errorf("error unmarshaling job for update: %v", err)
	}

	oldId := jobData.JobId
	oldStatus := jobData.Status
	oldKey := jobIndexKey(jobData)
	if err := fxn(&jobData); err != nil {
		return fmt.Errorf("error updating job: %v", err)
	}

	if jobData.JobId != oldId {
		return fmt.Errorf("invalid update: job id changed from %d to %d",
			uint64(oldId), uint64(jobData.JobId))
	}

	newKey := jobIndexKey(jobData)
	newStatus := jobData.Status

	raw, err := msgpack.Marshal(&jobData)
	if err != nil {
		return fmt.Errorf("error marshaling job for update: %v", err)
	}

	if err := jb.Put(js, raw); err != nil {
		return fmt.Errorf("error saving updated job: %v", err)
	}

	if !bytes.Equal(oldKey, newKey) {
		log.Printf("reindexing job '%s' with new key %v", jobData.Name, newKey)
		if indBkt := jb.Bucket(jobIndexBucket); indBkt != nil {
			if err := indBkt.Delete(oldKey); err != nil {
				return fmt.Errorf("error updatiing job indices: %v", err)
			}

			if err := indBkt.Put(newKey, []byte{}); err != nil {
				return fmt.Errorf("error updatiing job indices: %v", err)
			}
		}
	}

	if newStatus != oldStatus {
		log.Printf("updating job status of '%s': %s -> %s", jobData.Name, oldStatus, newStatus)
		oldStatusBucket, err := d.getStatusBucket(tx, oldStatus)
		if err != nil {
			return fmt.Errorf("error updating status: status bucket '%s' does not exit",
				oldStatus)
		}

		newStatusBucket, err := d.getStatusBucket(tx, newStatus)
		if err != nil {
			return err
		}

		if err := oldStatusBucket.RemoveJob(jobData.JobId); err != nil {
			return err
		}

		if err := newStatusBucket.AddJob(jobData.JobId); err != nil {
			return err
		}
	}

	return nil
}

func (d *JobStore) setJobStatus(tx *bolt.Tx, jobId fuq.JobId, status fuq.JobStatus) (fuq.JobStatus, error) {
	prevStatus := fuq.JobStatus(0)
	err := d.updateJobInTx(tx, jobId, func(desc *fuq.JobDescription) error {
		prevStatus = desc.Status
		desc.Status = status
		return nil
	})

	if err != nil {
		return fuq.JobStatus(0), fmt.Errorf("Error setting job status: %v", err)
	}

	return prevStatus, nil
}

func (d *JobStore) fetchJobTasksFromBkt(jb *bolt.Bucket, jobId fuq.JobId) (*fuq.JobTaskData, error) {
	taskData := fuq.JobTaskData{}
	jobTaskKey := getJobTaskKey(jobId)
	rawTaskData := jb.Get(jobTaskKey)
	if rawTaskData != nil {
		if err := msgpack.Unmarshal(rawTaskData, &taskData); err != nil {
			return nil, fmt.Errorf("error unmarshaling task data for job %d: %v",
				uint64(jobId), err)
		}
	} else {
		desc, err := fetchJob(jb, jobId)
		if err != nil {
			return nil, fmt.Errorf("error fetching job description to generate task data: %v",
				err)
		}
		taskData.JobId = jobId
		taskData.Pending = make([]int, desc.NumTasks)
		for i := 0; i < desc.NumTasks; i++ {
			taskData.Pending[i] = i + 1
		}
	}

	return &taskData, nil

}

func (d *JobStore) updateJobTasksInTx(tx *bolt.Tx, jobId fuq.JobId, fxn func(*fuq.JobTaskData) error) error {
	jb, err := getJobBucket(tx)
	if err != nil {
		return err
	}

	taskData, err := d.fetchJobTasksFromBkt(jb, jobId)
	if err != nil {
		return fmt.Errorf("error fetching task data for job %d: %v",
			jobId, err)
	}

	if err := fxn(taskData); err != nil {
		return err
	}

	jobTaskKey := getJobTaskKey(jobId)
	rawTaskData, err := msgpack.Marshal(taskData)
	if err != nil {
		return fmt.Errorf("error marshaling updated task data for job %d: %v",
			uint64(jobId), err)
	}

	if err := jb.Put(jobTaskKey, rawTaskData); err != nil {
		return fmt.Errorf("error updating task data for job %d: %v",
			uint64(jobId), err)
	}

	return nil
}

func (d *JobStore) UpdateTaskStatus(update fuq.JobStatusUpdate) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		numRunning := -1
		numPending := -1
		err := d.updateJobTasksInTx(tx, update.JobId, func(tasks *fuq.JobTaskData) error {
			for i, v := range tasks.Running {
				if v != update.Task {
					continue
				}

				tasks.Running[i] = tasks.Running[len(tasks.Running)-1]
				tasks.Running = tasks.Running[:len(tasks.Running)-1]
				break
			}
			numRunning = len(tasks.Running)
			numPending = len(tasks.Pending)

			if update.Success {
				tasks.Finished = append(tasks.Finished, update.Task)
			} else {
				tasks.Errors = append(tasks.Errors, update.Task)
				tasks.ErrorMessages = append(tasks.ErrorMessages, update.Status)
			}

			return nil
		})

		if err != nil {
			return err
		}

		log.Printf("job %d has %d tasks running and %d tasks pending",
			update.JobId, numRunning, numPending)

		if numRunning == 0 && numPending == 0 {
			log.Printf("changing job %d status to finished", update.JobId)
			return d.updateJobInTx(tx, update.JobId, func(desc *fuq.JobDescription) error {
				desc.Status = fuq.Finished
				return nil
			})
		}

		return nil
	})
}

func (d *JobStore) nextTasksForJob(tx *bolt.Tx, jobId fuq.JobId, maxTasks int) ([]int, error) {
	var tasklist []int

	err := d.updateJobTasksInTx(tx, jobId, func(taskData *fuq.JobTaskData) error {
		if len(taskData.Pending) == 0 {
			return nil
		}

		log.Printf("trying to fetch %d tasks from job %d; %d available",
			maxTasks, jobId, len(taskData.Pending))

		if maxTasks > len(taskData.Pending) {
			maxTasks = len(taskData.Pending)
		}

		/* FIXME: we should probably wait for some kind of response from
		 * the node ("I'm running tasks <task_list>") before marking
		 * these as 'Running'.
		 *
		 * But that's probably a goal for a future version.
		 */
		tasklist = taskData.Pending[0:maxTasks]
		taskData.Pending = taskData.Pending[maxTasks:]
		taskData.Running = append(taskData.Running, tasklist...)

		return nil
	})

	if err != nil {
		return nil, err
	}

	return tasklist, nil
}

func (d *JobStore) getAvailableTasks(tx *bolt.Tx, jobId fuq.JobId, max int) ([]fuq.Task, error) {
	jobTasks, err := d.nextTasksForJob(tx, jobId, max)
	if err != nil {
		return nil, err
	}

	if jobTasks == nil {
		return nil, nil
	}

	desc, err := fetchJobFromTx(tx, jobId)
	if err != nil {
		log.Printf("error fetching job %d description: %v",
			uint64(jobId), err)
		return nil, err
	}

	if desc.JobId != jobId {
		return nil, fmt.Errorf("error queuing tasks: job %d has mismatched job_id = %d",
			jobId, desc.JobId)
	}

	log.Printf("queuing %d additional tasks from job %d", len(jobTasks), uint64(jobId))
	tasks := make([]fuq.Task, len(jobTasks))
	for i, taskInd := range jobTasks {
		tasks[i] = fuq.Task{
			Task:           taskInd,
			JobDescription: desc,
		}
	}

	return tasks, nil
}

func (d *JobStore) FetchJobTaskStatus(jobId fuq.JobId) (fuq.JobTaskStatus, error) {
	var status fuq.JobTaskStatus

	err := d.db.View(func(tx *bolt.Tx) error {
		jb, err := getJobBucket(tx)
		if err != nil {
			return err
		}

		desc, err := fetchJobFromTx(tx, jobId)
		if err != nil {
			return err
		}

		taskData, err := d.fetchJobTasksFromBkt(jb, jobId)
		if err != nil {
			return err
		}

		status.Description = desc
		status.TasksFinished = len(taskData.Finished)
		status.TasksPending = len(taskData.Pending)
		status.TasksRunning = make([]int, len(taskData.Running))
		status.TasksWithErrors = make([]int, len(taskData.Errors))

		for i, task := range taskData.Running {
			status.TasksRunning[i] = task
		}

		for i, task := range taskData.Errors {
			status.TasksWithErrors[i] = task
		}

		return nil
	})

	return status, err
}

func (d *JobStore) FetchPendingTasks(nproc int) ([]fuq.Task, error) {
	tasks := []fuq.Task{}

	err := d.db.Update(func(tx *bolt.Tx) error {
		rb, err := d.getStatusBucket(tx, fuq.Running)
		if err != nil {
			return err
		}

		for jobId := rb.NextJob(); jobId != 0 && len(tasks) < nproc; jobId = rb.NextJob() {
			// log.Printf("job %d currently running", jobId)
			newTasks, err := d.getAvailableTasks(tx, jobId, nproc-len(tasks))
			if err != nil {
				return err
			}

			if newTasks == nil {
				continue
			}

			log.Printf("queuing %d additional tasks for job %d", len(newTasks), jobId)
			tasks = append(tasks, newTasks...)
		}

		if len(tasks) >= nproc {
			return nil
		}

		// see if we can enqueue a waiting job
		wb, err := d.getStatusBucket(tx, fuq.Waiting)
		if err != nil {
			return err
		}

		if err := wb.ForEach(func(k, v []byte) error {
			jobId := wb.keyToId(k)
			log.Printf("job %d / key %v", jobId, k)
			return nil
		}); err != nil {
			log.Printf("error iterating over waiting bucket: %v", err)
		}

		for len(tasks) < nproc {
			jobId := wb.FirstJob()
			if jobId == 0 {
				break
			}

			/* mark job as running */
			err := d.updateJobInTx(tx, jobId, func(job *fuq.JobDescription) error {
				job.Status = fuq.Running
				return nil
			})

			if err != nil {
				return fmt.Errorf("error enqueuing job %d: %v",
					uint64(jobId), err)
			}

			newTasks, err := d.getAvailableTasks(tx, jobId, nproc-len(tasks))
			if err != nil {
				return err
			}

			if newTasks == nil {
				continue
			}

			log.Printf("queuing %d additional tasks", len(newTasks))
			tasks = append(tasks, newTasks...)

		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error fetching pending tasks: %v", err)
	}

	return tasks, nil
}
