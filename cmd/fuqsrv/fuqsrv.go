package main

/* What's left to do?
 * 1. Generate configuration file
 * 2. Configure TLS
 * 3. Read node file / automatic discovery of the foreman
 * 4. Testing on cluster
 * 5. Write unit tests?
 */

import (
	"flag"
	"fmt"
	"github.com/sfstewman/fuq"
	"github.com/sfstewman/fuq/srv"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// import _ "net/http/pprof"

func WriteFuqConfig(cfg fuq.Config) {
	// write fuq config:
	//   hostname, port to contact
}

/* API:
 *
 * The API is entirely json.
 *
 * Worker API
 *
 * HELLO: register worker with queue
 *
 *	POST /hello			*done*
 *	{
 *	 	"auth" : password,
 *		"node" : node_name,
 *		"nproc" : number_of_processors,
 *	}
 *
 *	Success:
 *	{
 *		"name" : unique_name,
 *		"cookie" : cookie,
 *	}
 *
 *	Failure:
 *	Returns http error code.
 *
 * REQUEST: request new jobs		*done*
 *
 *	POST /job/request
 *	{
 *		"cookie" : cookie,
 *		"msg" : {
 *			"nproc"  : number_of_procs,
 *		},
 *	}
 *	Note: nproc is optional, and if omitted will default to 1.
 *
 *	(if absent, defaults to 1)
 *
 *	Success:
 *	[ job_info1, job_info2, ..., job_infoN ]
 *
 *	where job_infoX:
 *	{
 *		"job_id" : unique_job_identifier,
 *		"name" : job_name,
 *		"task" : task_number,
 *		"working_dir" : job_working_directory,
 *		"command" : job_command,
 *	}
 *
 *	Failure:
 *	http error code
 *
 * STATUS: update job status		*done*
 *
 *	POST /job/status
 *	{
 *		"cookie" : cookie,
 *		"msg" : {
 *			"job_id" : unique_job_identifier,
 *			"task" : task_id,
 *			"success" : true or false,
 *			"status" : job_status,
 *			"newjob" : {	# newjob is optional
 *				"nproc"  : number_of_procs,
 *					(if absent, defaults to 1)
 *			}
 *		}
 *	}
 *
 *	where job_status can be: "done" (if "success" is true) or a
 *	description of the error (if possible).
 *
 *	Success:
 *	If newjob exists, returns the same as REQUEST
 *	otherwise, returns http success code.
 *
 *
 * Client API:
 *
 * All client calls have an authentication entry:
 *
 *	{
 *		"auth" : {
 *	 		"password" : password,
 *	 		"client" : client_identifier,
 *		}
 *	}
 *
 *	The actual part that varies from call to call is
 *	in the "msg" part.
 *
 * NODE LIST: list nodes			*done*
 *	POST /client/nodes
 *	{
 *		"auth" : <client_auth>,
 *	}
 *	NB: there's no msg part
 *
 *	Success:
 *	[ node_info1, node_info2, ..., node_infoN ]
 *
 *	where node_infoX is
 *	{
 *		"node" : node_name,
 *		"uniq_name" : unique_node_name,
 *		"nproc" : number_of_processors,
 *	}
 *
 * JOB LIST: list jobs			*done*
 *
 *	POST /client/job/list
 *	(note that all fields are optional)
 *	{
 *		"auth" : <client_auth>,
 *		"msg" : {
 *			"queue" : queue_name,
 *			"status" : < "pending", "running", "finished", "error" >,
 *			"name" : job_name_or_glob,
 *		}
 *	}
 *
 *	Success:
 *	[ job_info1, job_info2, ..., job_infoN ]
 *
 *	where job_infoX:
 *	{
 *		"name" : job_name,
 *		"queue" : queue_name,
 *		"working_dir" : job_working_directory,
 *		"command" : job_command,
 *		"num_tasks" : task_interval_list,
 *		"status" : < "pending", "running", "finished", "error" >,
 *	}
 *
 *	where task_interval_list is a list of integers.  A negative
 *	integer must follow a positive integer, and indicates a
 *	interval: ..., 3, -15, ...  indicates a range 3-17.
 *
 *	Failure:
 *	HTTP error code
 *
 *
 * JOB ADD: add jobs			*done*
 *
 *	POST /client/job/new
 *	{
 *		"auth" : <client_auth>,
 *		"msg" : {
 *			"name" : job_name,
 *			"num_tasks" : number_of_tasks,
 *			"working_dir" : job_working_directory,
 *			"command" : job_command,
 *		}
 *	}
 *
 *	Success:
 *	{ "job_id" : <job_id> }
 *
 *	Failure:
 *	HTTP failure code
 *
 *
 * CHANGE JOB STATE: Changes the job state.
 *
 * This holds, releases, or cancels a job.
 *
 *	POST /client/job/state
 *	{
 *		"auth" : <client_auth>,
 *		"msg" : {
 *			"action" : <hold_release_or_cancel>,
 *
 *			"job_ids" : [ <job_id>, <job_id>, ... ],
 *		    -or-
 *			"job_names" : [ <job_name>, <job_name>, ... ],
 *		},
 *	}
 *	NB: either job_ids or job_names can be given.  If both are given,
 *	job_ids is used.
 *
 *	The action field can be "hold", "release", or "cancel".  This
 *	will hold a job, release the job, or cancels the job.
 *
 *	A hold will prevent tasks from being dispatched for the job.  A
 *	release will remove a hold on the job, allowing further tasks to
 *	be queued.  A cancel will permanently remove the job from the
 *	queue.
 *
 *	Success:
 *	[state_change1, state_change2, ..., state_changeN]
 *
 *	where state_changeN:
 *	{
 *		"job_id" : <job_id>,
 *		"prev_status" : <previous status>,
 *		"new_status" : <new status>,
 *	}
 *
 *
 *	Failure:
 *	HTTP failure code
 *
 *
 * JOB MOVE: move job between queues
 *
 *	POST /client/job/move
 *	{
 *		"auth" : <client_auth>,
 *		"msg" : {
 *			"new_queue" : queue_name,
 *
 *			"job_id" : <job_id>,
 *		    -or-
 *			"job_name" : <job_name>,
 *	}
 *	NB: either job_id or name can be given.  If both are given,
 *	job_id is used.
 *
 *	Success:
 *	HTTP success code
 *
 *	Failure:
 *	HTTP error code
 *
 *
 * QUEUE LIST: list queues
 *
 *	POST /client/queue/list
 *	{
 *		"names" : <optional queue names>
 *	}
 *
 *	Success:
 *	[ queue_info1, queue_info2, ... ]
 *
 *	where queue_infoX is:
 *	{
 *		"name" : queue_name,
 *		"status" : <running | empty | hold>,
 *		"jobs" : [ job_name1, job_name2, ... ],
 *	}
 *
 *	Failure:
 *	http error code
 *
 * QUEUE ADD: add a new queue
 *
 *	POST /client/queue/list
 *	{
 *		"name" : queue_name,
 *	}
 *
 *	Success:
 *	http success code
 *
 *	Failure:
 *	http error code
 *
 * QUEUE DELETE: add a new queue
 *
 *	POST /client/queue/delete
 *	{
 *		"name" : queue_name,
 *	}
 *
 *	Success:
 *	http success code
 *
 *	Failure:
 *	http error code
 *
 * QUEUE HOLD: hold all jobs on queue
 *
 *	POST /client/queue/list
 *	{
 *		"name" : queue_name,
 *	}
 *
 *	Success:
 *	http success code
 *
 *	Failure:
 *	http error code
 *
 * WORKER LIST: lists registered workers
 *
 *	POST /client/workers/list
 *	{
 *	}
 *
 *	Success:
 *	[ worker_info1, worker_info2, ... ]
 *
 *	where worker_infoX is:
 *	{
 *		"name" : unique_name,
 *		"node" : worker_node,
 *	}
 *
 *	Failure:
 *	http error code
 *
 * NODE ACTION: asks a registered worker to pause, resume, or stop
 *
 *	POST /client/node/stop
 *	{
 *		"auth" : <client_auth>,
 *		"msg" : {
 *			"name" : <unique_name>,
 *		}
 *	}
 *
 *	Success:
 *	HTTP success code
 *
 *	Failure:
 *	HTTP error code
 *
 * SHUTDOWN: shuts down fuq			*done*
 *
 *	POST /client/shutdown
 *	{
 *		"auth" : <client_auth>,
 *	}
 *
 *	Success:
 *	HTTP success code
 *
 *	Failure:
 *	HTTP error code
 *
 */

type RefreshCookieAction struct{}

type WaitAction struct {
	Interval time.Duration
}

const (
	DefaultInterval  = 5 * time.Second
	IntervalIncrease = 500 * time.Millisecond
	MaxInterval      = 60 * time.Second
)

func (w WaitAction) Wait() {
	delay := w.Interval
	if delay == 0 {
		delay = DefaultInterval
	}
	time.Sleep(delay)
}

type StopAction struct{}

type RunAction fuq.Task

const (
	MaxLogRetry     = 10000
	MaxRefreshTries = 5
)

func (r RunAction) Run(logger *log.Logger) (fuq.JobStatusUpdate, error) {
	status := fuq.JobStatusUpdate{
		JobId: r.JobId,
		Task:  r.Task,
	}

	runner := exec.Command(r.Command, strconv.Itoa(r.Task))
	runner.Dir = r.WorkingDir

	/* XXX add logging directory option */
	runner.Stdin = nil

	// XXX make path handling less Unix-y
	logName := fmt.Sprintf("%s/%s_%d.log", r.LoggingDir, r.Name, r.Task)
	logFile, err := os.OpenFile(logName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil && os.IsExist(err) {
		for i := 1; i <= MaxLogRetry && os.IsExist(err); i++ {
			logName = fmt.Sprintf("%s/%s_%d-%d.log", r.LoggingDir, r.Name, r.Task, i)
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

	logger.Printf("running: wd='%s', cmd='%s', args='%v', log='%s'",
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

type Worker struct {
	Logger *log.Logger
	*fuq.Endpoint
	Name     string
	Cookie   fuq.Cookie
	NodeInfo fuq.NodeInfo
}

func NewWorker(name string, config fuq.Config) (*Worker, error) {
	endpoint, err := fuq.NewEndpoint(config)
	if err != nil {
		return nil, err
	}

	w := &Worker{
		Endpoint: endpoint,
		Name:     name,
	}

	return w, nil
}

func (w *Worker) Hello() error {
	w.NodeInfo = fuq.NodeInfo{
		Node:    w.Name,
		NumProc: 1,
	}

	hello := fuq.Hello{
		Auth:     w.Config.Auth,
		NodeInfo: w.NodeInfo,
	}

	ret := srv.HelloResponseEnv{
		Name:   &w.NodeInfo.UniqName,
		Cookie: &w.Cookie,
	}

	if err := w.CallEndpoint("hello", &hello, &ret); err != nil {
		return err
	}

	return nil
}

func (w *Worker) RefreshCookie() error {
	hello := fuq.Hello{
		Auth:     w.Config.Auth,
		NodeInfo: w.NodeInfo,
	}

	req := srv.NodeRequestEnvelope{
		Cookie: w.Cookie,
		Msg:    &hello,
	}

	name := ""
	ret := srv.HelloResponseEnv{
		Name:   &name,
		Cookie: &w.Cookie,
	}

	if err := w.CallEndpoint("node/reauth", &req, &ret); err != nil {
		return err
	}

	if name != w.NodeInfo.UniqName {
		log.Fatalf("invalid: cookie refresh changed unique name from %s to %s",
			w.NodeInfo.UniqName, name)
	}

	return nil
}

func (w *Worker) LogIfError(err error, pfxFmt string, args ...interface{}) {
	if err == nil {
		return
	}

	pfx := fmt.Sprintf(pfxFmt, args...)
	w.Log("%s: %v", pfx, err)
}

func (w *Worker) Log(format string, args ...interface{}) {
	if w.Logger != nil {
		w.Logger.Printf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

/* This shouldn't return interface{}, but I'm not entirely sure what it
* should return
* XXX
 */
func (w *Worker) RequestAction(nproc int) (interface{}, error) {

	req := srv.NodeRequestEnvelope{
		Cookie: w.Cookie,
		Msg: fuq.JobRequest{
			NumProc: nproc,
		},
	}

	ret := []fuq.Task{}
	ntries := 0

retry:
	err := w.CallEndpoint("job/request", &req, &ret)
	if err != nil {
		if fuq.IsForbidden(err) && ntries < MaxRefreshTries {
			ntries++
			w.Log("Stale cookie")
			err = w.RefreshCookie()
			if err == nil {
				w.Log("Refreshed cookie")
				log.Printf("%s: REAUTH successful", w.NodeInfo.UniqName)
				req.Cookie = w.Cookie
				goto retry
			}
		}

		return nil, err
	}

	// w.Log("job request finished: %v", ret)

	if len(ret) > 1 {
		panic("more than one task not yet supported")
	}

	if len(ret) == 0 {
		return WaitAction{}, nil
	}

	w.Log("received job request: %v", ret)

	act := RunAction(ret[0])
	return act, nil
}

func (w *Worker) UpdateAndRequestAction(status fuq.JobStatusUpdate, nproc int) (interface{}, error) {
	if nproc > 0 {
		status.NewJob = &fuq.JobRequest{NumProc: nproc}
	}

	req := srv.NodeRequestEnvelope{
		Cookie: w.Cookie,
		Msg:    &status,
	}

	ret := []fuq.Task{}

	if err := w.CallEndpoint("job/status", &req, &ret); err != nil {
		return nil, err
	}

	if len(ret) > 1 {
		panic("more than one task not yet supported")
	}

	if len(ret) == 0 {
		return WaitAction{}, nil
	}

	w.Log("received job request: %v", ret)

	act := RunAction(ret[0])
	return act, nil
}

func (w *Worker) Loop() {
	// 'HELLO' handshake with foreman
	if err := w.Hello(); err != nil {
		log.Fatal(err)
	}

	// sanitize colons by replacing them with underscores
	uniqName := strings.Replace(w.NodeInfo.UniqName, ":", "_", -1)

	log.Printf("HELLO finished.  Unique name is '%s'",
		w.NodeInfo.UniqName)

	logPath := filepath.Join(w.Config.LogDir,
		fmt.Sprintf("%s.log", uniqName))

	logFile, err := os.Create(logPath)
	fuq.FatalIfError(err, "error creating worker log '%s'", logPath)

	w.Logger = log.New(logFile, "w:"+uniqName, log.LstdFlags)
	defer w.Logger.SetOutput(os.Stderr)
	defer logFile.Close()

	numWaits := 0
run_loop:
	for {
		// request a job from the foreman
		req, err := w.RequestAction(1)
		if err != nil {
			w.Log("error requesting job: %v", err)
			req = WaitAction{}
		}

	req_switch:
		switch r := req.(type) {
		case WaitAction:
			// add -1.5 to 1.5 second variability so not all
			// clients contact at once...
			randDelay := time.Duration(rand.Intn(300)-150) * 10 * time.Millisecond
			r.Interval = DefaultInterval + time.Duration(numWaits)*IntervalIncrease + randDelay
			if r.Interval > MaxInterval {
				r.Interval = MaxInterval
			}
			r.Wait()
			numWaits++
			continue run_loop

		case StopAction:
			break run_loop

		case RunAction:
			numWaits = 0 // reset wait counter
			if r.LoggingDir == "" {
				r.LoggingDir = w.Config.LogDir
			}

			status, err := r.Run(w.Logger)
			w.LogIfError(err, "error encountered while running job")

			if status.Success {
				w.Log("job %d:%d completed successfully", status.JobId, status.Task)
			} else {
				w.Log("job %d:%d encountered error: %s",
					status.JobId, status.Task, status.Status)
			}

			req, err = w.UpdateAndRequestAction(status, 1)
			if err != nil {
				w.Log("error requesting job: %v", err)
				req = WaitAction{}
			}
			goto req_switch
		default:
			w.Log("unexpected result when requesting job: %v", req)
		}
	}
}

func (w *Worker) Close() {
	/* nop for now */
}

func main() {
	var (
		err                          error
		isForeman                    bool
		hostname, workerName         string
		srvConfigFile, sysConfigFile string
		config                       fuq.Config
		overwriteConfig              bool
		numCPUs                      int
		u                            *user.User
	)

	// command line options
	isForeman = false

	// set worker name, defaults to hostname
	hostname, err = os.Hostname()
	if err != nil {
		log.Printf("error retrieving hostname: %v", err)
	}

	u, err = user.Current()
	if err != nil {
		log.Fatalf("error retrieving current user: %v", err)
	}

	workerName = hostname
	if workerName == "" {
		workerName = "worker"
	}

	// default config file
	srvConfigFile = fuq.DefaultServerConfigPath()
	sysConfigFile = fuq.DefaultSystemConfigPath()

	// Setup command-line flags
	flag.BoolVar(&isForeman, "f", false, "invoke fuq as foreman")
	flag.IntVar(&numCPUs, "np", 1, "number of concurrent cores")
	flag.StringVar(&workerName, "p", workerName, "worker prefix")
	flag.StringVar(&srvConfigFile, "srv", srvConfigFile, "server configuration file")
	flag.StringVar(&sysConfigFile, "cfg", sysConfigFile, "configuration file")
	flag.BoolVar(&overwriteConfig, "force_cfg", overwriteConfig,
		"overwrite config file (if foreman)")

	flag.StringVar(&config.DbPath, "db", "", "path to database")
	flag.StringVar(&config.LogDir, "log", "", "queue logging directory")
	flag.IntVar(&config.Port, "port", 0, "foreman port")

	flag.StringVar(&config.KeyFile, "key", "", "path to TLS key file")
	flag.StringVar(&config.CertFile, "cert", "", "path to TLS cert file")
	flag.StringVar(&config.RootCAFile, "ca", "", "path to TLS root ca file")
	flag.StringVar(&config.CertName, "certname", "", "name in TLS certificate")

	flag.Parse()

	if srvConfigFile == "" {
		log.Fatalf("No default or given server config file")
	}

	if sysConfigFile == "" {
		log.Fatalf("No default or given config file")
	}

	if isForeman {
		if err := config.ReadConfig(sysConfigFile); err != nil {
			// igore if the system configuration file does not exist
			if !os.IsNotExist(err) {
				log.Fatalf("error reading config file '%s': %v",
					sysConfigFile, err)
			}
		}

		if err := config.GenerateConfig(srvConfigFile, overwriteConfig); err != nil {
			log.Fatalf("error generating config file '%s': %v",
				srvConfigFile, err)
		}
	} else {
		if err := config.ReadConfig(srvConfigFile); err != nil {
			log.Fatalf("error reading config file '%s': %v",
				srvConfigFile, err)
		}

		if err := config.ReadConfig(sysConfigFile); err != nil {
			// igore if the system configuration file does not exist
			if !os.IsNotExist(err) {
				log.Fatalf("error reading config file '%s': %v",
					sysConfigFile, err)
			}
		}
	}

	config.SetupTLS(fuq.DefaultConfigDir(u))

	fmt.Printf("Configuration is:\n%#v\n\n", config)

	// Start
	if numCPUs <= 0 || numCPUs > runtime.NumCPU() {
		numCPUs = runtime.NumCPU()
	}

	log.Printf("Limiting to %d CPUs", numCPUs)
	runtime.GOMAXPROCS(numCPUs)

	log.Printf("Log directory is %s", config.LogDir)
	if err := os.Mkdir(config.LogDir, 0700); err != nil && !os.IsExist(err) {
		log.Fatalf("error making logging directory: %v", err)
	}

	done := make(chan struct{})
	intr := make(chan os.Signal, 1)
	signal.Notify(intr, os.Interrupt)

	defer log.Println("Exiting...")

	nproc := numCPUs

	if isForeman {
		log.Printf("Starting foreman")
		go func() {
			f, err := NewForeman(config, done)
			defer close(done)

			if err != nil {
				log.Printf("error starting foreman: %v", err)
				return
			}
			defer f.Close()

			if err := f.Run(); err != nil {
				log.Printf("error starting foreman: %v", err)
				return
			}
		}()
		nproc--
	}

	for i := 0; i < nproc; i++ {
		log.Printf("Starting worker")
		go func() {
			w, err := NewWorker(workerName, config)
			if err != nil {
				log.Printf("error starting worker %d: %v", err)
				close(done)
				return
			}
			defer w.Close()

			/*
				go func() {
					log.Println(http.ListenAndServe("localhost:6060", nil))
				}()
			*/

			w.Loop()
		}()
	}

runloop:
	for {
		select {
		case <-intr:
			// runtime.GC()
			log.Println("Keyboard interrupt detected... press again to shut down")
			select {
			case <-intr:
				log.Println("Keyboard interrupt detected... shutting down")
				break runloop
			case <-time.After(5 * time.Second):
				continue runloop
			}
		case <-done:
			log.Println("Programmatic quit signal... shutting down")
			break runloop
		}
	}
}
