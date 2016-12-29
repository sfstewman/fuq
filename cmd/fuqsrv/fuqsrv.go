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
	"net"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

/*
import (
	"net/http"
	_ "net/http/pprof"
)
*/

type RefreshCookieAction struct{}

type WaitAction struct {
	Interval time.Duration
}

const (
	MaxConfigRetries = 10

	HelloTries      = 10
	HelloBackoff    = 500 * time.Millisecond
	HelloMaxBackoff = 1 * time.Minute

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

type StopAction struct {
	All bool
}

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

type WorkerConfig struct {
	mu       sync.RWMutex
	Cookie   fuq.Cookie
	NodeInfo fuq.NodeInfo
	allStop  bool
}

func NewWorkerConfig(nodeName string, nproc int, tags []string) *WorkerConfig {
	return &WorkerConfig{
		NodeInfo: fuq.NodeInfo{
			Node:    nodeName,
			NumProc: nproc,
			Tags:    tags,
		},
	}
}

func (wc *WorkerConfig) IsAllStop() bool {
	wc.mu.RLock()
	defer wc.mu.RUnlock()
	return wc.allStop
}

func (wc *WorkerConfig) AllStop() {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	wc.allStop = true
}

func (wc *WorkerConfig) GetCookie() fuq.Cookie {
	wc.mu.RLock()
	defer wc.mu.RUnlock()

	return wc.Cookie
}

func (wc *WorkerConfig) NewCookie(ep *fuq.Endpoint) error {
	wc.mu.Lock()
	defer wc.mu.Unlock()

	hello := fuq.Hello{
		Auth:     ep.Config.Auth,
		NodeInfo: wc.NodeInfo,
	}

	ret := srv.HelloResponseEnv{
		Name:   &wc.NodeInfo.UniqName,
		Cookie: &wc.Cookie,
	}

	log.Print("Calling HELLO endpoint")

	if err := ep.CallEndpoint("hello", &hello, &ret); err != nil {
		return err
	}

	log.Printf("name is %s.  cookie is %s\n", wc.NodeInfo.UniqName, wc.Cookie)

	return nil
}

func (wc *WorkerConfig) NewCookieWithRetries(ep *fuq.Endpoint, maxtries int) error {
	var err error
	backoff := HelloBackoff

	for ntries := 0; ntries < maxtries; ntries++ {
		err = wc.NewCookie(ep)
		if err == nil {
			return nil
		}
		// log.Printf("connection error: %#v", err)

		netErr, ok := err.(net.Error)
		if !ok {
			log.Printf("other error: %v", err)
			return err
		}

		switch e := netErr.(type) {
		case *url.Error:
			log.Printf("error in dialing: %v", e)
		case *net.OpError:
			log.Printf("op error: %v (temporary? %v)", e, e.Temporary())
		case *net.AddrError:
			log.Printf("addr error: %v (temporary? %v)", e, e.Temporary())
		default:
			log.Printf("other net error: %v (temporary? %v)", e, e.Temporary())
		}

		log.Printf("waiting %.2f seconds before retrying", backoff.Seconds())
		time.Sleep(backoff)
		backoff *= 2
		if backoff > HelloMaxBackoff {
			backoff = HelloMaxBackoff
		}
	}

	return err
}

func (wc *WorkerConfig) RefreshCookie(ep *fuq.Endpoint, oldCookie fuq.Cookie) error {
	wc.mu.Lock()
	defer wc.mu.Unlock()

	if wc.Cookie != oldCookie {
		return nil
	}

	hello := fuq.Hello{
		Auth:     ep.Config.Auth,
		NodeInfo: wc.NodeInfo,
	}

	req := srv.NodeRequestEnvelope{
		Cookie: wc.Cookie,
		Msg:    &hello,
	}

	name := ""
	ret := srv.HelloResponseEnv{
		Name:   &name,
		Cookie: &wc.Cookie,
	}

	if err := ep.CallEndpoint("node/reauth", &req, &ret); err != nil {
		return err
	}

	if name != wc.NodeInfo.UniqName {
		log.Fatalf("invalid: cookie refresh changed unique name from %s to %s",
			wc.NodeInfo.UniqName, name)
	}

	return nil
}

type Worker struct {
	Seq    int
	Logger *log.Logger
	*fuq.Endpoint
	Name   string
	Config *WorkerConfig
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

/*
func NewWorkerWithNodeInfo(ni fuq.NodeInfo, config fuq.Config) (*Worker, error) {
	endpoint, err := fuq.NewEndpoint(config)
	if err != nil {
		return nil, err
	}

	w := &Worker{
		Endpoint: endpoint,
		Name:     ni.Name,
	}

	return w, nil
}
*/

/*
func NodeHello(name string, c fuq.Config) (fuq.NodeInfo, error) {

}
*/

func (w *Worker) Cookie() fuq.Cookie {
	return w.Config.GetCookie()
}

func (w *Worker) UniqName() string {
	return w.Config.NodeInfo.UniqName
}

func (w *Worker) RefreshCookie(oldCookie fuq.Cookie) error {
	return w.Config.RefreshCookie(w.Endpoint, oldCookie)
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
	cookie := w.Cookie()

	req := srv.NodeRequestEnvelope{
		Cookie: cookie,
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
			err = w.RefreshCookie(cookie)
			if err == nil {
				w.Log("Refreshed cookie")
				req.Cookie = w.Cookie()
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

	// FIXME: this is a hack.
	if ret[0].Task < 0 && ret[0].JobDescription.JobId == 0 && ret[0].JobDescription.Name == "::stop::" {
		w.Log("received node stop request: %v", ret)
		return StopAction{All: true}, nil
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
		Cookie: w.Cookie(),
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
	// sanitize colons by replacing them with underscores
	uniqName := strings.Replace(w.UniqName(), ":", "_", -1)

	log.Printf("HELLO finished.  Unique name is '%s'",
		w.UniqName())

	logPath := filepath.Join(w.Endpoint.Config.LogDir,
		fmt.Sprintf("%s-%d.log", uniqName, w.Seq))

	logFile, err := os.Create(logPath)
	fuq.FatalIfError(err, "error creating worker log '%s'", logPath)

	w.Logger = log.New(logFile, "w:"+uniqName, log.LstdFlags)
	defer w.Logger.SetOutput(os.Stderr)
	defer logFile.Close()

	numWaits := 0
run_loop:
	for !w.Config.IsAllStop() {
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
			if r.All {
				w.Config.AllStop()
			}
			break run_loop

		case RunAction:
			numWaits = 0 // reset wait counter
			if r.LoggingDir == "" {
				r.LoggingDir = w.Endpoint.Config.LogDir
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

func startWorkers(wcfg *WorkerConfig, nproc int, config fuq.Config, wg *sync.WaitGroup) error {
	if nproc <= 0 {
		return nil
	}

	ep, err := fuq.NewEndpoint(config)
	if err != nil {
		return fmt.Errorf("error starting workers: %v", err)
	}

	if err := wcfg.NewCookieWithRetries(ep, HelloTries); err != nil {
		return fmt.Errorf("error obtaining cookies: %v", err)
	}

	for i := 0; i < nproc; i++ {
		log.Printf("Starting worker %d", i+1)
		wg.Add(1)
		go func(seq int) {
			defer wg.Done()

			w := Worker{
				Seq:      seq,
				Endpoint: ep,
				Name:     wcfg.NodeInfo.Node,
				Config:   wcfg,
			}
			defer w.Close()

			w.Loop()
		}(i + 1)
	}

	return nil
}

func main() {
	var (
		err                          error
		isForeman, onlyWriteConfig   bool
		hostname, workerName         string
		srvConfigFile, sysConfigFile string
		retryServerConfig            bool
		workerTag                    string
		initialWait                  int
		config                       fuq.Config
		pv                           *fuq.PathVars
		overwriteConfig              bool
		numCPUs                      int
		u                            *user.User
	)

	// command line options
	isForeman = false
	onlyWriteConfig = false

	// set worker name, defaults to hostname
	hostname, err = os.Hostname()
	if err != nil {
		log.Printf("error retrieving hostname: %v", err)
	}

	pv, err = fuq.SetupPaths()
	if err != nil {
		log.Fatalf("error setting up paths: %v", err)
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
	flag.BoolVar(&onlyWriteConfig, "w", false, "invoke fuq and foreman, write config, and exit")
	flag.IntVar(&numCPUs, "np", 1, "number of concurrent cores")
	flag.StringVar(&workerName, "p", workerName, "worker prefix")
	flag.StringVar(&srvConfigFile, "srv", srvConfigFile, "server configuration file")
	flag.StringVar(&sysConfigFile, "cfg", sysConfigFile, "configuration file")
	flag.BoolVar(&overwriteConfig, "force_cfg", overwriteConfig,
		"overwrite config file (if foreman)")

	flag.IntVar(&initialWait, "wait0", initialWait, "seconds to wait before starting")
	flag.BoolVar(&retryServerConfig, "retry_cfg", retryServerConfig,
		"retries reading the server config on error")

	flag.StringVar(&config.DbPath, "db", "", "path to database")
	flag.StringVar(&config.LogDir, "log", "", "queue logging directory")
	flag.StringVar(&config.ForemanLogFile, "flog", "", "foreman logging file")
	flag.IntVar(&config.Port, "port", 0, "foreman port")

	flag.StringVar(&config.KeyFile, "key", "", "path to TLS key file")
	flag.StringVar(&config.CertFile, "cert", "", "path to TLS cert file")
	flag.StringVar(&config.RootCAFile, "ca", "", "path to TLS root ca file")
	flag.StringVar(&config.CertName, "certname", "", "name in TLS certificate")

	flag.StringVar(&workerTag, "tag", "", "tag for workers")

	flag.Parse()

	if srvConfigFile == "" {
		log.Fatalf("No default or given server config file")
	}

	if sysConfigFile == "" {
		log.Fatalf("No default or given config file")
	}

	if initialWait > 0 {
		log.Printf("sleeping %d seconds before startup", initialWait)
		time.Sleep(time.Duration(initialWait) * time.Second)
	}

	if onlyWriteConfig {
		isForeman = true
	}

	if isForeman {
		if err := config.ReadConfig(sysConfigFile, pv); err != nil {
			// igore if the system configuration file does not exist
			if !os.IsNotExist(err) {
				log.Fatalf("error reading config file '%s': %v",
					sysConfigFile, err)
			}
		}

		if err := config.GenerateConfigFile(srvConfigFile, pv, overwriteConfig); err != nil {
			log.Fatalf("error generating config file '%s': %v",
				srvConfigFile, err)
		}

		if onlyWriteConfig {
			os.Exit(0)
		}
	} else {
		config0 := config

		for retries := 0; true; retries++ {
			err := config.ReadConfig(srvConfigFile, pv)
			if err == nil {
				break
			}
			log.Printf("error reading config file '%s': %v",
				srvConfigFile, err)

			if !retryServerConfig || retries >= MaxConfigRetries {
				os.Exit(1)
			}

			time.Sleep(2 * time.Second) // XXX: remove hard-coded value

			// reset in case we had a bad read
			config = config0
		}

		if err := config.ReadConfig(sysConfigFile, pv); err != nil {
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

	wg := sync.WaitGroup{}

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

		// decrement so we only start N-1 workers
		nproc--
	}

	var tags []string
	if workerTag != "" {
		tags = []string{workerTag}
	}

	wc := NewWorkerConfig(workerName, nproc, tags)
	if err := startWorkers(wc, nproc, config, &wg); err != nil {
		log.Fatalf("error starting workers: %v", err)
	}

	if !isForeman {
		/* exit when the last worker is done.  this goroutine
		 * waits on wg and then issues a done signal
		 */
		go func() {
			defer close(done)
			wg.Wait()
			log.Println("Last worker is done, finishing up")
		}()
	}

	/*
		if isForeman {
			go func() {
				log.Println(http.ListenAndServe("localhost:6060", nil))
			}()
		}
	*/

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
