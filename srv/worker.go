package srv

import (
	"fmt"
	"github.com/sfstewman/fuq"
	"log"
	"math/rand"
	"net"
	"net/url"
	"sync"
	"time"
)

const (
	HelloBackoff    = 500 * time.Millisecond
	HelloMaxBackoff = 1 * time.Minute

	DefaultInterval  = 5 * time.Second
	IntervalIncrease = 500 * time.Millisecond
	MaxInterval      = 60 * time.Second

	MaxLogRetry     = 10000
	MaxRefreshTries = 5
)

type WorkerConfig struct {
	mu       sync.RWMutex
	cookie   fuq.Cookie
	NodeInfo fuq.NodeInfo
	allStop  bool
}

func NewWorkerConfig(nproc int, tags []string) (*WorkerConfig, error) {
	ni, err := fuq.NewNodeInfo(nproc, tags...)
	if err != nil {
		return nil, err
	}

	return &WorkerConfig{NodeInfo: ni}, nil
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

func (wc *WorkerConfig) Cookie() fuq.Cookie {
	wc.mu.RLock()
	defer wc.mu.RUnlock()

	return wc.cookie
}

func (wc *WorkerConfig) NewCookie(ep *fuq.Endpoint) error {
	wc.mu.Lock()
	defer wc.mu.Unlock()

	hello := fuq.Hello{
		Auth:     ep.Config.Auth,
		NodeInfo: wc.NodeInfo,
	}

	ret := HelloResponseEnv{
		Name:   &wc.NodeInfo.UniqName,
		Cookie: &wc.cookie,
	}

	log.Print("Calling HELLO endpoint")

	if err := ep.CallEndpoint("hello", &hello, &ret); err != nil {
		return err
	}

	log.Printf("name is %s.  cookie is %s\n", wc.NodeInfo.UniqName, wc.cookie)

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

	if wc.cookie != oldCookie {
		return nil
	}

	hello := fuq.Hello{
		Auth:     ep.Config.Auth,
		NodeInfo: wc.NodeInfo,
	}

	req := NodeRequestEnvelope{
		Cookie: wc.cookie,
		Msg:    &hello,
	}

	name := ""
	ret := HelloResponseEnv{
		Name:   &name,
		Cookie: &wc.cookie,
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

type Queuer interface {
	RequestAction(nproc int) (interface{}, error)
	UpdateAndRequestAction(status fuq.JobStatusUpdate, nproc int) (interface{}, error)
}

type Endpoint struct {
	*fuq.Endpoint
	Config *WorkerConfig
	Logger *log.Logger
}

func (ep *Endpoint) Cookie() fuq.Cookie {
	return ep.Config.Cookie()
}

func (ep *Endpoint) RefreshCookie(oldCookie fuq.Cookie) error {
	return ep.Config.RefreshCookie(ep.Endpoint, oldCookie)
}

/* This shouldn't return interface{}, but I'm not entirely sure what it
* should return
* XXX
 */
func (w *Endpoint) RequestAction(nproc int) (interface{}, error) {
	cookie := w.Cookie()

	req := NodeRequestEnvelope{
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

func (w *Endpoint) UpdateAndRequestAction(status fuq.JobStatusUpdate, nproc int) (interface{}, error) {
	if nproc > 0 {
		status.NewJob = &fuq.JobRequest{NumProc: nproc}
	}

	req := NodeRequestEnvelope{
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

// XXX
func (w *Endpoint) Log(format string, args ...interface{}) {
	if w.Logger != nil {
		w.Logger.Printf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

type Worker struct {
	Seq           int
	Logger        *log.Logger
	Name          string
	Config        *WorkerConfig
	Queuer        Queuer
	DefaultLogDir string
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

func (w *Worker) Loop() {
	numWaits := 0

run_loop:
	for !w.Config.IsAllStop() {
		// request a job from the foreman
		req, err := w.Queuer.RequestAction(1)
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
				r.LoggingDir = w.DefaultLogDir
			}

			status, err := r.Run(w.Logger)
			w.LogIfError(err, "error encountered while running job")

			if status.Success {
				w.Log("job %d:%d completed successfully", status.JobId, status.Task)
			} else {
				w.Log("job %d:%d encountered error: %s",
					status.JobId, status.Task, status.Status)
			}

			req, err = w.Queuer.UpdateAndRequestAction(status, 1)
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
