package srv

import (
	"github.com/sfstewman/fuq"
	"log"
)

const (
	MaxRefreshTries = 5
)

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
