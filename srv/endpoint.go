package srv

import (
	"context"
	"log"

	"github.com/sfstewman/fuq"
	"github.com/sfstewman/fuq/node"
)

const (
	MaxRefreshTries = 5
)

type Endpoint struct {
	*fuq.Endpoint
	Config *NodeConfig
	Logger *log.Logger
}

func (ep *Endpoint) Cookie() fuq.Cookie {
	return ep.Config.Cookie()
}

func (ep *Endpoint) RefreshCookie(oldCookie fuq.Cookie) error {
	return ep.Config.RefreshCookie(ep.Endpoint, oldCookie)
}

func (w *Endpoint) RequestAction(ctx context.Context, nproc int) (node.WorkerAction, error) {
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
	_, err := w.CallEndpoint("job/request", &req, &ret)
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
		return node.WaitAction{}, nil
	}

	// FIXME: this is a hack.
	if ret[0].Task < 0 && ret[0].JobDescription.JobId == 0 && ret[0].JobDescription.Name == "::stop::" {
		w.Log("received node stop request: %v", ret)
		return node.StopAction{All: true}, nil
	}

	w.Log("received job request: %v", ret)

	act := node.RunAction(ret[0])
	return act, nil
}

func (w *Endpoint) UpdateAndRequestAction(ctx context.Context, status fuq.JobStatusUpdate, nproc int) (node.WorkerAction, error) {
	if nproc > 0 {
		status.NewJob = &fuq.JobRequest{NumProc: nproc}
	}

	req := NodeRequestEnvelope{
		Cookie: w.Cookie(),
		Msg:    &status,
	}

	ret := []fuq.Task{}

	if _, err := w.CallEndpoint("job/status", &req, &ret); err != nil {
		return nil, err
	}

	if len(ret) > 1 {
		panic("more than one task not yet supported")
	}

	if len(ret) == 0 {
		return node.WaitAction{}, nil
	}

	w.Log("received job request: %v", ret)

	act := node.RunAction(ret[0])
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
