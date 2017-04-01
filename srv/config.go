package srv

import (
	"github.com/sfstewman/fuq"
	"log"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

const (
	HelloBackoff    = 500 * time.Millisecond
	HelloMaxBackoff = 1 * time.Minute
)

type NodeConfig struct {
	mu       sync.RWMutex
	cookie   fuq.Cookie
	NodeInfo fuq.NodeInfo
	allStop  bool
}

func NewNodeConfig(nproc int, tags []string) (*NodeConfig, error) {
	ni, err := fuq.NewNodeInfo(nproc, tags...)
	if err != nil {
		return nil, err
	}

	return &NodeConfig{NodeInfo: ni}, nil
}

func (wc *NodeConfig) IsStopped() bool {
	wc.mu.RLock()
	defer wc.mu.RUnlock()
	return wc.allStop
}

func (wc *NodeConfig) Stop() {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	wc.allStop = true
}

func (wc *NodeConfig) Cookie() fuq.Cookie {
	wc.mu.RLock()
	defer wc.mu.RUnlock()

	return wc.cookie
}

func (wc *NodeConfig) NewCookie(ep *fuq.Endpoint) error {
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

	resp, err := ep.CallEndpoint("hello", &hello, &ret)
	if err != nil {
		return err
	}

	ep.AddResponseCookies(resp)

	log.Printf("name is %s.  cookie is %s\n", wc.NodeInfo.UniqName, wc.cookie)

	return nil
}

func (wc *NodeConfig) NewCookieWithRetries(ep *fuq.Endpoint, maxtries int) error {
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

func (wc *NodeConfig) RefreshCookie(ep *fuq.Endpoint, oldCookie fuq.Cookie) error {
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

	resp, err := ep.CallEndpoint("node/reauth", &req, &ret)
	if err != nil {
		return err
	}

	if name != wc.NodeInfo.UniqName {
		log.Fatalf("invalid: cookie refresh changed unique name from %s to %s",
			wc.NodeInfo.UniqName, name)
	}

	ep.AddResponseCookies(resp)

	return nil
}
