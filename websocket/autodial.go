package websocket

import (
	"context"
	"crypto/tls"
	"errors"
	"github.com/gorilla/websocket"
	"github.com/sfstewman/fuq/proto"
	"log"
	"net/http"
	"sync"
	"time"
)

type ConnectObserver interface {
	Connected(*Autodial, *Messenger)
}

type OnConnectFunc func(*Autodial, *Messenger)

func (f OnConnectFunc) Connected(ad *Autodial, m *Messenger) {
	f(ad, m)
}

type Autodial struct {
	mu sync.Mutex

	*Messenger
	Timeout time.Duration

	URL       string
	Jar       http.CookieJar
	TLSConfig *tls.Config

	MaxConnAttempts int
	MaxSendAttempts int
	MinWait         time.Duration
	MaxWait         time.Duration

	HBName    string
	OnConnect ConnectObserver

	hbCancel func()
}

const (
	DefaultConnAttempts = 100
	DefaultSendAttempts = 5

	DefaultMinWait = 1 * time.Second
	DefaultMaxWait = 5 * time.Minute
)

var ErrMaxAttempts = errors.New("too many failed attempts to connect")

func (ad *Autodial) tryConnect() (*Messenger, error) {
	ad.mu.Lock()
	defer ad.mu.Unlock()

	var (
		m    *Messenger
		resp *http.Response
		err  error
	)

	maxConnAttempts := ad.MaxConnAttempts
	if maxConnAttempts == 0 {
		maxConnAttempts = DefaultConnAttempts
	}

	wait := ad.MinWait
	if wait == 0 {
		wait = DefaultMinWait
	}

	maxWait := ad.MinWait
	if maxWait == 0 {
		maxWait = DefaultMaxWait
	}

	m = ad.Messenger

	for nattempts := 0; nattempts < maxConnAttempts; nattempts++ {
		if m != nil && !m.IsClosed() {
			log.Printf("websocket.Autodial(%p): m=%p, m.IsClosed() = %v",
				ad, m, m.IsClosed())
			return m, nil
		}

		if ad.hbCancel != nil {
			ad.hbCancel()
		}

		log.Printf("websocket.Autodial(%p): dialing...", ad)
		m, resp, err = DialWithTLS(ad.URL, ad.Jar, ad.TLSConfig)
		if err == nil {
			log.Printf("websocket.Autodial(%p): success", ad)
			m.Timeout = ad.Timeout

			ad.Messenger = m
			resp.Body.Close()

			hbCtx, hbCancel := context.WithCancel(context.TODO())
			go m.Heartbeat(hbCtx, "w2f_"+ad.HBName)
			ad.hbCancel = hbCancel

			if ad.OnConnect != nil {
				ad.OnConnect.Connected(ad, m)
			}

			return m, nil
		}

		log.Printf("websocket.Autodial(%p): dial failed, error is: %v",
			ad, err)
		if resp != nil {
			// check if response can be handled or not...
			log.Printf("websocket.Autodial(%p): dial failed, response is %d: %s",
				ad, resp.StatusCode, resp.Status)
		} else {
			log.Printf("websocket.Autodial(%p): dial failed, no response", ad)
		}

		// retry if we haven't given up
		log.Printf("websocket.Autodial(%p): waiting %8f seconds",
			ad, int(wait.Seconds()))
		<-time.After(wait)

		if wait < maxWait {
			wait *= 2
			if wait > maxWait {
				wait = maxWait
			}
		}

	}

	return nil, ErrMaxAttempts
}

func (ad *Autodial) Send(msg proto.Message) error {
	maxSendAttempts := ad.MaxSendAttempts
	if maxSendAttempts == 0 {
		maxSendAttempts = DefaultSendAttempts
	}

	for nattempts := 0; nattempts < maxSendAttempts; nattempts++ {
		m, err := ad.tryConnect()
		if err != nil {
			// XXX - this or proto.ErrClosed ?
			return err
		}

		err = m.Send(msg)
		log.Printf("websocket.Autodial(%p) send: err=%v", ad, err)

		switch {
		case err == nil:
			return nil

		case err == websocket.ErrCloseSent:
			continue

		case err == proto.ErrClosed:
			continue

		default:
			// socket is in a corrupted state, reconnect
			log.Printf("autodialer reconnecting after error: %v", err)
			m.CloseNow()

			continue
		}
	}

	return ErrMaxAttempts
}

func (ad *Autodial) Receive() (proto.Message, error) {
	maxSendAttempts := ad.MaxSendAttempts
	if maxSendAttempts == 0 {
		maxSendAttempts = DefaultSendAttempts
	}

	for nattempts := 0; nattempts < maxSendAttempts; nattempts++ {
		m, err := ad.tryConnect()
		if err != nil {
			// XXX - this or proto.ErrClosed ?
			return proto.Message{}, err
		}

		log.Printf("websocket.Autodial(%p): m=%p, calling Receive()...", ad, m)

		msg, err := m.Receive()
		log.Printf("websocket.Autodial(%p): received msg=%#v, err=%#v",
			ad, msg, err)

		switch {
		case err == nil:
			return msg, nil

		case err == websocket.ErrCloseSent:
			continue

		case err == proto.ErrClosed:
			continue

		default:
			return msg, err
		}
	}

	return proto.Message{}, ErrMaxAttempts
}

func (ad *Autodial) Close() error {
	return ad.Messenger.Close()
}
