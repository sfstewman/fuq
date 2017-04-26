package websocket

import (
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/sfstewman/fuq/proto"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sync"
	"testing"
	"time"
)

type serverJob int

const (
	doClose serverJob = iota
	doSend
	doReceive
)

type result struct {
	msg proto.Message
	err error
}

func TestMessengerSendReceive(t *testing.T) {
	// Tests that the messenger:
	//
	// 1. Connects
	//
	// 2. Can receive/send messages
	//
	// 3. Indicates that one side has closed

	// jobCh tells the server what to do: send/receive/close
	// note that closing jobCh will result in the server
	// closing the connection (zero value is doClose)
	jobCh := make(chan serverJob)

	// channel to signal that the client has connected to the server
	// if this channel is closed, the server does not allow further
	// websocket connections
	serverConnect := make(chan struct{})

	// channel to signal that a client goroutine has started
	clientSignal := make(chan struct{})

	// result channels
	clientResult := make(chan result)
	serverResult := make(chan result)
	var sr, cr result

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		m, err := Upgrade(w, r)
		if err != nil {
			// log the error but ignore it... the client
			// should pick the error up
			t.Logf("error upgrading connection: %v", err)
			return
		}
		defer m.Close()

		select {
		case <-serverConnect:
		case <-ctx.Done():
			return
		}

		defer close(serverResult)

		for {
			select {
			case <-ctx.Done():
				return
			case what := <-jobCh:
				switch what {
				case doClose:
					return
				case doSend:
					msg := proto.OkayMessage(1, 1, 0)
					err := m.Send(msg)
					serverResult <- result{msg, err}
				case doReceive:
					msg, err := m.Receive()
					serverResult <- result{msg, err}
				}
			}
		}
	}))
	defer s.Close()

	// 1. Test that client can connect
	client, _, err := Dial(s.URL, nil)
	if err != nil {
		t.Fatalf("error connecting: %v", err)
	}
	defer client.CloseNow()

	// make sure the server has connected...
	serverConnect <- struct{}{}

	// 2a. Test that the client can send and the server receive
	go func() {
		clientSignal <- struct{}{}

		// try to receive to indicate that the connection is closed
		msg := proto.OkayMessage(3, 1, 1)
		err := client.Send(msg)
		clientResult <- result{msg, err}
	}()
	<-clientSignal

	jobCh <- doReceive

	cr = <-clientResult
	t.Logf("client result: %#v", cr)
	if cr.err != nil {
		t.Fatalf("error sending message: %v", cr.err)
	}

	sr = <-serverResult
	t.Logf("server result: %#v", sr)
	if sr.err != nil {
		t.Fatalf("error receiving message: %v", sr.err)
	}
	if expected := proto.OkayMessage(3, 1, 1); !reflect.DeepEqual(sr.msg, expected) {
		t.Errorf("expected to receive '%v', but received '%v'",
			expected, sr.msg)
	}

	// 2b. Test that the server can send and the client receive
	go func() {
		clientSignal <- struct{}{}

		// try to receive to indicate that the connection is closed
		msg, err := client.Receive()
		clientResult <- result{msg, err}
	}()
	<-clientSignal

	jobCh <- doSend

	sr = <-serverResult
	t.Logf("server result: %#v", sr)
	if sr.err != nil {
		t.Fatalf("error receiving message: %v", sr.err)
	}

	cr = <-clientResult
	t.Logf("client result: %#v", cr)
	if cr.err != nil {
		t.Fatalf("error sending message: %v", cr.err)
	}

	if expected := proto.OkayMessage(1, 1, 0); !reflect.DeepEqual(cr.msg, expected) {
		t.Errorf("expected to receive '%v', but received '%v'",
			expected, cr.msg)
	}

	// 3. Test that a close is detected
	go func() {
		clientSignal <- struct{}{}

		// try to receive to indicate that the connection is closed
		msg, err := client.Receive()
		clientResult <- result{msg, err}
	}()
	<-clientSignal

	jobCh <- doClose

	if _, ok := <-serverResult; ok {
		t.Error("expected server result channel to be closed")
	}

	cr = <-clientResult
	t.Logf("client result: %#v", cr)
	if cr.err == nil {
		t.Fatal("expected client result to have an error")
	}
	if !client.IsClosed() {
		t.Fatal("expected client.IsClosed() to be true")
	}
}

type laggedConn struct {
	net.Conn
	mu  sync.Mutex
	lag time.Duration
}

func (lt *laggedConn) SetLag(dt time.Duration) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.lag = dt
}

func (lt *laggedConn) Lag() time.Duration {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	return lt.lag
}

func (lt *laggedConn) Write(b []byte) (n int, err error) {
	lag := lt.Lag()
	if lag > 0 {
		time.Sleep(lag)
	}
	return lt.Conn.Write(b)
}

func laggedDial(rawurl string) (*Messenger, error) {
	dialer := websocket.Dialer{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		NetDial: func(netw, addr string) (net.Conn, error) {
			c, e := net.Dial(netw, addr)
			if e != nil {
				return c, e
			}
			return &laggedConn{Conn: c}, nil
		},
	}

	url, err := url.Parse(rawurl)
	if err != nil {
		return nil, fmt.Errorf("error parsing url %s: %v",
			rawurl, err)
	}

	switch url.Scheme {
	case "http", "ws":
		url.Scheme = "ws"
	case "https", "wss":
		url.Scheme = "wss"
	default:
		return nil, fmt.Errorf("unknown url scheme: %s", url.Scheme)
	}

	wsConn, _, err := dialer.Dial(url.String(), nil)
	if err != nil {
		return nil, err
	}

	return &Messenger{C: wsConn}, nil
}

func TestMessengerSendTimeout(t *testing.T) {
	doneSignal := make(chan struct{})
	defer close(doneSignal)

	// Tests that Messengers correctly handle send timeouts
	//

	// channel to signal that the client has connected to the server
	// if this channel is closed, the server does not allow further
	// websocket connections
	serverConnect := make(chan struct{})

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		upgrader := websocket.Upgrader{
			ReadBufferSize:  0,
			WriteBufferSize: 0,
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			// log the error but ignore it... the client
			// should pick the error up
			t.Logf("error upgrading connection: %v", err)
			return
		}
		m := &Messenger{C: conn}
		defer m.Close()

		select {
		case <-serverConnect:
		case <-ctx.Done():
			return
		}

		for {
			if ctx.Err() != nil {
				return
			}

			msg, err := m.Receive()
			t.Logf("received: %v , %v", msg, err)
			if err != nil {
				return
			}
		}

	}))
	defer s.Close()

	client, err := laggedDial(s.URL)
	if err != nil {
		t.Fatalf("error connecting: %v", err)
	}
	defer client.CloseNow()

	// start read loop so we can process close events
	go func() {
		conn := client.C
		for {
			if _, _, err := conn.NextReader(); err != nil {
				conn.Close()
				break
			}
		}
	}()

	// make sure the server has connected...
	serverConnect <- struct{}{}

	// get the lagged connection we use to test timeouts
	laggedConn := client.C.UnderlyingConn().(*laggedConn)
	defer laggedConn.SetLag(0)

	// 1a. Test that setting client.Timeout = 0 won't
	//     set a timeout
	client.Timeout = time.Duration(0)
	laggedConn.SetLag(1 * time.Millisecond) // no timeout

	err = client.Send(proto.OkayMessage(3, 1, 1))
	if err != nil {
		t.Fatalf("error sending message: %v", err)
	}

	// 1b. Test that the client can send messages if the lag is
	//     somewhat less than the timeout
	client.Timeout = 2 * time.Millisecond
	laggedConn.SetLag(1 * time.Millisecond) // lag < timeout

	err = client.Send(proto.OkayMessage(3, 1, 1))
	if err != nil {
		t.Fatalf("error sending message: %v", err)
	}

	// 1c. Test that longer lags will cause a timeout
	client.Timeout = 2 * time.Millisecond
	laggedConn.SetLag(5 * time.Millisecond) // lag > timeout

	err = client.Send(proto.OkayMessage(3, 1, 1))
	if err == nil {
		t.Fatal("expected error message")
	}

	// XXX - check that error is a timeout!
	netErr, ok := err.(net.Error)
	if !ok || !netErr.Timeout() {
		t.Fatal("expected error to be a net.Error with a timeout error")
	}

	client.CloseNow()
}
