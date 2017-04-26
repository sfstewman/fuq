package websocket

import (
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/sfstewman/fuq/proto"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestAutodialReceive(t *testing.T) {
	// Test that the autodialer:
	//
	// 1. automatically connects on a Receive() call
	//
	// 2. starts a heartbeat <-- TODO
	//
	// 3. Signals that a connection has been made (via the
	//    OnConnect closure)
	//
	// 4. If the connection closes, reconnects and signals
	//    via the closure
	//
	// 5.
	//

	// response tells the server whether to send a message over the
	// websocket or to close it:
	//   * if response is closed, then the server will close
	//   * if an (empty) message is sent on response, then the
	//     server will send a message
	//
	response := make(chan struct{})

	// channel to signal that the client has connected to the server
	// if this channel is closed, the server does not allow further
	// websocket connections
	serverConnect := make(chan struct{})

	// channel to signal that the OnConnect closure
	// has been called
	connected := make(chan struct{})

	// result channel
	type result struct {
		msg proto.Message
		err error
	}
	resultCh := make(chan result)
	var ret result

	// counts the retries after the connection has been closed...
	var numRetries int32

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := <-serverConnect; !ok {
			atomic.AddInt32(&numRetries, 1)
			http.Error(w, "Finished", http.StatusUnauthorized)
			return
		}

		// Wait for a second signal.  This allows the test
		// goroutine to guarantee things happen after the
		// connect but before the response
		<-serverConnect

		upgrader := websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("error upgrading connection: %v", err)
			return
		}
		defer conn.Close()

		// m := Messenger{C: conn, Timeout: 1 * time.Millisecond}
		m := Messenger{C: conn}

		// avoids race where we close(result) to tell the
		// currently running server to exit and then reassign
		// result for the next server
		rch := response
		for {
			if _, ok := <-rch; !ok {
				return
			}

			if err := m.Send(proto.OkayMessage(1, 1, 0)); err != nil {
				panic(err)
			}
		}
	}))
	defer s.Close()

	ad := Autodial{
		Timeout: 5 * time.Millisecond,

		MaxConnAttempts: 5,
		MinWait:         1 * time.Millisecond,
		MaxWait:         1 * time.Millisecond,

		URL:    s.URL,
		HBName: "test",
		OnConnect: OnConnectFunc(func(dialer *Autodial, m *Messenger) {
			// signal that OnConnect has been called
			close(connected)
		}),
	}

	go func() {
		// try to receive to indicate that the connection is closed
		msg, err := ad.Receive()
		resultCh <- result{msg, err}
	}()

	// make sure the client has connected...
	serverConnect <- struct{}{}
	serverConnect <- struct{}{}

	// make sure OnConnect has been called
	<-connected

	// send a response
	response <- struct{}{}
	ret = <-resultCh
	if ret.err != nil {
		t.Fatalf("error in Receive(): %v", ret.err)
	}

	// recreate the connected channel
	connected = make(chan struct{})
	go func() {
		// try to receive to indicate that the connection is closed
		msg, err := ad.Receive()
		resultCh <- result{msg, err}
	}()

	// close the connection
	close(response)

	// the autodialer should retry... check for a connection
	serverConnect <- struct{}{}
	response = make(chan struct{}) // recreate between serverConnect sends
	serverConnect <- struct{}{}

	// make sure OnConnect has been called
	<-connected

	response <- struct{}{}
	ret = <-resultCh
	if ret.err != nil {
		t.Fatalf("error in Receive(): %v", ret.err)
	}

	// tell the server to refuse further websocket connections
	close(response)
	close(serverConnect)

	// receive should retry ad.MaxConnAttempts times and sould return
	// and error
	_, err := ad.Receive()
	if err == nil {
		t.Fatal("autodialer should error")
	}

	if nr := atomic.LoadInt32(&numRetries); int(nr) != ad.MaxConnAttempts {
		t.Fatalf("expected %d retries, found %d", ad.MaxConnAttempts, nr)
	}
}

func TestAutodialSend(t *testing.T) {
	// Test that the autodialer:
	//
	// 1. automatically connects on a Send() call
	//
	// 2. starts a heartbeat <--- TODO
	//
	// 3. Signals that a connection has been made (via the
	//    OnConnect closure)
	//
	// 4. If the connection closes, reconnects and signals
	//    via the closure
	//

	// response tells the server whether to receive a message over the
	// websocket or to close it:
	//   * if response is closed, then the server will close
	//   * if an (empty) message is sent on response, then the
	//     server will send a message
	//
	response := make(chan struct{})

	// channel to signal that the client has connected to the server
	// if this channel is closed, the server does not allow further
	// websocket connections
	serverConnect := make(chan struct{})

	// chanel to signal that the server has exited
	serverExited := make(chan struct{})

	// channel to signal that the OnConnect closure
	// has been called
	connected := make(chan struct{})

	// result channel
	type result struct {
		msg proto.Message
		err error
	}
	resultCh := make(chan result)
	var ret result

	// counts the retries after the connection has been closed...
	var numRetries int32

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := <-serverConnect; !ok {
			atomic.AddInt32(&numRetries, 1)
			http.Error(w, "Finished", http.StatusUnauthorized)
			return
		}

		// Wait for a second signal.  This allows the test
		// goroutine to guarantee things happen after the
		// connect but before the response
		<-serverConnect

		upgrader := websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("error upgrading connection: %v", err)
			return
		}

		defer func() {
			err := conn.Close()
			close(serverExited)
			if err != nil {
				fmt.Printf("error closing: %v", err)
				panic(err)
			}
		}()

		// m := Messenger{C: conn, Timeout: 1 * time.Millisecond}
		m := Messenger{C: conn}

		// avoids race where we close(result) to tell the
		// currently running server to exit and then reassign
		// result for the next server
		rch := response
		for {
			if _, ok := <-rch; !ok {
				return
			}

			_, err := m.Receive()
			if err != nil {
				panic(err)
			}
		}
	}))
	defer s.Close()

	var ad Autodial
	ad = Autodial{
		Timeout: 5 * time.Millisecond,

		MaxConnAttempts: 5,
		MinWait:         1 * time.Millisecond,
		MaxWait:         1 * time.Millisecond,

		URL:    s.URL,
		HBName: "test",
		OnConnect: OnConnectFunc(func(dialer *Autodial, m *Messenger) {
			// signal that OnConnect has been called
			started := make(chan struct{})
			go func() {
				c := m.C

				close(started)
				// dummy Receive call to handle
				// close,ping,pong events
				for {
					if _, _, err := c.NextReader(); err != nil {
						c.Close()
						break
					}
				}
				// websocket events

			}()
			<-started // wait for receive loop to start

			close(connected)
		}),
	}

	go func() {
		// try to receive to indicate that the connection is closed
		msg := proto.OkayMessage(7, 6, 0)
		err := ad.Send(msg)
		resultCh <- result{msg, err}
	}()

	// make sure the client has connected...
	serverConnect <- struct{}{}
	serverConnect <- struct{}{}

	// make sure OnConnect has been called
	<-connected

	// have server receive the message
	response <- struct{}{}
	ret = <-resultCh
	if ret.err != nil {
		t.Fatalf("error in Send(): %v", ret.err)
	}

	// recreate the connected channel
	connected = make(chan struct{})

	// close server and wait for it to finish
	close(response)
	<-serverExited

	go func() {
		// try to receive to indicate that the connection is closed
		msg := proto.OkayMessage(7, 6, 0)
		err := ad.Send(msg)
		resultCh <- result{msg, err}
	}()

	// the autodialer should retry... check for a connection
	serverConnect <- struct{}{}

	// recreate signaling channels between serverConnect sends
	response = make(chan struct{})
	serverExited = make(chan struct{})

	serverConnect <- struct{}{}

	// make sure OnConnect has been called
	<-connected

	response <- struct{}{}
	ret = <-resultCh
	if ret.err != nil {
		t.Fatalf("error in Receive(): %v", ret.err)
	}

	// tell the server to refuse further websocket connections
	close(response)
	close(serverConnect)
	<-serverExited

	// receive should retry ad.MaxConnAttempts times and sould return
	// and error
	err := ad.Send(proto.OkayMessage(1, 2, 1))
	if err == nil {
		t.Fatal("autodialer should error")
	}

	if nr := atomic.LoadInt32(&numRetries); int(nr) != ad.MaxConnAttempts {
		t.Fatalf("expected %d retries, found %d", ad.MaxConnAttempts, nr)
	}
}
