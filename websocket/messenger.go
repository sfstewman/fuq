package websocket

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/sfstewman/fuq/proto"
	"log"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultTimeout = 5 * time.Minute
	MinHeartbeat   = 50 * time.Millisecond
	MaxHeartbeat   = 60 * time.Minute
)

type hasLock struct{ locked bool }

type Messenger struct {
	C       *websocket.Conn
	Timeout time.Duration
	closed  struct {
		sync.Mutex
		err error
	}
}

func Upgrade(resp http.ResponseWriter, req *http.Request) (*Messenger, error) {
	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}

	conn, err := upgrader.Upgrade(resp, req, nil)
	if err != nil {
		return nil, err
	}

	return &Messenger{C: conn}, nil
}

func Dial(rawurl string, jar http.CookieJar) (*Messenger, *http.Response, error) {
	return DialWithTLS(rawurl, jar, nil)
}

func DialWithTLS(rawurl string, jar http.CookieJar, tlsCfg *tls.Config) (*Messenger, *http.Response, error) {
	dialer := websocket.Dialer{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		Jar:             jar,
		TLSClientConfig: tlsCfg,
	}

	url, err := url.Parse(rawurl)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing url %s: %v",
			rawurl, err)
	}

	switch url.Scheme {
	case "http", "ws":
		url.Scheme = "ws"
	case "https", "wss":
		url.Scheme = "wss"
	default:
		return nil, nil, fmt.Errorf("unknown url scheme: %s", url.Scheme)
	}

	wsConn, resp, err := dialer.Dial(url.String(), nil)
	if err != nil {
		return nil, resp, err
	}

	return &Messenger{C: wsConn}, resp, nil
}

func Connect(conn net.Conn) (*Messenger, error) {
	dialer := websocket.Dialer{
		NetDial: func(network, addr string) (net.Conn, error) {
			return conn, nil
		},
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}

	ws, _, err := dialer.Dial("ws://localhost", nil)
	if err != nil {
		return nil, err
	}

	return &Messenger{C: ws}, nil
}

func IsCloseError(err error) bool {
	_, ok := err.(*websocket.CloseError)
	return ok
}

func NewMessenger(conn *websocket.Conn, timeout time.Duration) *Messenger {
	messenger := &Messenger{
		C:       conn,
		Timeout: timeout,
	}

	conn.SetCloseHandler(messenger.closeHandler)
	return messenger
}

func (ws *Messenger) setClosed(lock bool, err error) {
	if lock {
		ws.closed.Lock()
		defer ws.closed.Unlock()
	}
	ws.closed.err = err
}

func (ws *Messenger) closeHandler(code int, text string) error {
	closedErr := &websocket.CloseError{code, text}
	ws.setClosed(true, closedErr)
	return nil
}

func (ws *Messenger) IsClosed() bool {
	ws.closed.Lock()
	defer ws.closed.Unlock()

	return ws.closed.err != nil
}

func (ws *Messenger) Dial() error {
	// XXX - do something useful here
	return nil
}

func (ws *Messenger) doclose() error {
	if ws.closed.err != nil {
		return nil
	}

	defer ws.C.Close()

	code := websocket.CloseGoingAway
	text := "Closing now"

	ws.closed.err = &websocket.CloseError{code, text}
	data := websocket.FormatCloseMessage(code, text)

	return ws.C.WriteMessage(websocket.CloseMessage, data)
}

func (ws *Messenger) CloseNow() error {
	ws.closed.Lock()
	defer ws.closed.Unlock()

	log.Printf("websocket.Messenger(%p): CloseNow()", ws)

	return ws.doclose()
}

func (ws *Messenger) CloseWithMessage(code int, text string) error {
	ws.closed.Lock()
	defer ws.closed.Unlock()

	if ws.closed.err != nil {
		return nil
	}

	log.Printf("websocket.Messenger(%p): CloseWithMessage(code=%d,text=\"%s\")",
		ws, code, text)

	data := websocket.FormatCloseMessage(code, text)
	err := ws.C.WriteMessage(websocket.CloseMessage, data)
	if err != nil {
		return err
	}

	ws.closed.err = &websocket.CloseError{code, text}
	ws.C.Close()
	log.Printf("websocket.Messenger(%p): connection is closed", ws)

	return nil
}

func (ws *Messenger) Close() error {
	return ws.CloseWithMessage(websocket.CloseNormalClosure, "closing")
}

func (ws *Messenger) sendPing(ctx context.Context, tag string, count uint) error {
	ws.closed.Lock()
	defer ws.closed.Unlock()

	msg := []byte(fmt.Sprintf("PING:%s:%d", tag, count))
	return ws.C.WriteMessage(websocket.PingMessage, msg)
}

func (ws *Messenger) Heartbeat(ctx context.Context, tag string) error {
	var (
		pingCount uint32
		pongCount uint32
	)

	log.Printf("websocket.Messenger(%p): starting heartbeat",
		ws)

	timeout := ws.Timeout
	hbInterval := timeout / 5

	if hbInterval < MinHeartbeat {
		hbInterval = MinHeartbeat
	}

	if hbInterval > MaxHeartbeat {
		hbInterval = MaxHeartbeat
	}

	ws.C.SetPongHandler(func(data string) error {
		count := atomic.AddUint32(&pongCount, 1)
		log.Printf("PONG: %s %d", data, count)

		return nil
	})

	ticker := time.NewTicker(hbInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("websocket.Messenger(%p): heartbeat canceled",
				ws)
			return nil
		case <-ticker.C:
			count := atomic.AddUint32(&pingCount, 1)
			check := atomic.LoadUint32(&pongCount)

			// handle overflow
			delta := count - check
			if check > count {
				tmp := ^(count ^ count)
				delta = count + (tmp - check) + 1
			}

			if delta > 1 {
				log.Printf("Mismatch in PING/PONG count: %d, %d.  Closing.",
					count, check)
				ws.CloseNow()
				return fmt.Errorf("mismatch in ping/pong count")
			}

			if err := ws.sendPing(ctx, tag, uint(count)); err != nil {
				return err
			}
			log.Printf("PING %d", count)
		}
	}
}

func (ws *Messenger) Send(msg proto.Message) (err error) {
	ws.closed.Lock()
	defer ws.closed.Unlock()

	dt := ws.Timeout
	if dt > 0 {
		t := time.Now()
		ws.C.SetWriteDeadline(t.Add(dt))
	} else {
		ws.C.SetWriteDeadline(time.Time{})
	}

	log.Printf("websocket.Messenger(%p): Send(%v) with timeout %f seconds",
		ws, msg, dt.Seconds())

	wr, err := ws.C.NextWriter(websocket.BinaryMessage)
	if err != nil {
		log.Printf("websocket.Messenger(%p): send error is %v", ws, err)
		if err == websocket.ErrCloseSent {
			return proto.ErrClosed
		}

		if closedErr, ok := err.(*websocket.CloseError); ok {
			ws.setClosed(false, closedErr)
			return proto.ErrClosed
		}

		ws.setClosed(false, err)

		log.Printf("error (%T) sending message: %v", err, err)
		return err
	}

	defer func() {
		errClose := wr.Close()
		if err == nil {
			err = errClose
			log.Printf("error closing Send() writer: %v", err)
		}
	}()

	if err := msg.Send(wr); err != nil {
		log.Printf("websocket.Messenger(%p): send error is %v", ws, err)
		if err == websocket.ErrCloseSent {
			return proto.ErrClosed
		}

		fmt.Printf("websocket.Messenger(%p): error sending message: %v",
			ws, err)

		return err
	}

	log.Printf("websocket.Messenger(%p): send is okay", ws)
	return nil
}

func (ws *Messenger) Receive() (proto.Message, error) {
	// zero value: no read deadlines
	ws.C.SetReadDeadline(time.Time{})

	mt, r, err := ws.C.NextReader()
	if err != nil {
		// log.Printf("websocket.Messenger(%p): error = %#v", ws, err)
		if closedErr, ok := err.(*websocket.CloseError); ok {
			// log.Printf("websocket.Messenger(%p): closed", ws)
			ws.setClosed(true, closedErr)
			return proto.Message{}, proto.ErrClosed
		}
		return proto.Message{}, err
	}

	if mt != websocket.BinaryMessage {
		return proto.Message{}, fmt.Errorf("invalid websocket message type: %d", mt)
	}

	return proto.ReceiveMessage(r)
}
