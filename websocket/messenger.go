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
		err *websocket.CloseError
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

func (ws *Messenger) closeHandler(code int, text string) error {
	ws.closed.Lock()
	defer ws.closed.Unlock()
	ws.closed.err = &websocket.CloseError{code, text}
	return nil
}

func (ws *Messenger) closedInfo(l hasLock) (bool, int, string) {
	if !l.locked {
		ws.closed.Lock()
		defer ws.closed.Unlock()
	}

	if ws.closed.err == nil {
		return false, 0, ""
	}

	return true, ws.closed.err.Code, ws.closed.err.Text
}

func (ws *Messenger) IsClosed() bool {
	closed, _, _ := ws.closedInfo(hasLock{false})
	return closed
}

func (ws *Messenger) ClosedInfo() (bool, int, string) {
	return ws.closedInfo(hasLock{false})
}

func (ws *Messenger) Dial() error {
	// XXX - do something useful here
	return nil
}

func (ws *Messenger) doclose() error {
	if ws.closed.err != nil {
		return nil
	}

	err := ws.C.Close()
	if err != nil {
		return err
	}

	pingTest := []byte("ping test")
	err = ws.C.WriteMessage(websocket.PingMessage, pingTest)
	if err == nil {
		panic("no error when writing after close")
	}

	closeErr, ok := err.(*websocket.CloseError)
	if !ok {
		panic(fmt.Sprintf("error is not a close error: %#v", err))
	}

	ws.closed.err = closeErr

	return nil
}

func (ws *Messenger) CloseNow() error {
	ws.closed.Lock()
	defer ws.closed.Unlock()

	return ws.doclose()
}

func (ws *Messenger) CloseWithMessage(code int, text string) error {
	ws.closed.Lock()
	defer ws.closed.Unlock()

	data := websocket.FormatCloseMessage(code, text)
	err := ws.C.WriteMessage(websocket.CloseMessage, data)
	if err != nil {
		return err
	}

	ws.closed.err = &websocket.CloseError{code, text}
	ws.C.Close()

	return nil
}

func (ws *Messenger) Close() error {
	return ws.CloseWithMessage(websocket.CloseNormalClosure, "closing")
}

func (ws *Messenger) sendPing(ctx context.Context, count uint) error {
	ws.closed.Lock()
	defer ws.closed.Unlock()

	msg := []byte(fmt.Sprintf("PING_%d", count))
	return ws.C.WriteMessage(websocket.PingMessage, msg)
}

func (ws *Messenger) Heartbeat(ctx context.Context) error {
	var pingCount uint

	timeout := ws.Timeout
	hbInterval := timeout / 5

	if hbInterval < MinHeartbeat {
		hbInterval = MinHeartbeat
	}

	if hbInterval > MaxHeartbeat {
		hbInterval = MaxHeartbeat
	}

	ws.C.SetPongHandler(func(data string) error {
		log.Printf("PONG: %s", data)
		return nil
	})

	ticker := time.NewTicker(hbInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			pingCount++
			if err := ws.sendPing(ctx, pingCount); err != nil {
				return err
			}
			log.Printf("PING %d", pingCount)
		}
	}
}

func (ws *Messenger) Send(msg proto.Message) error {
	ws.closed.Lock()
	defer ws.closed.Unlock()

	dt := ws.Timeout
	t := time.Now()

	ws.C.SetWriteDeadline(t.Add(dt))

	wr, err := ws.C.NextWriter(websocket.BinaryMessage)
	if err != nil {
		if _, ok := err.(*websocket.CloseError); ok {
			return proto.ErrClosed
		}
		return err
	}
	defer wr.Close()

	return msg.Send(wr)
}

func (ws *Messenger) Receive() (proto.Message, error) {
	// zero value: no read deadlines
	ws.C.SetReadDeadline(time.Time{})

	mt, r, err := ws.C.NextReader()
	if err != nil {
		if _, ok := err.(*websocket.CloseError); ok {
			return proto.Message{}, proto.ErrClosed
		}
		return proto.Message{}, err
	}

	if mt != websocket.BinaryMessage {
		return proto.Message{}, fmt.Errorf("invalid websocket message type: %d", mt)
	}

	return proto.ReceiveMessage(r)
}
