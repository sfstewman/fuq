package proto

import (
	"fmt"
	"github.com/gorilla/websocket"
	"net"
	"net/http"
	"time"
)

type Messenger interface {
	Dial() error
	Close() error

	Send(m Message) error
	Receive() (Message, error)
}

type ConnMessenger struct {
	Conn    net.Conn
	Flusher http.Flusher
	Timeout time.Duration
}

func (m ConnMessenger) Send(msg Message) error {
	dt := m.Timeout
	conn := m.Conn

	t := time.Now()
	conn.SetWriteDeadline(t.Add(dt))

	defer m.Flusher.Flush()
	if err := msg.Send(conn); err != nil {
		return err
	}

	return nil
}

func (m ConnMessenger) Receive() (Message, error) {
	return ReceiveMessage(m.Conn)
}

func (m ConnMessenger) Dial() error {
	// XXX - do something useful here
	return nil
}

func (m ConnMessenger) Close() error {
	return m.Conn.Close()
}

type WebsocketMessenger struct {
	C       *websocket.Conn
	Timeout time.Duration
}

func (ws WebsocketMessenger) Dial() error {
	// XXX - do something useful here
	return nil
}

func (ws WebsocketMessenger) Close() error {
	return ws.C.Close()
}

func (ws WebsocketMessenger) Send(msg Message) error {
	dt := ws.Timeout
	t := time.Now()

	ws.C.SetWriteDeadline(t.Add(dt))

	wr, err := ws.C.NextWriter(websocket.BinaryMessage)
	if err != nil {
		return err
	}
	defer wr.Close()

	return msg.Send(wr)
}

func (ws WebsocketMessenger) Receive() (Message, error) {
	dt := ws.Timeout
	t := time.Now()

	ws.C.SetReadDeadline(t.Add(dt))

	mt, r, err := ws.C.NextReader()
	if err != nil {
		return Message{}, err
	}

	if mt != websocket.BinaryMessage {
		return Message{}, fmt.Errorf("invalid websocket message type: %d", mt)
	}

	return ReceiveMessage(r)
}
