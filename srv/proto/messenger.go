package proto

import (
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
