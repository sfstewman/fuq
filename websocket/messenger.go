package websocket

import (
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/sfstewman/fuq/proto"
	"time"
)

type Messenger struct {
	C       *websocket.Conn
	Timeout time.Duration
}

func (ws Messenger) Dial() error {
	// XXX - do something useful here
	return nil
}

func (ws Messenger) Close() error {
	return ws.C.Close()
}

func (ws Messenger) Send(msg proto.Message) error {
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

func (ws Messenger) Receive() (proto.Message, error) {
	dt := ws.Timeout
	t := time.Now()

	ws.C.SetReadDeadline(t.Add(dt))

	mt, r, err := ws.C.NextReader()
	if err != nil {
		return proto.Message{}, err
	}

	if mt != websocket.BinaryMessage {
		return proto.Message{}, fmt.Errorf("invalid websocket message type: %d", mt)
	}

	return proto.ReceiveMessage(r)
}
