package main

import (
	"encoding/binary"
	"fmt"
	"github.com/sfstewman/fuq"
	"github.com/sfstewman/fuq/srv"
	"gopkg.in/vmihailenco/msgpack.v2"
	"io"
	"net"
	"net/http"
	"syscall"
	"time"
)

const convoMagic = 0xC047

type MType uint16

const (
	MTypeOK     MType = 0x0001
	MTypeJob          = 0x0002
	MTypeUpdate       = 0x0003
	MTypeStop         = 0x0004
)

const StopImmed = ^uint32(0)

type header struct {
	mtype   MType
	padding uint32
	arg0    uint32
}

type Convo struct {
	NumProc int

	I io.Reader
	O io.Writer
	F http.Flusher
}

func (c *Convo) send(data []byte) error {
write:
	nb, err := c.O.Write(data)
	switch err {
	case nil:
		return nil
	case syscall.EINTR:
		if nb > 0 && nb < len(data) {
			data = data[nb:]
		}
		goto write
	default:
		return fmt.Errorf("error sending OK(%d): %v", nb, err)
	}
}

func (c *Convo) sendData(h header, rest []byte) error {
	var data [12]byte
	binary.LittleEndian.PutUint16(data[0:2], convoMagic)
	binary.LittleEndian.PutUint16(data[2:4], uint16(h.mtype))
	binary.LittleEndian.PutUint32(data[4:8], h.padding)
	binary.LittleEndian.PutUint32(data[8:12], h.arg0)

	if err := c.send(data[:]); err != nil {
		return err
	}

	if len(rest) > 0 {
		if err := c.send(rest); err != nil {
			return err
		}
	}

	c.F.Flush()
	return nil
}

func (c *Convo) SendOK(n uint32) error {
	return c.sendData(header{
		mtype: MTypeOK,
		arg0:  n,
	}, nil)
}

func (c *Convo) SendStop(n uint32) error {
	return c.sendData(header{
		mtype: MTypeStop,
		arg0:  n,
	}, nil)
}

func (c *Convo) SendStopImmed() error {
	return c.sendData(header{
		mtype: MTypeStop,
		arg0:  StopImmed,
	}, nil)
}

func (c *Convo) recvHdr() (header, error) {
	var data [12]byte
	var hdr header

	_, err := io.ReadFull(c.I, data[:])
	if err != nil {
		return hdr, err
	}

	if magic := binary.LittleEndian.Uint16(data[0:2]); magic != convoMagic {
		return hdr, fmt.Errorf("message header has invalid magic 0x%4x (0x%4x expected)",
			magic, convoMagic)
	}

	hdr.mtype = MType(binary.LittleEndian.Uint16(data[2:4]))
	hdr.padding = binary.LittleEndian.Uint32(data[4:8])
	hdr.arg0 = binary.LittleEndian.Uint32(data[8:12])

	return hdr, nil
}

func (c *Convo) ReceiveOK() (uint32, error) {
	h, err := c.recvHdr()
	if err != nil {
		return 0, err
	}

	if h.mtype != MTypeOK {
		return 0, fmt.Errorf("expected OK message, received type 0x%4x", h.mtype)
	}

	return h.arg0, nil
}

func (c *Convo) SendJob(job fuq.Task) error {
	data, err := msgpack.Marshal(&job)
	if err != nil {
		return fmt.Errorf("error encoding job for send: %v", err)
	}

	if len(data) > int(^uint32(0)) {
		return fmt.Errorf("length of message is %d, exceeds 32-bits", len(data))
	}

	h := header{
		mtype: MTypeJob,
		arg0:  uint32(len(data)),
	}

	err = c.sendData(h, data)
	if err != nil {
		return fmt.Errorf("error sending data: %v", err)
	}

	return nil
}

func (c *Convo) ReceiveJob() (fuq.Task, error) {
	var t fuq.Task

	h, err := c.recvHdr()
	if err != nil {
		return t, fmt.Errorf("error receiving job header: %v", err)
	}

	if h.mtype != MTypeJob {
		return t, fmt.Errorf("expected JOB message, received type 0x%4x", h.mtype)
	}

	nb := h.arg0
	data := make([]byte, nb)

	_, err = io.ReadFull(c.I, data)
	if err != nil {
		return t, fmt.Errorf("error reading JOB data: %v", err)
	}

	if err := msgpack.Unmarshal(data, &t); err != nil {
		return t, fmt.Errorf("error unmarshaling JOB data: %v", err)
	}

	return t, nil
}

func (c *Convo) SendUpdate(update fuq.JobStatusUpdate) error {
	data, err := msgpack.Marshal(&update)
	if err != nil {
		return fmt.Errorf("error encoding update for send: %v", err)
	}

	if len(data) > int(^uint32(0)) {
		return fmt.Errorf("length of message is %d, exceeds 32-bits", len(data))
	}

	h := header{
		mtype: MTypeUpdate,
		arg0:  uint32(len(data)),
	}

	err = c.sendData(h, data)
	if err != nil {
		return fmt.Errorf("error sending data: %v", err)
	}

	return nil
}

func (c *Convo) ReceiveUpdate() (fuq.JobStatusUpdate, error) {
	var u fuq.JobStatusUpdate

	h, err := c.recvHdr()
	if err != nil {
		return u, fmt.Errorf("error receiving update header: %v", err)
	}

	if h.mtype != MTypeUpdate {
		return u, fmt.Errorf("expected UPDATE message, received type 0x%4x", h.mtype)
	}

	nb := h.arg0
	data := make([]byte, nb)

	_, err = io.ReadFull(c.I, data)
	if err != nil {
		return u, fmt.Errorf("error reading UPDATE data: %v", err)
	}

	if err := msgpack.Unmarshal(data, &u); err != nil {
		return u, fmt.Errorf("error unmarshaling UPDATE data: %v", err)
	}

	return u, nil
}

func (c *Convo) ReceiveMessage() (MType, interface{}, error) {
	var raw []byte
	var data interface{}

	h, err := c.recvHdr()
	if err != nil {
		return 0, nil, fmt.Errorf("error receiving message header: %v", err)
	}

	switch h.mtype {
	case MTypeOK, MTypeStop:
		data = h.arg0

	case MTypeJob:
		data = &fuq.Task{}
		raw = make([]byte, h.arg0)
	case MTypeUpdate:
		data = &fuq.JobStatusUpdate{}
		raw = make([]byte, h.arg0)
	default:
		return 0, nil, fmt.Errorf("unknown message type 0x%4x", h.mtype)
	}

	if len(raw) > 0 {
		_, err = io.ReadFull(c.I, raw)
		if err != nil {
			return 0, nil, fmt.Errorf("error reading message data: %v", err)
		}

		if err := msgpack.Unmarshal(raw, data); err != nil {
			return 0, nil, fmt.Errorf("error unmarshaling message data: %v", err)
		}
	}

	return h.mtype, data, nil
}

/*
SV_INIT0	-		send OK(0)				SV_INIT1
SV_INIT1	on OK(n)	record nproc=n				SV_INIT2
SV_INIT2	-		send OK(nproc)				SV_LOOP0

SV_LOOP0	-		request npending<=nproc pending tasks	SV_LOOP1
SV_LOOP1	-		if npending > 0, send JOB(npending)	SV_LOOP2
				else					SV_LOOP2

SV_LOOP2	on JOB_QUEUED	-					SV_LOOP0
		on UPDATE(n)	record task status, set nproc=n		SV_LOOP3
		on STOP_REQ(n)	send STOP(n)				SV_LOOP0
		on STOP_IMMED	send STOP(immed)			SV_LOOP0
		on DISCONNECT	-					SV_EXIT

SV_LOOP3	-		send OK(nproc)				SV_LOOP0

SV_EXIT		-		exit server loop
*/

type ServerState int

func (s ServerState) String() string {
	switch s {
	case SvLoop0:
		return "SvLoop0"
	case SvLoop1:
		return "SvLoop1"
	case SvLoop2:
		return "SvLoop2"
	case SvExit:
		return "SvExit"
	default:
		return fmt.Sprintf("SvUnknown_%d", s)
	}
}

const (
	_                   = iota // zero
	SvLoop0 ServerState = iota // 1
	SvLoop1
	SvLoop2
	SvExit
)

type SvEventType uint32

const (
	_                      = iota
	SvEvQueued SvEventType = iota
	SvEvStop
)

type SvEvent struct {
	Type SvEventType
	Arg  uint32
}

type ServerConvo struct {
	Convo
	State       ServerState
	NumProc     uint32
	MaxProc     uint32
	Callback    func(ServerState) error
	Queue       srv.JobQueuer
	ReadTimeout time.Duration
}

func (sv *ServerConvo) SendPendingJobs(n uint32) (uint32, error) {
	tasks, err := sv.Queue.FetchPendingTasks(int(n))
	if err != nil {
		return 0, err
	}

	// FIXME: send pending tasks!

	return uint32(len(tasks)), nil
}

func (sv *ServerConvo) UpdateTaskStatus(update fuq.JobStatusUpdate) error {
	return sv.Queue.UpdateTaskStatus(update)
}

func timeoutError(err error) bool {
	if err == nil {
		return false
	}

	if netErr, ok := err.(net.Error); ok {
		return netErr.Timeout()
	}

	return false
}

type message struct {
	Type  MType
	Data  interface{}
	Error error
}

func (sv *ServerConvo) readLoop(dataCh chan<- message, done <-chan struct{}) {
	// check for read timeout
	netConn, hasTimeout := sv.Convo.I.(net.Conn)
loop:
	for {
		select {
		case <-done:
			break loop
		default:
			if hasTimeout {
				deadline := time.Now().Add(sv.ReadTimeout)
				netConn.SetReadDeadline(deadline)
			}
			mt, data, err := sv.ReceiveMessage()
			if err == io.EOF {
				break loop
			}
			// ignore timeout errors...
			if timeoutError(err) {
				continue loop
			}

			select {
			case <-done:
				break loop
			case dataCh <- message{mt, data, err}:
				// nop
			}
		}
	}

	close(dataCh)
}

func (sv *ServerConvo) Loop(evCh <-chan SvEvent) error {
	var (
		err error
	)

	// NOTE: rethink error handling

	// SvInit0
	if err := sv.SendOK(0); err != nil {
		return fmt.Errorf("error in sending initial OK: %v", err)
	}

	// SvInit1
	np, err := sv.ReceiveOK()
	if err != nil {
		return fmt.Errorf("error receiving OK(n): %v", err)
	}

	sv.NumProc = np
	sv.MaxProc = np

	if err = sv.SendOK(sv.NumProc); err != nil {
		return fmt.Errorf("error echoing OK(n): %v", err)
	}

	done := make(chan struct{})
	msgCh := make(chan message)
	go sv.readLoop(msgCh, done)

	for sv.State != SvExit {
		stNext := sv.State

	event_switch:
		switch sv.State {
		case SvLoop0:
			var nj uint32
			nj, err = sv.SendPendingJobs(sv.NumProc)
			sv.NumProc -= nj
			stNext = SvLoop1

		case SvLoop1:
			// check for events:
			// on JOB_QUEUED	-					SV_LOOP0
			// on UPDATE(n)		record task status, set nproc=n		SV_LOOP2
			// on STOP_REQ(n)	send STOP(n)				SV_LOOP0
			// on STOP_IMMED	send STOP(immed)			SV_LOOP0
			// on DISCONNECT	-					SV_EXIT
			select {
			case msg, closed := <-msgCh:
				if closed { // disconnect
					stNext = SvExit
					break event_switch
				}

				// check message
				if msg.Error != nil {
					err = msg.Error
					break event_switch
				}

				if msg.Type != MTypeUpdate {
					err = fmt.Errorf("invalid message type for state %s: 0x%4x",
						sv.State, msg.Type)
					break event_switch
				}

				taskUpdate := msg.Data.(*fuq.JobStatusUpdate)
				err = sv.UpdateTaskStatus(*taskUpdate)
				stNext = SvLoop2

			case ev, closed := <-evCh:
				if closed { // quit request
					sv.SendStopImmed()
					stNext = SvExit
					break event_switch
				}

				switch ev.Type {
				case SvEvQueued:
					stNext = SvLoop0
				case SvEvStop:
					err = sv.SendStop(ev.Arg)
					stNext = SvLoop0
				default:
					panic("unknown server event type")
				}
			}

		case SvLoop2:
			err = sv.SendOK(sv.NumProc)
			stNext = SvLoop0

		case SvExit:

		default:
			panic("unknown server state")
		}

		if err != nil {
			return fmt.Errorf("error encountered in state %s: %v", sv.State, err)
		}

		if sv.Callback != nil {
			stCurr := sv.State
			if err = sv.Callback(stNext); err != nil {
				return fmt.Errorf("error in callback (%s -> %s): %v",
					stCurr, stNext, err)
			}
		}

		sv.State = stNext
	}

	close(done)

	return nil
}
