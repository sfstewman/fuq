package main

import (
	"encoding/binary"
	"fmt"
	"github.com/sfstewman/fuq"
	"gopkg.in/vmihailenco/msgpack.v2"
	"io"
	"net/http"
	"syscall"
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
type JobStatusUpdate struct {
	JobId   JobId       `json:"job_id"`
	Task    int         `json:"task"`
	Success bool        `json:"success"`
	Status  string      `json:"status"`
	NewJob  *JobRequest `json:"newjob"`
}
*/

/// func (c *Convo) Send
