package proto

import (
	"fmt"
	"gopkg.in/vmihailenco/msgpack.v2"
	"io"
)

type MType uint8

const (
	_             = iota // skip 0
	MTypeOK MType = iota
	MTypeHello
	MTypeJob
	MTypeUpdate
	MTypeStop
	MTypeError
	MTypeReset
)

const maxDataSize = (1 << 32) - 1

func (mt MType) String() string {
	switch mt {
	case MTypeOK:
		return "MType_OK"
	case MTypeHello:
		return "MType_Hello"
	case MTypeJob:
		return "MType_Job"
	case MTypeUpdate:
		return "MType_Update"
	case MTypeStop:
		return "MType_Stop"
	case MTypeError:
		return "MType_Error"
	case MTypeReset:
		return "MType_Reset"
	default:
		return fmt.Sprintf("MType(0x%2X)", uint8(mt))
	}
}

type MError uint8

const (
	MErrOK MError = iota
	MErrOverflow
	MErrNoProcs
)

const StopImmed = ^uint32(0)

type header struct {
	mtype   MType
	errcode MError
	seq     uint32
	padding uint32
	arg0    uint32
}

type mtErrorData struct {
	Errcode MError
	Arg0    uint32
}

func (m MType) encodeShort(w io.Writer, seq, arg0 uint32) error {
	h := header{
		mtype: m,
		seq:   seq,
		arg0:  arg0,
	}

	// log.Printf("%s.encodeShort(%v, %d, %d)", m, w, seq, arg0)
	return h.Encode(w)
}

func (m MType) encodeError(w io.Writer, seq uint32, errcode MError, arg0 uint32) error {
	h := header{
		mtype:   m,
		errcode: errcode,
		seq:     seq,
		arg0:    arg0,
	}
	return h.Encode(w)
}

func (m MType) encode(w io.Writer, seq uint32, data interface{}) error {
	if data == nil {
		return m.rawSend(w, seq, nil)
	}

	encoded, err := msgpack.Marshal(data)
	if err != nil {
		return fmt.Errorf("error encoding data for type %d: %v", m, err)
	}

	return m.rawSend(w, seq, encoded)
}

func (m MType) rawSend(w io.Writer, seq uint32, data []byte) error {
	if len(data) > maxDataSize {
		return fmt.Errorf("length of message is %d, exceeds 32-bits", len(data))
	}

	h := header{
		mtype: m,
		seq:   seq,
		arg0:  uint32(len(data)),
	}

	if err := h.Encode(w); err != nil {
		return err
	}

	if data == nil {
		return nil
	}

	_, err := writeAll(w, data)
	return err
}

func (mt MType) Send(w io.Writer, seq uint32, data interface{}) error {
	switch mt {
	case MTypeOK, MTypeStop, MTypeReset:
		arg0 := data.(uint32)
		return mt.encodeShort(w, seq, arg0)

	case MTypeError:
		errData := data.(mtErrorData)
		return mt.encodeError(w, seq, errData.Errcode, errData.Arg0)

	case MTypeHello, MTypeJob, MTypeUpdate:
		return mt.encode(w, seq, data)

	default:
		return fmt.Errorf("unknown message type 0x%4x (%s)", mt)
	}
}
