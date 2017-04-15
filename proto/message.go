package proto

import (
	"encoding/binary"
	"fmt"
	"github.com/sfstewman/fuq"
	"gopkg.in/vmihailenco/msgpack.v2"
	"io"
)

const convoMagic = 0xC047

type HelloData struct {
	NumProcs int
	Running  []fuq.Task
}

type Message struct {
	Type MType
	Seq  uint32
	Data interface{}

	after func()
}

// Sets the after function on a message.  If set, the after function is
// called by the connection after the message is sent.
func (m *Message) After(fn func()) {
	m.after = fn
}

func (m Message) DoAfter() {
	if m.after != nil {
		m.after()
	}
}

func (m Message) GoString() string {
	return fmt.Sprintf("%s[%v]", m.Type, m.Data)
}

func (m Message) IsStopImmed() bool {
	if m.Type != MTypeStop {
		return false
	}

	return m.Data.(uint32) == StopImmed
}

func (m Message) AsStop() uint32 {
	if m.Type != MTypeStop {
		panic("error decoding: message is not STOP")
	}

	return m.Data.(uint32)
}

func (m Message) AsOkay() (nproc, nrun uint16) {
	if m.Type != MTypeOK {
		panic("error decoding: message is not OK")
	}

	arg0 := m.Data.(uint32)
	nproc, nrun = U32ToNProcs(arg0)
	return
}

func (m Message) AsError() (MError, uint32) {
	if m.Type != MTypeError {
		panic("error decoding: message is not an error")
	}

	errData := m.Data.(mtErrorData)
	return errData.Errcode, errData.Arg0
}

// func arg0Okay(arg0 uint32) (nproc, nrun uint16) {
func NProcsToU32(nproc, nrun uint16) uint32 {
	return uint32(nproc)<<16 | uint32(nrun)
}

// func okayArg0(nproc, nrun uint16) uint32 {
func U32ToNProcs(arg0 uint32) (nproc, nrun uint16) {
	nproc = uint16(arg0 >> 16)
	nrun = uint16(arg0 & 0xffff)
	return
}

func OkayMessage(nproc, nrun uint16, seq uint32) Message {
	return Message{
		Type: MTypeOK,
		Seq:  seq,
		Data: NProcsToU32(nproc, nrun),
	}
}

func ErrorMessage(errcode MError, arg0 uint32, seq uint32) Message {
	return Message{
		Type: MTypeError,
		Seq:  seq,
		Data: mtErrorData{errcode, arg0},
	}
}

func okayHeader(nproc, nrun uint16, seq uint32) header {
	return header{
		mtype: MTypeOK,
		seq:   seq,
		arg0:  NProcsToU32(nproc, nrun),
	}
}

func stopHeader(n uint32, seq uint32) header {
	return header{
		mtype: MTypeStop,
		seq:   seq,
		arg0:  n,
	}
}

func (h header) Encode(w io.Writer) error {
	var buf [16]byte
	binary.LittleEndian.PutUint16(buf[0:2], convoMagic)
	buf[2] = uint8(h.mtype)
	buf[3] = uint8(h.errcode)
	binary.LittleEndian.PutUint32(buf[4:8], h.seq)
	binary.LittleEndian.PutUint32(buf[8:12], h.padding)
	binary.LittleEndian.PutUint32(buf[12:16], h.arg0)
	_, err := writeAll(w, buf[:])
	return err
}

func (h *header) Decode(buf []byte) error {
	if magic := binary.LittleEndian.Uint16(buf[0:2]); magic != convoMagic {
		return fmt.Errorf("message header has invalid magic 0x%4x (0x%4x expected)",
			magic, convoMagic)
	}

	h.mtype = MType(buf[2])
	h.errcode = MError(buf[3])
	h.seq = binary.LittleEndian.Uint32(buf[4:8])
	h.padding = binary.LittleEndian.Uint32(buf[8:12])
	h.arg0 = binary.LittleEndian.Uint32(buf[12:16])

	return nil
}

func receiveHeader(r io.Reader) (header, error) {
	var data [16]byte
	var hdr header

	_, err := io.ReadFull(r, data[:])
	if err != nil {
		return hdr, err
	}

	err = hdr.Decode(data[:])
	return hdr, err
}

func (m Message) Send(w io.Writer) error {
	switch mt := m.Type; mt {
	case MTypeOK, MTypeStop, MTypeReset:
		arg0 := m.Data.(uint32)
		return m.encodeShort(w, arg0)

	case MTypeError:
		errData := m.Data.(mtErrorData)
		return m.encodeError(w, errData)

	case MTypeHello, MTypeJob, MTypeUpdate, MTypeCancel:
		return m.encodeData(w)

	default:
		return fmt.Errorf("unknown message type 0x%02x (%s)", byte(mt), mt)
	}
}

func ReceiveMessage(r io.Reader) (Message, error) {
	var raw []byte
	var m Message

	h, err := receiveHeader(r)
	if err != nil {
		return m, fmt.Errorf("error receiving message header: %v", err)
	}

	switch h.mtype {
	case MTypeOK, MTypeStop, MTypeError, MTypeReset:
		m.Data = h.arg0

	case MTypeHello:
		m.Data = &HelloData{}
		raw = make([]byte, h.arg0)
	case MTypeJob:
		m.Data = &[]fuq.Task{}
		raw = make([]byte, h.arg0)
	case MTypeCancel:
		m.Data = &[]fuq.TaskPair{}
		raw = make([]byte, h.arg0)
	case MTypeUpdate:
		m.Data = &fuq.JobStatusUpdate{}
		raw = make([]byte, h.arg0)
	default:
		return m, fmt.Errorf("unknown message type 0x%02x", h.mtype)
	}

	m.Type = h.mtype
	m.Seq = h.seq

	if len(raw) == 0 {
		return m, nil
	}

	if _, err := io.ReadFull(r, raw); err != nil {
		return m, fmt.Errorf("error reading message data: %v", err)
	}

	if err := msgpack.Unmarshal(raw, m.Data); err != nil {
		return m, fmt.Errorf("error unmarshaling message data: %v", err)
	}

	// In the cases above, we had to pass a pointer to unmarshal the
	// data correctly.  We could just return the message with Data
	// as the pointer type, but this creates a lot of unnecessary
	// pointer dereferencing for the client.
	//
	// Instead, we dereference the pointers here, setting the data
	// fields to the dereferenced values.
	switch h.mtype {
	case MTypeHello:
		hd := m.Data.(*HelloData)
		if hd == nil {
			m.Data = nil
			return m, fmt.Errorf("HELLO message lacks data")
		}
		m.Data = *hd
	case MTypeJob:
		tl := m.Data.(*[]fuq.Task)
		if tl == nil {
			m.Data = nil
			return m, fmt.Errorf("JOB message lacks data")
		}
		m.Data = *tl
	case MTypeCancel:
		tp := m.Data.(*[]fuq.TaskPair)
		if tp == nil {
			m.Data = nil
			return m, fmt.Errorf("CANCEL message lacks data")
		}
		m.Data = *tp
	case MTypeUpdate:
		upd := m.Data.(*fuq.JobStatusUpdate)
		if upd == nil {
			m.Data = nil
			return m, fmt.Errorf("UPDATE message lacks data")
		}
		m.Data = *upd
	}

	return m, nil
}

func (m Message) encodeShort(w io.Writer, arg0 uint32) error {
	h := header{
		mtype: m.Type,
		seq:   m.Seq,
		arg0:  arg0,
	}

	// log.Printf("%s.encodeShort(%v, %d, %d)", m, w, seq, arg0)
	return h.Encode(w)
}

func (m Message) encodeError(w io.Writer, errData mtErrorData) error {
	h := header{
		mtype:   m.Type,
		errcode: errData.Errcode,
		seq:     m.Seq,
		arg0:    errData.Arg0,
	}
	return h.Encode(w)
}

func (m Message) encodeData(w io.Writer) error {
	var encoded []byte

	if m.Data != nil {
		var err error
		encoded, err = msgpack.Marshal(m.Data)
		if err != nil {
			return fmt.Errorf("error encoding data for type %s: %v", m.Type, err)
		}
	}

	return m.rawSend(w, encoded)
}

func (m Message) rawSend(w io.Writer, data []byte) error {
	if len(data) > maxDataSize {
		return fmt.Errorf("length of message is %d, exceeds 32-bits", len(data))
	}

	h := header{
		mtype: m.Type,
		seq:   m.Seq,
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
