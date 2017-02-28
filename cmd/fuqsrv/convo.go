package main

import (
	"encoding/binary"
	"fmt"
	"github.com/sfstewman/fuq"
	"gopkg.in/vmihailenco/msgpack.v2"
	"io"
	"net/http"
	"sync"
	"syscall"
	// "log"
)

const convoMagic = 0xC047

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

type sender interface {
	io.Writer
	WriteHeader(h header) error
	Send(h header, rest []byte) error
}

type receiver interface {
	io.Reader
	ReadHeader() (header, error)
}

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

type message struct {
	Type MType
	Seq  uint32
	Data interface{}
}

func (m message) GoString() string {
	return fmt.Sprintf("%s[%v]", m.Type, m.Data)
}

func (m message) IsStopImmed() bool {
	if m.Type != MTypeStop {
		return false
	}

	return m.Data.(uint32) == StopImmed
}

func (m message) okayData() (nproc, nrun uint16, err error) {
	if m.Type != MTypeOK {
		err = fmt.Errorf("error decoding message, type is %s, not %s",
			m.Type, MTypeOK)
		return
	}

	arg0, ok := m.Data.(uint32)
	if !ok {
		err = fmt.Errorf("error decoding message, data is %T, not uint32",
			m.Data)
		return
	}

	nproc, nrun = arg0Okay(arg0)
	return
}

func okayMessage(nproc, nrun uint16, seq uint32) message {
	return message{
		Type: MTypeOK,
		Seq:  seq,
		Data: okayArg0(nproc, nrun),
	}
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
		return m.send(w, seq, nil)
	}

	encoded, err := msgpack.Marshal(data)
	if err != nil {
		return fmt.Errorf("error encoding data for type %d: %v", m, err)
	}

	return m.send(w, seq, encoded)
}

func (m MType) send(w io.Writer, seq uint32, data []byte) error {
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

type ConvoFlag uint32

const (
	CFNone   ConvoFlag = 0
	CFWorker ConvoFlag = 1 << iota
)

type HelloData struct {
	NumProcs int
	Running  []fuq.Task
}

type Sequencer interface {
	Next() uint32
	Current() uint32
	Update(n uint32)
}

type LockedSequence struct {
	mu  sync.Mutex
	seq uint32
}

func (s *LockedSequence) Lock() {
	s.mu.Lock()
}

func (s *LockedSequence) Unlock() {
	s.mu.Unlock()
}

func (s *LockedSequence) Next() uint32 {
	s.Lock()
	defer s.Unlock()
	seq := s.seq
	s.seq++
	return seq
}

func (s *LockedSequence) Current() uint32 {
	s.Lock()
	defer s.Unlock()
	return s.seq
}

func (s *LockedSequence) Update(n uint32) {
	s.Lock()
	defer s.Unlock()
	if n > s.seq {
		s.seq = n
	}
}

/*
func (c *Convo) nextSeq() uint32 {
	s := (atomic.AddUint32(&c.sequence, 1) - 1) << 1
	if !c.IsWorker() {
		s = s | 0x1
	}

	return s
}

func (c Convo) updateSeq(new uint32) {
	new = new >> 1
	sold := uint32(0)
	for {
		s := atomic.LoadUint32(&c.sequence)
		if sold > s {
			panic("sequence is not monotonically increasing")
		}

		if new < s || atomic.CompareAndSwapUint32(&c.sequence, s, new+1) {
			return
		}

		sold = s
	}
}

func (c Convo) Sequence() uint32 {
	return atomic.LoadUint32(&c.sequence)
}
*/

func okayArg0(nproc, nrun uint16) uint32 {
	return uint32(nproc)<<16 | uint32(nrun)
}

func arg0Okay(arg0 uint32) (nproc, nrun uint16) {
	nproc = uint16(arg0 >> 16)
	nrun = uint16(arg0 & 0xffff)
	return
}

func okayHeader(nproc, nrun uint16, seq uint32) header {
	return header{
		mtype: MTypeOK,
		seq:   seq,
		arg0:  okayArg0(nproc, nrun),
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

func recvHdr(r io.Reader) (header, error) {
	var data [16]byte
	var hdr header

	_, err := io.ReadFull(r, data[:])
	if err != nil {
		return hdr, err
	}

	err = hdr.Decode(data[:])
	return hdr, err
}

func writeAll(w io.Writer, data []byte) (n int, err error) {
	var nb int

write:
	nb, err = w.Write(data[n:])
	n += nb

	if err == syscall.EINTR {
		goto write
	}

	return
}

type writerSend struct {
	mu sync.Mutex
	w  io.Writer
	f  http.Flusher
}

func newWriterSend(w io.Writer, f http.Flusher) *writerSend {
	return &writerSend{w: w, f: f}
}

func (ws *writerSend) Write(data []byte) (n int, err error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if n, err = writeAll(ws.w, data); err != nil {
		return
	}

	ws.f.Flush()
	return
}

func (ws *writerSend) WriteHeader(h header) error {
	return h.Encode(ws.w)
}

func (ws *writerSend) Send(h header, rest []byte) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if err := h.Encode(ws.w); err != nil {
		return err
	}

	if len(rest) > 0 {
		if _, err := writeAll(ws.w, rest); err != nil {
			return err
		}
	}

	ws.f.Flush()
	return nil
}

type readerRecv struct {
	r io.Reader
}

func (r readerRecv) Read(buf []byte) (int, error) {
	// return io.ReadFull(r.r, buf)
	return r.r.Read(buf)
}

func (r readerRecv) ReadHeader() (header, error) {
	return recvHdr(r.r)
}

type Convo struct {
	// NumProc      int
	Flags ConvoFlag
	s     sender
	r     receiver
	// I io.Reader
	// O io.Writer
	// F http.Flusher

	Seq Sequencer

	sendLock sync.Mutex
}

func (c Convo) IsWorker() bool {
	return c.Flags&CFWorker == CFWorker
}

func (c Convo) nextSeq() uint32 {
	s := c.Seq.Next() << 1
	if !c.IsWorker() {
		s = s | 0x1
	}
	return s
}

func (c Convo) Sequence() uint32 {
	return c.Seq.Current()
}

// Called by ReceiveMessage() to update the sequence number
// when a new message has been received
func (c Convo) updateSeq(new uint32) {
	new = new >> 1
	c.Seq.Update(new + 1)
}

func (c *Convo) sendData(h header, rest []byte) error {
	return c.s.Send(h, rest)
}

func (c *Convo) recvHdr() (header, error) {
	return c.r.ReadHeader()
}

const maxDataSize = (1 << 32) - 1

func (c *Convo) sendDataAsMessage(mtype MType, data []byte) (uint32, error) {
	if len(data) > maxDataSize {
		return 0, fmt.Errorf("length of message is %d, exceeds 32-bits", len(data))
	}

	seq := c.nextSeq()
	h := header{
		mtype: mtype,
		seq:   seq,
		arg0:  uint32(len(data)),
	}

	if err := c.sendData(h, data); err != nil {
		return 0, err
	}

	return seq, nil
}

func (c *Convo) SendHello(hello HelloData) (seq uint32, err error) {
	seq = c.nextSeq()
	err = MTypeHello.encode(c.s, seq, &hello)
	return
}

func (c *Convo) ReplyOK(nproc, nrun uint16, seq uint32) error {
	h := okayHeader(nproc, nrun, seq)
	return h.Encode(c.s)
}

func (c *Convo) SendOK(nproc uint16, nrun uint16) (seq uint32, err error) {
	seq, err = c.SendShortMessage(MTypeOK, okayArg0(nproc, nrun))
	return
}

func (c *Convo) SendStop(n uint32) (seq uint32, err error) {
	seq, err = c.SendShortMessage(MTypeStop, n)
	return
}

func (c *Convo) SendStopImmed() error {
	_, err := c.SendShortMessage(MTypeStop, StopImmed)
	return err
}

func (c *Convo) ReceiveOK(seq uint32) (nproc, nrun uint16, err error) {
	var h header
	if h, err = c.recvHdr(); err != nil {
		return
	}

	// even if there's an error, we need to update the sequence number
	seq = h.seq
	c.updateSeq(seq)

	switch {
	case h.mtype != MTypeOK:
		err = fmt.Errorf("expected OK message, received type 0x%4x", h.mtype)

	case h.seq != seq:
		err = fmt.Errorf("expected sequence %d, found %d", seq, h.seq)

	default:
		nproc, nrun = arg0Okay(h.arg0)
	}

	return
}

func (c *Convo) SendShortMessage(mt MType, arg0 uint32) (seq uint32, err error) {
	seq = c.nextSeq()
	err = mt.encodeShort(c.s, seq, arg0)
	return
}

func (c *Convo) SendDataMessage(mt MType, data interface{}) (seq uint32, err error) {
	seq = c.nextSeq()
	err = mt.encode(c.s, seq, data)
	return
}

func (c *Convo) SendJob(job []fuq.Task) (seq uint32, err error) {
	seq, err = c.SendDataMessage(MTypeJob, &job)
	return
}

func (c *Convo) SendUpdate(update fuq.JobStatusUpdate) (seq uint32, err error) {
	seq, err = c.SendDataMessage(MTypeUpdate, &update)
	return
}

func (c *Convo) SendMessage(m message) error {
	switch mt := m.Type; mt {
	case MTypeOK, MTypeStop, MTypeReset:
		arg0 := m.Data.(uint32)
		return mt.encodeShort(c.s, m.Seq, arg0)

	case MTypeError:
		ed := m.Data.(mtErrorData)
		return mt.encodeError(c.s, m.Seq, ed.Errcode, ed.Arg0)

	case MTypeHello, MTypeJob, MTypeUpdate:
		return mt.encode(c.s, m.Seq, m.Data)

	default:
		return fmt.Errorf("unknown message type 0x%4x (%s)", mt)
	}
}

func (c *Convo) ReceiveMessage() (message, error) {
	var raw []byte
	var m message

	h, err := c.recvHdr()
	if err != nil {
		return m, fmt.Errorf("error receiving message header: %v", err)
	}

	// log.Printf("==> INCOMING: msg %d: %v", h.seq, h)

	// even if there's an error, we need to update the sequence number
	c.updateSeq(h.seq)

	switch h.mtype {
	case MTypeOK, MTypeStop, MTypeError, MTypeReset:
		m.Data = h.arg0

	case MTypeHello:
		m.Data = &HelloData{}
		raw = make([]byte, h.arg0)
	case MTypeJob:
		m.Data = &[]fuq.Task{}
		raw = make([]byte, h.arg0)
	case MTypeUpdate:
		m.Data = &fuq.JobStatusUpdate{}
		raw = make([]byte, h.arg0)
	default:
		return m, fmt.Errorf("unknown message type 0x%4x", h.mtype)
	}

	m.Type = h.mtype
	m.Seq = h.seq

	if len(raw) > 0 {
		/*
			decoder := msgpack.NewDecoder(c.r)
			if err := decoder.Decode(m.Data); err != nil {
				return m, fmt.Errorf("error decoding message data (type %v): %v", h.mtype, err)
			}
		*/

		if _, err := c.r.Read(raw); err != nil {
			return m, fmt.Errorf("error reading message data: %v", err)
		}

		if err := msgpack.Unmarshal(raw, m.Data); err != nil {
			return m, fmt.Errorf("error unmarshaling message data: %v", err)
		}
	}

	return m, nil
}
