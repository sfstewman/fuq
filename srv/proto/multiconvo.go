package proto

import (
	"context"
	"fmt"
	"github.com/sfstewman/fuq"
	"log"
	"net"
	"net/http"
	"time"
)

type MessageHandler interface {
	Handle(msg message) (response message)
}

type MessageHandlerFunc func(message) message

func (h MessageHandlerFunc) Handle(msg message) message {
	return h(msg)
}

type reply struct {
	M message
	E error
}

type sendData struct {
	MType MType
	Data  interface{}
	Reply chan<- reply
}

type MultiConvo struct {
	conn     net.Conn
	flusher  http.Flusher
	seq      Sequencer
	isClient bool

	ctx context.Context

	handlers [256]MessageHandler

	sendCh chan sendData
	/*
		c        *Convo
		Done     chan struct{}

		sendLock sync.Mutex

		pending struct {
			sync.Mutex
			isWaiting bool
			waitingOn uint32
			waitingCh chan<- message
		}
	*/

	Timeout time.Duration
}

type MultiConvoOpts struct {
	Conn    net.Conn
	Flusher http.Flusher
	Context context.Context
	Client  bool
}

func NewMultiConvo(opts MultiConvoOpts) *MultiConvo {
	/*
		ws := newWriterSend(conn, fl)
		rr := readerRecv{conn}
		c := &Convo{
			r:   rr,
			s:   ws,
			Seq: seq,
		}

		return &MultiConvo{
			conn:    conn,
			c:       c,
			Done:    make(chan struct{}),
			Timeout: 2 * time.Second,
		}
	*/

	return &MultiConvo{
		conn:     opts.Conn,
		flusher:  opts.Flusher,
		seq:      &LockedSequence{},
		isClient: opts.Client,
		ctx:      opts.Context,
		sendCh:   make(chan sendData),
	}
}

func (mc *MultiConvo) nextSeq() uint32 {
	s := mc.seq.Next() << 1
	if !mc.isClient {
		s = s | 0x1
	}
	return s
}

func (mc *MultiConvo) Sequence() uint32 {
	return mc.seq.Current()
}

// Called by ReceiveMessage() to update the sequence number
// when a new message has been received
func (mc *MultiConvo) updateSeq(new uint32) {
	new = new >> 1
	mc.seq.Update(new + 1)
}

func (mc *MultiConvo) sendDataMessage(mt MType, data interface{}) (uint32, error) {
	seq := mc.nextSeq()
	err := mt.encode(mc.conn, seq, data)
	return seq, err
}

func (mc *MultiConvo) sendShortMessage(mt MType, arg0 uint32) (uint32, error) {
	seq := mc.nextSeq()
	err := mt.encodeShort(mc.conn, seq, arg0)
	return seq, err
}

func (mc *MultiConvo) xmit(mt MType, seq uint32, data interface{}) error {
	dt := mc.Timeout
	conn := mc.conn

	t := time.Now()
	conn.SetWriteDeadline(t.Add(dt))

	err := mt.Send(conn, seq, data)
	if err != nil {
		return err
	}

	mc.flusher.Flush()
	return nil
}

func (mc *MultiConvo) xmitMessage(data sendData) (seq uint32, err error) {
	seq = mc.nextSeq()
	err = mc.xmit(data.MType, seq, data.Data)
	return
}

func (mc *MultiConvo) Close() {
}

func (mc *MultiConvo) OnMessage(mt MType, h MessageHandler) {
	mc.handlers[uint8(mt)] = h
}

func (mc *MultiConvo) OnMessageFunc(mt MType, f MessageHandlerFunc) {
	mc.OnMessage(mt, f)
}

func (mc *MultiConvo) incomingLoop(msgCh chan<- message, errorCh chan<- reply, done <-chan struct{}) {
	dt := mc.Timeout
	conn := mc.conn

	defer close(msgCh)
	defer func() {
		if r := recover(); r != nil {
			m := reply{E: fmt.Errorf("recovered panic %v", r)}
			select {
			case errorCh <- m:
			default:
				panic(r)
			}
		}
	}()
	// defer log.Printf("--[ mc(%p) incoming loop stopping ]--", mc)

	for {
		done := mc.ctx.Done()

		// check done channel before we receive a message
		select {
		case <-done:
			return
		default:
		}

		t := time.Now()
		conn.SetReadDeadline(t.Add(dt))

		msg, err := ReceiveMessage(conn)

		// even if there's an error, we need to update the sequence number
		mc.updateSeq(msg.Seq)

		if err != nil {
			// check done channel before setting error
			select {
			case <-done:
			case errorCh <- reply{msg, err}:
			default:
				panic(err) // XXX - is there a better way?
			}
			return
		}

		select {
		case msgCh <- msg:
			/* nop */
		case <-done:
			return
		}
	}
}

func (mc *MultiConvo) dispatchMessage(m message) error {
	t := m.Type
	h := mc.handlers[uint8(t)]
	if h == nil {
		h = mc.handlers[0]
	}

	if t != MTypeOK && h == nil {
		return fmt.Errorf("no handler for message %v", m)
	}

	// log.Printf("%p --> MD: dispatching handler for message %d", mc, m.Seq)
	seq := m.Seq

	if h == nil {
		// XXX - error response on nil
		// handler?
		// log.Printf("%p --> MD: no handlers for message %d", mc, m.Seq)
		return nil
	}

	resp := h.Handle(m)

	if m.Type == MTypeOK {
		// log.Printf("%p --> MD: OK message, no response.", mc)
		return nil
	}

	resp.Seq = seq
	// log.Printf("%p --> MD: sending response %#v", mc, resp)
	err := mc.xmit(resp.Type, seq, resp.Data)
	if err != nil {
		return err
	}

	return nil
}

func (mc *MultiConvo) ConversationLoop() error {
	var (
		sendCh chan sendData

		waitingSeq uint32
		replyCh    chan<- reply

		pending *message

		errorCh    chan reply    = make(chan reply, 1)
		incomingCh chan message  = make(chan message)
		closingCh  chan struct{} = make(chan struct{})
	)

	defer close(closingCh)
	// defer log.Printf("%p --> CL: STOP", mc)

	go mc.incomingLoop(incomingCh, errorCh, closingCh)

	// log.Printf("%p --> CL: START", mc)

recv_loop:
	for {
		sendCh = nil
		if replyCh == nil {
			sendCh = mc.sendCh
		}
		done := mc.ctx.Done()

		select {
		case msg := <-sendCh:
			seq, err := mc.xmitMessage(msg)
			if err != nil {
				msg.Reply <- reply{E: err}
				continue
			}

			waitingSeq = seq
			replyCh = msg.Reply

		case msg, ok := <-incomingCh:
			if !ok {
				break recv_loop
			}

			if replyCh == nil || msg.Seq < waitingSeq || msg.IsStopImmed() {
				goto dispatch
			}

			if msg.Seq == waitingSeq {
				replyCh <- reply{M: msg}
				replyCh = nil
				waitingSeq = 0

				goto dispatch
			}

			// if we reach here:
			//     replyCh != nil
			// AND msg.Seq > waitingSeq
			// AND !msg.IsStopImmed()
			//
			// so we're waiting on a response, so
			// mark this one as pending and wait for
			// the reply of the one we're waiting on
			//
			// if there's already one pending, this is a
			// serious error (the protocol shouldn't allow
			// for this)
			//
			if pending != nil {
				log.Printf("%p PANICKING: pending queue is full", mc)
				panic("pending queue is full!  other side should block until reply")
			}

			pending = &msg
			continue recv_loop

		dispatch:
			if err := mc.dispatchMessage(msg); err != nil {
				log.Printf("ERROR dispatching message [%v]: %v", msg, err)
			}

			if pending != nil && replyCh == nil {
				msg = *pending
				pending = nil
				goto dispatch
			}

		case r := <-errorCh:
			log.Printf("ERROR receiving message [%v]: %v", r.M, r.E)
			return r.E

		case <-done:
			break recv_loop
		}
	}

	return nil
}

func (mc *MultiConvo) sendMessage(mt MType, data interface{}) (message, error) {
	replyCh := make(chan reply)
	mc.sendCh <- sendData{mt, data, replyCh}

	select {
	case repl := <-replyCh:
		// log.Printf("reply: %v", repl)
		return repl.M, repl.E
	case <-mc.ctx.Done():
		return message{}, mc.ctx.Err()
	}
}

func (mc *MultiConvo) SendJob(job []fuq.Task) (message, error) {
	return mc.sendMessage(MTypeJob, &job)
}

func (mc *MultiConvo) SendUpdate(update fuq.JobStatusUpdate) (message, error) {
	return mc.sendMessage(MTypeUpdate, &update)
}

func (mc *MultiConvo) SendStop(nproc uint32) (message, error) {
	return mc.sendMessage(MTypeStop, nproc)
}

func (mc *MultiConvo) SendStopImmed() error {
	_, err := mc.sendMessage(MTypeStop, StopImmed)
	return err
}

func (mc *MultiConvo) SendHello(hello HelloData) (message, error) {
	return mc.sendMessage(MTypeHello, &hello)
}
