package proto

import (
	"context"
	"fmt"
	"github.com/sfstewman/fuq"
	"log"
	"net/http"
	"time"
)

const ErrorChannelTimeout = 1 * time.Second

type MessageHandler interface {
	Handle(msg Message) (response Message)
}

type MessageHandlerFunc func(Message) Message

func (h MessageHandlerFunc) Handle(msg Message) Message {
	return h(msg)
}

type reply struct {
	M Message
	E error
}

type outgoingMessage struct {
	M Message
	R chan<- reply
}

type NopFlusher struct{}

func (NopFlusher) Flush() {}

type Conn struct {
	handlers   [256]MessageHandler
	messenger  Messenger
	seq        Sequencer
	outgoingCh chan outgoingMessage

	Timeout time.Duration

	isWorker bool
}

type Opts struct {
	Messenger Messenger
	Flusher   http.Flusher
	Worker    bool
}

func NewConn(opts Opts) *Conn {
	return &Conn{
		messenger:  opts.Messenger,
		seq:        &LockedSequence{},
		isWorker:   opts.Worker,
		outgoingCh: make(chan outgoingMessage),
	}
}

func (mc *Conn) nextSeq() uint32 {
	s := mc.seq.Next() << 1
	if !mc.isWorker {
		s = s | 0x1
	}
	return s
}

func (mc *Conn) Sequence() uint32 {
	return mc.seq.Current()
}

// Called by ReceiveMessage() to update the sequence number
// when a new message has been received
func (mc *Conn) updateSeq(new uint32) {
	new = new >> 1
	mc.seq.Update(new + 1)
}

func (mc *Conn) xmit(m Message) error {
	return mc.messenger.Send(m)
}

func (mc *Conn) xmitWithSeq(m Message) (seq uint32, err error) {
	seq = mc.nextSeq()
	m.Seq = seq
	err = mc.xmit(m)
	return
}

func (mc *Conn) Close() {
}

func (mc *Conn) OnMessage(mt MType, h MessageHandler) {
	mc.handlers[uint8(mt)] = h
}

func (mc *Conn) OnMessageFunc(mt MType, f MessageHandlerFunc) {
	mc.OnMessage(mt, f)
}

func (mc *Conn) sendError(ctx context.Context, errorCh chan<- reply, r reply) {
	// if the context is canceled, don't send an error
	if ctx.Err() != nil {
		return
	}

	errCtx, _ := context.WithTimeout(ctx, ErrorChannelTimeout)
	// check done channel before setting error
	select {
	case <-errCtx.Done():
		goto canceled

	case errorCh <- r:
		return
	}

canceled:
	switch err := errCtx.Err(); err {
	case context.Canceled:
		return

	case context.DeadlineExceeded:
		// If we can't send a reply over the error channel, it
		// either indicates that the conversation loop is
		// blocked or that the conversation loop was canceled
		// but the incoming loop was not.
		//
		// Both are programming errors, so we panic.
		panic(fmt.Sprintf("could not send reply %v", err))

	default:
		panic(fmt.Sprintf("invalid context error: %v", err))
	}
}

func (mc *Conn) incomingLoop(ctx context.Context, msgCh chan<- Message, errorCh chan<- reply) {
	defer close(msgCh)

	done := ctx.Done()
	for {
		// check done channel before we receive a message
		select {
		case <-done:
			return
		default:
		}

		msg, err := mc.messenger.Receive()

		// even if there's an error, we need to update the sequence number
		mc.updateSeq(msg.Seq)

		if err != nil {
			log.Printf("error in incoming loop: %v", err)
			mc.sendError(ctx, errorCh, reply{msg, err})
		}

		select {
		case msgCh <- msg:
			/* nop */
		case <-done:
			return
		}
	}
}

func (mc *Conn) dispatchMessage(m Message) error {
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
	return mc.xmit(resp)
}

func (mc *Conn) ConversationLoop(ctx context.Context) error {
	var (
		outgoingCh chan outgoingMessage

		waitingSeq uint32
		replyCh    chan<- reply

		pending *Message

		errorCh    chan reply   = make(chan reply, 1)
		incomingCh chan Message = make(chan Message)
	)

	incomingCtx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()
	// defer log.Printf("%p --> CL: STOP", mc)

	go mc.incomingLoop(incomingCtx, incomingCh, errorCh)

	// log.Printf("%p --> CL: START", mc)

recv_loop:
	for {
		outgoingCh = nil
		if replyCh == nil {
			outgoingCh = mc.outgoingCh
		}
		done := ctx.Done()

		select {
		case msg := <-outgoingCh:
			seq, err := mc.xmitWithSeq(msg.M)
			if err != nil {
				msg.R <- reply{E: err}
				continue
			}

			waitingSeq = seq
			replyCh = msg.R

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

func (mc *Conn) sendMessage(ctx context.Context, mt MType, data interface{}) (Message, error) {
	m := Message{Type: mt, Data: data}
	replyCh := make(chan reply)
	mc.outgoingCh <- outgoingMessage{M: m, R: replyCh}

	select {
	case repl := <-replyCh:
		// log.Printf("reply: %v", repl)
		return repl.M, repl.E
	case <-ctx.Done():
		return Message{}, ctx.Err()
	}
}

func (mc *Conn) SendJob(ctx context.Context, job []fuq.Task) (Message, error) {
	return mc.sendMessage(ctx, MTypeJob, &job)
}

func (mc *Conn) SendUpdate(ctx context.Context, update fuq.JobStatusUpdate) (Message, error) {
	return mc.sendMessage(ctx, MTypeUpdate, &update)
}

func (mc *Conn) SendStop(ctx context.Context, nproc uint32) (Message, error) {
	return mc.sendMessage(ctx, MTypeStop, nproc)
}

func (mc *Conn) SendStopImmed(ctx context.Context) error {
	_, err := mc.sendMessage(ctx, MTypeStop, StopImmed)
	return err
}

func (mc *Conn) SendHello(ctx context.Context, hello HelloData) (Message, error) {
	return mc.sendMessage(ctx, MTypeHello, &hello)
}
