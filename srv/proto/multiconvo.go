package proto

import (
	"context"
	"fmt"
	"github.com/sfstewman/fuq"
	"log"
	"net/http"
	"time"
)

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

type MultiConvo struct {
	handlers   [256]MessageHandler
	messenger  Messenger
	seq        Sequencer
	outgoingCh chan outgoingMessage

	Timeout time.Duration

	isWorker bool
}

type MultiConvoOpts struct {
	Messenger Messenger
	Flusher   http.Flusher
	Worker    bool
}

func NewMultiConvo(opts MultiConvoOpts) *MultiConvo {
	return &MultiConvo{
		messenger:  opts.Messenger,
		seq:        &LockedSequence{},
		isWorker:   opts.Worker,
		outgoingCh: make(chan outgoingMessage),
	}
}

func (mc *MultiConvo) nextSeq() uint32 {
	s := mc.seq.Next() << 1
	if !mc.isWorker {
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

func (mc *MultiConvo) xmit(m Message) error {
	return mc.messenger.Send(m)
}

func (mc *MultiConvo) xmitWithSeq(m Message) (seq uint32, err error) {
	seq = mc.nextSeq()
	m.Seq = seq
	err = mc.xmit(m)
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

func (mc *MultiConvo) incomingLoop(ctx context.Context, msgCh chan<- Message, errorCh chan<- reply) {
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
		done := ctx.Done()

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

			select {
			case <-done:
				// ignore error
				return
			default:
				/* report error */
			}

			// check done channel before setting error
			log.Printf("error in incoming loop: %#v", err)
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

func (mc *MultiConvo) dispatchMessage(m Message) error {
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

func (mc *MultiConvo) ConversationLoop(ctx context.Context) error {
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

func (mc *MultiConvo) sendMessage(ctx context.Context, mt MType, data interface{}) (Message, error) {
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

func (mc *MultiConvo) SendJob(ctx context.Context, job []fuq.Task) (Message, error) {
	return mc.sendMessage(ctx, MTypeJob, &job)
}

func (mc *MultiConvo) SendUpdate(ctx context.Context, update fuq.JobStatusUpdate) (Message, error) {
	return mc.sendMessage(ctx, MTypeUpdate, &update)
}

func (mc *MultiConvo) SendStop(ctx context.Context, nproc uint32) (Message, error) {
	return mc.sendMessage(ctx, MTypeStop, nproc)
}

func (mc *MultiConvo) SendStopImmed(ctx context.Context) error {
	_, err := mc.sendMessage(ctx, MTypeStop, StopImmed)
	return err
}

func (mc *MultiConvo) SendHello(ctx context.Context, hello HelloData) (Message, error) {
	return mc.sendMessage(ctx, MTypeHello, &hello)
}
