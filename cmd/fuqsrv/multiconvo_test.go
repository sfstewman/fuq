package main

import (
	"context"
	"fmt"
	"github.com/sfstewman/fuq"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	// "log"
)

type testConvo struct {
	pworker, pforeman net.Conn
	mcw, mcf          *MultiConvo

	cancelFunc context.CancelFunc

	syncCh chan struct{}
}

func newTestConvo() testConvo {
	tc := testConvo{}

	tc.pworker, tc.pforeman = net.Pipe()

	ctx, cancelFunc := context.WithCancel(context.Background())

	tc.cancelFunc = cancelFunc

	tc.mcw = NewMultiConvo(MultiConvoOpts{
		Conn:    tc.pworker,
		Flusher: nilFlusher{},
		Context: ctx,
		Client:  true,
	})

	tc.mcf = NewMultiConvo(MultiConvoOpts{
		Conn:    tc.pforeman,
		Flusher: nilFlusher{},
		Context: ctx,
		Client:  false,
	})

	tc.syncCh = make(chan struct{})

	return tc
}

func closeIfNonNil(ch chan struct{}) {
	if ch != nil {
		close(ch)
	}
}

func callCloseIfNonNil(cl io.Closer) {
	if cl != nil {
		cl.Close()
	}
}

func (t *testConvo) Close() {
	// first shut down Done channels
	t.cancelFunc()
	t.mcf.Close()
	// closeIfNonNil(t.mcf.Done)
	// closeIfNonNil(t.mcw.Done)

	// shut down sync channel
	closeIfNonNil(t.syncCh)

	// then shut down pipes
	callCloseIfNonNil(t.pworker)
	callCloseIfNonNil(t.pforeman)
}

// goPanicOnError Starts a goroutine that will panic if f returns a
// non-nil error value
func goPanicOnError(f func() error) {
	trace := make([]byte, 2048)
	n := runtime.Stack(trace, false)
	trace = trace[:n]
	go func() {
		if err := f(); err != nil {
			panic(fmt.Sprintf("%s\n\nconversation loop error: %v", trace, err))
		}
	}()
}

func TestOnMessage(t *testing.T) {
	tc := newTestConvo()
	defer tc.Close()

	syncCh, mcw, mcf := tc.syncCh, tc.mcw, tc.mcf
	received := message{}

	mcw.OnMessageFunc(MTypeJob, func(msg message) message {
		received = msg
		close(syncCh)
		return okayMessage(17, 5, msg.Seq)
	})

	goPanicOnError(mcw.ConversationLoop)
	goPanicOnError(mcf.ConversationLoop)

	job := fuq.Task{Task: 23, JobDescription: fuq.JobDescription{JobId: fuq.JobId(7)}}

	msg, err := mcf.SendJob([]fuq.Task{job})
	rnp, rnr, err := msg.okayData()
	if err != nil {
		t.Fatalf("error decoding OK return: %v", err)
	}

	if rnp != 17 || rnr != 5 {
		t.Errorf("expected reply of OK(17|5) but received OK(%d|%d)", rnp, rnr)
	}

	// make sure the OnMessage handler was called...
	<-syncCh
	tc.syncCh = nil

	if received.Type != MTypeJob {
		t.Errorf("wrong job type, expected JOB, found %d", received.Type)
	}

	recvJobPtr, ok := received.Data.(*[]fuq.Task)
	if !ok {
		t.Fatalf("wrong data type, expected []fuq.Task, found %T", received.Data)
	}
	recvJob := *recvJobPtr

	if len(recvJob) != 1 {
		t.Fatalf("expected one job item, found %d", len(recvJob))
	}

	if job != recvJob[0] {
		t.Errorf("sent job %v, received job %v", job, recvJob[0])
	}
}

func TestSecondSendBlocksUntilReply(t *testing.T) {
	tc := newTestConvo()
	defer tc.Close()

	var (
		order     = make(chan int, 4)
		recvSync  = make(chan struct{})
		sendWait1 = make(chan struct{})
		sendWait2 = make(chan struct{})
		replWait  = make(chan struct{})
	)

	_, mcw, mcf := tc.syncCh, tc.mcw, tc.mcf

	mcw.OnMessageFunc(MTypeJob, func(msg message) message {
		// log.Printf("RECEIVED: JOB")
		data := msg.Data.(*[]fuq.Task)
		close(recvSync) // signal that first message has been received

		<-replWait
		order <- (*data)[0].Task
		return okayMessage(17, 5, msg.Seq)
	})

	goPanicOnError(mcw.ConversationLoop)
	goPanicOnError(mcf.ConversationLoop)

	// first send
	go func() {
		<-sendWait1
		_, err := mcf.SendJob([]fuq.Task{
			fuq.Task{
				Task:           23,
				JobDescription: fuq.JobDescription{JobId: fuq.JobId(7)},
			},
		})
		if err != nil {
			panic(err)
		}
		order <- 1
		<-sendWait1
	}()

	// make sure goroutine of first send starts
	sendWait1 <- struct{}{}

	// make sure first message has been received
	<-recvSync
	recvSync = make(chan struct{})

	// second send
	go func() {
		<-sendWait2
		_, err := mcf.SendJob([]fuq.Task{
			fuq.Task{
				Task:           24,
				JobDescription: fuq.JobDescription{JobId: fuq.JobId(7)},
			},
		})
		if err != nil {
			panic(err)
		}
		order <- 2
	}()

	// make sure goroutine of second send starts
	sendWait2 <- struct{}{}

	// quick test that second job has not been received
	select {
	case <-recvSync:
		t.Fatal("second job received before reply to first job has been sent")
	default:
	}

	// signal for first reply (and all subsequent replies) to finish
	close(replWait)
	sendWait1 <- struct{}{}

	// make sure that second message has been received
	<-recvSync

	// now check the order
	recv1 := <-order
	repl1 := <-order

	recv2 := <-order
	repl2 := <-order

	if repl1 != 1 || repl2 != 2 {
		t.Errorf("error in reply order: expected 1,2 but found %d,%d",
			repl1, repl2)
	}

	if recv1 != 23 || recv2 != 24 {
		t.Errorf("error in receive order: expected tasks 23,24 but found %d,%d",
			recv1, recv2)
	}
}

func TestHoldMessageUntilReply(t *testing.T) {
	var (
		order       chan int      = make(chan int, 4)
		sendWait    chan struct{} = make(chan struct{})
		recvWait    chan struct{} = make(chan struct{})
		recvSignal1 chan struct{} = make(chan struct{})
		recvSignal2 chan struct{} = make(chan struct{})
	)

	tc := newTestConvo()
	defer tc.Close()

	mcw, mcf := tc.mcw, tc.mcf

	mcw.OnMessageFunc(MTypeJob, func(msg message) message {
		order <- 3
		// log.Printf("RECEIVED: JOB")

		// indicate that first message has been received
		close(recvSignal1)

		// wait for signal to send reply
		<-recvWait
		return okayMessage(17, 5, msg.Seq)
	})

	mcf.OnMessageFunc(MTypeUpdate, func(msg message) message {
		// signal that second send has been received
		close(recvSignal2)

		order <- 4
		// log.Printf("RECEIVED: UPDATE")
		return okayMessage(18, 4, msg.Seq)
	})

	goPanicOnError(mcw.ConversationLoop)
	goPanicOnError(mcf.ConversationLoop)

	job := fuq.Task{
		Task:           23,
		JobDescription: fuq.JobDescription{JobId: fuq.JobId(7)},
	}

	upd := fuq.JobStatusUpdate{
		JobId:   fuq.JobId(7),
		Task:    23,
		Success: true,
		Status:  "done",
	}

	// send first message
	go func() {
		<-sendWait
		_, err := mcf.SendJob([]fuq.Task{job})
		if err != nil {
			panic(fmt.Sprintf("error sending job: %v", err))
		}
		order <- 1
	}()

	// make sure first send has started
	sendWait <- struct{}{}

	// make sure first message received
	<-recvSignal1

	go func() {
		<-sendWait
		_, err := mcw.SendUpdate(upd)
		if err != nil {
			t.Fatalf("error sending update: %v", err)
		}
		order <- 2
	}()

	// signal second send to start
	sendWait <- struct{}{}

	// check that second message sent hasn't been received (not
	// conclusive)
	select {
	case <-recvSignal2:
		t.Fatal("second message received before first reply")
	default:
	}

	close(recvWait)

	r1 := <-order
	r2 := <-order
	r3 := <-order
	r4 := <-order

	if r1 != 3 || r2 != 1 || r3 != 4 || r4 != 2 {
		t.Errorf("reply order should be 3,1,4,2.  found %d,%d,%d,%d",
			r1, r2, r3, r4)
	}
}

func TestSequencesAreIncreasing(t *testing.T) {
	type seqTuple struct {
		wh  int
		seq uint32
	}

	var (
		seqnums []seqTuple
		seqlock sync.Mutex
	)

	tc := newTestConvo()
	defer tc.Close()

	mcw, mcf := tc.mcw, tc.mcf

	addSeq := func(wh int, seq uint32) {
		seqlock.Lock()
		defer seqlock.Unlock()

		seqnums = append(seqnums, seqTuple{wh, seq})
	}
	getSeqs := func() []seqTuple {
		seqlock.Lock()
		defer seqlock.Unlock()

		s := seqnums
		seqnums = nil
		return s
	}

	mcw.OnMessageFunc(MTypeJob, func(msg message) message {
		addSeq(1, msg.Seq)
		// log.Printf("RECEIVED: JOB")
		return okayMessage(17, 5, msg.Seq)
	})

	mcf.OnMessageFunc(MTypeUpdate, func(msg message) message {
		addSeq(2, msg.Seq)
		// signal that second send has been received
		// log.Printf("RECEIVED: UPDATE")
		return okayMessage(18, 4, msg.Seq)
	})

	goPanicOnError(mcw.ConversationLoop)
	goPanicOnError(mcf.ConversationLoop)

	for i := 23; i <= 25; i++ {
		task := fuq.Task{
			Task:           i,
			JobDescription: fuq.JobDescription{JobId: fuq.JobId(7)},
		}

		if _, err := mcf.SendJob([]fuq.Task{task}); err != nil {
			panic(fmt.Errorf("error sending job: %v", err))
		}
	}

	for i := 23; i <= 25; i++ {
		upd := fuq.JobStatusUpdate{
			JobId:   fuq.JobId(7),
			Task:    i,
			Success: true,
			Status:  "done",
		}
		if _, err := mcw.SendUpdate(upd); err != nil {
			panic(fmt.Errorf("error sending job: %v", err))
		}
	}

	for i := 26; i <= 28; i++ {
		task := fuq.Task{
			Task:           i,
			JobDescription: fuq.JobDescription{JobId: fuq.JobId(7)},
		}

		if _, err := mcf.SendJob([]fuq.Task{task}); err != nil {
			panic(fmt.Errorf("error sending job: %v", err))
		}

		upd := fuq.JobStatusUpdate{
			JobId:   fuq.JobId(7),
			Task:    i,
			Success: true,
			Status:  "done",
		}
		if _, err := mcw.SendUpdate(upd); err != nil {
			panic(fmt.Errorf("error sending job: %v", err))
		}
	}

	finalSeqs := getSeqs()
	if len(finalSeqs) != 12 {
		t.Fatalf("sequences should have 6 numbers")
	}

	for i := 1; i < len(finalSeqs); i++ {
		seq0, seq1 := finalSeqs[i-1].seq, finalSeqs[i].seq
		if seq0 >= seq1 {
			t.Logf("seqs: %v", finalSeqs)
			t.Errorf("sequences should be decreasing: at %d and %d, seqs [%d,%d] are increasing",
				i-1, i, seq0, seq1)
		}
	}
}
