package proto

import (
	"context"
	"fmt"
	"github.com/sfstewman/fuq"
	"github.com/sfstewman/fuq/fuqtest"
	"io"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"
	// "log"
)

type convoTestRigger interface {
	MCW() *MultiConvo
	MCF() *MultiConvo

	SyncCh() chan struct{}
	NilSyncCh()

	Close()
}

type testRigMaker func() convoTestRigger

func testWithRig(mk testRigMaker, testFunc func(convoTestRigger, *testing.T)) func(*testing.T) {
	return func(t *testing.T) {
		rig := mk()
		defer rig.Close()

		testFunc(rig, t)
	}
}

type testConvo struct {
	pworker, pforeman net.Conn
	mcw1, mcf1        *MultiConvo

	cancelFunc context.CancelFunc

	syncCh1 chan struct{}
}

func (tc *testConvo) MCW() *MultiConvo {
	return tc.mcw1
}

func (tc *testConvo) MCF() *MultiConvo {
	return tc.mcf1
}

func (tc *testConvo) SyncCh() chan struct{} {
	return tc.syncCh1
}

func (tc *testConvo) NilSyncCh() {
	tc.syncCh1 = nil
}

func newTestConvo() testConvo {
	tc := testConvo{}

	tc.pworker, tc.pforeman = net.Pipe()

	ctx, cancelFunc := context.WithCancel(context.Background())

	tc.cancelFunc = cancelFunc

	tc.mcw1 = NewMultiConvo(MultiConvoOpts{
		Messenger: ConnMessenger{
			Conn:    tc.pworker,
			Flusher: NopFlusher{},
		},
		Context: ctx,
		Worker:  true,
	})

	tc.mcf1 = NewMultiConvo(MultiConvoOpts{
		Messenger: ConnMessenger{
			Conn:    tc.pforeman,
			Flusher: NopFlusher{},
		},
		Context: ctx,
		Worker:  false,
	})

	tc.syncCh1 = make(chan struct{})

	return tc
}

type wsConvo struct {
	pair       *fuqtest.WSPair
	mcw, mcf   *MultiConvo
	cancelFunc context.CancelFunc
	syncCh     chan struct{}
}

func (wsc *wsConvo) Close() {
	wsc.cancelFunc()
	wsc.mcf.Close()
	wsc.mcw.Close()

	// shut down sync channel
	closeIfNonNil(wsc.syncCh)
	wsc.pair.Close()
}

func (wsc *wsConvo) MCW() *MultiConvo {
	return wsc.mcw
}

func (wsc *wsConvo) MCF() *MultiConvo {
	return wsc.mcf
}

func (wsc *wsConvo) SyncCh() chan struct{} {
	return wsc.syncCh
}

func (wsc *wsConvo) NilSyncCh() {
	wsc.syncCh = nil
}

func newWebSocketConvo() *wsConvo {
	wsc := wsConvo{}
	defer func() {
		r := recover()
		if r == nil {
			return
		}

		wsc.Close()
		panic(r)
	}()

	tmpDir, err := ioutil.TempDir(os.TempDir(), "wsc_test")
	if err != nil {
		panic(err)
	}

	wsc.pair = fuqtest.NewWSPair(tmpDir)

	ctx, cancelFunc := context.WithCancel(context.Background())
	wsc.cancelFunc = cancelFunc

	wsc.mcw = NewMultiConvo(MultiConvoOpts{
		Messenger: WebsocketMessenger{
			C:       wsc.pair.CConn,
			Timeout: 60 * time.Second,
		},
		Context: ctx,
		Worker:  true,
	})

	wsc.mcf = NewMultiConvo(MultiConvoOpts{
		Messenger: WebsocketMessenger{
			C:       wsc.pair.SConn,
			Timeout: 60 * time.Second,
		},
		Context: ctx,
		Worker:  false,
	})

	wsc.syncCh = make(chan struct{})

	return &wsc
}

type checkFlusher struct {
	signal  chan struct{}
	flushed bool
}

func newCheckFlusher() *checkFlusher {
	cf := &checkFlusher{
		signal: make(chan struct{}),
	}

	return cf
}

func (cf *checkFlusher) Flush() {
	cf.flushed = true
	close(cf.signal)
}

func (cf *checkFlusher) IsFlushed() bool {
	<-cf.signal
	return cf.flushed
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
	t.mcf1.Close()
	t.mcw1.Close()

	// shut down sync channel
	closeIfNonNil(t.syncCh1)

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

func runTestsWithRig(mk testRigMaker, t *testing.T) {
	t.Run("OnMessage", testWithRig(mk, OnMessageTest))
	t.Run("SendJob", testWithRig(mk, SendJobTest))
	t.Run("SendUpdate", testWithRig(mk, SendUpdateTest))
	t.Run("SendStop", testWithRig(mk, SendStopTest))
	t.Run("SendHello", testWithRig(mk, SendHelloTest))
	t.Run("SecondSendBlocksUntilReply", testWithRig(mk, SecondSendBlocksUntilReplyTest))
	t.Run("HoldMessageUntilReply", testWithRig(mk, HoldMessageUntilReplyTest))
	t.Run("SequencesAreIncreasing", testWithRig(mk, SequencesAreIncreasingTest))
}

func TestNetConn(t *testing.T) {
	mk := func() convoTestRigger {
		tc := newTestConvo()
		return &tc
	}
	runTestsWithRig(mk, t)
}

func TestWebSocket(t *testing.T) {
	mk := func() convoTestRigger {
		return newWebSocketConvo()
	}
	runTestsWithRig(mk, t)
}

func OnMessageTest(tc convoTestRigger, t *testing.T) {
	syncCh, mcw, mcf := tc.SyncCh(), tc.MCW(), tc.MCF()
	received := Message{}

	mcw.OnMessageFunc(MTypeJob, func(msg Message) Message {
		received = msg
		close(syncCh)
		return OkayMessage(17, 5, msg.Seq)
	})

	goPanicOnError(mcw.ConversationLoop)
	goPanicOnError(mcf.ConversationLoop)

	job := fuq.Task{Task: 23, JobDescription: fuq.JobDescription{JobId: fuq.JobId(7)}}

	msg, err := mcf.SendJob([]fuq.Task{job})
	rnp, rnr, err := msg.AsOkay()
	if err != nil {
		t.Fatalf("error decoding OK return: %v", err)
	}

	if rnp != 17 || rnr != 5 {
		t.Errorf("expected reply of OK(17|5) but received OK(%d|%d)", rnp, rnr)
	}

	// make sure the OnMessage handler was called...
	<-syncCh
	tc.NilSyncCh()

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

func checkOK(t *testing.T, m Message, nproc0, nrun0 uint16) {
	nproc, nrun, err := m.AsOkay()
	if err != nil {
		t.Fatalf("cannot decode response: %v", err)
	}

	if nproc != nproc0 || nrun != nrun0 {
		t.Errorf("expected OK(%d|%d), but received OK(%d|%d)",
			nproc0, nrun0, nproc, nrun)
	}
}

func SendJobTest(tc convoTestRigger, t *testing.T) {
	syncCh, mcw, mcf := tc.SyncCh(), tc.MCW(), tc.MCF()
	received := Message{}

	// mcf.flusher = fl

	mcw.OnMessageFunc(MTypeJob, func(msg Message) Message {
		received = msg
		tc.NilSyncCh()
		close(syncCh)
		return OkayMessage(17, 5, msg.Seq)
	})

	goPanicOnError(mcw.ConversationLoop)
	goPanicOnError(mcf.ConversationLoop)

	tsend := []fuq.Task{
		fuq.Task{
			Task: 6,
			JobDescription: fuq.JobDescription{
				JobId:      fuq.JobId(3),
				Name:       "test_job",
				NumTasks:   13,
				WorkingDir: "/home/foo",
				LoggingDir: "/home/foo/logs",
				Command:    "/bin/echo",
				Status:     fuq.Running,
			},
		},
	}

	repl, err := mcf.SendJob(tsend)
	if err != nil {
		t.Fatalf("error sending JOB: %v", err)
	}

	if received.Type != MTypeJob {
		t.Fatalf("expected job message")
	}

	trecvPtr, ok := received.Data.(*[]fuq.Task)
	if !ok {
		t.Fatalf("expected JOB data, but found: %#v", received.Data)
	}

	if !reflect.DeepEqual(tsend, *trecvPtr) {
		t.Errorf("Jobs sent %v disagree with jobs received %v", tsend, *trecvPtr)
	}

	checkOK(t, repl, 17, 5)
}

func SendUpdateTest(tc convoTestRigger, t *testing.T) {
	syncCh, mcw, mcf := tc.SyncCh(), tc.MCW(), tc.MCF()
	received := Message{}

	mcf.OnMessageFunc(MTypeUpdate, func(msg Message) Message {
		received = msg
		tc.NilSyncCh()
		close(syncCh)
		return OkayMessage(12, 3, msg.Seq)
	})

	goPanicOnError(mcw.ConversationLoop)
	goPanicOnError(mcf.ConversationLoop)

	usend := fuq.JobStatusUpdate{
		JobId:   fuq.JobId(7),
		Task:    8,
		Success: true,
		Status:  "done",
	}

	resp, err := mcw.SendUpdate(usend)
	if err != nil {
		t.Fatalf("error sending UPDATE: %v", err)
	}

	// channel sync: make sure received is set
	<-syncCh

	if received.Type != MTypeUpdate {
		t.Fatalf("expected job update message")
	}

	urecvPtr, ok := received.Data.(*fuq.JobStatusUpdate)
	if !ok {
		t.Fatalf("expected job update data, but found: %#v", received.Data)
	}

	if !reflect.DeepEqual(usend, *urecvPtr) {
		t.Errorf("UPDATE received, but update = %v, expected %v", *urecvPtr, usend)
	}

	checkOK(t, resp, 12, 3)
}

func SendStopTest(tc convoTestRigger, t *testing.T) {
	syncCh, mcw, mcf := tc.SyncCh(), tc.MCW(), tc.MCF()
	received := Message{}

	mcw.OnMessageFunc(MTypeStop, func(msg Message) Message {
		received = msg
		tc.NilSyncCh()
		close(syncCh)
		return OkayMessage(4, 3, msg.Seq)
	})

	goPanicOnError(mcw.ConversationLoop)
	goPanicOnError(mcf.ConversationLoop)

	resp, err := mcf.SendStop(3)
	if err != nil {
		t.Fatalf("error sending STOP(3): %v", err)
	}

	// channel sync: make sure received is set
	<-syncCh

	if received.Type != MTypeStop {
		t.Fatalf("expected stop message")
	}

	nstop, err := received.AsStop()
	if err != nil {
		t.Fatalf("error decoding stop data: %#v", err)
	}

	if nstop != 3 {
		t.Errorf("STOP received, but nproc = %d, expected %d", nstop, 3)
	}

	checkOK(t, resp, 4, 3)
}

func SendHelloTest(tc convoTestRigger, t *testing.T) {
	syncCh, mcw, mcf := tc.SyncCh(), tc.MCW(), tc.MCF()
	received := Message{}

	mcf.OnMessageFunc(MTypeHello, func(msg Message) Message {
		received = msg
		tc.NilSyncCh()
		close(syncCh)
		return OkayMessage(4, 3, msg.Seq)
	})

	goPanicOnError(mcw.ConversationLoop)
	goPanicOnError(mcf.ConversationLoop)

	hello := HelloData{NumProcs: 11}
	resp, err := mcw.SendHello(hello)
	if err != nil {
		t.Fatalf("error sending STOP(3): %v", err)
	}
	// log.Printf("received response: %v", resp)

	// channel sync: make sure received is set
	<-syncCh

	if received.Type != MTypeHello {
		t.Fatalf("expected stop message")
	}

	hrecvPtr, ok := received.Data.(*HelloData)
	if !ok {
		t.Fatalf("expected hello data, but found: %#v", received.Data)
	}

	if !reflect.DeepEqual(hello, *hrecvPtr) {
		t.Errorf("HELLO received, but update = %v, expected %v", *hrecvPtr, hello)
	}

	checkOK(t, resp, 4, 3)
}

func SecondSendBlocksUntilReplyTest(tc convoTestRigger, t *testing.T) {
	var (
		order     = make(chan int, 4)
		recvSync  = make(chan struct{})
		sendWait1 = make(chan struct{})
		sendWait2 = make(chan struct{})
		replWait  = make(chan struct{})
	)

	mcw, mcf := tc.MCW(), tc.MCF()

	mcw.OnMessageFunc(MTypeJob, func(msg Message) Message {
		// log.Printf("RECEIVED: JOB")
		data := msg.Data.(*[]fuq.Task)
		close(recvSync) // signal that first message has been received

		<-replWait
		order <- (*data)[0].Task
		return OkayMessage(17, 5, msg.Seq)
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

func HoldMessageUntilReplyTest(tc convoTestRigger, t *testing.T) {
	var (
		order       = make(chan int, 4)
		sendWait    = make(chan struct{})
		recvWait    = make(chan struct{})
		recvSignal1 = make(chan struct{})
		recvSignal2 = make(chan struct{})
	)

	mcw, mcf := tc.MCW(), tc.MCF()

	mcw.OnMessageFunc(MTypeJob, func(msg Message) Message {
		order <- 3
		// log.Printf("RECEIVED: JOB")

		// indicate that first message has been received
		close(recvSignal1)

		// wait for signal to send reply
		<-recvWait
		return OkayMessage(17, 5, msg.Seq)
	})

	mcf.OnMessageFunc(MTypeUpdate, func(msg Message) Message {
		// signal that second send has been received
		close(recvSignal2)

		order <- 4
		// log.Printf("RECEIVED: UPDATE")
		return OkayMessage(18, 4, msg.Seq)
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

	// two orderings should be possible:
	// 	1. [3] mcw receives job
	// 	2. [1] mcf receives rsponse
	// 	3. [4] mcf receives update
	// 	4. [2] mcw receives rsponse
	// OR
	// 	1. [3] mcw receives job
	// 	2. [4] mcf receives update
	// 	3. [1] mcf receives rsponse
	// 	4. [2] mcw receives rsponse
	//

	if r1 == 3 && r2 == 1 && r3 == 4 && r4 == 2 {
		/* okay */
		return
	}

	if r1 == 3 && r2 == 4 && r3 == 1 && r4 == 2 {
		/* okay */
		return
	}

	t.Errorf("reply order should be 3,1,4,2 or 3,4,1,2.  found %d,%d,%d,%d",
		r1, r2, r3, r4)
}

func SequencesAreIncreasingTest(tc convoTestRigger, t *testing.T) {
	type seqTuple struct {
		wh  int
		seq uint32
	}

	var (
		seqnums []seqTuple
		seqlock sync.Mutex
	)

	mcw, mcf := tc.MCW(), tc.MCF()

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

	mcw.OnMessageFunc(MTypeJob, func(msg Message) Message {
		addSeq(1, msg.Seq)
		// log.Printf("RECEIVED: JOB")
		return OkayMessage(17, 5, msg.Seq)
	})

	mcf.OnMessageFunc(MTypeUpdate, func(msg Message) Message {
		addSeq(2, msg.Seq)
		// signal that second send has been received
		// log.Printf("RECEIVED: UPDATE")
		return OkayMessage(18, 4, msg.Seq)
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
