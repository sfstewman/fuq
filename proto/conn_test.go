package proto_test

import (
	"context"
	"fmt"
	"github.com/sfstewman/fuq"
	"github.com/sfstewman/fuq/fuqtest"
	"github.com/sfstewman/fuq/proto"
	"github.com/sfstewman/fuq/websocket"
	"io"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

type connTestRigger interface {
	MCW() *proto.Conn
	MCF() *proto.Conn

	SyncCh() chan struct{}
	NilSyncCh()

	Context() context.Context

	Close()
}

type testRigMaker func() connTestRigger

func testWithRig(mk testRigMaker, testFunc func(connTestRigger, *testing.T)) func(*testing.T) {
	return func(t *testing.T) {
		rig := mk()
		defer rig.Close()

		testFunc(rig, t)
	}
}

type testConn struct {
	pworker, pforeman net.Conn
	mcw1, mcf1        *proto.Conn

	ctx        context.Context
	cancelFunc context.CancelFunc

	syncCh1 chan struct{}
}

func (tc *testConn) MCW() *proto.Conn {
	return tc.mcw1
}

func (tc *testConn) MCF() *proto.Conn {
	return tc.mcf1
}

func (tc *testConn) SyncCh() chan struct{} {
	return tc.syncCh1
}

func (tc *testConn) NilSyncCh() {
	tc.syncCh1 = nil
}

func (tc *testConn) Context() context.Context {
	return tc.ctx
}

func newTestConn() testConn {
	tc := testConn{}

	tc.pworker, tc.pforeman = net.Pipe()

	tc.ctx, tc.cancelFunc = context.WithCancel(context.Background())

	tc.mcw1 = proto.NewConn(proto.Opts{
		Messenger: proto.ConnMessenger{
			Conn:    tc.pworker,
			Flusher: proto.NopFlusher{},
		},
		Worker: true,
	})

	tc.mcf1 = proto.NewConn(proto.Opts{
		Messenger: proto.ConnMessenger{
			Conn:    tc.pforeman,
			Flusher: proto.NopFlusher{},
		},
		Worker: false,
	})

	tc.syncCh1 = make(chan struct{})

	return tc
}

type wsConn struct {
	pair       *fuqtest.WSPair
	mcw, mcf   *proto.Conn
	ctx        context.Context
	cancelFunc context.CancelFunc
	syncCh     chan struct{}
}

func (wsc *wsConn) Close() {
	wsc.cancelFunc()
	wsc.mcf.Close()
	wsc.mcw.Close()

	// shut down sync channel
	closeIfNonNil(wsc.syncCh)
	wsc.pair.Close()
}

func (wsc *wsConn) MCW() *proto.Conn {
	return wsc.mcw
}

func (wsc *wsConn) MCF() *proto.Conn {
	return wsc.mcf
}

func (wsc *wsConn) SyncCh() chan struct{} {
	return wsc.syncCh
}

func (wsc *wsConn) NilSyncCh() {
	wsc.syncCh = nil
}

func (wsc *wsConn) Context() context.Context {
	return wsc.ctx
}

func newWebSocketConn() *wsConn {
	wsc := wsConn{}
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

	wsc.ctx, wsc.cancelFunc = context.WithCancel(context.Background())

	wsc.mcw = proto.NewConn(proto.Opts{
		Messenger: &websocket.Messenger{
			C:       wsc.pair.CConn,
			Timeout: 60 * time.Second,
		},
		Worker: true,
	})

	wsc.mcf = proto.NewConn(proto.Opts{
		Messenger: &websocket.Messenger{
			C:       wsc.pair.SConn,
			Timeout: 60 * time.Second,
		},
		Worker: false,
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

func (t *testConn) Close() {
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

func runTestsWithRig(mk testRigMaker, t *testing.T) {
	t.Run("OnMessage", testWithRig(mk, OnMessageTest))
	t.Run("SendJob", testWithRig(mk, SendJobTest))
	t.Run("SendUpdate", testWithRig(mk, SendUpdateTest))
	t.Run("SendCancel", testWithRig(mk, SendCancelTest))
	t.Run("SendStop", testWithRig(mk, SendStopTest))
	t.Run("SendHello", testWithRig(mk, SendHelloTest))
	t.Run("SecondSendBlocksUntilReply", testWithRig(mk, SecondSendBlocksUntilReplyTest))
	t.Run("HoldMessageUntilReply", testWithRig(mk, HoldMessageUntilReplyTest))
	t.Run("SequencesAreIncreasing", testWithRig(mk, SequencesAreIncreasingTest))
	t.Run("MessageAfterFunc", testWithRig(mk, MessageAfterFuncTest))
}

func TestNetConn(t *testing.T) {
	mk := func() connTestRigger {
		tc := newTestConn()
		return &tc
	}
	runTestsWithRig(mk, t)
}

func TestWebSocket(t *testing.T) {
	mk := func() connTestRigger {
		return newWebSocketConn()
	}
	runTestsWithRig(mk, t)
}

func OnMessageTest(tc connTestRigger, t *testing.T) {
	syncCh, mcw, mcf := tc.SyncCh(), tc.MCW(), tc.MCF()
	received := proto.Message{}

	mcw.OnMessageFunc(proto.MTypeJob, func(msg proto.Message) proto.Message {
		received = msg
		close(syncCh)
		return proto.OkayMessage(17, 5, msg.Seq)
	})

	ctx := tc.Context()
	fuqtest.GoPanicOnError(ctx, mcw.ConversationLoop)
	fuqtest.GoPanicOnError(ctx, mcf.ConversationLoop)

	job := fuq.Task{Task: 23, JobDescription: fuq.JobDescription{JobId: fuq.JobId(7)}}

	msg, err := mcf.SendJob(ctx, []fuq.Task{job})
	if err != nil {
		t.Fatalf("error sending job: %v", err)
	}

	rnp, rnr := msg.AsOkay()
	if rnp != 17 || rnr != 5 {
		t.Errorf("expected reply of OK(17|5) but received OK(%d|%d)", rnp, rnr)
	}

	// make sure the OnMessage handler was called...
	<-syncCh
	tc.NilSyncCh()

	if received.Type != proto.MTypeJob {
		t.Errorf("wrong job type, expected JOB, found %d", received.Type)
	}

	recvJob, ok := received.Data.([]fuq.Task)
	if !ok {
		t.Fatalf("wrong data type, expected []fuq.Task, found %T", received.Data)
	}

	if len(recvJob) != 1 {
		t.Fatalf("expected one job item, found %d", len(recvJob))
	}

	if job != recvJob[0] {
		t.Errorf("sent job %v, received job %v", job, recvJob[0])
	}
}

func checkOK(t *testing.T, m proto.Message, nproc0, nrun0 uint16) {
	nproc, nrun := m.AsOkay()
	if nproc != nproc0 || nrun != nrun0 {
		t.Errorf("expected OK(%d|%d), but received OK(%d|%d)",
			nproc0, nrun0, nproc, nrun)
	}
}

func SendJobTest(tc connTestRigger, t *testing.T) {
	syncCh, mcw, mcf := tc.SyncCh(), tc.MCW(), tc.MCF()
	received := proto.Message{}

	// mcf.flusher = fl

	mcw.OnMessageFunc(proto.MTypeJob, func(msg proto.Message) proto.Message {
		received = msg
		tc.NilSyncCh()
		close(syncCh)
		return proto.OkayMessage(17, 5, msg.Seq)
	})

	ctx := tc.Context()
	fuqtest.GoPanicOnError(ctx, mcw.ConversationLoop)
	fuqtest.GoPanicOnError(ctx, mcf.ConversationLoop)

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

	repl, err := mcf.SendJob(ctx, tsend)
	if err != nil {
		t.Fatalf("error sending JOB: %v", err)
	}

	if received.Type != proto.MTypeJob {
		t.Fatalf("expected job message")
	}

	trecv, ok := received.Data.([]fuq.Task)
	if !ok {
		t.Fatalf("expected JOB data, but found: %#v", received.Data)
	}

	if !reflect.DeepEqual(tsend, trecv) {
		t.Errorf("Jobs sent %v disagree with jobs received %v", tsend, trecv)
	}

	checkOK(t, repl, 17, 5)
}

func SendUpdateTest(tc connTestRigger, t *testing.T) {
	syncCh, mcw, mcf := tc.SyncCh(), tc.MCW(), tc.MCF()
	received := proto.Message{}

	mcf.OnMessageFunc(proto.MTypeUpdate, func(msg proto.Message) proto.Message {
		received = msg
		tc.NilSyncCh()
		close(syncCh)
		return proto.OkayMessage(12, 3, msg.Seq)
	})

	ctx := tc.Context()
	fuqtest.GoPanicOnError(ctx, mcw.ConversationLoop)
	fuqtest.GoPanicOnError(ctx, mcf.ConversationLoop)

	usend := fuq.JobStatusUpdate{
		JobId:   fuq.JobId(7),
		Task:    8,
		Success: true,
		Status:  "done",
	}

	resp, err := mcw.SendUpdate(ctx, usend)
	if err != nil {
		t.Fatalf("error sending UPDATE: %v", err)
	}

	// channel sync: make sure received is set
	<-syncCh

	if received.Type != proto.MTypeUpdate {
		t.Fatalf("expected job update message")
	}

	urecv, ok := received.Data.(fuq.JobStatusUpdate)
	if !ok {
		t.Fatalf("expected job update data, but found: %#v", received.Data)
	}

	if !reflect.DeepEqual(usend, urecv) {
		t.Errorf("UPDATE received, but update = %v, expected %v", urecv, usend)
	}

	checkOK(t, resp, 12, 3)
}

func SendCancelTest(tc connTestRigger, t *testing.T) {
	syncCh, mcw, mcf := tc.SyncCh(), tc.MCW(), tc.MCF()
	received := proto.Message{}

	mcf.OnMessageFunc(proto.MTypeCancel, func(msg proto.Message) proto.Message {
		received = msg
		tc.NilSyncCh()
		close(syncCh)
		return proto.OkayMessage(12, 3, msg.Seq)
	})

	ctx := tc.Context()
	fuqtest.GoPanicOnError(ctx, mcw.ConversationLoop)
	fuqtest.GoPanicOnError(ctx, mcf.ConversationLoop)

	usend := []fuq.TaskPair{
		{JobId: fuq.JobId(6), Task: 14},
	}

	resp, err := mcw.SendCancel(ctx, usend)
	if err != nil {
		t.Fatalf("error sending CANCEL: %v", err)
	}

	// channel sync: make sure received is set
	<-syncCh

	if received.Type != proto.MTypeCancel {
		t.Fatalf("expected job cancel message")
	}

	urecv, ok := received.Data.([]fuq.TaskPair)
	if !ok {
		t.Fatalf("expected job cancel data, but found: %#v", received.Data)
	}

	if !reflect.DeepEqual(usend, urecv) {
		t.Errorf("UPDATE received, but update = %v, expected %v", *urecvPtr, usend)
	}

	checkOK(t, resp, 12, 3)
}

func SendStopTest(tc connTestRigger, t *testing.T) {
	syncCh, mcw, mcf := tc.SyncCh(), tc.MCW(), tc.MCF()
	received := proto.Message{}

	mcw.OnMessageFunc(proto.MTypeStop, func(msg proto.Message) proto.Message {
		received = msg
		tc.NilSyncCh()
		close(syncCh)
		return proto.OkayMessage(4, 3, msg.Seq)
	})

	ctx := tc.Context()
	fuqtest.GoPanicOnError(ctx, mcw.ConversationLoop)
	fuqtest.GoPanicOnError(ctx, mcf.ConversationLoop)

	resp, err := mcf.SendStop(ctx, 3)
	if err != nil {
		t.Fatalf("error sending STOP(3): %v", err)
	}

	// channel sync: make sure received is set
	<-syncCh

	if received.Type != proto.MTypeStop {
		t.Fatalf("expected stop message")
	}

	nstop := received.AsStop()
	if nstop != 3 {
		t.Errorf("STOP received, but nproc = %d, expected %d", nstop, 3)
	}

	checkOK(t, resp, 4, 3)
}

func SendHelloTest(tc connTestRigger, t *testing.T) {
	syncCh, mcw, mcf := tc.SyncCh(), tc.MCW(), tc.MCF()
	received := proto.Message{}

	mcf.OnMessageFunc(proto.MTypeHello, func(msg proto.Message) proto.Message {
		received = msg
		tc.NilSyncCh()
		close(syncCh)
		return proto.OkayMessage(4, 3, msg.Seq)
	})

	ctx := tc.Context()
	fuqtest.GoPanicOnError(ctx, mcw.ConversationLoop)
	fuqtest.GoPanicOnError(ctx, mcf.ConversationLoop)

	hello := proto.HelloData{NumProcs: 11}
	resp, err := mcw.SendHello(ctx, hello)
	if err != nil {
		t.Fatalf("error sending STOP(3): %v", err)
	}
	// log.Printf("received response: %v", resp)

	// channel sync: make sure received is set
	<-syncCh

	if received.Type != proto.MTypeHello {
		t.Fatalf("expected stop message")
	}

	hrecv, ok := received.Data.(proto.HelloData)
	if !ok {
		t.Fatalf("expected hello data, but found: %#v", received.Data)
	}

	if !reflect.DeepEqual(hello, hrecv) {
		t.Errorf("HELLO received, but update = %v, expected %v", *hrecvPtr, hello)
	}

	checkOK(t, resp, 4, 3)
}

func SecondSendBlocksUntilReplyTest(tc connTestRigger, t *testing.T) {
	// This test is meant to test the following situation:
	//   two goroutines in Endpoint1 each send a message in the order
	//   message1,message2
	//
	// Endpoint1 should wait to receive a reply to message1 before
	// sending message2
	//

	var (
		waitCh      = make(chan struct{})
		received    = make(chan int)
		replied     = make(chan int)
		sentSignal1 = make(chan struct{})
		sentSignal2 = make(chan struct{})
	)

	mcw, mcf := tc.MCW(), tc.MCF()

	// on JOB message:
	//   1. signal that the message has been received by sending the
	//      task number to the received channel
	//   2. reply with OK(Task-5|5)
	mcw.OnMessageFunc(proto.MTypeJob, func(msg proto.Message) proto.Message {
		tasks := msg.Data.([]fuq.Task)
		received <- tasks[0].Task
		np := uint16(tasks[0].Task - 5)
		return proto.OkayMessage(np, 5, msg.Seq)
	})

	// register OK handler to detect when we've received the reply
	// on a reply (must be OK), sends the nproc field to the replied
	// channel
	mcf.OnMessageFunc(proto.MTypeOK, func(msg proto.Message) proto.Message {
		np, nr := msg.AsOkay()
		t.Logf("received reply OK(%d|%d)", np, nr)
		replied <- int(np)
		return proto.Message{} // reply is ignore for OK and Error
	})

	ctx := tc.Context()
	fuqtest.GoPanicOnError(ctx, mcw.ConversationLoop)
	fuqtest.GoPanicOnError(ctx, mcf.ConversationLoop)

	// first send:
	//   1. wait for signal to start (receive on waitCh)
	//   2. send JOB, wait for AFTER signal on sentSignal1
	//   3. wait for a reply
	//   4. close(sentSignal1) to signal reply received
	go func() {
		// wait for signal to start
		<-waitCh

		desc := fuq.JobDescription{JobId: fuq.JobId(7)}
		tasks := []fuq.Task{{Task: 23, JobDescription: desc}}

		m := proto.Message{Type: proto.MTypeJob, Data: tasks}
		m.After(func() {
			// signal that message was sent
			sentSignal1 <- struct{}{}
		})

		_, err := mcf.SendMessage(ctx, m)
		if err != nil {
			panic(err)
		}

		// signal that reply was received
		close(sentSignal1)
	}()

	// make sure goroutine of first send starts
	waitCh <- struct{}{}

	// ensure that first message has been sent
	<-sentSignal1

	// NB: we don't receive on the 'received' channel
	// until the second send is ready to go

	// second send:
	//   1. wait for signal to start (receive on waitCh)
	//   2. send JOB, wait for AFTER signal on sentSignal2
	//   3. wait for a reply
	//   4. close(sentSignal2) to signal reply received
	go func() {
		<-waitCh // wait for signal to start
		desc := fuq.JobDescription{JobId: fuq.JobId(7)}
		tasks := []fuq.Task{{Task: 24, JobDescription: desc}}

		m := proto.Message{Type: proto.MTypeJob, Data: tasks}
		m.After(func() {
			// signal that message was sent
			sentSignal2 <- struct{}{}
		})

		if _, err := mcf.SendMessage(ctx, m); err != nil {
			panic(err)
		}

		// signal that reply was received
		close(sentSignal2)
	}()

	// make sure goroutine of second send starts
	waitCh <- struct{}{}

	// allow the reply to proceed
	tnum := <-received
	if tnum != 23 {
		t.Errorf("JOB receive should be 23, but was %d", tnum)
	}

	// now we need to check the ordering: we should receive the
	// signal of the reply before the signal that message 2 has been
	// sent.
	select {
	case r := <-replied:
		if r != 18 {
			t.Errorf("expected replied channel message to be 18, but found %d",
				r)
		}
	case <-sentSignal2:
		t.Fatal("sent second message before reply to first")
	}

	// ensure that the reply has been returned
	<-sentSignal1

	// expect the second message to be sent
	<-sentSignal2

	// check that it's been received
	tnum = <-received
	if tnum != 24 {
		t.Errorf("JOB receive should be 24, but was %d", tnum)
	}

	r := <-replied
	if r != 19 {
		t.Errorf("expected replied channel message to be 19, but found %d",
			r)
	}

	// ensure that the reply has been returned
	<-sentSignal2
}

func HoldMessageUntilReplyTest(tc connTestRigger, t *testing.T) {
	var (
		order       = make(chan int, 4)
		sendWait    = make(chan struct{})
		recvWait    = make(chan struct{})
		recvSignal1 = make(chan struct{})
		recvSignal2 = make(chan struct{})
	)

	mcw, mcf := tc.MCW(), tc.MCF()

	mcw.OnMessageFunc(proto.MTypeJob, func(msg proto.Message) proto.Message {
		order <- 3
		// log.Printf("RECEIVED: JOB")

		// indicate that first message has been received
		close(recvSignal1)

		// wait for signal to send reply
		<-recvWait
		return proto.OkayMessage(17, 5, msg.Seq)
	})

	mcf.OnMessageFunc(proto.MTypeUpdate, func(msg proto.Message) proto.Message {
		// signal that second send has been received
		close(recvSignal2)

		order <- 4
		// log.Printf("RECEIVED: UPDATE")
		return proto.OkayMessage(18, 4, msg.Seq)
	})

	ctx := tc.Context()
	fuqtest.GoPanicOnError(ctx, mcw.ConversationLoop)
	fuqtest.GoPanicOnError(ctx, mcf.ConversationLoop)

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
		_, err := mcf.SendJob(ctx, []fuq.Task{job})
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
		_, err := mcw.SendUpdate(ctx, upd)
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

func SequencesAreIncreasingTest(tc connTestRigger, t *testing.T) {
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

	mcw.OnMessageFunc(proto.MTypeJob, func(msg proto.Message) proto.Message {
		addSeq(1, msg.Seq)
		// log.Printf("RECEIVED: JOB")
		return proto.OkayMessage(17, 5, msg.Seq)
	})

	mcf.OnMessageFunc(proto.MTypeUpdate, func(msg proto.Message) proto.Message {
		addSeq(2, msg.Seq)
		// signal that second send has been received
		// log.Printf("RECEIVED: UPDATE")
		return proto.OkayMessage(18, 4, msg.Seq)
	})

	ctx := tc.Context()
	fuqtest.GoPanicOnError(ctx, mcw.ConversationLoop)
	fuqtest.GoPanicOnError(ctx, mcf.ConversationLoop)

	for i := 23; i <= 25; i++ {
		task := fuq.Task{
			Task:           i,
			JobDescription: fuq.JobDescription{JobId: fuq.JobId(7)},
		}

		if _, err := mcf.SendJob(ctx, []fuq.Task{task}); err != nil {
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
		if _, err := mcw.SendUpdate(ctx, upd); err != nil {
			panic(fmt.Errorf("error sending job: %v", err))
		}
	}

	for i := 26; i <= 28; i++ {
		task := fuq.Task{
			Task:           i,
			JobDescription: fuq.JobDescription{JobId: fuq.JobId(7)},
		}

		if _, err := mcf.SendJob(ctx, []fuq.Task{task}); err != nil {
			panic(fmt.Errorf("error sending job: %v", err))
		}

		upd := fuq.JobStatusUpdate{
			JobId:   fuq.JobId(7),
			Task:    i,
			Success: true,
			Status:  "done",
		}
		if _, err := mcw.SendUpdate(ctx, upd); err != nil {
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

func MessageAfterFuncTest(tc connTestRigger, t *testing.T) {
	// Tests that message After functions are run in the correct
	// order.
	//
	// To test this, we:
	//
	//   1. send a message (M) with an After function.
	//   2. receive the message and send a reply (R) with an after
	//      function
	//
	//   The observable events are:
	//
	//	MA: M After function called after send
	//	MR: M received
	//	RA: R After function called
	//	RR: R received (M send function returns)
	//
	// We receive on an unbuffered channel to detect these events.
	//
	// The possible orderings:
	//
	//	1a. MA or MR
	//	1b. MR or MA (whatever isn't first)
	//
	//	2a. RA or RR
	//	2b. RR or RA (whatever isn't first)
	//
	// The orderings for the pairs 1a,b and 2a,b are not
	// well-determined, but if the After function is called after a
	// message is sent (and not before), then we can do the
	// following:
	//
	//   1. send message M
	//   2. block on the message receive (MR) event
	//   3. block on the message after (MA) event
	//
	// If the message After function is called before the message is
	// sent, this will deadlock.  We use a similar sequence for the
	// reply.
	//
	// We also check the ordering of MR and RR.  RR should occur
	// strictly after MR.
	//

	mcf := tc.MCF()
	mcw := tc.MCW()

	signalMA := make(chan struct{})
	signalMR := make(chan struct{})

	signalRA := make(chan struct{})
	signalRR := make(chan struct{})

	mcf.OnMessageFunc(proto.MTypeHello, func(msg proto.Message) proto.Message {
		t.Logf("signaling HELLO received: %v", msg)
		// signal start
		signalMR <- struct{}{}

		m := proto.OkayMessage(17, 11, msg.Seq)

		// signal after message is sent
		m.After(func() {
			t.Logf("signaling that reply AFTER func called")
			signalRA <- struct{}{}
		})

		t.Logf("set AFTER func on reply %v and returning", m)
		return m
	})

	ctx := tc.Context()
	fuqtest.GoPanicOnError(ctx, mcf.ConversationLoop)
	fuqtest.GoPanicOnError(ctx, mcw.ConversationLoop)

	go func() {
		hello := proto.HelloData{NumProcs: 11}
		msg := proto.Message{Type: proto.MTypeHello, Data: hello}
		msg.After(func() {
			t.Logf("signaling that message AFTER func called")
			signalMA <- struct{}{}
		})

		_, err := mcw.SendMessage(context.TODO(), msg)
		if err != nil {
			panic(err)
		}
		signalRR <- struct{}{}
	}()

	t.Log("waiting for message received and message after events")

	// Test that either RA or MR event happens, and that
	// the reply-received event doesn't happen.
	select {
	case <-signalMR:
		t.Log("message received event")
		signalMR = nil

	case <-signalRR:
		t.Fatal("reply receive event before message received and message after")
	}

	select {
	case <-signalMA:
		t.Log("message after event")
		signalMA = nil
	case <-signalRR:
		t.Fatal("reply receive event before message received and message after")
		// Note that we can't control the order of the
		// reply-after event, so we don't block on it.
	}

	t.Log("waiting for reply received and reply after events")

	<-signalRR
	t.Log("reply received event")

	<-signalRA
	t.Log("reply after event")
}
