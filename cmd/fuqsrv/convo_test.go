package main

import (
	"bytes"
	"fmt"
	"github.com/sfstewman/fuq"
	"net"
	"testing"
)

type checkFlusher bool

func (cf *checkFlusher) Flush() {
	*cf = true
}

type nilFlusher struct{}

func (nilFlusher) Flush() {}

const ignoreSeq = ^uint32(0)

func newConvo(flags ConvoFlag) (Convo, *bytes.Buffer, *checkFlusher) {
	b := &bytes.Buffer{}
	f := new(checkFlusher)
	seq := &LockedSequence{}
	ws := newWriterSend(b, f)
	rr := readerRecv{b}
	c := Convo{r: rr, s: ws, Flags: flags, Seq: seq}
	return c, b, f
}

func checkReceive(c *Convo, expectedType MType, expectedSeq uint32) (interface{}, error) {
	msg, err := c.ReceiveMessage()
	if err != nil {
		return nil, fmt.Errorf("error receving message: %v", err)
	}

	if msg.Type != expectedType {
		return nil, fmt.Errorf("expected message type 0x%4x, but found: 0x%4x",
			expectedType, msg.Type)
	}

	// if all bits set, ignore expectedSeq
	if expectedSeq != ignoreSeq && msg.Seq != expectedSeq {
		return nil, fmt.Errorf("expected message sequence number %d, but found %d",
			expectedSeq, msg.Seq)
	}

	return msg.Data, nil
}

func TestSendOK(t *testing.T) {
	c, _, f := newConvo(CFWorker)

	seqSend, err := c.SendOK(17, 0)
	if err != nil {
		t.Fatalf("error sending OK(17): %v", err)
	}

	if bool(*f) != true {
		t.Errorf("flush not called after SendOK")
	}

	nproc, nrun, err := c.ReceiveOK(seqSend)
	if err != nil {
		t.Fatalf("error waiting for OK: %v", err)
	}

	if nproc != 17 || nrun != 0 {
		t.Errorf("OK(n|r) received, but (n|r) = (%d|%d), expected (17|0)", nproc, nrun)
	}

	/* check receiving with ReceiveMessage() */
	*f = false
	seqSend, err = c.SendOK(5, 11)
	if err != nil {
		t.Fatalf("error sending OK(5|11): %v", err)
	}
	if bool(*f) != true {
		t.Errorf("flush not called after SendOK")
	}

	data, err := checkReceive(&c, MTypeOK, seqSend)
	if err != nil {
		t.Fatal(err)
	}

	arg, ok := data.(uint32)
	if !ok {
		t.Fatalf("expected OK data, but found: %#v", data)
	}

	nproc = uint16(arg >> 16)
	nrun = uint16(arg & 0xffff)

	if nproc != 5 || nrun != 11 {
		t.Errorf("OK(n|r) received, but (n|r) = (%d|%d), expected (5|11)", nproc, nrun)
	}
}

func TestSendJob(t *testing.T) {
	c, _, f := newConvo(CFWorker)

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

	sendSeq, err := c.SendJob(tsend)
	if err != nil {
		t.Fatalf("error sending JOB: %v", err)
	}
	if bool(*f) != true {
		t.Errorf("flush not called after SendJob")
	}

	data, err := checkReceive(&c, MTypeJob, sendSeq)
	if err != nil {
		t.Fatalf("%v", err)
	}

	trecvPtr, ok := data.(*[]fuq.Task)
	if !ok {
		t.Fatalf("expected JOB data, but found: %#v", data)
	}

	if len(tsend) != len(*trecvPtr) {
		t.Fatalf("JOB list received, but sent %d jobs, received %d jobs: %v and %v",
			len(tsend), len(*trecvPtr), tsend, *trecvPtr)
	}

	for i, rv := range *trecvPtr {
		if sv := tsend[i]; sv != rv {
			t.Errorf("JOB %d disagrees: sent %v, receivved %v", i, sv, rv)
		}
	}
}

func TestSendUpdate(t *testing.T) {
	c, _, f := newConvo(CFWorker)

	usend := fuq.JobStatusUpdate{
		JobId:   fuq.JobId(7),
		Task:    8,
		Success: true,
		Status:  "done",
	}

	sendSeq, err := c.SendUpdate(usend)
	if err != nil {
		t.Fatalf("error sending UPDATE: %v", err)
	}
	if bool(*f) != true {
		t.Errorf("flush not called after SendUpdate")
	}

	data, err := checkReceive(&c, MTypeUpdate, sendSeq)
	if err != nil {
		t.Fatal("%v", err)
	}

	urecvPtr, ok := data.(*fuq.JobStatusUpdate)
	if !ok {
		t.Fatalf("expected UPDATE data, but found: %#v", data)
	}

	if usend != *urecvPtr {
		t.Errorf("UPDATE received, but update = %v, expected %v", *urecvPtr, usend)
	}
}

func TestSendStop(t *testing.T) {
	c, _, f := newConvo(CFWorker)

	sendSeq, err := c.SendStop(3)
	if err != nil {
		t.Fatalf("error sending STOP(3): %v", err)
	}
	if bool(*f) != true {
		t.Errorf("flush not called after SendStop")
	}

	data, err := checkReceive(&c, MTypeStop, sendSeq)
	if err != nil {
		t.Fatal("%v", err)
	}

	arg, ok := data.(uint32)
	if !ok {
		t.Fatalf("expected STOP data, but found: %#v", data)
	}
	if arg != 3 {
		t.Errorf("STOP(n) received, but n = %d, expected n = 3", arg)
	}

	/* Check STOP(immed) */
	if err := c.SendStopImmed(); err != nil {
		t.Fatalf("error sending STOP(immed): %v", err)
	}
	if bool(*f) != true {
		t.Errorf("flush not called after SendStop")
	}

	data, err = checkReceive(&c, MTypeStop, ignoreSeq)
	if err != nil {
		t.Fatal("%v", err)
	}

	arg, ok = data.(uint32)
	if !ok {
		t.Fatalf("expected STOP data, but found: %#v", data)
	}
	if arg != StopImmed {
		t.Errorf("STOP(n) received, but n = %d, expected n = immed (%d)", arg, StopImmed)
	}

}

func TestSequenceNumbers(t *testing.T) {
	pworker, pforeman := net.Pipe()
	defer func() {
		pworker.Close()
		pforeman.Close()
	}()

	sf := &LockedSequence{}
	sw := &LockedSequence{}

	ww := newWriterSend(pworker, nilFlusher{})
	wf := newWriterSend(pforeman, nilFlusher{})

	cw := Convo{r: readerRecv{pworker}, s: ww, Flags: CFWorker, Seq: sw}
	cf := Convo{r: readerRecv{pforeman}, s: wf, Flags: CFNone, Seq: sf}

	s0 := cw.Sequence()

	seqCh := make(chan uint32, 1) // buffer of 1 so we don't deadlock

	// --- TEST HELLO ---
	go func() {
		seq, err := cw.SendHello(HelloData{})
		if err != nil {
			t.Fatalf("error sending HELLO: %v", err)
		}

		seqCh <- seq
	}()

	msg, err := cf.ReceiveMessage()
	if err != nil {
		t.Fatalf("error receiving message: %v", err)
	}

	if mseq := <-seqCh; msg.Seq != mseq {
		t.Fatalf("sequence send/receive mismatch: sent %d, received %d", mseq, msg.Seq)
	}

	if s := cw.Sequence(); s <= s0 {
		t.Fatalf("sequence must increase on a send, original is %d, new is %d", s0, s)
	}

	if s1, s2 := cw.Sequence(), cf.Sequence(); s1 != s2 {
		t.Fatalf("sequence of worker / foreman do not match after receive: %d != %d", s1, s2)
	}

	// --- TEST REPLY ---
	s0 = cw.Sequence()
	go func() {
		if err := cf.ReplyOK(0, 0, msg.Seq); err != nil {
			t.Fatalf("error replying OK: %v", err)
		}
	}()

	np, nr, err := cw.ReceiveOK(msg.Seq)
	if err != nil {
		t.Fatalf("error receiving OK reply: %v", err)
	}

	if np != 0 || nr != 0 {
		t.Errorf("expected OK(0|0) but received OK(%d|%d)", np, nr)
	}

	if s1 := cw.Sequence(); s1 != s0 {
		t.Fatalf("sequence of worker changed after reply, should be %d, found %d", s0, s1)
	}

	if s2 := cf.Sequence(); s2 != s0 {
		t.Fatalf("sequence of foreman changed after reply, should be %d, found %d", s0, s2)
	}

	// --- TEST ONE MORE MESSAGE ---
	go func() {
		seq, err := cf.SendJob([]fuq.Task{})
		if err != nil {
			t.Fatalf("error sending job: %v", err)
		}

		seqCh <- seq
	}()

	msg, err = cw.ReceiveMessage()
	if err != nil {
		t.Fatalf("error receiving JOB message: %v", err)
	}

	if msg.Type != MTypeJob {
		t.Fatalf("wrong message type, expected JOB", err)
	}

	if mseq := <-seqCh; msg.Seq != mseq {
		t.Fatalf("sequence send/receive mismatch: sent %d, received %d", mseq, msg.Seq)
	}

	if actual, expected := msg.Seq, (s0<<1 | 0x1); actual != expected {
		t.Errorf("Expected JOB message with sequence %d, but found %d",
			expected, actual)
	}

	if s1 := cf.Sequence(); s1 <= s0 {
		t.Fatalf("sequence must increase on a send, original is %d, new is %d", s0, s1)
	}

	if s1, s2 := cw.Sequence(), cf.Sequence(); s1 != s2 {
		t.Fatalf("sequence of worker / foreman do not match after receive: %d != %d", s1, s2)
	}
}

/*
func TestServerLoop(t *testing.T) {
	c1, c2 := net.Pipe()
	defer func() {
		c1.Close()
		c2.Close()
	}()

}
*/
