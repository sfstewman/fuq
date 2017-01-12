package main

import (
	"bytes"
	"fmt"
	"github.com/sfstewman/fuq"
	"testing"
)

type checkFlusher bool

func (cf *checkFlusher) Flush() {
	*cf = true
}

func checkReceive(c *Convo, expectedType MType) (interface{}, error) {
	mt, data, err := c.ReceiveMessage()
	if err != nil {
		return nil, fmt.Errorf("error receving message: %v", err)
	}

	if mt != expectedType {
		return nil, fmt.Errorf("expected message type 0x%4x, but found: 0x%4x",
			expectedType, mt)
	}

	return data, nil
}

func TestSendOK(t *testing.T) {
	b := bytes.Buffer{}
	f := checkFlusher(false)
	c := Convo{I: &b, O: &b, F: &f}

	if err := c.SendOK(17); err != nil {
		t.Fatalf("error sending OK(17): %v", err)
	}
	if bool(f) != true {
		t.Errorf("flush not called after SendOK")
	}

	arg, err := c.ReceiveOK()
	if err != nil {
		t.Fatalf("error waiting for OK: %v", err)
	}

	if arg != 17 {
		t.Errorf("OK(n) received, but n = %d, expected n = 17", arg)
	}

	/* check receiving with ReceiveMessage() */
	f = false
	if err := c.SendOK(5); err != nil {
		t.Fatalf("error sending OK(17): %v", err)
	}
	if bool(f) != true {
		t.Errorf("flush not called after SendOK")
	}

	data, err := checkReceive(&c, MTypeOK)
	if err != nil {
		t.Fatal("%v", err)
	}

	arg, ok := data.(uint32)
	if !ok {
		t.Fatalf("expected OK data, but found: %#v", data)
	}
	if arg != 5 {
		t.Errorf("OK(n) received, but n = %d, expected n = 5", arg)
	}
}

func TestSendJob(t *testing.T) {
	b := bytes.Buffer{}
	f := checkFlusher(false)
	c := Convo{I: &b, O: &b, F: &f}

	tsend := fuq.Task{
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
	}

	if err := c.SendJob(tsend); err != nil {
		t.Fatalf("error sending JOB: %v", err)
	}
	if bool(f) != true {
		t.Errorf("flush not called after SendJob")
	}

	data, err := checkReceive(&c, MTypeJob)
	if err != nil {
		t.Fatal("%v", err)
	}

	trecvPtr, ok := data.(*fuq.Task)
	if !ok {
		t.Fatalf("expected JOB data, but found: %#v", data)
	}

	if tsend != *trecvPtr {
		t.Errorf("JOB received, but job = %v, expected %v", *trecvPtr, tsend)
	}
}

func TestSendUpdate(t *testing.T) {
	b := bytes.Buffer{}
	f := checkFlusher(false)
	c := Convo{I: &b, O: &b, F: &f}

	usend := fuq.JobStatusUpdate{
		JobId:   fuq.JobId(7),
		Task:    8,
		Success: true,
		Status:  "done",
	}

	if err := c.SendUpdate(usend); err != nil {
		t.Fatalf("error sending UPDATE: %v", err)
	}
	if bool(f) != true {
		t.Errorf("flush not called after SendUpdate")
	}

	data, err := checkReceive(&c, MTypeUpdate)
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
	b := bytes.Buffer{}
	f := checkFlusher(false)
	c := Convo{I: &b, O: &b, F: &f}

	if err := c.SendStop(3); err != nil {
		t.Fatalf("error sending STOP(3): %v", err)
	}
	if bool(f) != true {
		t.Errorf("flush not called after SendStop")
	}

	data, err := checkReceive(&c, MTypeStop)
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
	if bool(f) != true {
		t.Errorf("flush not called after SendStop")
	}

	data, err = checkReceive(&c, MTypeStop)
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
