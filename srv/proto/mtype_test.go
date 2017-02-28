package proto

import (
	"bytes"
	"testing"
	// "github.com/sfstewman/fuq"
	// "fmt"
)

func TestOkayMessage(t *testing.T) {
	buf := &bytes.Buffer{}

	msg := okayMessage(17, 0, 2)
	if err := msg.Send(buf); err != nil {
		t.Fatalf("error sending OK(17): %v", err)
	}

	incoming, err := ReceiveMessage(buf)
	if err != nil {
		t.Fatalf("error waiting for OK: %v", err)
	}

	if incoming.Seq != 2 {
		t.Errorf("bad sequence waiting for OK, expected %d but found %d",
			2, incoming.Seq)
	}

	nproc, nrun, err := incoming.AsOkay()
	if err != nil {
		t.Fatalf("error waiting for OK: %v", err)
	}

	if nproc != 17 || nrun != 0 {
		t.Errorf("OK(n|r) received, but (n|r) = (%d|%d), expected (17|0)", nproc, nrun)
	}

	/* check receiving with ReceiveMessage() */
	msg = okayMessage(5, 11, 3)
	if err := msg.Send(buf); err != nil {
		t.Fatalf("error sending OK(5|11): %v", err)
	}

	incoming, err = ReceiveMessage(buf)
	if err != nil {
		t.Fatalf("error waiting for OK: %v", err)
	}

	if incoming.Seq != 3 {
		t.Errorf("bad sequence waiting for OK, expected %d but found %d",
			2, incoming.Seq)
	}

	nproc, nrun, err = incoming.AsOkay()
	if err != nil {
		t.Fatalf("error waiting for OK: %v", err)
	}

	if nproc != 5 || nrun != 11 {
		t.Errorf("OK(n|r) received, but (n|r) = (%d|%d), expected (5|11)", nproc, nrun)
	}

	// Check arg0 encoding explicitly
	arg, ok := incoming.Data.(uint32)
	if !ok {
		t.Fatalf("expected OK data, but found: %#v", incoming.Data)
	}

	nproc = uint16(arg >> 16)
	nrun = uint16(arg & 0xffff)

	if nproc != 5 || nrun != 11 {
		t.Errorf("OK(n|r) received, but (n|r) = (%d|%d), expected (5|11)", nproc, nrun)
	}
}