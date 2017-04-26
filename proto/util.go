package proto

import (
	"io"
	"syscall"
)

func writeAll(w io.Writer, data []byte) (n int, err error) {
	var nb int

write:
	nb, err = w.Write(data[n:])
	n += nb

	if err == syscall.EINTR {
		goto write
	}

	return n, err
}
