package fuqtest

import (
	"github.com/gorilla/websocket"
	"net"
	"net/http"
	"os"
	"path/filepath"
)

type WSPair struct {
	Http     http.Server
	SConn    *websocket.Conn
	CConn    *websocket.Conn
	TmpDir   string
	Listener net.Listener
	Server   http.Server
}

func (wsp *WSPair) Close() {
	// wsc.Server.Shutdown()
	wsp.Listener.Close()
	os.Remove(wsp.TmpDir)
}

func NewWSPair(tmpDir string) *WSPair {
	var wsp WSPair

	wsp.TmpDir = tmpDir
	if err := os.Chmod(tmpDir, 0700); err != nil {
		panic(err)
	}

	// XXX - is there a better/portable way?
	socketPath := filepath.Join(tmpDir, "socket.web")
	addr, err := net.ResolveUnixAddr("unix", socketPath)
	if err != nil {
		panic(err)
	}

	listener, err := net.ListenUnix("unix", addr)
	if err != nil {
		panic(err)
	}

	wsp.Listener = listener

	connCh := make(chan *websocket.Conn)
	handler := func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			panic(err)
		}

		connCh <- conn
	}

	wsp.Http = http.Server{
		Handler: http.HandlerFunc(handler),
	}

	// start the server side...
	go wsp.Http.Serve(listener)

	// start the client side...
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		panic(err)
	}

	dialer := websocket.Dialer{
		NetDial: func(network, addr string) (net.Conn, error) {
			return conn, nil
		},
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}

	cconn, _, err := dialer.Dial("ws://localhost", nil)
	if err != nil {
		panic(err)
	}
	wsp.CConn = cconn
	wsp.SConn = <-connCh

	return &wsp
}
