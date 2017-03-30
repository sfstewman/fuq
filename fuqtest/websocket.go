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

	connCh     chan *websocket.Conn
	socketPath string
}

func (wsp *WSPair) Close() {
	// wsc.Server.Shutdown()
	wsp.Listener.Close()
	os.Remove(wsp.TmpDir)
}

func UnconnectedWSPair(tmpDir string) *WSPair {
	var wsp WSPair

	wsp.TmpDir = tmpDir
	if err := os.Chmod(tmpDir, 0700); err != nil {
		panic(err)
	}

	// XXX - is there a better/portable way?
	wsp.socketPath = filepath.Join(tmpDir, "socket.web")
	addr, err := net.ResolveUnixAddr("unix", wsp.socketPath)
	if err != nil {
		panic(err)
	}

	listener, err := net.ListenUnix("unix", addr)
	if err != nil {
		panic(err)
	}

	wsp.Listener = listener

	wsp.connCh = make(chan *websocket.Conn)

	wsp.Http = http.Server{
		Handler: &wsp,
	}

	return &wsp
}

func (wsp *WSPair) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		panic(err)
	}

	wsp.connCh <- conn
}

func (wsp *WSPair) StartServer() {
	wsp.Http.Serve(wsp.Listener)
}

func (wsp *WSPair) Dial() error {
	// start the client side...
	conn, err := net.Dial("unix", wsp.socketPath)
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
	wsp.SConn = <-wsp.connCh

	return nil
}

func NewWSPair(tmpDir string) *WSPair {
	wsp := UnconnectedWSPair(tmpDir)

	// start the server side...
	go wsp.StartServer()

	wsp.Dial()
	return wsp
}
