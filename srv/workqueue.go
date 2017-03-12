package srv

import (
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/sfstewman/fuq"
	"github.com/sfstewman/fuq/srv/proto"
	"net/http"
	"time"
)

/* WorkerPool maintains a pool of workers, and communicates with the
 * Foreman to distribute work to these workers.
 *
 */
type WorkPool struct {
	W      []Worker
	T      []fuq.Task
	Config *WorkerConfig
	E      *fuq.Endpoint
	dialer websocket.Dialer
	conn   *websocket.Conn
}

func (p *WorkPool) Cookie() fuq.Cookie {
	return p.Config.Cookie()
}

func (p *WorkPool) RefreshCookie(oldCookie fuq.Cookie) error {
	return p.Config.RefreshCookie(p.E, oldCookie)
}

const MaxUint16 = int(^uint16(0))

func (p *WorkPool) NumProcs() (nproc, nrun int) {
	nw := len(p.W)
	nrun = len(p.W)
	nproc = nw - nrun

	if nproc < 0 {
		panic(fmt.Sprintf("invalid nproc: nproc=%d, nproc < 0", nproc))
	}

	return
}

func (p *WorkPool) QueueLoop() error {
	if p.conn == nil {
		conn, resp, err := p.Dial()
		if err != nil {
			_ = resp
			return err
		}

		p.conn = conn
	}

	conn := p.conn

	nproc, nrun := p.NumProcs()

	// send HELLO
	hello := proto.HelloData{
		NumProcs: nproc,
		Running:  p.T,
	}

	if err := conn.WriteJSON(&hello); err != nil {
		return fmt.Errorf("error in HELLO: %v", err)
	}

	for {
		// conn.ReadJSON(
		return nil
		// req := NodeRequestEnvelope{
	}
}

func (p *WorkPool) Dial() (*websocket.Conn, *http.Response, error) {
	cookie := p.Config.Cookie()

	tlsConfig, err := fuq.SetupTLSRootCA(p.E.Config)
	if err != nil {
		return nil, nil, fmt.Errorf("error setting up tls config: %v", err)
	}
	dialer := websocket.Dialer{
		TLSClientConfig:   tlsConfig,
		HandshakeTimeout:  30 * time.Second,
		ReadBufferSize:    1024,
		WriteBufferSize:   1024,
		EnableCompression: false,
	}

	url := p.E.Config.EndpointURL("node/converse")
	headers := http.Header{}
	headers.Add("Cookie", string(cookie))
	return dialer.Dial(url, headers)
}

/*
func makeEndpoint(c Config, configHTTP2 bool) (*Endpoint, error) {
	tlsConfig, err := fuq.SetupTLSRootCA(c)
	if err != nil {
		return nil, fmt.Errorf("error setting up tls config: %v", err)
	}

	transport := &http.Transport{
		TLSClientConfig:    tlsConfig,
		DisableCompression: true,
	}

	// enable http/2 support
	if configHTTP2 {
		if err := http2.ConfigureTransport(transport); err != nil {
			return nil, fmt.Errorf("error adding http/2 support: %v", err)
		}
	}

	client := &http.Client{
		Transport: transport,
	}

	return &Endpoint{
		Config: c,
		Client: client,
	}, nil
}
*/
