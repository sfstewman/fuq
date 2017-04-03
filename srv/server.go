package srv

import (
	"fmt"
	"github.com/sfstewman/fuq"
	"golang.org/x/net/http2"
	"log"
	"net/http"
)

type Handler struct {
	H    http.Handler
	Path string
}

func (h Handler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		fuq.BadMethod(resp, req)
		return
	}

	if req.URL.Path != h.Path {
		fuq.Forbidden(resp, req)
		return
	}

	h.H.ServeHTTP(resp, req)
}

func AddHandler(mux *http.ServeMux, path string, handler http.HandlerFunc) {
	mux.Handle(path, Handler{
		H:    handler,
		Path: path,
	})
}

func StartAPIServer(f *Foreman, config fuq.Config) error {
	log.Printf("Starting API server on %s:%d",
		config.Foreman, config.Port)

	mux := http.NewServeMux()

	log.Printf("Adding handlers")
	AddHandler(mux, "/hello", f.HandleHello)
	AddHandler(mux, "/node/reauth", f.HandleNodeReauth)
	AddHandler(mux, "/node/persistent", f.HandleNodePersistent)

	f.AddNodeHandler(mux, "/job/request", f.HandleNodeJobRequest)
	f.AddNodeHandler(mux, "/job/status", f.HandleNodeJobUpdate)

	f.AddClientHandler(mux, "/client/nodes/list", f.HandleClientNodeList)
	f.AddClientHandler(mux, "/client/nodes/shutdown", f.HandleClientNodeShutdown)

	f.AddClientHandler(mux, "/client/job/list", f.HandleClientJobList)
	f.AddClientHandler(mux, "/client/job/new", f.HandleClientJobNew)
	f.AddClientHandler(mux, "/client/job/clear", f.HandleClientJobClear)
	f.AddClientHandler(mux, "/client/job/state", f.HandleClientJobState)

	f.AddClientHandler(mux, "/client/shutdown", f.HandleClientShutdown)

	tlsConfig, err := fuq.SetupTLS(config)
	if err != nil {
		log.Printf("error setting up TLS: %v", err)
		return fmt.Errorf("Error starting foreman: %v", err)
	}

	addrPortPair := fmt.Sprintf("%s:%d", config.Foreman, config.Port)

	srv := http.Server{
		Addr:      addrPortPair,
		Handler:   mux,
		TLSConfig: tlsConfig,
	}

	// enable http/2 support
	if err := http2.ConfigureServer(&srv, nil); err != nil {
		return fmt.Errorf("error adding http/2 support: %v", err)
	}
	// tlsConfig.NextProtos = append(tlsConfig.NextProtos, "h2")

	if err := srv.ListenAndServeTLS("", ""); err != nil {
		return fmt.Errorf("Error starting foreman: %v", err)
	}

	return nil
}
