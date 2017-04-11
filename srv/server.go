package srv

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/sfstewman/fuq"
	"github.com/sfstewman/fuq/websocket"
	"golang.org/x/net/http2"
	"log"
	"net/http"
	"os"
	"time"
)

const (
	ServerCookie    = `fuq_foreman_auth`
	ServerCookieAge = 14 * 24 * time.Hour
	MaxCookieLength = 128

	HelloPath = "hello"

	NodeReauthPath     = "node/reauth"
	NodePersistentPath = "node/persistent"

	JobRequestPath = "job/request"
	JobUpdatePath  = "job/update"

	ClientNodeListPath     = "client/nodes/list"
	ClientNodeShutdownPath = "client/nodes/shutdown"
	ClientJobListPath      = "client/job/list"
	ClientJobNewPath       = "client/job/new"
	ClientJobClearPath     = "client/job/clear"
	ClientJobStatePath     = "client/job/state"
	ClientShutdownPath     = "client/shutdown"
)

type nodeInfoKey struct{}

type Handler struct {
	H      http.Handler
	Method string
	Path   string
}

func (h Handler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	if req.Method != h.Method {
		fuq.BadMethod(resp, req)
		return
	}

	if req.URL.Path != h.Path {
		fuq.Forbidden(resp, req)
		return
	}

	h.H.ServeHTTP(resp, req)
}

func AddHandler(mux *http.ServeMux, path string, handler http.Handler) {
	if len(path) > 0 && path[0] != '/' {
		path = "/" + path
	}

	mux.Handle(path, Handler{
		H:      handler,
		Method: "POST",
		Path:   path,
	})
}

func AddHandlerFunc(mux *http.ServeMux, path string, handler http.HandlerFunc) {
	if len(path) > 0 && path[0] != '/' {
		path = "/" + path
	}

	mux.Handle(path, Handler{
		H:      handler,
		Method: "POST",
		Path:   path,
	})
}

func SetupRoutes(s *Server) *http.ServeMux {
	mux := http.NewServeMux()

	log.Printf("Adding handlers")
	AddHandlerFunc(mux, HelloPath, s.HandleHello)
	AddHandlerFunc(mux, NodeReauthPath, s.HandleNodeReauth)

	// XXX - check that this fails on no/bad cookie in tests
	mux.Handle("/"+NodePersistentPath, Handler{
		H:      RequireValidCookie(http.HandlerFunc(s.HandleNodePersistent), s),
		Method: "GET",
		Path:   "/" + NodePersistentPath,
	})

	AddHandler(mux, JobRequestPath, RequireValidCookie(http.HandlerFunc(s.HandleNodeJobRequest), s))
	AddHandler(mux, JobUpdatePath, RequireValidCookie(http.HandlerFunc(s.HandleNodeJobUpdate), s))

	s.AddClientHandler(mux, ClientNodeListPath, s.HandleClientNodeList)
	s.AddClientHandler(mux, ClientNodeShutdownPath, s.HandleClientNodeShutdown)

	s.AddClientHandler(mux, ClientJobListPath, s.HandleClientJobList)
	s.AddClientHandler(mux, ClientJobNewPath, s.HandleClientJobNew)
	s.AddClientHandler(mux, ClientJobClearPath, s.HandleClientJobClear)
	s.AddClientHandler(mux, ClientJobStatePath, s.HandleClientJobState)

	s.AddClientHandler(mux, ClientShutdownPath, s.HandleClientShutdown)

	return mux
}

func StartAPIServer(s *Server, config fuq.Config) error {
	log.Printf("Starting API server on %s:%d",
		config.Foreman, config.Port)

	mux := SetupRoutes(s)

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

type AuthChecker interface {
	CheckAuth(cred string) bool
	CheckClient(client fuq.Client) bool
}

type Server struct {
	*Foreman
	fuq.CookieMaker
	Auth AuthChecker
}

type ServerOpts struct {
	Auth        AuthChecker
	Queuer      JobQueuer
	CookieMaker fuq.CookieMaker
	Done        chan<- struct{}
}

func NewServer(opts ServerOpts) (*Server, error) {
	f, err := NewForeman(opts.Queuer, opts.Done)
	if err != nil {
		return nil, err
	}

	return &Server{
		Foreman:     f,
		CookieMaker: opts.CookieMaker,
		Auth:        opts.Auth,
	}, nil
}

func (s *Server) CheckAuth(cred string) bool {
	return s.Auth.CheckAuth(cred)
}

func (s *Server) CheckClient(client fuq.Client) bool {
	return s.Auth.CheckClient(client)
}

func (s *Server) addAuthCookie(resp http.ResponseWriter, cookie fuq.Cookie) {
	expires := time.Now().Add(ServerCookieAge)
	respCookie := fuq.AsHTTPCookie(ServerCookie, cookie, expires)
	http.SetCookie(resp, respCookie)
}

func (s *Server) HandleHello(resp http.ResponseWriter, req *http.Request) {
	dec := json.NewDecoder(req.Body)
	hello := fuq.Hello{}
	fmt.Fprintf(os.Stderr, "--> HELLO from %s\n", req.RemoteAddr)
	log.Printf("received HELLO request from %s", req.RemoteAddr)
	log.Printf("protocol is %s.  version %d.%d", req.Proto, req.ProtoMajor, req.ProtoMinor)
	if err := dec.Decode(&hello); err != nil {
		log.Printf("error unmarshaling request at %s: %v",
			req.URL, err)
		fuq.BadRequest(resp, req)
		return
	}

	/* check authentication */
	if !s.CheckAuth(hello.Auth) {
		log.Printf("invalid authentication from request %v", req)
		fuq.Forbidden(resp, req)
		return
	}

	log.Printf("--> Making a cookie for %s with info %v\n",
		req.RemoteAddr, hello.NodeInfo)
	cookie, err := s.MakeCookie(hello.NodeInfo)
	if err != nil {
		log.Printf("error registering workers: %v", err)
		fuq.BadRequest(resp, req)
		return
	}

	s.addAuthCookie(resp, cookie)

	ni, _ := s.Lookup(cookie)
	ret := struct {
		Name   string
		Cookie fuq.Cookie
	}{ni.UniqName, cookie}
	enc := json.NewEncoder(resp)
	enc.Encode(&ret)
}

func (s *Server) HandleNodeReauth(resp http.ResponseWriter, req *http.Request) {
	hello := fuq.Hello{}
	envelope := NodeRequestEnvelope{Msg: &hello}

	dec := json.NewDecoder(req.Body)
	if err := dec.Decode(&envelope); err != nil {
		log.Printf("error unmarshaling node request at %s: %v",
			req.URL, err)
		fuq.BadRequest(resp, req)
		return
	}

	log.Printf("received REAUTH request from %s, env=%#v, hello=%#v", req.RemoteAddr, envelope, hello)

	// check cookie
	ni, err := s.Lookup(envelope.Cookie)
	if err != nil {
		log.Printf("error looking up cookie: %v", err)
		fuq.Forbidden(resp, req)
		return
	}

	switch {
	case ni.Node == "":
		log.Printf("REAUTH from %s: stored node name is empty, aborting", req.RemoteAddr)
		fuq.InternalError(resp, req)
		return

	case ni.UniqName != hello.NodeInfo.UniqName:
		fallthrough
	case ni.Node != hello.NodeInfo.Node:
		log.Printf("request (%s,%s) node and unique_name don't agree with stored (%s,%s)",
			hello.NodeInfo.Node, hello.NodeInfo.UniqName,
			ni.Node, ni.UniqName)
		fuq.Forbidden(resp, req)
		return
	}

	if err := s.ExpireCookie(envelope.Cookie); err != nil {
		log.Printf("error expiring cookie: %v", err)
	}

	/* check authentication */
	if !s.CheckAuth(hello.Auth) {
		log.Printf("invalid authentication from request %v", req)
		fuq.Forbidden(resp, req)
		return
	}

	cookie, err := s.RenewCookie(hello.NodeInfo)
	if err != nil {
		log.Printf("error regenerating cookie: %v", err)
		fuq.BadRequest(resp, req)
		return
	}

	ni, err = s.Lookup(cookie)
	if err != nil {
		log.Printf("error looking up cookie: %v", err)
		fuq.InternalError(resp, req)
		return
	}

	s.addAuthCookie(resp, cookie)

	RespondWithJSON(resp, &struct {
		Name   string
		Cookie fuq.Cookie
	}{ni.UniqName, cookie})
}

func (s *Server) HandleNodeJobUpdate(resp http.ResponseWriter, req *http.Request) {
	rawNI := req.Context().Value(nodeInfoKey{})
	ni, ok := rawNI.(fuq.NodeInfo)
	if !ok {
		log.Printf("%s: associated node info is missing or wrong (%v)",
			req.RemoteAddr, rawNI)
		fuq.InternalError(resp, req)
		return
	}

	jobUpdate := fuq.JobStatusUpdate{}
	decoder := json.NewDecoder(req.Body)
	if err := decoder.Decode(&jobUpdate); err != nil {
		log.Printf("error unmarshaling job request at %s: %v",
			req.URL, err)
		fuq.BadRequest(resp, req)
		return
	}

	if err := s.UpdateTaskStatus(jobUpdate); err != nil {
		log.Printf("error updating task status (update=%v): %v",
			jobUpdate, err)
		fuq.InternalError(resp, req)
		return
	}

	if jobUpdate.NewJob == nil {
		emptyTasks := []fuq.Task{}
		RespondWithJSON(resp, emptyTasks)
		return
	}

	jobReq := *jobUpdate.NewJob

	if jobReq.NumProc > ni.NumProc {
		jobReq.NumProc = ni.NumProc
	}

	s.replyToRequest(resp, req, ni, jobReq)
}

func (s *Server) replyWithShutdown(resp http.ResponseWriter, req *http.Request) {
	// FIXME: this is a hack...
	repl := []fuq.Task{
		fuq.Task{
			Task: -1,
			JobDescription: fuq.JobDescription{
				JobId: 0,
				Name:  "::stop::",
			},
		},
	}

	RespondWithJSON(resp, repl)
}

func (s *Server) replyToRequest(resp http.ResponseWriter, req *http.Request, ni fuq.NodeInfo, jobReq fuq.JobRequest) {
	var (
		tasks []fuq.Task
		err   error
	)

	nproc := jobReq.NumProc
	if nproc == 0 {
		nproc = 1
	}

	ctx := req.Context()

	// request jobs
	tasks, err = s.fetchNextTasks(ctx, jobReq.NumProc, ni)
	if err != nil {
		log.Printf("error fetching pending tasks: %v", err)
		fuq.InternalError(resp, req)
		return
	}

	if len(tasks) == 0 {
		if s.IsNodeShutdown(ni.UniqName) {
			s.replyWithShutdown(resp, req)
			return
		}

		log.Printf("request %p canceled from node %s", req, ni.UniqName)
		RespondWithJSON(resp, tasks)
	}

	for _, t := range tasks {
		log.Printf("dispatching task %d:%s:%d to node %s",
			t.JobId, t.Name, t.Task, ni.UniqName)
	}

	RespondWithJSON(resp, tasks)
}

func (s *Server) HandleNodeJobRequest(resp http.ResponseWriter, req *http.Request) {
	rawNI := req.Context().Value(nodeInfoKey{})
	ni, ok := rawNI.(fuq.NodeInfo)
	if !ok {
		log.Printf("%s: associated node info is missing or wrong (%v)",
			req.RemoteAddr, rawNI)
		fuq.InternalError(resp, req)
		return
	}

	jobReq := fuq.JobRequest{}
	decoder := json.NewDecoder(req.Body)
	if err := decoder.Decode(&jobReq); err != nil {
		log.Printf("%s: error unmarshaling job request at %s: %v",
			req.RemoteAddr, req.URL, err)
		fuq.BadRequest(resp, req)
		return
	}

	log.Printf("%s: job request %#v", req.RemoteAddr, jobReq)

	if jobReq.NumProc > ni.NumProc {
		jobReq.NumProc = ni.NumProc
	}

	s.replyToRequest(resp, req, ni, jobReq)
}

/*
func (s *Server) http2Convo(resp http.ResponseWriter, req *http.Request, ni fuq.NodeInfo) {
}
*/

func (s *Server) HandleNodePersistent(resp http.ResponseWriter, req *http.Request) {
	log.Printf("%s: HandleNodePersistent", req.RemoteAddr)
	if req.Proto == "HTTP/2.0" {
		http.Error(resp, "http/2 support not implemented", http.StatusNotImplemented)
		return
	}

	log.Printf("%s: upgrading connection to websocket", req.RemoteAddr)
	messenger, err := websocket.Upgrade(resp, req)
	if err != nil {
		log.Printf("%s: error upgrading connection: %v",
			req.RemoteAddr, err)
		return
	}

	defer func() {
		if !messenger.IsClosed() {
			messenger.Close()
		}
	}()

	rawNI := req.Context().Value(nodeInfoKey{})
	ni, ok := rawNI.(fuq.NodeInfo)
	if !ok {
		log.Printf("%s: associated node info is missing or wrong (%v)",
			req.RemoteAddr, rawNI)
		fuq.InternalError(resp, req)
		return
	}

	// spin up a persistent connection...
	log.Print("%s: spinning up consistent connection", req.RemoteAddr)
	ctx := req.Context()
	loopCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	messenger.Timeout = 5 * time.Minute

	// XXX - deal with errors
	go messenger.Heartbeat(loopCtx, "f2w_"+ni.UniqName)
	pc := newPersistentConn(s.Foreman, ni, messenger)

	s.Foreman.connections.AddConn(pc)
	defer s.Foreman.connections.DelConn(pc)

	err = pc.Loop(loopCtx)

	if err != nil {
		log.Printf("%s: connection error: %v", req.RemoteAddr, err)
	} else {
		log.Printf("%s: connection finished", req.RemoteAddr)
	}
}

func (s *Server) HandleClientNodeList(resp http.ResponseWriter, req *http.Request, mesg []byte) {
	var (
		msg struct {
			CookieList bool `json:"cookie_list"`
			JobList    bool `json:"job_list"`
		}
		nodes []fuq.NodeInfo
		err   error
	)

	if err = json.Unmarshal(mesg, &msg); err != nil {
		log.Printf("error unmarshaling node list request at %s: %v",
			req.URL, err)
		fuq.BadRequest(resp, req)
		return
	}

	if msg.CookieList {
		nodes, err = s.AllNodes()
		if err != nil {
			log.Printf("error fetching all nodes: %v", err)
			fuq.InternalError(resp, req)
			return
		}
	} else {
		err = s.Foreman.connections.EachConn(func(pc *persistentConn) error {
			nodes = append(nodes, pc.NodeInfo)
			return nil
		})
		if err != nil {
			log.Printf("error fetching connected nodes: %v", err)
			fuq.InternalError(resp, req)
			return
		}
	}

	RespondWithJSON(resp, &nodes)
}

func (s *Server) HandleClientNodeShutdown(resp http.ResponseWriter, req *http.Request, mesg []byte) {
	var msg struct {
		UniqNames []string `json:"uniq_names"`
	}

	if err := json.Unmarshal(mesg, &msg); err != nil {
		log.Printf("error unmarshaling node shutdown request at %s: %v",
			req.URL, err)
		fuq.BadRequest(resp, req)
		return
	}

	log.Printf("shutdown: received message: %s", mesg)
	log.Printf("shutdown: unmarshalled: %s", msg)

	for _, n := range msg.UniqNames {
		log.Printf("shutdown request for node '%s'", n)
	}
	s.ShutdownNodes(msg.UniqNames)

	// wakeup all listeners so anything we want to shut down won't
	// keep waiting until a job is queued...
	s.WakeupListeners()

	RespondWithJSON(resp, struct{ ok bool }{true})
}

func (s *Server) HandleClientJobList(resp http.ResponseWriter, req *http.Request, mesg []byte) {
	listReq := fuq.ClientJobListReq{}
	if len(mesg) > 0 {
		if err := json.Unmarshal(mesg, &listReq); err != nil {
			log.Printf("error unmarshaling mesg `%s` (len %d) from request at %s: %v",
				mesg, len(mesg), req.URL, err)
			fuq.BadRequest(resp, req)
			return
		}
	}

	jobs, err := FetchJobs(s, listReq.Name, listReq.Status)
	if err != nil {
		log.Printf("error retrieving job list: %v", err)
		fuq.InternalError(resp, req)
		return
	}

	jtStatus := make([]fuq.JobTaskStatus, len(jobs))

	for i, j := range jobs {
		if listReq.Status == "" && j.Status != fuq.Running && j.Status != fuq.Paused {
			jtStatus[i].Description = j
			continue
		}

		jtStatus[i], err = s.FetchJobTaskStatus(j.JobId)
		if err != nil {
			log.Printf("error retrieving tasks for job %d: %v", j.JobId, err)
			fuq.InternalError(resp, req)
			return
		}
	}

	RespondWithJSON(resp, &jtStatus)
}

func (s *Server) HandleClientJobNew(resp http.ResponseWriter, req *http.Request, mesg []byte) {
	jobDesc := fuq.JobDescription{}

	if err := json.Unmarshal(mesg, &jobDesc); err != nil {
		log.Printf("error unmarshaling request at %s: %v",
			req.URL, err)
		fuq.BadRequest(resp, req)
		return
	}

	/* XXX: log client information.  Requires that we pass
	* fuq.Client in
	 */

	/* validate job */
	if !isValidJob(jobDesc) {
		fuq.BadRequest(resp, req)
		return
	}

	jobId, err := s.AddJob(jobDesc)
	if err != nil {
		log.Printf("error adding job: %v", err)
		fuq.InternalError(resp, req)
		return
	}

	if jobId == 0 {
		log.Printf("invalid job id (%d) but no error", jobId)
		fuq.InternalError(resp, req)
		return
	}

	jobDesc.JobId = jobId

	s.WakeupListeners()
	log.Printf("queued job %v from host %s", jobDesc, req.Host)

	RespondWithJSON(resp, &fuq.NewJobResponse{JobId: jobId})
}

func (s *Server) HandleClientJobClear(resp http.ResponseWriter, req *http.Request, mesg []byte) {
	if err := s.ClearJobs(); err != nil {
		log.Printf("error clearing jobs: %v", err)
		fuq.InternalError(resp, req)
		return
	}

	RespondWithJSON(resp, struct{ ok bool }{true})
}

func (s *Server) HandleClientJobState(resp http.ResponseWriter, req *http.Request, mesg []byte) {
	stateChange := fuq.ClientStateChangeReq{}
	if err := json.Unmarshal(mesg, &stateChange); err != nil {
		log.Printf("error unmarshaling mesg `%s` (len %d) from request at %s: %v",
			mesg, len(mesg), req.URL, err)
		fuq.BadRequest(resp, req)
		return
	}

	var err error
	var newState, prevState fuq.JobStatus

	if len(stateChange.JobIds) == 0 {
		fuq.BadRequest(resp, req)
		return
	}

	switch stateChange.Action {
	case "hold":
		newState = fuq.Paused
	case "release":
		newState = fuq.Waiting
	case "cancel":
		newState = fuq.Cancelled
	default:
		log.Printf("invalid action for changing the job state: %s", stateChange.Action)
		fuq.BadRequest(resp, req)
		return
	}

	/* XXX - this uses N transactions when we could do it in one. */

	// check that all job ids are valid...
	for _, jobId := range stateChange.JobIds {
		_, err = s.FetchJobId(jobId)
		if err != nil {
			log.Printf("%s: error fetching job id %d: %v",
				req.RemoteAddr, jobId, err)
			fuq.BadRequest(resp, req)
			return
		}
	}

	ret := make([]fuq.JobStateChangeResponse, len(stateChange.JobIds))
	for i, jobId := range stateChange.JobIds {
		prevState, err = s.ChangeJobState(jobId, newState)
		if err != nil {
			log.Printf("error changing the job state to %s: %v", stateChange.Action, err)
			fuq.InternalError(resp, req)
			return
		}

		ret[i] = fuq.JobStateChangeResponse{
			JobId:      jobId,
			PrevStatus: prevState,
			NewStatus:  newState,
		}
	}

	RespondWithJSON(resp, &ret)
}

func (s *Server) HandleClientShutdown(resp http.ResponseWriter, req *http.Request, mesg []byte) {
	log.Printf("client shutdown requested")
	if s.Done != nil {
		defer close(s.Done)
	}

	RespondWithJSON(resp, struct{ ok bool }{true})
}

func (s *Server) AddClientHandler(mux *http.ServeMux, path string, handler ClientRequestHandler) {
	AddHandler(mux, path, http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		msg := json.RawMessage{}
		envelope := ClientRequestEnvelope{Msg: &msg}

		dec := json.NewDecoder(req.Body)
		if err := dec.Decode(&envelope); err != nil {
			log.Printf("error unmarshaling node request at %s: %v",
				req.URL, err)
			fuq.BadRequest(resp, req)
			return
		}

		// check auth
		if !s.CheckClient(envelope.Auth) {
			log.Printf("%s: invalid client auth", req.RemoteAddr)
			fuq.Forbidden(resp, req)
			return
		}

		handler(resp, req, []byte(msg))
	}))
}
