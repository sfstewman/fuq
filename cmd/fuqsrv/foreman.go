package main

import (
	"encoding/json"
	"fmt"
	"github.com/sfstewman/fuq"
	"github.com/sfstewman/fuq/srv"
	"golang.org/x/net/http2"
	"log"
	"net/http"
	"os"
	"sync"
)

type Foreman struct {
	srv.JobQueuer
	srv.CookieMaker
	stores *srv.Stores

	// XXX: worth replacing with something that can scale?
	// we need to lock the database with every request; should
	// we just use that lock?
	shutdownReq struct {
		mu           sync.RWMutex
		shutdownHost map[string]struct{}
	}

	Config fuq.Config

	jobsSignal struct {
		mu   sync.Mutex
		cond *sync.Cond
	}
	Done chan<- struct{}
}

func (f *Foreman) Close() error {
	return f.stores.Close()
}

func NewForeman(config fuq.Config, done chan<- struct{}) (*Foreman, error) {
	stores, err := srv.NewStores(srv.Files{Jobs: config.DbPath})
	if err != nil {
		return nil, err
	}

	f := Foreman{
		Config:      config,
		JobQueuer:   stores.Jobs,
		CookieMaker: stores.Cookies,
		stores:      stores,
		Done:        done,
	}

	f.jobsSignal.cond = sync.NewCond(&f.jobsSignal.mu)
	f.shutdownReq.shutdownHost = make(map[string]struct{})

	return &f, nil
}

func (f *Foreman) IsNodeShutdown(name string) bool {
	f.shutdownReq.mu.RLock()
	defer f.shutdownReq.mu.RUnlock()

	_, ok := f.shutdownReq.shutdownHost[name]
	log.Printf("checking if node %s has a shutdown request: %v",
		name, ok)
	return ok
}

func (f *Foreman) CheckAuth(cred string) bool {
	return f.Config.CheckAuth(cred)
}

func (f *Foreman) CheckClient(client fuq.Client) bool {
	return f.Config.CheckClient(client)
}

func (f *Foreman) HandleHello(resp http.ResponseWriter, req *http.Request) {
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
	if !f.CheckAuth(hello.Auth) {
		log.Printf("invalid authentication from request %v", req)
		fuq.Forbidden(resp, req)
		return
	}

	fmt.Fprintf(os.Stderr, "--> Making a cookie for %s with info %v\n",
		req.RemoteAddr, hello.NodeInfo)
	cookie, err := f.MakeCookie(hello.NodeInfo)
	if err != nil {
		log.Printf("error registering workers: %v", err)
		fuq.BadRequest(resp, req)
		return
	}

	ni, _ := f.Lookup(cookie)
	ret := struct {
		Name   string
		Cookie fuq.Cookie
	}{ni.UniqName, cookie}
	enc := json.NewEncoder(resp)
	enc.Encode(&ret)
}

func (f *Foreman) HandleNodeReauth(resp http.ResponseWriter, req *http.Request) {
	hello := fuq.Hello{}
	envelope := srv.NodeRequestEnvelope{Msg: &hello}

	dec := json.NewDecoder(req.Body)
	if err := dec.Decode(&envelope); err != nil {
		log.Printf("error unmarshaling node request at %s: %v",
			req.URL, err)
		fuq.BadRequest(resp, req)
		return
	}

	log.Printf("received REAUTH request from %s, env=%#v, hello=%#v", req.Host, envelope, hello)

	// check cookie
	ni, err := f.Lookup(envelope.Cookie)
	if err != nil {
		log.Printf("error looking up cookie: %v", err)
		fuq.InternalError(resp, req)
		return
	}

	if ni.Node != "" {
		if err := f.ExpireCookie(envelope.Cookie); err != nil {
			log.Printf("error expiring cookie: %v", err)
		}
	}

	/* check authentication */
	if !f.CheckAuth(hello.Auth) {
		log.Printf("invalid authentication from request %v", req)
		fuq.Forbidden(resp, req)
		return
	}

	cookie, err := f.RenewCookie(hello.NodeInfo)
	if err != nil {
		log.Printf("error regenerating cookie: %v", err)
		fuq.BadRequest(resp, req)
		return
	}

	ni, err = f.Lookup(cookie)
	if err != nil {
		log.Printf("error looking up cookie: %v", err)
		fuq.InternalError(resp, req)
		return
	}

	srv.RespondWithJSON(resp, &struct {
		Name   string
		Cookie fuq.Cookie
	}{ni.UniqName, cookie})
}

func (f *Foreman) HandleNodeJobUpdate(resp http.ResponseWriter, req *http.Request, mesg []byte, ni fuq.NodeInfo) {
	jobUpdate := fuq.JobStatusUpdate{}
	if err := json.Unmarshal(mesg, &jobUpdate); err != nil {
		log.Printf("error unmarshaling job request at %s: %v",
			req.URL, err)
		fuq.BadRequest(resp, req)
		return
	}

	if err := f.UpdateTaskStatus(jobUpdate); err != nil {
		log.Printf("error updating task status (update=%v): %v",
			jobUpdate, err)
		fuq.InternalError(resp, req)
		return
	}

	if jobUpdate.NewJob == nil {
		fuq.OK(resp, req)
		return
	}

	jobReq := *jobUpdate.NewJob

	if jobReq.NumProc > ni.NumProc {
		jobReq.NumProc = ni.NumProc
	}

	f.replyToRequest(resp, req, ni, jobReq)
}

func (f *Foreman) replyWithShutdown(resp http.ResponseWriter, req *http.Request) {
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

	srv.RespondWithJSON(resp, repl)
}

func (f *Foreman) replyToRequest(resp http.ResponseWriter, req *http.Request, ni fuq.NodeInfo, jobReq fuq.JobRequest) {
	var (
		jobsAvail chan struct{}
		tasks     []fuq.Task
		err       error
	)

	nproc := jobReq.NumProc
	if nproc == 0 {
		nproc = 1
	}

	ctx := req.Context()

	for {
		if f.IsNodeShutdown(ni.UniqName) {
			f.replyWithShutdown(resp, req)
			return
		}

		// request jobs
		tasks, err = f.FetchPendingTasks(jobReq.NumProc)
		if err != nil {
			log.Printf("error fetching pending tasks: %v", err)
			fuq.InternalError(resp, req)
			return
		}

		if len(tasks) > 0 {
			for _, t := range tasks {
				log.Printf("dispatching task %d:%s:%d to node %s",
					t.JobId, t.Name, t.Task, ni.UniqName)
			}

			srv.RespondWithJSON(resp, tasks)
			return
		}

		// lazy allocation so we don't allocate if we don't need
		// it
		if jobsAvail == nil {
			jobsAvail = make(chan struct{})
		}

		go func() {
			f.jobsSignal.mu.Lock()
			defer f.jobsSignal.mu.Unlock()
			log.Printf("request from %s waiting on more jobs", ni.UniqName)
			f.jobsSignal.cond.Wait()
			jobsAvail <- struct{}{}
		}()

		select {
		case <-ctx.Done():
			log.Printf("request %p canceled from node %s", req, ni.UniqName)
			srv.RespondWithJSON(resp, tasks)
		case <-jobsAvail:
			log.Printf("trying to queue jobs for node %s", ni.UniqName)
			continue
			/*
				case <-f.Done:
					// XXX: fast exit -- although we should send a shutdown request
					return
			*/
		}
	}
}

func (f *Foreman) HandleNodeJobRequest(resp http.ResponseWriter, req *http.Request, mesg []byte, ni fuq.NodeInfo) {
	jobReq := fuq.JobRequest{}
	if err := json.Unmarshal(mesg, &jobReq); err != nil {
		log.Printf("error unmarshaling job request at %s: %v",
			req.URL, err)
		fuq.BadRequest(resp, req)
		return
	}

	if jobReq.NumProc > ni.NumProc {
		jobReq.NumProc = ni.NumProc
	}

	f.replyToRequest(resp, req, ni, jobReq)
}

/*
func (f *Foreman) http2Convo(resp http.ResponseWriter, req *http.Request, ni fuq.NodeInfo) {
}
*/

func (f *Foreman) websocketConvo(hj http.Hijacker, req *http.Request, ni fuq.NodeInfo) error {
	conn, bufrw, err := hj.Hijack()
	_ = bufrw
	if err != nil {
		return err
	}
	defer conn.Close()
	return nil
}

func (f *Foreman) HandleNodeConversation(resp http.ResponseWriter, req *http.Request, ni fuq.NodeInfo) {
	if req.Proto == "HTTP/2.0" {
		http.Error(resp, "http/2 support not implemented", http.StatusNotImplemented)
		return
	}

	if hj, ok := resp.(http.Hijacker); ok {
		if err := f.websocketConvo(hj, req, ni); err != nil {
			http.Error(resp, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	http.Error(resp, "requires http/2 or websocket support", http.StatusInternalServerError)
}

func (f *Foreman) HandleClientNodeList(resp http.ResponseWriter, req *http.Request, mesg []byte) {
	nodes, err := f.AllNodes()
	if err != nil {
		log.Printf("error fetching all nodes: %v", err)
		fuq.InternalError(resp, req)
		return
	}

	srv.RespondWithJSON(resp, &nodes)
}

func (f *Foreman) HandleClientNodeShutdown(resp http.ResponseWriter, req *http.Request, mesg []byte) {
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

	f.shutdownReq.mu.Lock()
	defer f.shutdownReq.mu.Unlock()
	for _, n := range msg.UniqNames {
		log.Printf("shutdown request for node '%s'", n)
		f.shutdownReq.shutdownHost[n] = struct{}{}
	}

	// wakeup all listeners so anything we want to shut down won't
	// keep waiting until a job is queued...
	f.WakeupListeners()

	srv.RespondWithJSON(resp, struct{ ok bool }{true})
}

func (f *Foreman) HandleClientJobList(resp http.ResponseWriter, req *http.Request, mesg []byte) {
	listReq := fuq.ClientJobListReq{}
	if len(mesg) > 0 {
		if err := json.Unmarshal(mesg, &listReq); err != nil {
			log.Printf("error unmarshaling mesg `%s` (len %d) from request at %s: %v",
				mesg, len(mesg), req.URL, err)
			fuq.BadRequest(resp, req)
			return
		}
	}

	/*
		if listReq.Status == "" && listReq.Name == "" {
		} else {
		}
	*/

	jobs, err := f.FetchJobs(listReq.Name, listReq.Status)
	if err != nil {
		log.Printf("error retrieving job list: %v", err)
		fuq.InternalError(resp, req)
		return
	}

	jtStatus := make([]fuq.JobTaskStatus, len(jobs))

	for i, j := range jobs {
		if j.Status != fuq.Running {
			jtStatus[i].Description = j
			continue
		}

		jtStatus[i], err = f.FetchJobTaskStatus(j.JobId)
		if err != nil {
			log.Printf("error retrieving tasks for job %d: %v", j.JobId, err)
			fuq.InternalError(resp, req)
			return
		}
	}

	srv.RespondWithJSON(resp, &jtStatus)
}

func isValidJob(job fuq.JobDescription) bool {
	return true
}

func (f *Foreman) WakeupListeners() {
	f.jobsSignal.mu.Lock()
	f.jobsSignal.cond.Broadcast()
	f.jobsSignal.mu.Unlock()
}

func (f *Foreman) HandleClientJobNew(resp http.ResponseWriter, req *http.Request, mesg []byte) {
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

	jobId, err := f.AddJob(jobDesc)
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

	f.WakeupListeners()
	log.Printf("queued job %v from host %s", jobDesc, req.Host)

	srv.RespondWithJSON(resp, &fuq.NewJobResponse{JobId: jobId})
}

func (f *Foreman) HandleClientJobClear(resp http.ResponseWriter, req *http.Request, mesg []byte) {
	if err := f.ClearJobs(); err != nil {
		log.Printf("error clearing jobs: %v", err)
		fuq.InternalError(resp, req)
		return
	}

	srv.RespondWithJSON(resp, struct{ ok bool }{true})
}

func (f *Foreman) HandleClientJobState(resp http.ResponseWriter, req *http.Request, mesg []byte) {
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
	ret := make([]fuq.JobStateChangeResponse, len(stateChange.JobIds))
	for i, jobId := range stateChange.JobIds {
		prevState, err = f.ChangeJobState(jobId, newState)
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

	srv.RespondWithJSON(resp, &ret)
}

func (f *Foreman) HandleClientShutdown(resp http.ResponseWriter, req *http.Request, mesg []byte) {
	log.Printf("client shutdown requested")
	defer close(f.Done)

	srv.RespondWithJSON(resp, struct{ ok bool }{true})
}

func (f *Foreman) AddNodeHandler(mux *http.ServeMux, path string, handler srv.NodeRequestHandler) {
	f.AddHandler(mux, path, http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		msg := json.RawMessage{}
		envelope := srv.NodeRequestEnvelope{Msg: &msg}

		dec := json.NewDecoder(req.Body)
		if err := dec.Decode(&envelope); err != nil {
			log.Printf("error unmarshaling node request at %s: %v",
				req.URL, err)
			fuq.BadRequest(resp, req)
			return
		}

		// check cookie
		ni, err := f.Lookup(envelope.Cookie)
		if err != nil {
			log.Printf("error looking up cookie: %v", err)
			fuq.InternalError(resp, req)
			return
		}

		log.Printf("cookie %s from node %v", envelope.Cookie, ni)

		if ni.Node == "" {
			fuq.Forbidden(resp, req)
			return
		}

		handler(resp, req, []byte(msg), ni)
	}))
}

func (f *Foreman) AddClientHandler(mux *http.ServeMux, path string, handler srv.ClientRequestHandler) {
	f.AddHandler(mux, path, http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		msg := json.RawMessage{}
		envelope := srv.ClientRequestEnvelope{Msg: &msg}

		dec := json.NewDecoder(req.Body)
		if err := dec.Decode(&envelope); err != nil {
			log.Printf("error unmarshaling node request at %s: %v",
				req.URL, err)
			fuq.BadRequest(resp, req)
			return
		}

		// check auth
		if !f.CheckClient(envelope.Auth) {
			log.Printf("invalid client auth %v", envelope.Auth)
			fuq.Forbidden(resp, req)
			return
		}

		handler(resp, req, []byte(msg))
	}))
}

func (f *Foreman) AddHandler(mux *http.ServeMux, path string, handler http.HandlerFunc) {
	mux.HandleFunc(path,
		func(resp http.ResponseWriter, req *http.Request) {
			// log.Printf("request %s %s", req.Method, req.URL)
			if req.Method != "POST" {
				fuq.BadMethod(resp, req)
				return
			}

			if req.URL.Path != path {
				fuq.Forbidden(resp, req)
				return
			}

			// log.Printf("request: %v", req)

			handler(resp, req)
		})
}

func (f *Foreman) StartAPIServer() error {
	log.Printf("Starting API server on %s:%d",
		f.Config.Foreman, f.Config.Port)

	mux := http.NewServeMux()

	log.Printf("Adding handlers")
	f.AddHandler(mux, "/hello", f.HandleHello)
	f.AddHandler(mux, "/node/reauth", f.HandleNodeReauth)

	f.AddNodeHandler(mux, "/job/request", f.HandleNodeJobRequest)
	f.AddNodeHandler(mux, "/job/status", f.HandleNodeJobUpdate)

	f.AddClientHandler(mux, "/client/nodes/list", f.HandleClientNodeList)
	f.AddClientHandler(mux, "/client/nodes/shutdown", f.HandleClientNodeShutdown)

	f.AddClientHandler(mux, "/client/job/list", f.HandleClientJobList)
	f.AddClientHandler(mux, "/client/job/new", f.HandleClientJobNew)
	f.AddClientHandler(mux, "/client/job/clear", f.HandleClientJobClear)
	f.AddClientHandler(mux, "/client/job/state", f.HandleClientJobState)

	f.AddClientHandler(mux, "/client/shutdown", f.HandleClientShutdown)

	tlsConfig, err := fuq.SetupTLS(f.Config)
	if err != nil {
		log.Printf("error setting up TLS: %v", err)
		return fmt.Errorf("Error starting foreman: %v", err)
	}

	addrPortPair := fmt.Sprintf("%s:%d", f.Config.Foreman, f.Config.Port)

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

func (f *Foreman) Run() error {
	// WriteFuqConfig(f.Config)

	// setup API server
	return f.StartAPIServer()
}
