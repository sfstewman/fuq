package srv

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/sfstewman/fuq"
	"github.com/sfstewman/fuq/fuqtest"
	"github.com/sfstewman/fuq/proto"
	"github.com/sfstewman/fuq/websocket"
	"math/rand"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sync"
	"testing"
	"time"
)

type okAuth struct{}

func (okAuth) CheckAuth(cred string) bool {
	return true
}

func (okAuth) CheckClient(client fuq.Client) bool {
	return true
}

type simpleCookieJar struct {
	mu sync.Mutex

	nodes      map[string]fuq.NodeInfo
	cookies    map[fuq.Cookie]string
	revCookies map[string]fuq.Cookie
}

func newSimpleCookieJar() *simpleCookieJar {
	return &simpleCookieJar{
		nodes:      make(map[string]fuq.NodeInfo),
		cookies:    make(map[fuq.Cookie]string),
		revCookies: make(map[string]fuq.Cookie),
	}
}

func (jar *simpleCookieJar) IsUniqueName(n string) (bool, error) {
	_, ok := jar.nodes[n]
	return ok, nil
}

func RandomText(r *rand.Rand, txt []byte) {
	for i := 0; i < len(txt); i++ {
		var x int
		if r != nil {
			x = r.Intn(62)
		} else {
			x = rand.Intn(62)
		}

		switch {
		case x < 26:
			txt[i] = byte('A' + x)
		case x < 52:
			txt[i] = byte('a' + (x - 26))
		default:
			txt[i] = byte('0' + (x - 52))
		}
	}
}

func (jar *simpleCookieJar) uniquifyName(ni fuq.NodeInfo) string {
	/* assumes that the lock is held by the caller */
	n := ni.UniqName

	if n != "" && n != ni.Node {
		if _, ok := jar.nodes[n]; !ok {
			return n
		}
	}

	i := 1
	for {
		uniqName := fmt.Sprintf("%s-%d", ni.Node, i)
		if _, ok := jar.nodes[n]; !ok {
			return uniqName
		}

		i++
	}
}

func (jar *simpleCookieJar) generateCookie() fuq.Cookie {
	/* assumes that the lock is held by the caller */
	var rawCookie [8]byte
	var cookie fuq.Cookie

	for {
		RandomText(nil, rawCookie[:])
		cookie = fuq.Cookie(rawCookie[:])
		if _, ok := jar.cookies[cookie]; !ok {
			return cookie
		}
	}
}

func (jar *simpleCookieJar) MakeCookie(ni fuq.NodeInfo) (fuq.Cookie, error) {
	jar.mu.Lock()
	defer jar.mu.Unlock()

	uniqName := jar.uniquifyName(ni)
	ni.UniqName = uniqName
	jar.nodes[uniqName] = ni

	if _, ok := jar.revCookies[uniqName]; ok {
		panic("cookie already associated with unique name " + ni.UniqName)
	}

	cookie := jar.generateCookie()
	jar.cookies[cookie] = uniqName
	jar.revCookies[uniqName] = cookie

	// fmt.Printf("\n\n(%v).MakeCookie finished.  revCookies map is %v\n\n", jar, jar.revCookies)

	return cookie, nil
}

func (jar *simpleCookieJar) RenewCookie(ni fuq.NodeInfo) (fuq.Cookie, error) {
	jar.mu.Lock()
	defer jar.mu.Unlock()

	// fmt.Printf("\n\n(%v).RenewCookie called.  revCookies map is %v\n\n",
	//	jar, jar.revCookies)

	uniqName := ni.UniqName
	prevCookie, ok := jar.revCookies[uniqName]
	if !ok {
		panic(fmt.Sprintf("no cookie associated with uniq name '%s'", uniqName))
	}

	delete(jar.cookies, prevCookie)

	cookie := jar.generateCookie()
	jar.cookies[cookie] = uniqName
	jar.revCookies[uniqName] = cookie

	return cookie, nil
}

func (jar *simpleCookieJar) ExpireCookie(c fuq.Cookie) error {
	jar.mu.Lock()
	defer jar.mu.Unlock()

	_, ok := jar.cookies[c]
	if !ok {
		panic("cannot expired unrecognized cookie")
	}

	delete(jar.cookies, c)

	return nil
}

func (jar *simpleCookieJar) Lookup(c fuq.Cookie) (fuq.NodeInfo, error) {
	jar.mu.Lock()
	defer jar.mu.Unlock()

	uniqName, ok := jar.cookies[c]
	if !ok {
		return fuq.NodeInfo{}, fmt.Errorf("unknown cookie: %s", c)
	}

	ni, ok := jar.nodes[uniqName]
	if !ok {
		panic(fmt.Sprintf("cookie '%s' associated with invalid unique name %s",
			c, uniqName))
	}

	return ni, nil
}

func (jar *simpleCookieJar) LookupName(uniqName string) (fuq.NodeInfo, fuq.Cookie, error) {
	var (
		cookie fuq.Cookie
		ni     fuq.NodeInfo
		ok     bool
	)

	jar.mu.Lock()
	defer jar.mu.Unlock()

	cookie, ok = jar.revCookies[uniqName]
	if !ok {
		return ni, cookie, fmt.Errorf("no cookie associated with name '%s'", uniqName)
	}

	ni, ok = jar.nodes[uniqName]
	if !ok {
		return ni, cookie, fmt.Errorf("no node associated with name '%s'", uniqName)
	}

	return ni, cookie, nil
}

func (jar *simpleCookieJar) AllNodes() ([]fuq.NodeInfo, error) {
	jar.mu.Lock()
	defer jar.mu.Unlock()

	nodes := make([]fuq.NodeInfo, len(jar.nodes))
	i := 0
	for _, ni := range jar.nodes {
		nodes[i] = ni
		i++
	}

	return nodes, nil
}

func newTestingServer() *Server {
	s, err := NewServer(ServerOpts{
		Auth:        okAuth{},
		Queuer:      &simpleQueuer{},
		CookieMaker: newSimpleCookieJar(),
		Done:        make(chan struct{}),
	})

	if err != nil {
		panic(fmt.Sprintf("error making server: %v", err))
	}

	return s
}

type roundTrip struct {
	T *testing.T

	Msg    interface{}
	Dst    interface{}
	Target string
}

func (rt roundTrip) TestHandler(fn func(http.ResponseWriter, *http.Request)) *http.Response {
	t := rt.T

	encode, err := json.Marshal(rt.Msg)
	if err != nil {
		t.Fatalf("error marshaling message: %v", err)
	}

	req := httptest.NewRequest("POST", rt.Target, bytes.NewReader(encode))
	wr := httptest.NewRecorder()

	fn(wr, req)

	resp := wr.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("response was '%s', not OK", resp.Status)
	}

	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(rt.Dst); err != nil {
		t.Fatalf("error unmarshaling response: %v", err)
	}

	return resp
}

func (rt roundTrip) TestClientHandler(fn func(http.ResponseWriter, *http.Request, []byte)) {
	t := rt.T

	mesg, err := json.Marshal(rt.Msg)
	if err != nil {
		t.Fatalf("error marshaling message: %v", err)
	}

	env := ClientRequestEnvelope{
		Auth: fuq.Client{Password: "some_password", Client: "some_client"},
		Msg:  rt.Msg,
	}

	encode, err := json.Marshal(&env)
	if err != nil {
		t.Fatalf("error marshaling client request: %v", err)
	}

	req := httptest.NewRequest("POST", rt.Target, bytes.NewReader(encode))
	wr := httptest.NewRecorder()

	fn(wr, req, mesg)

	resp := wr.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("response was '%s', not OK", resp.Status)
	}

	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(rt.Dst); err != nil {
		t.Fatalf("error unmarshaling response: %v", err)
	}
}

func (rt roundTrip) TestNodeHandler(fn func(http.ResponseWriter, *http.Request, []byte, fuq.NodeInfo), ni fuq.NodeInfo) {
	t := rt.T

	mesg, err := json.Marshal(rt.Msg)
	if err != nil {
		t.Fatalf("error marshaling message: %v", err)
	}

	env := ClientRequestEnvelope{
		Auth: fuq.Client{Password: "some_password", Client: "some_client"},
		Msg:  rt.Msg,
	}

	encode, err := json.Marshal(&env)
	if err != nil {
		t.Fatalf("error marshaling client request: %v", err)
	}

	req := httptest.NewRequest("POST", rt.Target, bytes.NewReader(encode))
	wr := httptest.NewRecorder()

	fn(wr, req, mesg, ni)

	resp := wr.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("response was '%s', not OK", resp.Status)
	}

	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(rt.Dst); err != nil {
		t.Fatalf("error unmarshaling response: %v", err)
	}
}

func sendHello(t *testing.T, s *Server, hello fuq.Hello, envp *HelloResponseEnv) *http.Response {
	return roundTrip{
		T:      t,
		Msg:    &hello,
		Dst:    envp,
		Target: "/hello",
	}.TestHandler(s.HandleHello)
}

func checkCookieInfo(t *testing.T, s *Server, cookie fuq.Cookie, ni fuq.NodeInfo) {
	jar := s.CookieMaker.(*simpleCookieJar)

	// test against cookie jar information
	ni1, cookie1, err := jar.LookupName(ni.UniqName)
	if err != nil {
		t.Fatalf("error looking up cookie info for '%s': %v", ni.UniqName, err)
	}

	if cookie != cookie1 {
		t.Fatalf("cookies do not match: expected %s but found %s",
			cookie, cookie1)
	}

	if !reflect.DeepEqual(ni, ni1) {
		t.Fatalf("node info is different: expected %v but found %v",
			ni, ni1)
	}

	ni2, err := jar.Lookup(cookie)
	if err != nil {
		t.Fatalf("error looking up info for cookie '%s': %v", cookie, err)
	}

	if !reflect.DeepEqual(ni, ni2) {
		t.Fatalf("node info is different: expected %v but found %v",
			ni, ni2)
	}
}

func checkWebCookie(t *testing.T, cookie fuq.Cookie, resp *http.Response) {
	webCookies := resp.Cookies()
	if len(webCookies) == 0 {
		t.Fatalf("no web cookies, but expected '%s' cookie to be set",
			ServerCookie)
	}

	for _, c := range webCookies {
		switch {
		case c.Name != ServerCookie:
			continue

		case c.Value != string(cookie):
			t.Logf("http cookies: %v", webCookies)
			t.Fatalf("http cookie '%s' does not match json cookie '%s'",
				c.Value, cookie)

		case c.HttpOnly == false:
			t.Fatalf("http cookie should HttpOnly attribute set")

		default:
			// make sure that it was issued as expiring
			// later
			if now := time.Now(); now.After(c.Expires) {
				t.Fatalf("cookie should not be expired: now=%s, expires=%s",
					now, c.Expires)
			}

			t.Logf("cookie is good, expiry is %v", c.Expires)

			return
		}
	}

	t.Fatalf("could not find Foreman cookie named '%s'", ServerCookie)
}

func TestServerHello(t *testing.T) {
	s := newTestingServer()

	ni := makeNodeInfo()
	hello := fuq.Hello{
		Auth:     "dummy_auth",
		NodeInfo: ni,
	}

	env := HelloResponseEnv{}
	resp := sendHello(t, s, hello, &env)

	if env.Name == nil || env.Cookie == nil {
		t.Fatalf("response is incomplete: %#v", env)
	}

	ni.UniqName = *env.Name
	cookie := *env.Cookie

	checkCookieInfo(t, s, cookie, ni)
	checkWebCookie(t, cookie, resp)
}

func TestServerNodeReauth(t *testing.T) {
	s := newTestingServer()
	ni := makeNodeInfo()
	hello := fuq.Hello{
		Auth:     "dummy_auth",
		NodeInfo: ni,
	}
	env := HelloResponseEnv{}
	sendHello(t, s, hello, &env)

	ni.UniqName = *env.Name
	cookie := *env.Cookie

	hello.NodeInfo = ni
	reqEnv := NodeRequestEnvelope{
		Cookie: cookie,
		Msg:    &hello,
	}

	ret := HelloResponseEnv{}
	resp := roundTrip{
		T:      t,
		Msg:    &reqEnv,
		Dst:    &ret,
		Target: "/node/reauth",
	}.TestHandler(s.HandleNodeReauth)

	if ret.Name == nil || ret.Cookie == nil {
		t.Fatalf("reauth response is incomplete: %#v", env)
	}

	newCookie := *ret.Cookie
	checkCookieInfo(t, s, newCookie, ni)
	checkWebCookie(t, newCookie, resp)
}

func serverAuth(t *testing.T, s *Server) (fuq.NodeInfo, fuq.Cookie, *http.Response) {
	ni := makeNodeInfo()
	hello := fuq.Hello{
		Auth:     "dummy_auth",
		NodeInfo: ni,
	}
	env := HelloResponseEnv{}
	resp := sendHello(t, s, hello, &env)

	ni.UniqName = *env.Name
	cookie := *env.Cookie

	return ni, cookie, resp
}

func doNodeAuth(t *testing.T, s *Server) (fuq.NodeInfo, fuq.Cookie) {
	ni := makeNodeInfo()
	cookie, err := s.MakeCookie(ni)
	if err != nil {
		t.Fatalf("could not make a cookie: %v", err)
	}

	return ni, cookie
}

func TestServerNodeJobRequestAndUpdate(t *testing.T) {
	s := newTestingServer()
	ni, cookie, _ := serverAuth(t, s)
	_ = cookie

	queue := s.JobQueuer.(*simpleQueuer)
	id, err := queue.AddJob(fuq.JobDescription{
		Name:       "job1",
		NumTasks:   8,
		WorkingDir: "/foo/bar",
		LoggingDir: "/foo/bar/logs",
		Command:    "/foo/foo_it.sh",
	})

	if err != nil {
		t.Fatalf("error queuing job: %v", err)
	}

	job, err := queue.FetchJobId(id)
	if err != nil {
		t.Fatalf("error fetching job: %v", err)
	}

	if job.Name != "job1" {
		t.Fatalf("expected job name '%s' but found '%s'",
			"job1", job.Name)
	}

	req := fuq.JobRequest{NumProc: 4}
	resp := []fuq.Task{}
	roundTrip{
		T:      t,
		Msg:    &req,
		Dst:    &resp,
		Target: "/job/request",
	}.TestNodeHandler(s.HandleNodeJobRequest, ni)
	t.Logf("response is %v", resp)

	if len(resp) != 4 {
		t.Errorf("expected len(resp) == 4, but len(resp) == %d", len(resp))
	}

	for i, task := range resp {
		if task.Task != i+1 {
			t.Errorf("resp[%d].Task is %d, expected %d",
				i, task.Task, i+1)
		}

		if task.JobDescription != job {
			t.Errorf("resp[%d].JobDescription is %v, expected %v",
				task.JobDescription, job)
		}
	}

	// ensure that the queuer is updated
	job1, err := queue.FetchJobId(id)
	if err != nil {
		t.Fatalf("error fetching job %d: %v", id, err)
	}

	if job1.Status != fuq.Running {
		t.Errorf("expected job %d to have status 'running', but status is '%s'",
			job.JobId, job1.Status)
	}

	job.Status = fuq.Running
	status0 := fuq.JobTaskStatus{
		Description:     job,
		TasksFinished:   0,
		TasksPending:    4,
		TasksRunning:    []int{1, 2, 3, 4},
		TasksWithErrors: []int{},
	}

	status, err := queue.FetchJobTaskStatus(id)
	if err != nil {
		t.Fatalf("error fetching job status: %v", err)
	}

	if !reflect.DeepEqual(status, status0) {
		t.Fatalf("expected job status '%v', but status is '%v'",
			status0, status)
	}

	// send an update
	upd := fuq.JobStatusUpdate{
		JobId:   id,
		Task:    3,
		Success: true,
		Status:  "done",
		NewJob:  &fuq.JobRequest{NumProc: 1},
	}

	resp = []fuq.Task{}
	roundTrip{
		T:      t,
		Msg:    &upd,
		Dst:    &resp,
		Target: "/job/status",
	}.TestNodeHandler(s.HandleNodeJobUpdate, ni)

	// check update
	if len(resp) != 1 {
		t.Fatalf("expected len(resp) == 1, but len(resp) == %d", len(resp))
	}

	if resp[0].Task != 5 || resp[0].JobDescription != job {
		t.Errorf("expected Task 5 of job %d, but received '%v'",
			id, resp[0])
	}

	// check that queue has updated the task information
	status0 = fuq.JobTaskStatus{
		Description:     job,
		TasksFinished:   1,
		TasksPending:    3,
		TasksRunning:    []int{1, 2, 4, 5},
		TasksWithErrors: []int{},
	}

	status, err = queue.FetchJobTaskStatus(id)
	if err != nil {
		t.Fatalf("error fetching job status: %v", err)
	}

	if !reflect.DeepEqual(status, status0) {
		t.Fatalf("expected job status '%v', but status is '%v'",
			status0, status)
	}
}

func TestServerClientJobNew(t *testing.T) {
	s := newTestingServer()

	job0 := fuq.JobDescription{
		Name:       "job1",
		NumTasks:   16,
		WorkingDir: "/foo/bar",
		LoggingDir: "/foo/bar/logs",
		Command:    "/foo/foo_it.sh",
	}

	resp := fuq.NewJobResponse{}
	roundTrip{
		T:      t,
		Msg:    &job0,
		Dst:    &resp,
		Target: "/client/job/new",
	}.TestClientHandler(s.HandleClientJobNew)
	id := resp.JobId

	job0.JobId = id
	job0.Status = fuq.Waiting

	queue := s.JobQueuer.(*simpleQueuer)

	job, err := queue.FetchJobId(id)
	if err != nil {
		t.Fatalf("error fetching by with id '%d': %v", id, err)
	}

	if !reflect.DeepEqual(job, job0) {
		t.Fatalf("expected job %v, found job %v", job0, job)
	}
}

func TestServerClientJobList(t *testing.T) {
	s := newTestingServer()

	jobs := []fuq.JobDescription{
		{
			Name:       "job1",
			NumTasks:   16,
			WorkingDir: "/foo/bar",
			LoggingDir: "/foo/bar/logs",
			Command:    "/foo/foo_it.sh",
		},
		{
			Name:       "job2",
			NumTasks:   27,
			WorkingDir: "/foo/baz",
			LoggingDir: "/foo/baz/logs",
			Command:    "/foo/baz_it.sh",
		},
	}

	for i, j := range jobs {
		id := addJob(t, s.Foreman, j)
		jobs[i].JobId = id
		jobs[i].Status = fuq.Waiting
	}

	queue := s.JobQueuer.(*simpleQueuer)

	resp := []fuq.JobTaskStatus{}
	roundTrip{
		T:      t,
		Msg:    &fuq.ClientJobListReq{},
		Dst:    &resp,
		Target: "/client/job/list",
	}.TestClientHandler(s.HandleClientJobList)
	// t.Logf("response is %v", resp)

	expectedResp := []fuq.JobTaskStatus{
		{
			Description:     jobs[0],
			TasksFinished:   0,
			TasksPending:    0,
			TasksRunning:    nil,
			TasksWithErrors: nil,
		},
		{
			Description:     jobs[1],
			TasksFinished:   0,
			TasksPending:    0,
			TasksRunning:    nil,
			TasksWithErrors: nil,
		},
	}

	if !reflect.DeepEqual(resp, expectedResp) {
		t.Fatalf("expected response '%#v', but found '%#v'",
			expectedResp, resp)
	}

	tasks, err := queue.FetchPendingTasks(17)
	if err != nil {
		t.Fatalf("error fetching tasks: %v", err)
	}

	if len(tasks) != 17 {
		t.Fatalf("expected 17 tasks returned, but recevied %d", len(tasks))
	}

	resp = []fuq.JobTaskStatus{}
	roundTrip{
		T:      t,
		Msg:    &fuq.ClientJobListReq{},
		Dst:    &resp,
		Target: "/client/job/list",
	}.TestClientHandler(s.HandleClientJobList)
	// t.Logf("response is %v", resp)

	jobs[0].Status = fuq.Running
	jobs[1].Status = fuq.Running
	expectedResp = []fuq.JobTaskStatus{
		{
			Description:     jobs[0],
			TasksFinished:   0,
			TasksPending:    0,
			TasksRunning:    []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			TasksWithErrors: []int{},
		},
		{
			Description:     jobs[1],
			TasksFinished:   0,
			TasksPending:    26,
			TasksRunning:    []int{1},
			TasksWithErrors: []int{},
		},
	}

	if !reflect.DeepEqual(resp, expectedResp) {
		t.Fatalf("expected response '%#v', but found '%#v'",
			expectedResp, resp)
	}

	queue.UpdateTaskStatus(fuq.JobStatusUpdate{
		JobId:   jobs[0].JobId,
		Task:    14,
		Success: true,
		Status:  "done",
	})

	queue.UpdateTaskStatus(fuq.JobStatusUpdate{
		JobId:   jobs[0].JobId,
		Task:    7,
		Success: false,
		Status:  "error: something went wrong",
	})

	queue.UpdateTaskStatus(fuq.JobStatusUpdate{
		JobId:   jobs[1].JobId,
		Task:    1,
		Success: true,
		Status:  "done",
	})

	resp = []fuq.JobTaskStatus{}
	roundTrip{
		T:      t,
		Msg:    &fuq.ClientJobListReq{},
		Dst:    &resp,
		Target: "/client/job/list",
	}.TestClientHandler(s.HandleClientJobList)
	t.Logf("response is %v", resp)

	expectedResp = []fuq.JobTaskStatus{
		{
			Description:     jobs[0],
			TasksFinished:   1,
			TasksPending:    0,
			TasksRunning:    []int{1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 15, 16},
			TasksWithErrors: []int{7},
		},
		{
			Description:     jobs[1],
			TasksFinished:   1,
			TasksPending:    26,
			TasksRunning:    []int{},
			TasksWithErrors: []int{},
		},
	}

	if !reflect.DeepEqual(resp, expectedResp) {
		t.Fatalf("expected response '%#v', but found '%#v'",
			expectedResp, resp)
	}

	for i := 1; i <= 16; i++ {
		if i == 7 || i == 14 {
			continue
		}

		queue.UpdateTaskStatus(fuq.JobStatusUpdate{
			JobId:   jobs[0].JobId,
			Task:    i,
			Success: true,
			Status:  "done",
		})
	}

	resp = []fuq.JobTaskStatus{}
	roundTrip{
		T:      t,
		Msg:    &fuq.ClientJobListReq{},
		Dst:    &resp,
		Target: "/client/job/list",
	}.TestClientHandler(s.HandleClientJobList)
	// t.Logf("response is %v", resp)

	expectedResp = []fuq.JobTaskStatus{
		{
			Description:     jobs[1],
			TasksFinished:   1,
			TasksPending:    26,
			TasksRunning:    []int{},
			TasksWithErrors: []int{},
		},
	}

	if !reflect.DeepEqual(resp, expectedResp) {
		t.Fatalf("expected response '%#v', but found '%#v'",
			expectedResp, resp)
	}

	resp = []fuq.JobTaskStatus{}
	roundTrip{
		T:      t,
		Msg:    &fuq.ClientJobListReq{Status: "finished"},
		Dst:    &resp,
		Target: "/client/job/list",
	}.TestClientHandler(s.HandleClientJobList)
	// t.Logf("response is %v", resp)

	jobs[0].Status = fuq.Finished
	expectedResp = []fuq.JobTaskStatus{
		{
			Description:     jobs[0],
			TasksFinished:   15,
			TasksPending:    0,
			TasksRunning:    []int{},
			TasksWithErrors: []int{7},
		},
	}

	if !reflect.DeepEqual(resp, expectedResp) {
		t.Fatalf("expected response '%#v', but found '%#v'",
			expectedResp, resp)
	}
}

func newTestClient(t *testing.T, s *Server) (*websocket.Messenger, testClient) {
	ni, _, resp := serverAuth(t, s)
	_ = ni

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("error allocating cookie jar: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(s.HandleNodePersistent))
	defer server.Close()

	theURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("error parsing server URL: %s", server.URL)
	}
	jar.SetCookies(theURL, resp.Cookies())

	messenger, _, err := websocket.Dial(server.URL, jar)
	messenger.Timeout = 60 * time.Second

	client := proto.NewConn(proto.Opts{
		Messenger: messenger,
		Worker:    true,
	})

	return messenger, testClient{Conn: client, NodeInfo: ni}
}

func TestServerNodeOnHello(t *testing.T) {
	s := newTestingServer()

	wsConn, client := newTestClient(t, s)
	defer wsConn.Close()
	defer client.Close()

	queue := s.JobQueuer.(*simpleQueuer)
	id, err := queue.AddJob(fuq.JobDescription{
		Name:       "job1",
		NumTasks:   8,
		WorkingDir: "/foo/bar",
		LoggingDir: "/foo/bar/logs",
		Command:    "/foo/foo_it.sh",
	})

	_, _ = id, err

	taskCh := make(chan []fuq.Task)

	var nproc, nrun uint16 = 7, 0
	client.OnMessageFunc(proto.MTypeJob, func(msg proto.Message) proto.Message {
		taskPtr := msg.Data.(*[]fuq.Task)
		tasks := *taskPtr
		t.Logf("%d tasks: %v", len(tasks), tasks)
		nproc -= uint16(len(tasks))
		nrun += uint16(len(tasks))

		if nproc < 0 {
			panic("invalid number of tasks")
		}

		repl := proto.OkayMessage(nproc, nrun, msg.Seq)
		taskCh <- tasks
		return repl
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fuqtest.GoPanicOnError(ctx, client.ConversationLoop)

	msg, err := client.SendHello(ctx, proto.HelloData{
		NumProcs: 7,
		Running:  nil,
	})

	if err != nil {
		t.Fatalf("error in HELLO: %v", err)
	}

	if msg.Type != proto.MTypeOK {
		t.Fatalf("expected OK reply, but received %v", msg)
	}

	np, nr := msg.AsOkay()
	if np != 7 && nr != 0 {
		t.Fatalf("expected OK(7|0), but received OK(%d|%d)", nproc, nrun)
	}

	// The foreman should dispatch nproc tasks
	tasks := <-taskCh
	if len(tasks) != 7 {
		t.Fatalf("expected 7 tasks to be queued")
	}

	if nproc != 0 && nrun != 7 {
		t.Fatalf("expected nproc=%d and nrun=%d, but found nproc=%d and nrun=%d",
			0, 7, nproc, nrun)
	}
}

func TestServerNodeOnUpdate(t *testing.T) {
	s := newTestingServer()

	wsConn, client := newTestClient(t, s)
	defer wsConn.Close()
	defer client.Close()

	queue := s.JobQueuer.(*simpleQueuer)
	id, err := queue.AddJob(fuq.JobDescription{
		Name:       "job1",
		NumTasks:   8,
		WorkingDir: "/foo/bar",
		LoggingDir: "/foo/bar/logs",
		Command:    "/foo/foo_it.sh",
	})

	_, _ = id, err

	taskCh := make(chan []fuq.Task)

	var nproc, nrun uint16 = 7, 0
	client.OnMessageFunc(proto.MTypeJob, func(msg proto.Message) proto.Message {
		taskPtr := msg.Data.(*[]fuq.Task)
		tasks := *taskPtr
		t.Logf("%d tasks: %v", len(tasks), tasks)
		nproc -= uint16(len(tasks))
		nrun += uint16(len(tasks))

		if nproc < 0 {
			panic("invalid number of tasks")
		}

		repl := proto.OkayMessage(nproc, nrun, msg.Seq)
		taskCh <- tasks
		return repl
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go client.ConversationLoop(ctx)

	msg, err := client.SendHello(ctx, proto.HelloData{
		NumProcs: 7,
		Running:  nil,
	})

	if err != nil {
		t.Fatalf("error in HELLO: %v", err)
	}

	if msg.Type != proto.MTypeOK {
		t.Fatalf("expected OK reply, but received %v", msg)
	}

	np, nr := msg.AsOkay()
	if np != 7 || nr != 0 {
		t.Fatalf("expected OK(7|0), but received OK(%d|%d)", nproc, nrun)
	}

	// The foreman should dispatch nproc tasks
	tasks := <-taskCh
	if len(tasks) != 7 {
		t.Fatalf("expected 7 tasks to be queued")
	}

	if nproc != 0 && nrun != 7 {
		t.Fatalf("expected nproc=%d and nrun=%d, but found nproc=%d and nrun=%d",
			0, 7, nproc, nrun)
	}

	msg, err = client.SendUpdate(ctx, fuq.JobStatusUpdate{
		JobId:   tasks[3].JobId,
		Task:    tasks[3].Task,
		Success: true,
		Status:  "done",
	})

	if err != nil {
		t.Fatalf("error sending job update: %v", err)
	}

	if msg.Type != proto.MTypeOK {
		t.Fatalf("expected OK(1|6), but message is %v", msg)
	}

	np, nr = msg.AsOkay()
	if np != 1 || nr != 6 {
		t.Fatalf("expected OK(1|6), received OK(%d|%d)", np, nr)
	}

	tasks1 := <-taskCh
	if len(tasks1) != 1 {
		t.Fatalf("expected 1 new task to be queued, received %v", tasks)
	}

	// check that updated task is marked as no longer running
	taskStatus, err := queue.FetchJobTaskStatus(tasks[3].JobId)
	if err != nil {
		t.Fatalf("error when fetching job task status")
	}

	desc := tasks[3].JobDescription
	desc.Status = fuq.Running

	expectedStatus := fuq.JobTaskStatus{
		Description:     desc,
		TasksFinished:   1,
		TasksPending:    0,
		TasksRunning:    []int{1, 2, 3, 5, 6, 7, 8},
		TasksWithErrors: []int{},
	}

	if !reflect.DeepEqual(taskStatus, expectedStatus) {
		t.Log("task status may not be updated")
		t.Fatalf("expected taskStatus='%v', but found '%v'",
			expectedStatus, taskStatus)
	}
}

func TestPConnNodeNewJobsQueued(t *testing.T) {
	s := newTestingServer()
	queue := s.JobQueuer.(*simpleQueuer)

	wsConn, client := newTestClient(t, s)
	defer wsConn.Close()
	defer client.Close()

	taskCh := make(chan []fuq.Task)

	var nproc, nrun uint16 = 8, 0
	client.OnMessageFunc(proto.MTypeJob, func(msg proto.Message) proto.Message {
		taskPtr := msg.Data.(*[]fuq.Task)
		tasks := *taskPtr

		t.Logf("onJob received %d tasks: %v", len(tasks), tasks)
		nproc -= uint16(len(tasks))
		nrun += uint16(len(tasks))

		if nproc < 0 {
			panic("invalid number of tasks")
		}

		repl := proto.OkayMessage(nproc, nrun, msg.Seq)
		taskCh <- tasks
		return repl
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go client.ConversationLoop(ctx)

	msg, err := client.SendHello(ctx, proto.HelloData{
		NumProcs: 8,
		Running:  nil,
	})

	if err != nil {
		t.Fatalf("error in HELLO: %v", err)
	}

	if msg.Type != proto.MTypeOK {
		t.Fatalf("expected OK reply, but received %v", msg)
	}

	np, nr := msg.AsOkay()
	if np != 8 || nr != 0 {
		t.Fatalf("expected OK(8|0), but received OK(%d|%d)", nproc, nrun)
	}

	syncCh := make(chan struct{})
	go func(q JobQueuer, signal chan struct{}) {
		_, err := q.AddJob(fuq.JobDescription{
			Name:       "job1",
			NumTasks:   8,
			WorkingDir: "/foo/bar",
			LoggingDir: "/foo/bar/logs",
			Command:    "/foo/foo_it.sh",
		})

		if err != nil {
			panic(fmt.Sprintf("error adding job: %v", err))
		}

		close(signal)
		s.WakeupListeners()
	}(queue, syncCh)

	order := make([]int, 0, 2)
	var tasks []fuq.Task
	for len(order) < 2 {
		select {
		case _, ok := <-syncCh:
			order = append(order, 1)
			if !ok {
				syncCh = nil
			}

		case tasks = <-taskCh:
			order = append(order, 2)
		}
	}

	if order[0] != 1 || order[1] != 2 {
		t.Fatalf("sequence 1,2 expected, but found %v", order)
	}

	if len(tasks) != 8 {
		t.Fatalf("expected 8 tasks to be queued")
	}

	if nproc != 0 && nrun != 8 {
		t.Fatalf("expected nproc=%d and nrun=%d, but found nproc=%d and nrun=%d",
			0, 8, nproc, nrun)
	}
}

func TestPConnStop(t *testing.T) {
	s := newTestingServer()
	q := s.JobQueuer.(*simpleQueuer)

	_, err := q.AddJob(fuq.JobDescription{
		Name:       "job1",
		NumTasks:   1,
		WorkingDir: "/foo/bar",
		LoggingDir: "/foo/bar/logs",
		Command:    "/foo/foo_it.sh",
	})

	if err != nil {
		panic(fmt.Sprintf("error adding job: %v", err))
	}

	wsConn, client := newTestClient(t, s)
	defer wsConn.Close()
	defer client.Close()

	ni := client.NodeInfo

	msgCh := make(chan proto.Message)
	taskCh := make(chan []fuq.Task)

	var nproc, nrun, nstop uint16 = 8, 0, 0
	client.OnMessageFunc(proto.MTypeJob, func(msg proto.Message) proto.Message {
		taskPtr := msg.Data.(*[]fuq.Task)
		tasks := *taskPtr

		if len(tasks) > int(nproc) {
			panic("invalid number of tasks")
		}

		t.Logf("onJob received %d tasks: %v", len(tasks), tasks)
		nproc -= uint16(len(tasks))
		nrun += uint16(len(tasks))

		repl := proto.OkayMessage(nproc, nrun, msg.Seq)
		taskCh <- tasks
		return repl
	})

	client.OnMessageFunc(proto.MTypeStop, func(msg proto.Message) proto.Message {
		ns := uint16(msg.AsStop())

		if ns+nstop > nproc+nrun {
			panic("nstop > nproc+nrun")
		}

		nstop += ns

		np := int(nproc) - int(nstop)
		if np < 0 {
			np = 0
		}

		repl := proto.OkayMessage(uint16(np), nrun, msg.Seq)
		msgCh <- msg

		return repl
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fuqtest.GoPanicOnError(ctx, client.ConversationLoop)

	msg, err := client.SendHello(ctx, proto.HelloData{
		NumProcs: 8,
		Running:  nil,
	})

	if err != nil {
		t.Fatalf("error in HELLO: %v", err)
	}

	if msg.Type != proto.MTypeOK {
		t.Fatalf("expected OK reply, but received %v", msg)
	}

	np, nr := msg.AsOkay()
	if np != 8 || nr != 0 {
		t.Fatalf("expected OK(8|0), but received OK(%d|%d)", nproc, nrun)
	}

	tasks := <-taskCh
	// JOB message received
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, but received %d tasks", len(tasks))
	}

	s.ShutdownNodes([]string{ni.UniqName})
	s.WakeupListeners()
	msg = <-msgCh
	// STOP message received

	// make sure that we can still send an UPDATE on the job that's
	// running
	msg, err = client.SendUpdate(ctx, fuq.JobStatusUpdate{
		JobId:   tasks[0].JobId,
		Task:    tasks[0].Task,
		Success: true,
		Status:  "done",
	})

	if err != nil {
		t.Fatalf("error sending job update: %v", err)
	}

	if msg.Type != proto.MTypeOK {
		t.Fatalf("expected OK(0|0), but message is %v", msg)
	}

	np, nr = msg.AsOkay()
	if np != 0 || nr != 0 {
		t.Fatalf("expected OK(0|0), but received OK(%d,%d)", np, nr)
	}
}
