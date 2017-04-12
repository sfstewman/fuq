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
	"io/ioutil"
	"log"
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

type simpleAuth struct {
	cred string
}

func (s simpleAuth) CheckAuth(cred string) bool {
	return cred == s.cred
}

func (s simpleAuth) CheckClient(client fuq.Client) bool {
	return client.Password == s.cred
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
		log.Printf("node %s, trying unique name %s", ni.Node, uniqName)
		if _, ok := jar.nodes[uniqName]; !ok {
			log.Print("success")
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

const (
	// very secure testing password
	testingPass = `dummy_auth1`

	// invalid password
	invalidPass = `foobarbaz`
)

func newTestingServer() *Server {
	s, err := NewServer(ServerOpts{
		Auth:        simpleAuth{testingPass},
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
	Cookie *http.Cookie
	T      *testing.T

	Msg    interface{}
	Dst    interface{}
	Target string
}

func (rt roundTrip) ExpectOK(fn func(http.ResponseWriter, *http.Request)) *http.Response {
	t := rt.T

	resp := rt.ExpectCode(fn, http.StatusOK)
	defer resp.Body.Close()

	if rt.Dst == nil {
		return resp
	}

	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(rt.Dst); err != nil {
		t.Fatalf("error unmarshaling response: %v", err)
	}

	return resp
}

func (rt roundTrip) ExpectCode(fn func(http.ResponseWriter, *http.Request), code int) *http.Response {
	t := rt.T

	encode, err := json.Marshal(rt.Msg)
	if err != nil {
		t.Fatalf("error marshaling message: %v", err)
	}

	target := rt.Target
	if len(target) > 0 && target[0] != '/' {
		target = "/" + target
	}

	req := httptest.NewRequest("POST", target, bytes.NewReader(encode))
	if rt.Cookie != nil {
		req.AddCookie(rt.Cookie)
	}
	wr := httptest.NewRecorder()

	fn(wr, req)

	resp := wr.Result()

	if resp.StatusCode != code {
		t.Fatalf("response was '%s', not %s",
			resp.Status, http.StatusText(code))
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

	target := rt.Target
	if len(target) > 0 && target[0] != '/' {
		target = "/" + target
	}

	req := httptest.NewRequest("POST", target, bytes.NewReader(encode))
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
	}.ExpectOK(s.HandleHello)
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

func checkNoCookies(t *testing.T, resp *http.Response) {
	// make sure we have no cookies attached
	if cookies := resp.Cookies(); len(cookies) > 0 {
		t.Fatalf("expected zero cookies attached, but found %d, cookies=%v",
			len(cookies), cookies)
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

func TestServerHelloSuccess(t *testing.T) {
	s := newTestingServer()

	ni := makeNodeInfo()
	hello := fuq.Hello{
		Auth:     testingPass,
		NodeInfo: ni,
	}

	/* test success case */
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

func TestServerHelloBadRequest(t *testing.T) {
	s := newTestingServer()

	// test bad request (unexpected json): should return BadRequest result
	resp := roundTrip{
		T:      t,
		Msg:    []string{"foo", "bar"},
		Dst:    nil,
		Target: "/hello",
	}.ExpectCode(s.HandleHello, http.StatusBadRequest)
	defer resp.Body.Close()

	// make sure we have no cookies attached to the response
	checkNoCookies(t, resp)
}

func TestServerHelloBadPass(t *testing.T) {
	s := newTestingServer()

	ni := makeNodeInfo()
	hello := fuq.Hello{
		Auth:     invalidPass,
		NodeInfo: ni,
	}

	// test bad password: should return Forbidden result
	resp := roundTrip{
		T:      t,
		Msg:    &hello,
		Dst:    nil,
		Target: "/hello",
	}.ExpectCode(s.HandleHello, http.StatusForbidden)
	defer resp.Body.Close()

	// make sure we have no cookies attached to the response
	checkNoCookies(t, resp)
}

func TestServerNodeReauthSuccess(t *testing.T) {
	s := newTestingServer()
	ni := makeNodeInfo()
	hello := fuq.Hello{
		Auth:     testingPass,
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
	}.ExpectOK(s.HandleNodeReauth)

	if ret.Name == nil || ret.Cookie == nil {
		t.Fatalf("reauth response is incomplete: %#v", env)
	}

	newCookie := *ret.Cookie
	checkCookieInfo(t, s, newCookie, ni)
	checkWebCookie(t, newCookie, resp)
}

func TestServerNodeReauthBadRequest(t *testing.T) {
	s := newTestingServer()
	ni, _, _ := serverAuth(t, s)

	// send bad/unexpected json
	reqEnv := []fuq.Hello{{Auth: testingPass, NodeInfo: ni}}

	resp := roundTrip{
		T:      t,
		Msg:    &reqEnv,
		Target: "/node/reauth",
	}.ExpectCode(s.HandleNodeReauth, http.StatusBadRequest)
	defer resp.Body.Close()

	// make sure we have no cookies attached to the response
	checkNoCookies(t, resp)
}

func TestServerNodeReauthEmptyCookie(t *testing.T) {
	s := newTestingServer()
	ni, _, _ := serverAuth(t, s)

	reqEnv := NodeRequestEnvelope{
		// no Cookie field: make sure empty cookie fails
		Msg: &fuq.Hello{
			Auth:     testingPass,
			NodeInfo: ni,
		},
	}

	resp := roundTrip{
		T:      t,
		Msg:    &reqEnv,
		Target: "/node/reauth",
	}.ExpectCode(s.HandleNodeReauth, http.StatusForbidden)
	defer resp.Body.Close()

	// make sure we have no cookies attached to the response
	checkNoCookies(t, resp)
}

func TestServerNodeReauthBadCookie(t *testing.T) {
	s := newTestingServer()
	ni, cookie, _ := serverAuth(t, s)

	badCookie := "AAAAAAA"
	for string(cookie) == badCookie {
		badCookie = badCookie + "_1"
	}

	reqEnv := NodeRequestEnvelope{
		Cookie: fuq.Cookie(badCookie),
		Msg: &fuq.Hello{
			Auth:     testingPass,
			NodeInfo: ni,
		},
	}

	resp := roundTrip{
		T:      t,
		Msg:    &reqEnv,
		Target: "/node/reauth",
	}.ExpectCode(s.HandleNodeReauth, http.StatusForbidden)
	defer resp.Body.Close()

	// make sure we have no cookies attached to the response
	checkNoCookies(t, resp)
}

func TestServerNodeReauthBadPassword(t *testing.T) {
	s := newTestingServer()
	ni, cookie, _ := serverAuth(t, s)

	reqEnv := NodeRequestEnvelope{
		Cookie: fuq.Cookie(cookie),
		Msg: &fuq.Hello{
			Auth:     invalidPass,
			NodeInfo: ni,
		},
	}

	resp := roundTrip{
		T:      t,
		Msg:    &reqEnv,
		Target: "/node/reauth",
	}.ExpectCode(s.HandleNodeReauth, http.StatusForbidden)
	defer resp.Body.Close()

	// make sure we have no cookies attached to the response
	checkNoCookies(t, resp)
}

func TestServerNodeReauthInvalidNode(t *testing.T) {
	s := newTestingServer()
	ni, cookie, _ := serverAuth(t, s)

	badNodeInfo := ni
	badNodeInfo.Node = "robocop"
	reqEnv := NodeRequestEnvelope{
		Cookie: fuq.Cookie(cookie),
		Msg: &fuq.Hello{
			Auth:     testingPass,
			NodeInfo: badNodeInfo,
		},
	}

	resp := roundTrip{
		T:      t,
		Msg:    &reqEnv,
		Target: "/node/reauth",
	}.ExpectCode(s.HandleNodeReauth, http.StatusForbidden)
	defer resp.Body.Close()

	// make sure we have no cookies attached to the response
	checkNoCookies(t, resp)
}

func TestServerNodeReauthInvalidUniqName(t *testing.T) {
	s := newTestingServer()
	ni, cookie, _ := serverAuth(t, s)

	badNodeInfo := ni
	badNodeInfo.UniqName = "robocop:1"
	reqEnv := NodeRequestEnvelope{
		Cookie: fuq.Cookie(cookie),
		Msg: &fuq.Hello{
			Auth:     testingPass,
			NodeInfo: badNodeInfo,
		},
	}

	resp := roundTrip{
		T:      t,
		Msg:    &reqEnv,
		Target: "/node/reauth",
	}.ExpectCode(s.HandleNodeReauth, http.StatusForbidden)
	defer resp.Body.Close()

	// make sure we have no cookies attached to the response
	checkNoCookies(t, resp)
}

func newCookieTest(t *testing.T) (http.Handler, *http.Cookie) {
	s := newTestingServer()
	_, _, resp := serverAuth(t, s)
	cookies := resp.Cookies()
	if len(cookies) != 1 {
		t.Fatalf("expected one web cookies, found %d: %v",
			len(cookies), cookies)
	}

	handler := RequireValidCookie(
		http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
			fmt.Fprintf(resp, "OK")
		}), s)

	return handler, cookies[0]
}

func TestRequireValidCookieSuccess(t *testing.T) {
	handler, cookie := newCookieTest(t)

	req := httptest.NewRequest("GET", "/", nil)
	req.AddCookie(cookie)
	wr := httptest.NewRecorder()

	handler.ServeHTTP(wr, req)
	resp := wr.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("response was '%s', not OK", resp.Status)
	}
}

func TestRequireValidCookieMissingCookie(t *testing.T) {
	handler, _ := newCookieTest(t)

	req := httptest.NewRequest("GET", "/", nil)
	// don't add cookie
	wr := httptest.NewRecorder()

	handler.ServeHTTP(wr, req)
	resp := wr.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("response was '%s', not Unauthorized", resp.Status)
	}
}

func TestRequireValidCookieInvalidCookie(t *testing.T) {
	var badCookie *http.Cookie

	handler, goodCookie := newCookieTest(t)

	// construct a bad cookie by auth'ing to a new server, then
	for {
		_, badCookie = newCookieTest(t)
		if badCookie.Value != goodCookie.Value {
			break
		}
	}
	t.Logf("good cookie = %s", goodCookie)
	t.Logf("bad cookie = %s", badCookie)

	req := httptest.NewRequest("GET", "/", nil)
	req.AddCookie(badCookie)
	wr := httptest.NewRecorder()

	handler.ServeHTTP(wr, req)
	resp := wr.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("response was '%s', not Unauthorized", resp.Status)
	}
}

func TestRequireValidCookieInvalidNode(t *testing.T) {
	s := newTestingServer()
	jar := s.CookieMaker.(*simpleCookieJar)
	handler := RequireValidCookie(
		http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
			fmt.Fprintf(resp, "OK")
		}), s)

	cookie, err := jar.MakeCookie(fuq.NodeInfo{})
	if err != nil {
		t.Fatalf("error making cookies with empty NodeInfo: %v", err)
	}

	webCookie := fuq.AsHTTPCookie(ServerCookie, cookie, time.Now().Add(24*time.Hour))
	req := httptest.NewRequest("GET", "/", nil)
	req.AddCookie(webCookie)
	wr := httptest.NewRecorder()

	handler.ServeHTTP(wr, req)
	resp := wr.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("response was '%s', not Forbidden", resp.Status)
	}
}

func serverAuthWithInfo(t *testing.T, s *Server, ni fuq.NodeInfo) (fuq.NodeInfo, fuq.Cookie, *http.Response) {
	hello := fuq.Hello{
		Auth:     testingPass,
		NodeInfo: ni,
	}
	env := HelloResponseEnv{}
	resp := sendHello(t, s, hello, &env)

	ni.UniqName = *env.Name
	cookie := *env.Cookie

	return ni, cookie, resp
}

func serverAuthNode(t *testing.T, s *Server, node string) (fuq.NodeInfo, fuq.Cookie, *http.Response) {
	ni := makeNodeInfoWithName(node)
	return serverAuthWithInfo(t, s, ni)
}

func serverAuth(t *testing.T, s *Server) (fuq.NodeInfo, fuq.Cookie, *http.Response) {
	ni := makeNodeInfo()
	return serverAuthWithInfo(t, s, ni)
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
	mux := SetupRoutes(s)
	_, cookie, _ := serverAuth(t, s)

	goodCookie := fuq.AsHTTPCookie(ServerCookie, cookie, time.Now().Add(24*time.Hour))

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
	repl := []fuq.Task{}

	var badCookie *http.Cookie
	for {
		_, badCookie = newCookieTest(t)
		if badCookie.Value != goodCookie.Value {
			break
		}
	}
	t.Logf("good cookie = %s", goodCookie)
	t.Logf("bad cookie = %s", badCookie)

	// Check error state: no cookie
	resp := roundTrip{
		T:      t,
		Msg:    req,
		Dst:    nil,
		Target: JobRequestPath,
	}.ExpectCode(mux.ServeHTTP, http.StatusUnauthorized)
	resp.Body.Close()

	// Check error state: bad cookie
	resp = roundTrip{
		Cookie: badCookie,
		T:      t,
		Msg:    req,
		Dst:    nil,
		Target: JobRequestPath,
	}.ExpectCode(mux.ServeHTTP, http.StatusUnauthorized)
	resp.Body.Close()

	// Check error state: bad json (message)
	resp = roundTrip{
		Cookie: goodCookie,
		T:      t,
		Msg:    []string{"foo", "bar", "baz"},
		Dst:    nil,
		Target: JobRequestPath,
	}.ExpectCode(mux.ServeHTTP, http.StatusBadRequest)
	resp.Body.Close()

	// Now test success states

	roundTrip{
		Cookie: goodCookie,
		T:      t,
		Msg:    &req,
		Dst:    &repl,
		Target: JobRequestPath,
	}.ExpectOK(mux.ServeHTTP)
	t.Logf("response is %v", resp)

	if len(repl) != 4 {
		t.Errorf("expected len(repl) == 4, but len(repl) == %d", len(repl))
	}

	// job should now be running...
	job.Status = fuq.Running
	expectedTasks := []fuq.Task{
		{1, job}, {2, job}, {3, job}, {4, job},
	}
	if !reflect.DeepEqual(repl, expectedTasks) {
		t.Fatalf("expected tasks '%v' but found '%v'",
			expectedTasks, repl)
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

	// Check error states on update

	// Check error state: no cookie
	resp = roundTrip{
		T:      t,
		Msg:    req,
		Dst:    nil,
		Target: JobUpdatePath,
	}.ExpectCode(mux.ServeHTTP, http.StatusUnauthorized)
	resp.Body.Close()

	// Check error state: bad cookie
	resp = roundTrip{
		Cookie: badCookie,
		T:      t,
		Msg:    req,
		Dst:    nil,
		Target: JobUpdatePath,
	}.ExpectCode(mux.ServeHTTP, http.StatusUnauthorized)
	resp.Body.Close()

	// Check error state: bad json (message)
	resp = roundTrip{
		Cookie: goodCookie,
		T:      t,
		Msg:    []string{"foo", "bar", "baz"},
		Dst:    nil,
		Target: JobUpdatePath,
	}.ExpectCode(mux.ServeHTTP, http.StatusBadRequest)
	resp.Body.Close()

	// Check successful path

	repl = []fuq.Task{}
	roundTrip{
		Cookie: goodCookie,
		T:      t,
		Msg:    &upd,
		Dst:    &repl,
		Target: JobUpdatePath,
	}.ExpectOK(mux.ServeHTTP)

	// check update
	if len(repl) != 1 {
		t.Fatalf("expected len(resp) == 1, but len(resp) == %d", len(repl))
	}

	if repl[0].Task != 5 || repl[0].JobDescription != job {
		t.Errorf("expected Task 5 of job %d, but received '%v'",
			id, repl[0])
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

func TestServerNodeJobRequestOnShutdown(t *testing.T) {
	s := newTestingServer()
	mux := SetupRoutes(s)
	ni, cookie, _ := serverAuth(t, s)

	goodCookie := fuq.AsHTTPCookie(ServerCookie, cookie, time.Now().Add(24*time.Hour))

	wait := make(chan struct{})

	go func() {
		wait <- struct{}{}
		req := fuq.JobRequest{NumProc: 4}
		repl := []fuq.Task{}

		roundTrip{
			Cookie: goodCookie,
			T:      t,
			Msg:    &req,
			Dst:    &repl,
			Target: JobRequestPath,
		}.ExpectOK(mux.ServeHTTP)

		close(wait)
	}()

	// wait for goroutine to start
	<-wait

	s.ShutdownNodes([]string{ni.UniqName})

	// wait for goroutine to finish
	<-wait
}

func stdClientErrorTests(t *testing.T, h http.Handler, target string, msg interface{}) {
	var env ClientRequestEnvelope

	// Check error state: bad password
	env = ClientRequestEnvelope{
		Auth: fuq.Client{Password: invalidPass, Client: "testing"},
		Msg:  msg,
	}

	resp := roundTrip{
		T:      t,
		Msg:    env,
		Dst:    nil,
		Target: target,
	}.ExpectCode(h.ServeHTTP, http.StatusForbidden)
	resp.Body.Close()

	// Check error state: bad json (envelope)
	resp = roundTrip{
		T:      t,
		Msg:    "foo bar baz",
		Dst:    nil,
		Target: target,
	}.ExpectCode(h.ServeHTTP, http.StatusBadRequest)
	resp.Body.Close()

	// Check error state: bad json (message)
	env = ClientRequestEnvelope{
		Auth: fuq.Client{Password: testingPass, Client: "testing"},
		Msg:  []string{"foo", "bar", "baz"},
	}

	resp = roundTrip{
		T:      t,
		Msg:    env,
		Dst:    nil,
		Target: target,
	}.ExpectCode(h.ServeHTTP, http.StatusBadRequest)
	resp.Body.Close()
}

func TestServerClientJobNew(t *testing.T) {
	s := newTestingServer()
	mux := SetupRoutes(s)

	job0 := fuq.JobDescription{
		Name:       "job1",
		NumTasks:   16,
		WorkingDir: "/foo/bar",
		LoggingDir: "/foo/bar/logs",
		Command:    "/foo/foo_it.sh",
	}

	stdClientErrorTests(t, mux, ClientJobNewPath, job0)

	resp := fuq.NewJobResponse{}
	roundTrip{
		T:      t,
		Msg:    &job0,
		Dst:    &resp,
		Target: ClientJobNewPath,
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
	mux := SetupRoutes(s)

	stdClientErrorTests(t, mux, ClientNodeShutdownPath, fuq.ClientNodeShutdownReq{})

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

	env := ClientRequestEnvelope{
		Auth: fuq.Client{Password: testingPass, Client: "some_client"},
		Msg:  &fuq.ClientJobListReq{},
	}

	resp := []fuq.JobTaskStatus{}

	roundTrip{
		T:      t,
		Msg:    env,
		Dst:    &resp,
		Target: ClientJobListPath,
	}.ExpectOK(mux.ServeHTTP)

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
		Msg:    env,
		Dst:    &resp,
		Target: ClientJobListPath,
	}.ExpectOK(mux.ServeHTTP)

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
		Msg:    env,
		Dst:    &resp,
		Target: ClientJobListPath,
	}.ExpectOK(mux.ServeHTTP)
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
		Msg:    env,
		Dst:    &resp,
		Target: ClientJobListPath,
	}.ExpectOK(mux.ServeHTTP)
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

	env = ClientRequestEnvelope{
		Auth: fuq.Client{Password: testingPass, Client: "some_client"},
		Msg:  fuq.ClientJobListReq{Status: "finished"},
	}

	resp = []fuq.JobTaskStatus{}
	roundTrip{
		T:      t,
		Msg:    env,
		Dst:    &resp,
		Target: ClientJobListPath,
	}.ExpectOK(mux.ServeHTTP)
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

func TestServerClientJobState(t *testing.T) {
	s := newTestingServer()

	mux := SetupRoutes(s)

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
		jobs[i].JobId = addJob(t, s.Foreman, j)
		jobs[i].Status = fuq.Waiting
	}

	origJobs := make([]fuq.JobDescription, len(jobs))
	copy(origJobs, jobs)

	// First, fetch some pending tasks to start the first job
	// running
	queue := s.JobQueuer.(*simpleQueuer)
	tasks, err := queue.FetchPendingTasks(8)
	if err != nil {
		t.Fatalf("error fetching tasks: %v", err)
	}

	_ = tasks

	jobs[0].Status = fuq.Running
	currJobs, err := AllJobs(queue)
	if err != nil {
		t.Fatalf("error fetching jobs: %v", err)
	}

	if !reflect.DeepEqual(jobs, currJobs) {
		t.Fatalf("expected all jobs to be '%#v', but found '%#v'",
			jobs, currJobs)
	}

	var env ClientRequestEnvelope

	// Check error state: bad password
	env = ClientRequestEnvelope{
		Auth: fuq.Client{Password: invalidPass, Client: "testing"},
		Msg: &fuq.ClientStateChangeReq{
			JobIds: []fuq.JobId{jobs[0].JobId},
			Action: "hold",
		},
	}

	resp := roundTrip{
		T:      t,
		Msg:    env,
		Dst:    nil,
		Target: "/" + ClientJobStatePath,
	}.ExpectCode(mux.ServeHTTP, http.StatusForbidden)
	resp.Body.Close()

	// Check error state: bad json (envelope)
	resp = roundTrip{
		T:      t,
		Msg:    "foo bar baz",
		Dst:    nil,
		Target: "/" + ClientJobStatePath,
	}.ExpectCode(mux.ServeHTTP, http.StatusBadRequest)
	resp.Body.Close()

	// Check error state: bad json (message)
	env = ClientRequestEnvelope{
		Auth: fuq.Client{Password: testingPass, Client: "testing"},
		Msg:  []string{"foo", "bar", "baz"},
	}

	resp = roundTrip{
		T:      t,
		Msg:    env,
		Dst:    nil,
		Target: "/" + ClientJobStatePath,
	}.ExpectCode(mux.ServeHTTP, http.StatusBadRequest)
	resp.Body.Close()

	// Check error state: invalid job id
	invalidId := jobs[1].JobId + 1
	if invalidId == jobs[0].JobId {
		invalidId++
	}
	env = ClientRequestEnvelope{
		Auth: fuq.Client{Password: testingPass, Client: "testing"},
		Msg: fuq.ClientStateChangeReq{
			JobIds: []fuq.JobId{invalidId},
			Action: "hold",
		},
	}

	resp = roundTrip{
		T:      t,
		Msg:    env,
		Dst:    nil,
		Target: "/" + ClientJobStatePath,
	}.ExpectCode(mux.ServeHTTP, http.StatusBadRequest)
	resp.Body.Close()

	// Check error state: invalid action
	env = ClientRequestEnvelope{
		Auth: fuq.Client{Password: testingPass, Client: "testing"},
		Msg: fuq.ClientStateChangeReq{
			JobIds: []fuq.JobId{jobs[0].JobId},
			Action: "foobar",
		},
	}

	resp = roundTrip{
		T:      t,
		Msg:    env,
		Dst:    nil,
		Target: "/" + ClientJobStatePath,
	}.ExpectCode(mux.ServeHTTP, http.StatusBadRequest)
	resp.Body.Close()

	// Check error state: empty jobid list
	env = ClientRequestEnvelope{
		Auth: fuq.Client{Password: testingPass, Client: "testing"},
		Msg: fuq.ClientStateChangeReq{
			JobIds: []fuq.JobId{},
			Action: "foobar",
		},
	}

	resp = roundTrip{
		T:      t,
		Msg:    env,
		Dst:    nil,
		Target: "/" + ClientJobStatePath,
	}.ExpectCode(mux.ServeHTTP, http.StatusBadRequest)
	resp.Body.Close()

	// Now test success states

	// : hold first job
	env = ClientRequestEnvelope{
		Auth: fuq.Client{Password: testingPass, Client: "testing"},
		Msg: fuq.ClientStateChangeReq{
			JobIds: []fuq.JobId{jobs[0].JobId},
			Action: "hold",
		},
	}

	repl := []fuq.JobStateChangeResponse{}
	roundTrip{
		T:      t,
		Msg:    env,
		Dst:    &repl,
		Target: "/" + ClientJobStatePath,
	}.ExpectOK(mux.ServeHTTP)
	t.Logf("response is %v", repl)

	expectedRepl := []fuq.JobStateChangeResponse{
		{jobs[0].JobId, fuq.Running, fuq.Paused},
	}

	if !reflect.DeepEqual(repl, expectedRepl) {
		t.Fatalf("expected response '%v' but found '%v'",
			expectedRepl, repl)
	}

	jobs[0].Status = fuq.Paused
	currJobs, err = AllJobs(queue)
	if err != nil {
		t.Fatalf("error fetching jobs: %v", err)
	}

	if !reflect.DeepEqual(jobs, currJobs) {
		t.Fatalf("expected all jobs to be '%#v', but found '%#v'",
			jobs, currJobs)
	}

	// Cancel second job
	env.Msg = fuq.ClientStateChangeReq{
		JobIds: []fuq.JobId{jobs[1].JobId},
		Action: "cancel",
	}
	repl = []fuq.JobStateChangeResponse{}

	roundTrip{
		T:      t,
		Msg:    env,
		Dst:    &repl,
		Target: "/" + ClientJobStatePath,
	}.ExpectOK(mux.ServeHTTP)
	t.Logf("response is %v", repl)

	expectedRepl = []fuq.JobStateChangeResponse{
		{jobs[1].JobId, fuq.Waiting, fuq.Cancelled},
	}

	if !reflect.DeepEqual(repl, expectedRepl) {
		t.Fatalf("expected response '%v' but found '%v'",
			expectedRepl, repl)
	}

	jobs = []fuq.JobDescription{jobs[0]}
	currJobs, err = AllJobs(queue)
	if err != nil {
		t.Fatalf("error fetching jobs: %v", err)
	}

	if !reflect.DeepEqual(jobs, currJobs) {
		t.Fatalf("expected all jobs to be '%#v', but found '%#v'",
			jobs, currJobs)
	}

	cancelledJob := origJobs[1]
	cancelledJob.Status = fuq.Cancelled
	checkCancelled, err := queue.FetchJobId(cancelledJob.JobId)
	if err != nil {
		t.Fatalf("error fetching cancelled job by id: %v", err)
	}

	if cancelledJob != checkCancelled {
		t.Fatalf("expected job '%v', but found '%v'",
			cancelledJob, checkCancelled)
	}

	// Resume first job
	env.Msg = fuq.ClientStateChangeReq{
		JobIds: []fuq.JobId{jobs[0].JobId},
		Action: "release",
	}
	repl = []fuq.JobStateChangeResponse{}

	roundTrip{
		T:      t,
		Msg:    env,
		Dst:    &repl,
		Target: "/" + ClientJobStatePath,
	}.ExpectOK(mux.ServeHTTP)
	t.Logf("response is %v", repl)

	expectedRepl = []fuq.JobStateChangeResponse{
		{jobs[0].JobId, fuq.Paused, fuq.Waiting},
	}

	if !reflect.DeepEqual(repl, expectedRepl) {
		t.Fatalf("expected response '%v' but found '%v'",
			expectedRepl, repl)
	}

	jobs[0].Status = fuq.Waiting
	currJobs, err = AllJobs(queue)
	if err != nil {
		t.Fatalf("error fetching jobs: %v", err)
	}

	if !reflect.DeepEqual(jobs, currJobs) {
		t.Fatalf("expected all jobs to be '%#v', but found '%#v'",
			jobs, currJobs)
	}
}

func TestServerClientJobClear(t *testing.T) {
	s := newTestingServer()
	mux := SetupRoutes(s)

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
		jobs[i].JobId = addJob(t, s.Foreman, j)
		jobs[i].Status = fuq.Waiting
	}

	// First, fetch some pending tasks to start the first job
	// running
	queue := s.JobQueuer.(*simpleQueuer)

	currJobs, err := AllJobs(queue)
	if err != nil {
		t.Fatalf("error fetching jobs: %v", err)
	}

	if !reflect.DeepEqual(jobs, currJobs) {
		t.Fatalf("expected all jobs to be '%#v', but found '%#v'",
			jobs, currJobs)
	}

	var env ClientRequestEnvelope

	// Check error state: bad password
	env = ClientRequestEnvelope{
		Auth: fuq.Client{Password: invalidPass, Client: "testing"},
		Msg:  nil,
	}

	resp := roundTrip{
		T:      t,
		Msg:    env,
		Dst:    nil,
		Target: ClientJobClearPath,
	}.ExpectCode(mux.ServeHTTP, http.StatusForbidden)
	resp.Body.Close()

	// Check error state: bad json (envelope)
	resp = roundTrip{
		T:      t,
		Msg:    "foo bar baz",
		Dst:    nil,
		Target: ClientJobClearPath,
	}.ExpectCode(mux.ServeHTTP, http.StatusBadRequest)
	resp.Body.Close()

	// Now test success states

	// : hold first job
	env = ClientRequestEnvelope{
		Auth: fuq.Client{Password: testingPass, Client: "testing"},
		Msg:  nil,
	}

	roundTrip{
		T:      t,
		Msg:    env,
		Dst:    nil,
		Target: ClientJobClearPath,
	}.ExpectOK(mux.ServeHTTP)

	currJobs, err = AllJobs(queue)
	if err != nil {
		t.Fatalf("error fetching jobs: %v", err)
	}

	if len(currJobs) != 0 {
		t.Fatalf("expected zero jobs, but found %d: %v",
			len(currJobs), currJobs)
	}
}

func TestServerClientNodeList(t *testing.T) {
	s := newTestingServer()
	mux := SetupRoutes(s)

	var env ClientRequestEnvelope

	// Check error state: bad password
	env = ClientRequestEnvelope{
		Auth: fuq.Client{Password: invalidPass, Client: "testing"},
		Msg:  fuq.ClientNodeListReq{},
	}

	resp := roundTrip{
		T:      t,
		Msg:    env,
		Dst:    nil,
		Target: "/" + ClientNodeListPath,
	}.ExpectCode(mux.ServeHTTP, http.StatusForbidden)
	resp.Body.Close()

	// Check error state: bad json (envelope)
	resp = roundTrip{
		T:      t,
		Msg:    "foo bar baz",
		Dst:    nil,
		Target: "/" + ClientNodeListPath,
	}.ExpectCode(mux.ServeHTTP, http.StatusBadRequest)
	resp.Body.Close()

	// Check error state: bad json (message)
	env = ClientRequestEnvelope{
		Auth: fuq.Client{Password: testingPass, Client: "testing"},
		Msg:  []string{"foo", "bar", "baz"},
	}

	resp = roundTrip{
		T:      t,
		Msg:    env,
		Dst:    nil,
		Target: "/" + ClientNodeListPath,
	}.ExpectCode(mux.ServeHTTP, http.StatusBadRequest)
	resp.Body.Close()

	// Now test success states

	// No nodes, only connected nodes (cookies=false)
	env = ClientRequestEnvelope{
		Auth: fuq.Client{Password: testingPass, Client: "testing"},
		Msg:  fuq.ClientNodeListReq{CookieList: false},
	}

	repl := []fuq.NodeInfo{}
	roundTrip{
		T:      t,
		Msg:    env,
		Dst:    &repl,
		Target: "/" + ClientNodeListPath,
	}.ExpectOK(mux.ServeHTTP)
	t.Logf("response is %v", repl)

	if len(repl) != 0 {
		t.Fatalf("expected zero nodes, but found %d: %v",
			len(repl), repl)
	}

	// No nodes, all registered nodes (cookies=true)
	env = ClientRequestEnvelope{
		Auth: fuq.Client{Password: testingPass, Client: "testing"},
		Msg:  fuq.ClientNodeListReq{CookieList: true},
	}

	repl = []fuq.NodeInfo{}
	roundTrip{
		T:      t,
		Msg:    env,
		Dst:    &repl,
		Target: "/" + ClientNodeListPath,
	}.ExpectOK(mux.ServeHTTP)
	t.Logf("response is %v", repl)

	if len(repl) != 0 {
		t.Fatalf("expected zero nodes, but found %d: %v",
			len(repl), repl)
	}

	// Register two nodes, request nodes connected (zero) and
	// registered nodes (two)

	ni1, _, _ := serverAuthNode(t, s, "voltron")
	ni2, _, _ := serverAuthNode(t, s, "robocop")
	expectedRepl := []fuq.NodeInfo{ni1, ni2}

	env = ClientRequestEnvelope{
		Auth: fuq.Client{Password: testingPass, Client: "testing"},
		Msg:  fuq.ClientNodeListReq{CookieList: false},
	}

	repl = []fuq.NodeInfo{}
	roundTrip{
		T:      t,
		Msg:    env,
		Dst:    &repl,
		Target: "/" + ClientNodeListPath,
	}.ExpectOK(mux.ServeHTTP)
	t.Logf("response is %v", repl)

	if len(repl) != 0 {
		t.Fatalf("expected zero nodes, but found %d: %v",
			len(repl), repl)
	}

	env = ClientRequestEnvelope{
		Auth: fuq.Client{Password: testingPass, Client: "testing"},
		Msg:  fuq.ClientNodeListReq{CookieList: true},
	}

	repl = []fuq.NodeInfo{}
	roundTrip{
		T:      t,
		Msg:    env,
		Dst:    &repl,
		Target: "/" + ClientNodeListPath,
	}.ExpectOK(mux.ServeHTTP)
	t.Logf("response is %v", repl)

	if len(repl) != 2 {
		t.Fatalf("expected two nodes, but found %d: %v",
			len(repl), repl)
	}

	// One node connected (fake connection), two registered.
	// Only one node should be listed.
	ni := makeNodeInfo()
	s.Foreman.connections.AddConn(&persistentConn{
		NodeInfo: ni,
	})

	env = ClientRequestEnvelope{
		Auth: fuq.Client{Password: testingPass, Client: "testing"},
		Msg:  fuq.ClientNodeListReq{CookieList: false},
	}

	repl = []fuq.NodeInfo{}
	roundTrip{
		T:      t,
		Msg:    env,
		Dst:    &repl,
		Target: "/" + ClientNodeListPath,
	}.ExpectOK(mux.ServeHTTP)
	t.Logf("response is %v", repl)

	if len(repl) != 1 {
		t.Fatalf("expected one node, but found %d: %v",
			len(repl), repl)
	}

	expectedRepl = []fuq.NodeInfo{ni}
	if !reflect.DeepEqual(repl, expectedRepl) {
		t.Fatalf("expected reply '%v' but found '%v'",
			repl, expectedRepl)
	}

	/*
		expectedRepl := []fuq.JobStateChangeResponse{
			{jobs[0].JobId, fuq.Running, fuq.Paused},
		}

		if !reflect.DeepEqual(repl, expectedRepl) {
			t.Fatalf("expected response '%v' but found '%v'",
				expectedRepl, repl)
		}
	*/
}

func TestServerClientNodeShutdown(t *testing.T) {
	s := newTestingServer()
	mux := SetupRoutes(s)

	stdClientErrorTests(t, mux, ClientNodeShutdownPath, fuq.ClientNodeShutdownReq{})

	// Now test success states

	var env ClientRequestEnvelope

	nodes := []string{"voltron-1", "robocop-1"}
	env = ClientRequestEnvelope{
		Auth: fuq.Client{Password: testingPass, Client: "testing"},
		Msg:  fuq.ClientNodeShutdownReq{UniqNames: nodes},
	}

	for _, n := range nodes {
		if s.IsNodeShutdown(n) {
			t.Errorf("expected IsShutdown(\"%s\") to be false, but it is true", n)
		}
	}

	roundTrip{
		T:      t,
		Msg:    env,
		Dst:    nil,
		Target: ClientNodeShutdownPath,
	}.ExpectOK(mux.ServeHTTP)
	// t.Logf("response is %v", repl)

	for _, n := range nodes {
		if !s.IsNodeShutdown(n) {
			t.Errorf("expected IsShutdown(\"%s\") to be true, but it is false", n)
		}
	}
}

func signalOnFinish(h http.Handler, signal chan<- struct{}) http.Handler {
	return http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		defer close(signal)
		h.ServeHTTP(resp, req)
	})
}

func newTestClient(t *testing.T, s *Server) (*websocket.Messenger, testClient) {
	ni, _, resp := serverAuth(t, s)
	_ = ni

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("error allocating cookie jar: %v", err)
	}

	serverFinished := make(chan struct{})

	mux := SetupRoutes(s)
	server := httptest.NewServer(
		signalOnFinish(mux, serverFinished))
	defer server.Close()

	theURL, err := url.Parse(server.URL)
	theURL.Path = "/node/persistent"
	if err != nil {
		t.Fatalf("error parsing server URL: %s", server.URL)
	}
	jar.SetCookies(theURL, resp.Cookies())

	messenger, resp, err := websocket.Dial(theURL.String(), jar)
	if err != nil {
		t.Logf("error dialing websocket at %s: %v", theURL.String(), err)
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("error reading response body: %v", err)
		}
		t.Logf("response is: %s", body)
		t.Fatalf("error dialing websocket: %v", err)
	}

	messenger.Timeout = 60 * time.Second

	client := proto.NewConn(proto.Opts{
		Messenger: messenger,
		Worker:    true,
	})

	return messenger, testClient{
		Conn:           client,
		NodeInfo:       ni,
		Messenger:      messenger,
		ServerFinished: serverFinished,
	}
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

func pconnHello(t *testing.T, ctx context.Context, client testClient, nproc int) {
	msg, err := client.SendHello(ctx, proto.HelloData{
		NumProcs: nproc,
		Running:  nil,
	})

	if err != nil {
		t.Fatalf("error in HELLO: %v", err)
	}

	if msg.Type != proto.MTypeOK {
		t.Fatalf("expected OK reply, but received %v", msg)
	}

	np, nr := msg.AsOkay()
	if int(np) != nproc || int(nr) != 0 {
		t.Fatalf("expected OK(%d|0), but received OK(%d|%d)", nproc, np, nr)
	}
}

type simpleJobHandler struct {
	sync.Mutex

	t      *testing.T
	nproc  int
	nrun   int
	taskCh chan []fuq.Task
}

func newSimpleJobHandler(t *testing.T, np int) *simpleJobHandler {
	return &simpleJobHandler{
		t:      t,
		nproc:  np,
		taskCh: make(chan []fuq.Task),
	}
}

func (sjh *simpleJobHandler) onJob(msg proto.Message) proto.Message {
	taskPtr := msg.Data.(*[]fuq.Task)
	tasks := *taskPtr

	sjh.Lock()
	defer sjh.Unlock()

	sjh.t.Logf("%d tasks: %v", len(tasks), tasks)

	sjh.nproc -= len(tasks)
	sjh.nrun += len(tasks)

	// XXX - check that nproc, nrun are in valid range for uint16
	if sjh.nproc < 0 {
		panic("invalid number of tasks")
	}

	repl := proto.OkayMessage(uint16(sjh.nproc), uint16(sjh.nrun), msg.Seq)
	sjh.taskCh <- tasks
	return repl
}

func (sjh *simpleJobHandler) Stats() (nproc, nrun int) {
	sjh.Lock()
	defer sjh.Unlock()

	return sjh.nproc, sjh.nrun
}

func (sjh *simpleJobHandler) TaskCh() <-chan []fuq.Task {
	sjh.Lock()
	defer sjh.Unlock()

	return sjh.taskCh
}

func TestServerPConnClosedWhileJobsRunning(t *testing.T) {
	s := newTestingServer()

	wsConn, client := newTestClient(t, s)
	defer wsConn.Close()
	// defer client.Close()

	queue := s.JobQueuer.(*simpleQueuer)
	id, err := queue.AddJob(fuq.JobDescription{
		Name:       "job1",
		NumTasks:   8,
		WorkingDir: "/foo/bar",
		LoggingDir: "/foo/bar/logs",
		Command:    "/foo/foo_it.sh",
	})

	_, _ = id, err

	sjh := newSimpleJobHandler(t, 7)
	taskCh := sjh.TaskCh()
	client.OnMessageFunc(proto.MTypeJob, sjh.onJob)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go client.ConversationLoop(ctx)

	// XXX
	pconnHello(t, ctx, client, 7)

	// make sure we're in the connection set...
	var ourPc *persistentConn
	err = s.connections.EachConn(func(pc *persistentConn) error {
		if pc.NodeInfo.UniqName == client.NodeInfo.UniqName {
			ourPc = pc
		}

		return nil
	})
	if err != nil {
		t.Fatalf("error looking for persistent connection: %v", err)
	}

	if ourPc == nil {
		t.Fatal("could not find persistent connection")
	}

	// The foreman should dispatch nproc tasks
	tasks := <-taskCh
	if len(tasks) != 7 {
		t.Fatalf("expected 7 tasks to be queued")
	}

	np, nr := sjh.Stats()
	if np != 0 && nr != 7 {
		t.Fatalf("expected nproc=%d and nrun=%d, but found nproc=%d and nrun=%d",
			0, 7, np, nr)
	}

	// abruptly close connection!
	client.Messenger.Close()

	// wait for request to finish...
	<-client.ServerFinished

	if s.connections.HasConn(ourPc) {
		t.Errorf("after close, should not have connection set")
	}
}

func TestServerPConnClosedWhileWaitingForJobs(t *testing.T) {
	s := newTestingServer()

	wsConn, client := newTestClient(t, s)
	defer wsConn.Close()

	sjh := newSimpleJobHandler(t, 7)
	client.OnMessageFunc(proto.MTypeJob, sjh.onJob)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go client.ConversationLoop(ctx)

	// XXX
	pconnHello(t, ctx, client, 7)

	// make sure we're in the connection set...
	var ourPc *persistentConn
	err := s.connections.EachConn(func(pc *persistentConn) error {
		if pc.NodeInfo.UniqName == client.NodeInfo.UniqName {
			ourPc = pc
		}

		return nil
	})

	if err != nil {
		t.Fatalf("error looking for persistent connection: %v", err)
	}

	if ourPc == nil {
		t.Fatal("could not find persistent connection")
	}

	// abruptly close connection!
	client.Messenger.Close()

	// wait for request to finish...
	<-client.ServerFinished

	if s.connections.HasConn(ourPc) {
		t.Errorf("after close, should not have connection set")
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
