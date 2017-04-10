package srv

import (
	"context"
	"github.com/sfstewman/fuq"
	"github.com/sfstewman/fuq/node"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"testing"
	// "net/http"
)

type endpointTest struct {
	S *Server
	C *Endpoint

	ServerConfig fuq.Config
	Server       *httptest.Server
}

func newEndpointTest(t *testing.T) *endpointTest {
	var err error

	et := &endpointTest{}

	et.S = newTestingServer()
	mux := SetupRoutes(et.S)

	et.ServerConfig = fuq.Config{
		Auth:       testingPass,
		KeyFile:    KeyData.KeyPath,
		CertFile:   KeyData.CertPath,
		RootCAFile: KeyData.CertPath,
	}

	et.Server = httptest.NewUnstartedServer(mux)
	et.Server.TLS, err = fuq.SetupTLS(et.ServerConfig)
	if err != nil {
		t.Fatalf("error setting up TLS: %v", err)
	}
	et.Server.StartTLS()

	url, err := url.Parse(et.Server.URL)
	if err != nil {
		t.Fatalf("error parsing URL '%s': %v", et.Server.URL, err)
	}

	portStr := url.Port()
	if portStr == "" {
		t.Fatalf("URL '%s' has no port string", et.Server.URL)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("error converting port '%s' to a number: %v", portStr, err)
	}

	clientConfig := fuq.Config{
		Foreman:    url.Hostname(),
		Port:       port,
		Auth:       testingPass,
		KeyFile:    KeyData.KeyPath,
		CertFile:   KeyData.CertPath,
		RootCAFile: KeyData.CertPath,
	}

	ni, _, _ := serverAuth(t, et.S)

	nodeConfig := &NodeConfig{
		NodeInfo: ni,
	}

	ep0, err := fuq.NewEndpoint(clientConfig)
	if err != nil {
		t.Fatalf("error initializing endpoint: %v", err)
	}

	if err := nodeConfig.NewCookie(ep0); err != nil {
		t.Fatalf("error requesting a new cookie: %v", err)
	}

	t.Logf("initialized client endpoint with cookie %s", nodeConfig.cookie)

	et.C = &Endpoint{
		Endpoint: ep0,
		Config:   nodeConfig,
	}

	return et
}

func (et *endpointTest) Close() {
	et.Server.Close()
}

func TestEndpointRequestAction(t *testing.T) {
	et := newEndpointTest(t)
	defer et.Close()

	job := fuq.JobDescription{
		Name:       "job1",
		NumTasks:   8,
		WorkingDir: "/foo/bar",
		LoggingDir: "/foo/bar/logs",
		Command:    "/foo/foo_it.sh",
	}

	queue := et.S.JobQueuer.(*simpleQueuer)
	id, err := queue.AddJob(job)
	if err != nil {
		t.Fatalf("error queuing job: %v", err)
	}
	job.JobId = id
	t.Logf("queued job %d", id)

	act, err := et.C.RequestAction(context.TODO(), 1)
	if err != nil {
		t.Fatalf("error calling RequestAction: %v", err)
	}
	t.Logf("received action %#v", act)

	runAct, ok := act.(node.RunAction)
	if !ok {
		t.Fatalf("expected node.RunAction but received %#v", act)
	}

	job.Status = fuq.Running
	expectedAct := node.RunAction(fuq.Task{Task: 1, JobDescription: job})
	if runAct != expectedAct {
		t.Fatalf("expected action '%v' but found '%v'", expectedAct, runAct)
	}
}

func TestEndpointUpdateAndRequestAction(t *testing.T) {
	et := newEndpointTest(t)
	defer et.Close()

	job := fuq.JobDescription{
		Name:       "job1",
		NumTasks:   8,
		WorkingDir: "/foo/bar",
		LoggingDir: "/foo/bar/logs",
		Command:    "/foo/foo_it.sh",
	}

	queue := et.S.JobQueuer.(*simpleQueuer)
	id, err := queue.AddJob(job)
	if err != nil {
		t.Fatalf("error queuing job: %v", err)
	}
	job.JobId = id
	t.Logf("queued job %d", id)

	tasks, err := queue.FetchPendingTasks(2)
	if err != nil {
		t.Fatalf("error pre-fetching one task: %v", err)
	}
	job.Status = fuq.Running

	// Make sure when nproc==0 we still update the state and receive
	// a wait action
	upd := fuq.JobStatusUpdate{
		JobId:   tasks[0].JobId,
		Task:    tasks[0].Task,
		Success: true,
		Status:  "done",
	}

	act, err := et.C.UpdateAndRequestAction(context.TODO(), upd, 0)
	if err != nil {
		t.Fatalf("error calling UpdateAndRequestAction: %v", err)
	}
	t.Logf("received action %#v", act)

	_, ok := act.(node.WaitAction)
	if !ok {
		t.Fatalf("expected node.WaitAction but received %#v", act)
	}

	status, err := queue.FetchJobTaskStatus(id)
	if err != nil {
		t.Fatalf("error while fetching job %d status: %v", id, err)
	}

	expectedStatus := fuq.JobTaskStatus{
		Description:     job,
		TasksFinished:   1,
		TasksPending:    6,
		TasksRunning:    []int{tasks[1].Task},
		TasksWithErrors: []int{},
	}

	if !reflect.DeepEqual(status, expectedStatus) {
		t.Fatalf("expected status to be '%v' but found '%v'",
			expectedStatus, status)
	}

	// Make sure when nproc==1 we update the tasks tate and receive
	// a job action
	upd = fuq.JobStatusUpdate{
		JobId:   tasks[1].JobId,
		Task:    tasks[1].Task,
		Success: true,
		Status:  "done",
	}

	act, err = et.C.UpdateAndRequestAction(context.TODO(), upd, 1)
	if err != nil {
		t.Fatalf("error calling RequestAction: %v", err)
	}
	t.Logf("received action %#v", act)

	runAct, ok := act.(node.RunAction)
	if !ok {
		t.Fatalf("expected node.RunAction but received %#v", act)
	}

	job.Status = fuq.Running
	expectedAct := node.RunAction(fuq.Task{Task: 3, JobDescription: job})
	if runAct != expectedAct {
		t.Fatalf("expected action '%v' but found '%v'", expectedAct, runAct)
	}

	status, err = queue.FetchJobTaskStatus(id)
	if err != nil {
		t.Fatalf("error while fetching job %d status: %v", id, err)
	}

	expectedStatus = fuq.JobTaskStatus{
		Description:     job,
		TasksFinished:   2,
		TasksPending:    5,
		TasksRunning:    []int{3},
		TasksWithErrors: []int{},
	}

	if !reflect.DeepEqual(status, expectedStatus) {
		t.Fatalf("expected status to be '%v' but found '%v'",
			expectedStatus, status)
	}
}
