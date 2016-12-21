package fuq

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
)

func LogIfError(err error, pfxFmt string, args ...interface{}) {
	if err == nil {
		return
	}

	pfx := fmt.Sprintf(pfxFmt, args...)
	log.Printf("%s: %v", pfx, err)
}

func FatalIfError(err error, pfxFmt string, args ...interface{}) {
	if err == nil {
		return
	}

	LogIfError(err, pfxFmt, args...)
	os.Exit(1)
}

func OK(resp http.ResponseWriter, req *http.Request) {
	resp.WriteHeader(http.StatusOK)
}

func BadMethod(resp http.ResponseWriter, req *http.Request) {
	resp.WriteHeader(http.StatusMethodNotAllowed)
	fmt.Fprintf(resp, "%d method %s not allowed\n",
		http.StatusMethodNotAllowed, req.Method)
}

func BadRequest(resp http.ResponseWriter, req *http.Request) {
	resp.WriteHeader(http.StatusBadRequest)
	fmt.Fprintf(resp, "%d bad request\n", http.StatusBadRequest)
}

func Forbidden(resp http.ResponseWriter, req *http.Request) {
	http.Error(resp,
		fmt.Sprintf("%d access forbidden", http.StatusForbidden),
		http.StatusForbidden)
}

func InternalError(resp http.ResponseWriter, req *http.Request) {
	resp.WriteHeader(http.StatusInternalServerError)
	fmt.Fprintf(resp, "%d internal error\n",
		http.StatusInternalServerError)
}

type Endpoint struct {
	Config Config
	Client *http.Client
}

type EndpointError struct {
	StatusCode int
	Status     string
}

func (e EndpointError) Error() string {
	return e.Status
}

func WithStatus(resp *http.Response) error {
	if resp.StatusCode == http.StatusOK {
		return nil
	}

	return EndpointError{
		StatusCode: resp.StatusCode,
		Status:     resp.Status,
	}
}

func IsForbidden(err error) bool {
	if ee, ok := err.(EndpointError); ok {
		return ee.StatusCode == http.StatusForbidden
	}
	return false
}

func NewEndpoint(c Config) (*Endpoint, error) {
	tlsConfig, err := SetupTLSRootCA(c)
	if err != nil {
		return nil, fmt.Errorf("error setting up tls config: %v", err)
	}

	transport := &http.Transport{
		TLSClientConfig:    tlsConfig,
		DisableCompression: true,
	}

	client := &http.Client{
		Transport: transport,
	}

	return &Endpoint{
		Config: c,
		Client: client,
	}, nil
}

func (e Endpoint) SendMessage(endpoint string, mesg interface{}) (*http.Response, error) {
	url := fmt.Sprintf("https://%s:%d/%s", e.Config.Foreman, e.Config.Port, endpoint)
	// log.Printf("calling endpoint '%s' at %s", endpoint, url)
	data, err := json.Marshal(mesg)
	if err != nil {
		return nil, fmt.Errorf("error while encoding for '%s': %v", endpoint, err)
	}

	// log.Printf("json marshalled to: %s", data)

	r := bytes.NewReader(data)
	resp, err := e.Client.Post(url, "application/json", r)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (e Endpoint) CallEndpoint(endpoint string, mesg, result interface{}) error {
	resp, err := e.SendMessage(endpoint, mesg)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return WithStatus(resp)
	}

	if result != nil {
		dec := json.NewDecoder(resp.Body)
		if err := dec.Decode(result); err != nil {
			return fmt.Errorf("error decoding response from '%s': %v", endpoint, err)
		}
	}

	return nil
}
