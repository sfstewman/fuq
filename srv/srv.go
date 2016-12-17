package srv

import (
	"encoding/json"
	"github.com/sfstewman/fuq"
	"net/http"
)

type ClientRequestHandler func(resp http.ResponseWriter, req *http.Request, mesg []byte)
type NodeRequestHandler func(resp http.ResponseWriter, req *http.Request, mesg []byte, ni fuq.NodeInfo)

type NodeRequestEnvelope struct {
	Cookie fuq.Cookie  `json:"cookie"`
	Msg    interface{} `json:"msg,omitempty"`
}

type ClientRequestEnvelope struct {
	Auth fuq.Client  `json:"auth"`
	Msg  interface{} `json:"msg,omitempty"`
}

type HelloResponseEnv struct {
	Name   *string     `json:"name"`
	Cookie *fuq.Cookie `json:"cookie"`
}

func RespondWithJSON(resp http.ResponseWriter, mesg interface{}) error {
	enc := json.NewEncoder(resp)
	return enc.Encode(mesg)
}
