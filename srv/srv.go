package srv

import (
	"context"
	"encoding/json"
	"github.com/sfstewman/fuq"
	"log"
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
	Name    *string     `json:"name"`
	Session *string     `json:"session"`
	Cookie  *fuq.Cookie `json:"cookie"`
}

func (env HelloResponseEnv) UpdateInfo(ni fuq.NodeInfo) fuq.NodeInfo {
	ni.UniqName = *env.Name
	ni.Session = *env.Session
	return ni
}

func RespondWithJSON(resp http.ResponseWriter, mesg interface{}) error {
	enc := json.NewEncoder(resp)
	return enc.Encode(mesg)
}

func RequireValidCookie(handler http.Handler, checker fuq.CookieMaker) http.Handler {
	return http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		cookie, err := req.Cookie(ServerCookie)
		if err != nil {
			log.Printf("%s: missing session cookie (cookies = %v)",
				req.RemoteAddr, req.Cookies())
			http.Error(resp, "missing session cookie", http.StatusUnauthorized)
			return
		}

		cookieData := fuq.Cookie(cookie.Value)
		if len(cookieData) > MaxCookieLength {
			log.Printf("%s: cookie is too long (%d bytes)", req.RemoteAddr, len(cookieData))
			http.Error(resp, "cannot validate cookie", http.StatusUnauthorized)
			return
		}

		ni, err := checker.Lookup(cookieData)
		if err != nil {
			log.Printf("%s: error looking up cookie: %v", req.RemoteAddr, err)
			http.Error(resp, "cannot validate cookie", http.StatusUnauthorized)
			return
		}

		if ni.Node == "" {
			log.Printf("%s: cookie '%s' not found", req.RemoteAddr, cookieData)
			http.Error(resp, "cannot validate cookie", http.StatusForbidden)
			return
		}

		log.Printf("%s: has valid cookie from %s", req.RemoteAddr, ni.UniqName)

		ctxWithNI := context.WithValue(req.Context(), nodeInfoKey{}, ni)
		reqWithNI := req.WithContext(ctxWithNI)

		handler.ServeHTTP(resp, reqWithNI)
	})
}
