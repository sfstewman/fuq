package fuq

import (
	"net/http"
	"time"
)

type CookieMaker interface {
	IsUniqueName(n string) (bool, error)
	MakeCookie(ni NodeInfo) (Cookie, error)
	RenewCookie(ni NodeInfo) (Cookie, error)
	ExpireCookie(c Cookie) error
	Lookup(c Cookie) (NodeInfo, error)
	AllNodes() ([]NodeInfo, error)
}

type Cookie string

func AsHTTPCookie(name string, cookie Cookie, expires time.Time) *http.Cookie {
	return &http.Cookie{
		Name:     name,
		Value:    string(cookie),
		Expires:  expires,
		Path:     "/",
		HttpOnly: true,
	}
}
