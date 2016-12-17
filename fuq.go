package fuq

type Client struct {
	Password string `json:"password"`
	Client   string `json:"client"`
}

type NodeInfo struct {
	Node     string `json:"node"`
	UniqName string `json:"unique_name,omitempty"`
	NumProc  int    `json:"nproc"`
}

type Hello struct {
	Auth string `json:"auth"`
	NodeInfo
}

type Cookie string

type Registration struct {
	Name   string
	Cookie Cookie
}
