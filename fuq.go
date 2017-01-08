package fuq

import (
	"fmt"
	"os"
)

type Client struct {
	Password string `json:"password"`
	Client   string `json:"client"`
}

type NodeInfo struct {
	Node     string   `json:"node"`
	Pid      int      `json:"pid"`
	UniqName string   `json:"unique_name,omitempty"`
	Tags     []string `json:"tags,omitempty"`
	NumProc  int      `json:"nproc"`
}

func (ni NodeInfo) Prefix() string {
	return fmt.Sprintf("%s:%d", ni.Node, ni.Pid)
}

func NewNodeInfo(nproc int, tags ...string) (NodeInfo, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return NodeInfo{}, err
	}

	pid := os.Getpid()

	return NodeInfo{
		Node:    hostname,
		Pid:     pid,
		Tags:    tags,
		NumProc: nproc,
	}, nil
}

/* HasTag returns whether the NodeInfo has a given tag.  At the moment,
 * this is a simple linear scan, which is probably enough for most
 * intended purposes.
 */
func (ni NodeInfo) HasTag(tag string) bool {
	for _, t := range ni.Tags {
		if t == tag {
			return true
		}
	}

	return false
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
