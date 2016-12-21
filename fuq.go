package fuq

type Client struct {
	Password string `json:"password"`
	Client   string `json:"client"`
}

type NodeInfo struct {
	Node     string   `json:"node"`
	UniqName string   `json:"unique_name,omitempty"`
	Tags     []string `json:"tags,omitempty"`
	NumProc  int      `json:"nproc"`
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
