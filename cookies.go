package fuq

type CookieMaker interface {
	IsUniqueName(n string) (bool, error)
	MakeCookie(ni NodeInfo) (Cookie, error)
	RenewCookie(ni NodeInfo) (Cookie, error)
	ExpireCookie(c Cookie) error
	Lookup(c Cookie) (NodeInfo, error)
	AllNodes() ([]NodeInfo, error)
}
