package fuq

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestParseKVFile(t *testing.T) {
	inp := `# First line is a comment
first_key	first_value	# separated by a tab
2nd_key         another_value   # separated by spaces
key3	        value	  	# separated by both
# empty line

# next has no comment, just a number and trailing whitespace
last_key	81412	
`

	expected := [][2]string{
		{"first_key", "first_value"},
		{"2nd_key", "another_value"},
		{"key3", "value"},
		{"last_key", "81412"},
	}

	r := strings.NewReader(inp)

	pair := 0
	err := ParseKVFile(r, func(k, v string) error {
		if k != expected[pair][0] || v != expected[pair][1] {
			t.Errorf("kv pair %d, expected (%s,%s), but found (%s,%v)",
				pair+1, expected[0], expected[1], k, v)
		}
		pair++

		return nil
	})

	if err != nil {
		t.Fatalf("error in parsing: %v", err)
	}

	if pair != len(expected) {
		t.Fatalf("expected %d kv pairs, but found %d",
			len(expected), pair)
	}
}

func TestPathToElements(t *testing.T) {
	// Should test this on Windows - XXX

	testElementsEqual := func(p, vol string, expected ...string) {
		v, elts := PathToElements(p)
		if v != vol {
			t.Errorf("path '%s' should have volume '%s' but found '%s'",
				p, vol, v)
			return
		}

		if len(elts) != len(expected) {
			t.Errorf("path '%s' should have %d elements but found %d",
				p, len(expected), len(elts))
			return
		}

		for i, e := range expected {
			if elts[i] != e {
				t.Errorf("path '%s' element %d should be '%s' but was '%s'",
					p, i, e, elts[i])
				return
			}
		}
	}

	vol := ""
	testElementsEqual("/foo/bar/baz", vol, "/", "foo", "bar", "baz")
	testElementsEqual("/foo/bar/../baz", vol, "/", "foo", "baz")
	testElementsEqual("@/test/data.txt", vol, ".", "@", "test", "data.txt")
	testElementsEqual("~/test/more_data.txt", vol, ".", "~", "test", "more_data.txt")
	testElementsEqual("~/${var}/more_data.txt", vol, ".", "~", "${var}", "more_data.txt")
}

type stringCompareTable []struct{ actual, expected, desc string }

func (tbl stringCompareTable) Check(t *testing.T) {
	for _, itm := range tbl {
		if itm.actual != itm.expected {
			if itm.desc != "" {
				t.Errorf("wrong %s, expected %q, actual %q",
					itm.desc, itm.expected, itm.actual)
			} else {
				t.Errorf("error: expected %q, actual %q",
					itm.expected, itm.actual)
			}
		}
	}
}

func checkPath(t *testing.T, pv *PathVars, input, expected string) {
	input = filepath.FromSlash(input)
	expected = filepath.FromSlash(expected)
	p, err := pv.ExpandPath(input)
	if err != nil {
		t.Fatalf("error expanding path '%s': %v", input, err)
	}

	if p != expected {
		t.Errorf("path '%s' expected to '%s' but expected '%s'",
			input, p, expected)
	}

	t.Logf("expanded '%s' -> '%s'", input, p)
}

func TestExpandPath(t *testing.T) {
	curr := "/foo/bar"
	home := "/home/baz"

	pv := NewPathVars(filepath.FromSlash(curr), filepath.FromSlash(home))
	pv.Add("ARCH", "amd64")
	pv.Add("OS", "darwin")

	checkPath(t, pv, "~/quux/readme.txt", "/home/baz/quux/readme.txt")
	checkPath(t, pv, "@/data.db", "/foo/bar/data.db")
	checkPath(t, pv, "@/data/${OS}/asset.txt", "/foo/bar/data/darwin/asset.txt")
	checkPath(t, pv, "@/data/${OS}_${ARCH}/asset.txt", "/foo/bar/data/darwin_amd64/asset.txt")
}

func TestReadConfig(t *testing.T) {
	curr := "/foo/bar"
	home := "/home/baz"

	pv := NewPathVars(filepath.FromSlash(curr), filepath.FromSlash(home))
	pv.Add("ARCH", "amd64")
	pv.Add("OS", "darwin")

	input := strings.NewReader(`# First line is a comment
dbpath		/path/to/db/queue.db
logdir		~/.fuq/logs
auth		NeedABetterPassword
port		13247
foreman		apollo-13.local
foremanlog	~/.fuq/srv.log
keyfile		~/.fuq/key.pem
certfile	~/.fuq/cert.pem
rootca 		~/.fuq/ca.pem
certname	frankie-valentine
`)

	cfg := Config{}
	if err := cfg.ReadConfig(input, pv); err != nil {
		t.Fatalf("error reading configuration: %v", err)
	}

	if cfg.Port != 13247 {
		t.Errorf("wrong port, expected %d but found %d", 13247, cfg.Port)
	}

	strCmpTable := stringCompareTable{
		{cfg.DbPath, "/path/to/db/queue.db", "config path"},
		{cfg.LogDir, "/home/baz/.fuq/logs", "log directory"},
		{cfg.Auth, "NeedABetterPassword", "auth string"},
		{cfg.Foreman, "apollo-13.local", "foreman host"},
		{cfg.ForemanLogFile, "/home/baz/.fuq/srv.log", "foreman log file"},
		{cfg.KeyFile, "/home/baz/.fuq/key.pem", "key file"},
		{cfg.CertFile, "/home/baz/.fuq/cert.pem", "cert file"},
		{cfg.RootCAFile, "/home/baz/.fuq/ca.pem", "root ca file"},
		{cfg.CertName, "frankie-valentine", "cert name"},
	}

	strCmpTable.Check(t)
}
