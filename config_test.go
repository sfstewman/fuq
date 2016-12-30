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

func TestExpandPath(t *testing.T) {
	curr := "/foo/bar"
	home := "/home/baz"

	pv := NewPathVars(filepath.FromSlash(curr), filepath.FromSlash(home))

	pathTests := []struct{ input, expected string }{
		{"~/quux/readme.txt", "/home/baz/quux/readme.txt"},
		{"@/data.db", "/foo/bar/data.db"},
	}

	for _, pair := range pathTests {
		input := filepath.FromSlash(pair.input)
		expected := filepath.FromSlash(pair.expected)
		if p := pv.ExpandPath(input); p != expected {
			t.Errorf("path '%s' expected to '%s' but expected '%s'",
				input, p, expected)
		}
	}
}
