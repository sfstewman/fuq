package fuq

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestReadJobFile(t *testing.T) {
	jfData := `
# Test data file... this is a comment
name FooBar
num_tasks 23
working_dir ~/sims/FooBar
log_dir ~/sims/FooBar/logs
command /bin/echo	# another comment
`

	r := bytes.NewReader([]byte(jfData))
	jd, err := ReadJobFile(r)
	if err != nil {
		t.Fatalf("error reading job file: %v", err)
	}

	if jd.NumTasks != 23 {
		t.Error("wrong number of tasks, expected 23, found: %d", jd.NumTasks)
	}

	strCmpTable := stringCompareTable{
		{jd.Name, "FooBar", "name"},
		{jd.WorkingDir, "~/sims/FooBar", "working directory"},
		{jd.LoggingDir, "~/sims/FooBar/logs", "logging directory"},
		{jd.Command, "/bin/echo", "command"},
	}

	strCmpTable.Check(t)
}

func checkParsedJobStatus(t *testing.T, s string, js JobStatus, expectOK bool) {
	pjs, err := ParseJobStatus(s)

	parseOK := (err == nil)
	if parseOK != expectOK {
		if expectOK {
			t.Fatalf("error parsing '%s': %v", s, err)
		} else {
			t.Fatalf("expected error parsing '%s' but none found")
		}
	}

	if !expectOK {
		return
	}

	if pjs != js {
		t.Errorf("parsed '%s' into JobStatus %v, but expect %v",
			s, pjs, js)
	}
}

func TestParseJobStatus(t *testing.T) {
	checkParsedJobStatus(t, `waiting`, Waiting, true)
	checkParsedJobStatus(t, `waitin`, Waiting, false)

	checkParsedJobStatus(t, `running`, Running, true)
	checkParsedJobStatus(t, `"running"`, Running, false)

	checkParsedJobStatus(t, `finished`, Finished, true)
	checkParsedJobStatus(t, `finish`, Finished, false)

	checkParsedJobStatus(t, `paused`, Paused, true)
	checkParsedJobStatus(t, `pause`, Paused, false)

	checkParsedJobStatus(t, `cancelled`, Cancelled, true)
	checkParsedJobStatus(t, `cancel`, Cancelled, false)
}

func TestJobStatusMarshalJSON(t *testing.T) {
	inp := make(map[string]JobStatus)

	inp["key1"] = Waiting
	inp["key2"] = Running
	inp["key3"] = Paused
	inp["key4"] = Finished
	inp["key5"] = Cancelled

	js, err := json.Marshal(inp)
	if err != nil {
		t.Fatalf("error marshaling json: %v", err)
	}

	var out bytes.Buffer
	if err := json.Indent(&out, js, "", "  "); err != nil {
		t.Fatalf("error indenting json: %v", err)
	}
	expected := `{
  "key1": "waiting",
  "key2": "running",
  "key3": "paused",
  "key4": "finished",
  "key5": "cancelled"
}`

	jsPP := out.String()
	if jsPP != expected {
		t.Errorf("expected json: '%v', but found '%v'", expected, jsPP)
	}
}

func TestJobStatusUnmarshalJSON(t *testing.T) {
	out := make(map[string]JobStatus)

	inp := []byte(`{
  "key1": "waiting",
  "key2": "running",
  "key3": "paused",
  "key4": "finished",
  "key5": "cancelled"
}`)

	if err := json.Unmarshal(inp, &out); err != nil {
		t.Fatalf("error unmarshaling json: %v", err)
	}

	table := []struct {
		k string
		v JobStatus
	}{
		{"key1", Waiting},
		{"key2", Running},
		{"key3", Paused},
		{"key4", Finished},
		{"key5", Cancelled},
	}

	for _, kvp := range table {
		key, value := kvp.k, kvp.v
		actual, found := out[kvp.k]
		if !found {
			t.Errorf("key '%s' not in map", key)
			continue
		}

		if actual != value {
			t.Errorf("key '%s' should have value %v but found %v",
				key, value, actual)
			continue
		}
	}
}
