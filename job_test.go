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
