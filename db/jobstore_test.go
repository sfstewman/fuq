package db_test

import (
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/sfstewman/fuq/db"
	"github.com/sfstewman/fuq/srv"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

func openTestDB() *bolt.DB {
	tmp, err := ioutil.TempFile("", "jobstore.")
	if err != nil {
		panic(err)
	}

	n := tmp.Name()
	tmp.Close()

	db, err := bolt.Open(n, 0600, nil)
	if err != nil {
		panic(err)
	}

	return db
}

func cleanupTestDB(db *bolt.DB) {
	p := db.Path()
	if err := db.Close(); err != nil {
		panic(err)
	}

	if err := os.Remove(p); err != nil {
		panic(err)
	}
}

func runJobStoreTest(t *testing.T, name string, jsTest func(*testing.T, srv.JobQueuer)) {
	tmp := openTestDB()
	defer cleanupTestDB(tmp)

	js, err := db.InitJobStore(tmp)
	if err != nil {
		log.Fatalf("error in creating job store: %v", err)
	}
	defer js.Close()

	t.Run(fmt.Sprintf("JobStorer: %s", name),
		func(t *testing.T) { jsTest(t, js) })
}

func TestJobStorer(t *testing.T) {
	for _, jqt := range jqTestTable {
		runJobStoreTest(t, jqt.Name, jqt.Test)
	}
}
