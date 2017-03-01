package main

import (
	"flag"
	"github.com/sfstewman/fuq/db"
	"log"
)

func fixupDatabase(dbPath string) error {
	d, err := db.NewJobStore(dbPath)
	if err != nil {
		return err
	}
	defer d.Close()

	if err := db.Fsck(d); err != nil {
		return err
	}

	return nil
}

func main() {
	flag.Parse()
	for i := 0; i < flag.NArg(); i++ {
		err := fixupDatabase(flag.Arg(i))
		if err != nil {
			log.Fatalf("error fixing database: %v", err)
		}
	}
}
