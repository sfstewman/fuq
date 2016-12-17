package main

import (
	"flag"
	"github.com/sfstewman/fuq"
	"github.com/sfstewman/fuq/srv"
	"log"
)

func fixupDatabase(dbPath string) error {
	cfg := fuq.Config{DbPath: dbPath}

	d, err := srv.NewDispatcher(cfg)
	if err != nil {
		return err
	}
	defer d.Close()

	if err := srv.Fsck(d); err != nil {
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
