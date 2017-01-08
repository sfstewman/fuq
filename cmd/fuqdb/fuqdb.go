package main

import (
	"fmt"
	"github.com/boltdb/bolt"
	"net/url"
	"os"
)

const indentSpacing = 2

/* fixme: do this more intelligently! XXX */
func printIndent(indent int) {
	for indent >= 16 {
		fmt.Printf("        ")
		indent -= 16
	}

	if indent >= 8 {
		fmt.Printf("        ")
		indent -= 8
	}

	if indent >= 4 {
		fmt.Printf("    ")
		indent -= 4
	}

	if indent >= 2 {
		fmt.Printf("  ")
		indent -= 2
	}

	if indent >= 1 {
		fmt.Printf(" ")
		indent -= 1
	}
}

func walkCursor(c *bolt.Cursor, indent int) error {
	cBkt := c.Bucket()

	printIndent(indent)
	fmt.Printf("cursor bucket is %p: %#v\n", cBkt, cBkt)
	for k, v := c.First(); k != nil; k, v = c.Next() {
		ks := string(k)
		printIndent(indent)

		if v != nil {
			vs := url.QueryEscape(string(v))
			if len(vs) > 20 {
				vs = vs[:20] + "..."
			}
			fmt.Printf("kv: %q\t%q\n", ks, vs)

			continue
		}

		bkt := cBkt.Bucket(k)
		if bkt == nil {
			fmt.Printf("kv: %q\t-nil-\n", ks)
			continue
		}

		fmt.Printf("---[ bucket: %q ]---\n", ks)
		err := walkCursor(bkt.Cursor(), indent+indentSpacing)
		if err != nil {
			return fmt.Errorf("error traversing %q: %v",
				ks, err)
		}
		printIndent(indent)
		fmt.Printf("---[ end of bucket: %q ]---\n", ks)
	}

	return nil
}

func walkRoot(tx *bolt.Tx) error {
	fmt.Println("--Begin: Root--")

	c := tx.Cursor()
	err := walkCursor(c, indentSpacing)
	if err != nil {
		return err
	}

	fmt.Println("--End: Root--")

	return nil
}

func runProgram() error {
	fmt.Println(os.Args)
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <db>\n", os.Args[0])
	}
	dbName := os.Args[1]

	fmt.Printf("opening database %s\n", dbName)

	db, err := bolt.Open(dbName, 0666, &bolt.Options{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("error opening db: %v\n", err)
	}
	defer db.Close()

	tx, err := db.Begin(false)
	if err != nil {
		return fmt.Errorf("error starting tx: %v", err)
	}
	defer tx.Rollback() // rollback... read-only database!

	return walkRoot(tx)
}

func main() {
	if err := runProgram(); err != nil {
		fmt.Fprintf(os.Stderr, "error encountered: %v\n", err)
		os.Exit(1)
	}
}
