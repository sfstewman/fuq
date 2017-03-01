package db

import (
	"fmt"
	"github.com/boltdb/bolt"
)

type Files struct {
	Jobs    string
	Cookies string
}

type Stores struct {
	Cookies   *CookieJar
	Jobs      *JobStore
	databases []*bolt.DB
}

func openDB(path string) (*bolt.DB, error) {
	db, err := bolt.Open(path, 0666, nil)
	if err != nil {
		return nil, fmt.Errorf("error opening database '%s': %v",
			path, err)
	}
	return db, nil
}

func (st *Stores) Close() error {
	if err := st.Jobs.Close(); err != nil {
		return err
	}

	if err := st.Cookies.Close(); err != nil {
		return err
	}

	for _, db := range st.databases {
		if err := db.Close(); err != nil {
			return err
		}
	}

	st.Jobs = nil
	st.Cookies = nil
	st.databases = nil

	return nil
}

func NewStores(f Files) (st *Stores, err error) {
	var (
		jobsDB, cookieDB *bolt.DB
	)

	st = &Stores{
		databases: make([]*bolt.DB, 0, 2),
	}

	defer func() {
		if err != nil {
			// error ... handle cleanup
			st.Close()
		}
	}()

	jobsDB, err = openDB(f.Jobs)
	if err != nil {
		return
	}

	st.databases = append(st.databases, jobsDB)
	st.Jobs, err = newJobStore(jobsDB)
	if err != nil {
		return
	}

	if f.Cookies != "" && f.Cookies != f.Jobs {
		cookieDB, err = openDB(f.Cookies)
		if err != nil {
			return
		}

		st.databases = append(st.databases, cookieDB)
	} else {
		cookieDB = jobsDB
	}

	st.Cookies, err = newCookieJar(cookieDB)
	if err != nil {
		return
	}

	return
}

func txRecreateBucket(tx *bolt.Tx, n []byte) (*bolt.Bucket, error) {
	if tx.Bucket(n) != nil {
		if err := tx.DeleteBucket(n); err != nil {
			return nil, fmt.Errorf("error deleting '%s' bucket", n)
		}
	}

	bkt, err := tx.CreateBucket(n)
	if err != nil {
		return nil, fmt.Errorf("error creating '%s' bucket", n)
	}

	return bkt, nil
}

func bktRecreateBucket(bkt *bolt.Bucket, n []byte) error {
	if bkt.Bucket(n) != nil {
		if err := bkt.DeleteBucket(n); err != nil {
			return fmt.Errorf("error deleting '%s' bucket", n)
		}
	}

	if _, err := bkt.CreateBucket(n); err != nil {
		return fmt.Errorf("error creating '%s' bucket", n)
	}

	return nil
}

func invalidSchema(reason string) {
	// XXX - should this be a panic?  an invalid schema is clearly a
	// programmer error and cannot be recovered from here
	panic(fmt.Sprintf("invalid database schema: %s", reason))
}
