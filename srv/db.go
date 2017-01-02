package srv

import (
	"fmt"
	"github.com/boltdb/bolt"
)

type Files struct {
	Jobs    string
	Cookies string
}

type Stores struct {
	Cookies CookieMaker
	Jobs    JobQueuer
}

func NewStores(f Files) (Stores, error) {
	st := Stores{}
	jobsStore, err := NewDbStore(f.Jobs)
	if err != nil {
		return st, err
	}

	st.Jobs = jobsStore
	st.Cookies = &CookieJar{db: jobsStore.db}

	return st, nil
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
