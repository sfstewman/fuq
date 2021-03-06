package db

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/sfstewman/fuq"
	"gopkg.in/vmihailenco/msgpack.v2"
	"log"
)

type CookieJar struct {
	db *bolt.DB
}

var _ fuq.CookieMaker = (*CookieJar)(nil)

func NewCookieJar(jarPath string) (*CookieJar, error) {
	db, err := bolt.Open(jarPath, 0666, nil)
	if err != nil {
		return nil, fmt.Errorf("error opening database '%s': %v",
			jarPath, err)
	}

	return newCookieJar(db)
}

func newCookieJar(db *bolt.DB) (*CookieJar, error) {
	if err := db.Update(createCookieBucket); err != nil {
		return nil, fmt.Errorf("error creating database scheme: %v", err)
	}

	return &CookieJar{db: db}, nil
}

func createCookieBucket(tx *bolt.Tx) error {
	_, err := tx.CreateBucketIfNotExists(namesBucket)
	if err != nil {
		return err
	}

	_, err = tx.CreateBucketIfNotExists(cookieBucket)
	if err != nil {
		return err
	}

	return nil
}

func (cj *CookieJar) Close() error {
	return nil
}

func (cj *CookieJar) namesBucket(tx *bolt.Tx) *bolt.Bucket {
	names := tx.Bucket(namesBucket)
	if names == nil {
		invalidSchema("missing names bucket")
	}

	return names
}

func (cj *CookieJar) cookiesBucket(tx *bolt.Tx) *bolt.Bucket {
	cookies := tx.Bucket(cookieBucket)
	if cookies == nil {
		invalidSchema("missing cookies bucket")
	}

	return cookies
}

func isNameRegistered(names *bolt.Bucket, name string) bool {
	exists := names.Get([]byte(name))
	return exists != nil
}

func addUniqueName(names *bolt.Bucket, name string) error {
	return names.Put([]byte(name), []byte{})
}

func uniquifyName(names *bolt.Bucket, name string) (string, error) {
	if !names.Writable() {
		panic("bucket passed to uniquifyNames must be writable (hold the write lock)")
	}

	for i := uint(1); i > 0; i++ {
		attempt := fmt.Sprintf("%s:%d", name, i)
		if !isNameRegistered(names, attempt) {
			err := addUniqueName(names, attempt)
			return attempt, err
		}
	}

	log.Panicf("could not uniquify name '%s'", name)

	// should not reach
	return "", nil
}

func (cj *CookieJar) SessionKey() (string, error) {
	sess, err := cj.makeCookie()
	return string(sess), err
}

func (cj *CookieJar) makeCookie() (fuq.Cookie, error) {
	raw := make([]byte, CookieSeqNumBytes)
	hashed := make([]byte, base64.RawStdEncoding.EncodedLen(len(raw)))

	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("error generating cookie: %v", err)
	}

	base64.RawStdEncoding.Encode(hashed, raw)
	return fuq.Cookie(hashed), nil
}

func (cj *CookieJar) generateCookie(tx *bolt.Tx, ni fuq.NodeInfo) (fuq.Cookie, error) {
	cookies := cj.cookiesBucket(tx)
	var cookie fuq.Cookie

	for {
		var err error

		cookie, err = cj.makeCookie()
		if err != nil {
			return "", fmt.Errorf("error generating cookie: %v", err)
		}

		log.Printf("node %s: cookie is %s", ni.Node, cookie)

		if exists := cookies.Get([]byte(cookie)); exists == nil {
			break
		}
		log.Printf("cookie exists, retrying")
	}

	b, err := msgpack.Marshal(&ni)
	if err != nil {
		return "", err
	}

	if err := cookies.Put([]byte(cookie), b); err != nil {
		return "", err
	}

	return cookie, nil
}

func (cj *CookieJar) needsUniqueName(names *bolt.Bucket, ni fuq.NodeInfo) bool {
	if ni.UniqName == "" || ni.UniqName == ni.Node {
		return true
	}

	return isNameRegistered(names, ni.UniqName)
}

func (cj *CookieJar) IsUniqueName(n string) (bool, error) {
	var ret bool

	err := cj.db.View(func(tx *bolt.Tx) error {
		names := cj.namesBucket(tx)
		ret = isNameRegistered(names, n)
		return nil
	})

	return ret, err
}

func (cj *CookieJar) MakeCookie(ni fuq.NodeInfo) (fuq.Cookie, error) {
	var cookie fuq.Cookie

	err := cj.db.Update(func(tx *bolt.Tx) error {
		var names *bolt.Bucket
		var err error

		names = cj.namesBucket(tx)
		if cj.needsUniqueName(names, ni) {
			ni.UniqName, err = uniquifyName(names, ni.Prefix())
			if err != nil {
				return err
			}
		}

		cookie, err = cj.generateCookie(tx, ni)
		if err != nil {
			return err
		}

		return nil
	})

	return cookie, err
}

func (cj *CookieJar) RenewCookie(ni fuq.NodeInfo) (fuq.Cookie, error) {
	var cookie fuq.Cookie

	if ni.UniqName == "" || ni.UniqName == ni.Node {
		return cookie, errors.New("invalid unique name")
	}

	err := cj.db.Update(func(tx *bolt.Tx) error {
		var names *bolt.Bucket
		var err error

		names = tx.Bucket(namesBucket)

		if !isNameRegistered(names, ni.UniqName) {
			addUniqueName(names, ni.UniqName)
		}

		cookie, err = cj.generateCookie(tx, ni)
		if err != nil {
			return err
		}

		return nil
	})

	return cookie, err
}

func (cj *CookieJar) ExpireCookie(cookie fuq.Cookie) error {
	bc := []byte(cookie)

	err := cj.db.Update(func(tx *bolt.Tx) error {
		cookies := cj.cookiesBucket(tx)
		curs := cookies.Cursor()
		k, _ := curs.Seek(bc)
		if !bytes.Equal(k, bc) {
			return nil
		}

		if err := curs.Delete(); err != nil {
			return fmt.Errorf("error deleting cookie '%s': %v",
				cookie, err)
		}

		return nil
	})

	return err
}

func (cj *CookieJar) Lookup(cookie fuq.Cookie) (fuq.NodeInfo, error) {
	ni := fuq.NodeInfo{}
	err := cj.db.View(func(tx *bolt.Tx) error {
		cookies := cj.cookiesBucket(tx)

		log.Printf("looking up cookie '%s'", cookie)
		v := cookies.Get([]byte(cookie))
		if v == nil {
			return nil
		}

		if err := msgpack.Unmarshal(v, &ni); err != nil {
			return err
		}
		log.Printf("found node '%s'", ni.UniqName)

		return nil
	})

	return ni, err
}

func (cj *CookieJar) AllNodes() ([]fuq.NodeInfo, error) {
	var nodes []fuq.NodeInfo = nil

	err := cj.db.View(func(tx *bolt.Tx) error {
		cookies := cj.cookiesBucket(tx)
		n := cookies.Stats().KeyN
		nodes = make([]fuq.NodeInfo, 0, n)

		err := cookies.ForEach(func(k, v []byte) error {
			ni := fuq.NodeInfo{}
			err := msgpack.Unmarshal(v, &ni)
			if err == nil {
				nodes = append(nodes, ni)
			}
			return err
		})

		return err
	})

	return nodes, err
}

// Note: tags are not indexed, so this requires a full scan of all
// nodes.  This should be fine unless the number of nodes gets pretty
// large.  At that point, we should add an index.
func (cj *CookieJar) NodesWithTag(tag string) ([]fuq.NodeInfo, error) {
	var nodes []fuq.NodeInfo = nil

	err := cj.db.View(func(tx *bolt.Tx) error {
		cookies := cj.cookiesBucket(tx)
		n := cookies.Stats().KeyN
		nodes = make([]fuq.NodeInfo, 0, n)

		err := cookies.ForEach(func(k, v []byte) error {
			ni := fuq.NodeInfo{}
			err := msgpack.Unmarshal(v, &ni)
			if err != nil {
				return err
			}

			// linear scan... we can do better here
			for _, t := range ni.Tags {
				if t == tag {
					nodes = append(nodes, ni)
					return nil
				}
			}

			return nil
		})

		return err
	})

	return nodes, err
}
