package srv

import (
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	// "github.com/sfstewman/fuq"
	"log"
)

func runFixups(tx *bolt.Tx, d *DbStore) error {
	log.Println("Pass 1: Check job state consistency")

	jb := tx.Bucket(jobBucket)
	if jb == nil {
		return errors.New("missing job bucket")
	}

	log.Printf("Dropping and rebuilding indices")
	if err := jb.DeleteBucket(jobIndexBucket); err != nil {
		return fmt.Errorf("error rebuilding primary job index: %v", err)
	}

	b := jb.Bucket(jobIndexBucket)
	if b != nil {
		if err := jb.DeleteBucket(jobIndexBucket); err != nil {
			return fmt.Errorf("error rebuilding primary job index: %v", err)
		}
	}

	indexBkt, err := jb.CreateBucket(jobIndexBucket)
	if err != nil {
		return fmt.Errorf("error recreating primary job index: %v",
			err)
	}

	for status, bktName := range statusBuckets {
		b := jb.Bucket(bktName)
		if b != nil {
			if err := jb.DeleteBucket(bktName); err != nil {
				return fmt.Errorf("error rebuilding index '%s': %v",
					status, err)
			}
		}

		if _, err := jb.CreateBucket(bktName); err != nil {
			return fmt.Errorf("error rebuilding index '%s': %v",
				status, err)
		}
	}

	// ids match and indexes are correct
	curs := jb.Cursor()
	k, v := curs.Seek([]byte(jobInfoPrefix))
	for ; k != nil; k, v = curs.Next() {
		ks := string(k)
		log.Printf("k = %s", ks)
		if !validJobKeyPrefix(ks) {
			break
		}

		jobId := jobKeyToId(ks)
		if jobId == 0 {
			log.Printf("invalid job id for job key '%s'", ks)
			continue
		}

		job, err := bytesToJob(v)
		if err != nil {
			log.Printf("error parsing job %d (key '%s')", jobId, ks)
			continue
		}

		if job.JobId != jobId {
			log.Printf("job id mismatch for job %d (key '%s')", jobId, ks)
			job.JobId = jobId
			nv, err := jobToBytes(&job)
			if err != nil {
				return err
			}
			if err := jb.Put(k, nv); err != nil {
				log.Printf("error updating job %d (key '%s')", jobId, ks)
			}
			// reset cursor
			curs.Seek(k)
		}

		indKey := jobIndexKey(job)
		if err := indexBkt.Put(indKey, []byte{}); err != nil {
			log.Printf("error updating primary index for job %d (key '%s')",
				jobId, ks)
		}

		statusBkt, err := d.getStatusBucket(tx, job.Status)
		if err != nil {
			log.Printf("job %d: error fetching status bucket '%s': %v",
				jobId, job.Status, err)
			continue
		}

		if err := statusBkt.AddJob(jobId); err != nil {
			return fmt.Errorf("error updating status '%s' index: %v",
				job.Status, err)
		}
		log.Printf("added job %d to index '%s'", job.JobId, job.Status)
	}

	return nil
}

func Fsck(d *DbStore) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		return runFixups(tx, d)
	})
}
