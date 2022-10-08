package service

import (
	"encoding/json"
	"fmt"
	"path"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	updateBucketName   = "UpdateEvents"
	positionBucketName = "PositionEvents"
	deleteBucketName   = "DeleteEvents"
)

type Store interface {
	Load(chan []EventLog) error
	Update(EventLog) error
	Position(EventLog) error
	Delete(EventLog) error
	Close() error
}

type BoltStore struct {
	db *bolt.DB
}

func NewBoltStore(root string) (*BoltStore, error) {
	path := path.Join(root, "crdt.db")
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	if err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(updateBucketName))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	if err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(positionBucketName))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	if err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(deleteBucketName))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return &BoltStore{
		db: db,
	}, nil
}

func (s *BoltStore) loadEventsToCRDTFromBucket(eventChan chan []EventLog, bucketName string) error {
	return s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var e EventLog
			err := json.Unmarshal(v, &e)
			if err != nil {
				return err
			}
			eventChan <- []EventLog{e}
		}
		return nil
	})
}

func (s *BoltStore) putEventInBucket(tx *bolt.Tx, e EventLog, bucketName string) error {
	b := tx.Bucket([]byte(bucketName))

	buf, err := json.Marshal(e)
	if err != nil {
		return err
	}

	return b.Put([]byte(e.ListItemKey), buf)
}

func (s *BoltStore) Load(c chan []EventLog) error {
	errChan := make(chan error)

	go func() {
		err := s.loadEventsToCRDTFromBucket(c, updateBucketName)
		if err != nil {
			errChan <- err
		}
		errChan <- nil
	}()

	go func() {
		err := s.loadEventsToCRDTFromBucket(c, positionBucketName)
		if err != nil {
			errChan <- err
		}
		errChan <- nil
	}()

	go func() {
		err := s.loadEventsToCRDTFromBucket(c, deleteBucketName)
		if err != nil {
			errChan <- err
		}
		errChan <- nil
	}()

	return <-errChan
}

func (s *BoltStore) Update(e EventLog) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return s.putEventInBucket(tx, e, updateBucketName)
	})
}

func (s *BoltStore) Position(e EventLog) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return s.putEventInBucket(tx, e, positionBucketName)
	})
}

func (s *BoltStore) Delete(e EventLog) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		if err := s.putEventInBucket(tx, e, deleteBucketName); err != nil {
			return err
		}

		// Delete the UpdateEvent key
		b := tx.Bucket([]byte(updateBucketName))
		return b.Delete([]byte(e.ListItemKey))
	})
}

func (s *BoltStore) Close() error {
	return s.db.Close()
}
