package leveldbset

import (
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const DECIMAL_BASE = 10

type LeveldbSet struct {
	name     string
	db       *leveldb.DB
	capacity int64
}

func New(name string, db *leveldb.DB) *LeveldbSet {
	return &LeveldbSet{name: name, db: db}
}

func (s *LeveldbSet) Add(element string) error {

	key := s.encodeKey(element)

	now := time.Now().Unix()
	nowS := strconv.FormatInt(now, DECIMAL_BASE)

	err := s.db.Put([]byte(key), []byte(nowS), nil)
	if err == nil {
		atomic.AddInt64(&(s.capacity), 1)
	}

	return err
}

func (s *LeveldbSet) encodeKey(key string) string {
	return s.name + "#" + key
}

func (s *LeveldbSet) decodeKey(key string) string {
	return strings.TrimPrefix(key, s.name+"#")
}

func (s *LeveldbSet) Capacity() int64 {
	return s.capacity
}

func (s *LeveldbSet) Remove(element string) error {
	key := s.encodeKey(element)

	_, err := s.db.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {

		return errors.New("key not found")
	} else if err != nil {

		return err
	}

	err = s.db.Delete([]byte(key), nil)
	if err == nil {
		atomic.AddInt64(&(s.capacity), -1)
	}

	return err
}

func (s *LeveldbSet) Pop() string {
	iter := s.db.NewIterator(nil, nil)

	iter.Next()
	key := iter.Key()
	s.db.Delete([]byte(key), nil)

	elem := s.decodeKey(string(key))

	return elem
}

func (s *LeveldbSet) IsEmpty() bool {

	if s.capacity == 0 {
		return true
	}

	return false
}
