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
	name string
	db   *leveldb.DB
	size int64
}

var ErrSetEmpty = errors.New("Set Empty")

//New returns a LeveldbSet object while creating/opening a leveldb file based on the name
//returns an error if there is any error opening the file
func New(name string) (*LeveldbSet, error) {

	db, err := leveldb.OpenFile("leveldbset/"+name, nil)
	if err != nil {
		return nil, err
	}

	return &LeveldbSet{name: name, db: db}, nil
}

//Add adds the supplied element to the Set
// returns the error if any encountered in put-ting to leveldb
func (s *LeveldbSet) Add(element string) error {

	key := s.encodeKey(element)

	now := time.Now().Unix()
	nowS := strconv.FormatInt(now, DECIMAL_BASE)

	err := s.db.Put([]byte(key), []byte(nowS), nil)
	if err == nil {
		atomic.AddInt64(&(s.size), 1)
	}

	return err
}

func (s *LeveldbSet) encodeKey(key string) string {
	return s.name + "#" + key
}

func (s *LeveldbSet) decodeKey(key string) string {
	return strings.TrimPrefix(key, s.name+"#")
}

//Size returns the size of the set
func (s *LeveldbSet) Size() int64 {
	return s.size
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
		atomic.AddInt64(&(s.size), -1)
	}

	return err
}

func (s *LeveldbSet) Pop() (string, error) {
	iter := s.db.NewIterator(nil, nil)

	if iter.Next() == false {
		return "", ErrSetEmpty
	}

	key := iter.Key()
	s.db.Delete([]byte(key), nil)

	elem := s.decodeKey(string(key))

	return elem, nil
}

func (s *LeveldbSet) IsEmpty() bool {

	if s.size == 0 {
		return true
	}

	return false
}
