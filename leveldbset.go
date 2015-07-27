package leveldbset

import "fmt"
import "github.com/syndtr/goleveldb/leveldb"
import "time"
import "strconv"
import "sync/atmoic"

func main() {
	fmt.Println("Hello, playground")
}


const DECIMAL_BASE = 10

type LeveldbSet struct{
	name string
	db   *leveldb.DB
	capacity uint64
}

func New(name string, db *leveldb.DB) *LeveldbSet {
	return &LeveldbSet{name: name, db: db}
}

func (s *LeveldbSet) Add(element string) error{

	key := name + "#" + element
	
	now := time.Now().Unix()
	nowS := strconv.FormatInt(now, DECIMAL_BASE)
	
	
	err := db.Put([]byte(key), []byte(nowS))
	if err == nil {
		atomic.AddUint64(&(s.capacity), 1)
	}
	
	return err
}

func (s *LeveldbSet) Capacity() uint64{
	return s.capacity
}

func (s *LeveldbSet) Remove(element string) error{
	key := name + "#" + element
	
	
	val, err := s.db.Get([]byte(key))
	if err == leveldb.ErrNotFound {
	
		return errors.New("key not found")
	} elseif err != nil {
	
		return errors.New(err)
	}
	
	
	err = s.db.Delete([]byte(key))
	if err == nil {
		atomic.AddUint64(&(s.capacity), -1)
	}
	
	return err
}

func (s *LeveldbSet) IsEmpty() bool {

	if s.capacity == 0{
		return true
	}
	
	return false
}
