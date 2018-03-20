package memtable

import (
	"sync"
	"github.com/jetuuuu/jstorage/disktable"
)

type MemTable struct {
	sync.RWMutex
	m map[string][]byte
	currentSize int
}

func New() *MemTable {
	return &MemTable{sync.RWMutex{}, make(map[string][]byte), 0}
}

func (mt MemTable) flush() disktable.DiskTable {
	table := disktable.New(nil)
	table.Flush()
	return table
}


func (mt *MemTable) Set(key string, value []byte) {
	mt.Lock()
	mt.m[key] = value
	mt.currentSize += len(key) + len(value)
	mt.Unlock()
}

func (mt MemTable) Get(key string) ([]byte, bool) {
	mt.RLock()
	val, ok := mt.m[key]
	mt.RUnlock()
	return val, ok
}
