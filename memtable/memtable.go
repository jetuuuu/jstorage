package memtable

import (
	"github.com/jetuuuu/jstorage/disktable"
	"github.com/jetuuuu/jstorage/utils/spinlock"
)

type MemTable struct {
	s *spinlock.SpinLock
	m map[string][]byte
	currentSize int
}

func New() *MemTable {
	return &MemTable{s: spinlock.New(), m: make(map[string][]byte), currentSize: 0}
}

func (mt MemTable) flush() disktable.DiskTable {
	table := disktable.New(nil)
	table.Flush()
	return table
}


func (mt *MemTable) Set(key string, value []byte) {
	mt.s.Lock()
	mt.m[key] = value
	mt.currentSize += len(key) + len(value)
	mt.s.Unlock()
}

func (mt MemTable) Get(key string) ([]byte, bool) {
	mt.s.Lock()
	val, ok := mt.m[key]
	mt.s.Unlock()
	return val, ok
}
