package memtable

import (
	"github.com/jetuuuu/jstorage/disktable"
	"github.com/jetuuuu/jstorage/utils/spinlock"
	"github.com/jetuuuu/jstorage/item"
)

type MemTable struct {
	s *spinlock.SpinLock
	m map[string]item.Item
	currentSize int
}

func New() *MemTable {
	return &MemTable{s: spinlock.New(), m: make(map[string]item.Item), currentSize: 0}
}

func (mt MemTable) flush() disktable.DiskTable {
	table := disktable.New(nil)
	table.Flush()
	return table
}


func (mt *MemTable) Set(key string, value []byte) {
	mt.s.Lock()
	mt.m[key] = item.Item{Value:value}
	mt.currentSize += len(key) + len(value)
	mt.s.Unlock()
}

func (mt MemTable) Get(key string) ([]byte, bool) {
	mt.s.Lock()
	val, ok := mt.m[key]
	mt.s.Unlock()
	return val.Value, ok
}

func (mt MemTable) Del(key string) {
	mt.s.Lock()
	mt.m[key] = item.Item{Status: item.Deleted}
	mt.s.Unlock()
}