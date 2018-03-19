package jstoradge

import (
	"github.com/jetuuuu/jstorage/memtable"
	"github.com/jetuuuu/jstorage/sstable"
)

const maxSize  =	100

type Jstorage struct {
	m *memtable.MemTable
	sstables []*sstable.SStable
}

func New() Jstorage {
	return Jstorage{memtable.New(), nil}
}
