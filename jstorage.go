package jstoradge

import (
	"github.com/jetuuuu/jstorage/memtable"
	"github.com/jetuuuu/jstorage/disktable"
)

const maxSize  =	100

type Jstorage struct {
	m *memtable.MemTable
	sstables []*disktable.DiskTable
}

func New() Jstorage {
	return Jstorage{memtable.New(), nil}
}
