package disktable

import (
	"bytes"
	"os"
	"encoding/binary"
	"github.com/spaolacci/murmur3"
	"github.com/jetuuuu/jstorage/bloom"
	"github.com/jetuuuu/jstorage/item"
	"github.com/jetuuuu/jstorage/utils/once"
	"errors"
)

/*

  md5 hash -- first 128 bits
  bloom filter size -- 64bits
  bloom filter data
  indexes size -- 64 bits
  index data
  data
*/

var (
	byteOrder = binary.LittleEndian
	missKey = errors.New("miss key")
)

type DiskTable struct {
	indexes indexes
	header *bytes.Buffer
	body *bytes.Buffer
	filter bloom.BloomFilter
	file *os.File
	endP int
	name string
	cmdChan chan cmd
	once *once.Once
}

type cmd struct {
	seek int64
	respChan chan item.Item
}

func New(to string, filter bloom.BloomFilter) *DiskTable {
	s := DiskTable{
		body: bytes.NewBuffer(nil),
		header: bytes.NewBuffer(nil),
		indexes: make(indexes),
		filter: filter,
		name: to,
		cmdChan: make(chan cmd, 10),
		once: once.New(),
	}

	return &s
}

func (s *DiskTable) Write(i item.Item) {
	offset := s.endP
	s.endP += i.Len()
	s.indexes.Set(i.Key, offset)
	b := i.Bytes()
	s.body.Write(b)
}

func (s DiskTable) Flush() error {
	var err error
	if s.file, err = os.Create(s.name); err == nil {
		bodyBytes := s.body.Bytes()

		h1, h2 := murmur3.Sum128(bodyBytes)
		binary.Write(s.header, byteOrder, h1)
		binary.Write(s.header, byteOrder, h2)

		bloomBytes := s.filter.Bytes()
		binary.Write(s.header, byteOrder, int32(len(bloomBytes)))
		binary.Write(s.header, byteOrder, bloomBytes)

		indexesBytes := s.indexes.Bytes()
		binary.Write(s.header, byteOrder, int32(len(indexesBytes)))
		binary.Write(s.header, byteOrder, indexesBytes)

		if _, err = s.file.Write(s.header.Bytes()); err == nil {
			if _, err = s.file.Write(bodyBytes); err == nil {
				s.file.Close()
			}
		}

		if err != nil {
			err = os.Remove(s.name)
		}
	}

	return err
}

func (s DiskTable) Get(key string) (item.Item, error) {
	s.once.Do(s.reader)

	i := item.Item{}
	if !s.filter.Test([]byte(key)) {
		return i, missKey
	}

	offset, ok := s.indexes.Get(key)
	if !ok {
		return i, missKey
	}

	c := make(chan item.Item, 1)
	s.cmdChan <- cmd{seek: int64(offset), respChan: c}
	i = <- c
	return i, nil
}

func (s DiskTable) reader() {
	go func () {
		f, _ := os.Open(s.name)
		for {
			cmd := <- s.cmdChan
			f.Seek(cmd.seek, 0)
			cmd.respChan <- item.Load(f)
		}
	}()
}