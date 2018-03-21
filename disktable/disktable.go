package disktable

import (
	"bytes"
	"os"
	"encoding/binary"
	"github.com/spaolacci/murmur3"
	"github.com/jetuuuu/jstorage/bloom"
	"github.com/jetuuuu/jstorage/item"
)

/*

  md5 hash -- first 128 bits
  bloom filter size -- 64bits
  bloom filter data
  indexes size -- 64 bits
  index data
  data
*/

var byteOrder = binary.LittleEndian

type DiskTable struct {
	indexes indexes
	header *bytes.Buffer
	body *bytes.Buffer
	filter bloom.BloomFilter
	file *os.File
	endP int
}

func New(filter bloom.BloomFilter) *DiskTable {
	s := DiskTable{
		body: bytes.NewBuffer(nil),
		header: bytes.NewBuffer(nil),
		indexes: make(indexes),
		filter: filter,
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

func (s DiskTable) Flush(to string) error {
	var err error
	if s.file, err = os.Create(to); err == nil {
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
			err = os.Remove(to)
		}
	}

	return err
}