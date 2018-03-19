package sstable

import (
	"bytes"
	"os"
	"encoding/binary"
	"math/rand"
	"github.com/spaolacci/murmur3"
	"github.com/jetuuuu/jstorage/bloom"
)

/*

  md5 hash -- first 128 bits
  bloom filter size -- 64bits
  indexes size -- 64 bits
  bloom filter data
  index data
  data
*/

var byteOrder = binary.LittleEndian

type SStable struct {
	indexes indexes
	b *bytes.Buffer
	filter bloom.BloomFilter
	file *os.File
	endP int
}

func New(filter bloom.BloomFilter) SStable {
	s := SStable{
		b: bytes.NewBuffer(nil),
		filter: filter,
	}

	//md5
	binary.Write(s.b, byteOrder, rand.Uint64())
	binary.Write(s.b, byteOrder, rand.Uint64())
	//bloom filter size
	binary.Write(s.b, byteOrder, rand.Uint64())
	//indexes size
	binary.Write(s.b, byteOrder, rand.Uint64())

	return s
}

func (s SStable) Write(i item) {

}

func (s SStable) Flush() error {
	fileName := ""

	var err error
	if s.file, err = os.Create(fileName); err == nil {
		data := s.b.Bytes()

		h1, h2 := murmur3.Sum128(data[16:])
		byteOrder.PutUint64(data, h1)
		byteOrder.PutUint64(data[8:], h2)

		_, err = s.file.Write(data)
	}
	return err
}

func (s SStable) Close() error {
	return s.file.Close()
}