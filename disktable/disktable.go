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
	"sync/atomic"
	"io"
	"github.com/jetuuuu/jstorage/disktable/indexes"
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
	byteOrder    = binary.LittleEndian
	missKeyError = errors.New("miss key")
	closedError  = errors.New("file closed")
	brokenFileError = errors.New("file broken")
)

type DiskTable struct {
	indexes indexes.Indexes
	header *bytes.Buffer
	body *bytes.Buffer
	filter bloom.BloomFilter
	file *os.File
	endP int
	name string
	cmdChan chan cmd
	once *once.Once
	writable uint32
}

type cmd struct {
	seek int64
	respChan chan item.Item
}

func New(to string, filter bloom.BloomFilter) *DiskTable {
	s := DiskTable{
		body: bytes.NewBuffer(nil),
		header: bytes.NewBuffer(nil),
		indexes: make(indexes.Indexes),
		filter: filter,
		name: to,
		cmdChan: make(chan cmd, 10),
		once: once.New(),
	}

	return &s
}

func (s *DiskTable) Write(i item.Item) error {
	if atomic.LoadUint32(&s.writable) == 1 {
		return closedError
	}
	offset := s.endP
	s.endP += i.Len()
	s.indexes.Set(i.Key, offset)
	_, err := s.body.Write(i.Bytes())

	return err
}

func (s *DiskTable) Flush() error {
	var err error
	if s.file, err = os.Create(s.name); err == nil {
		atomic.StoreUint32(&s.writable, 1)
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
		binary.Write(s.header, byteOrder, int32(len(bodyBytes)))

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
		return i, missKeyError
	}

	offset, ok := s.indexes.Get(key)
	if !ok {
		return i, missKeyError
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

func Load(from string) (*DiskTable , error) {
	d := DiskTable{
		name: from,
		writable: 1,
		once: once.New(),
		cmdChan: make(chan cmd, 10),}

	f, err := os.Open(from)
	if err != nil {
		return nil, err
	}

	rh := readerHelper{f}
	var h1, h2 uint64
	rh.Read(&h1)
	rh.Read(&h2)

	var (
		bloomSize int32
		bloomBits []byte
	)
	rh.Read(&bloomSize)
	bloomBits = make([]byte, bloomSize)
	rh.Read(&bloomBits)

	var (
		indexesSize int32
		indexesBits []byte
	)
	rh.Read(&indexesSize)
	indexesBits = make([]byte, indexesSize)
	rh.Read(&indexesBits)

	var (
		bodySize int32
		bodyBits []byte
		)

	rh.Read(&bodySize)
	bodyBits = make([]byte, bodySize)
	rh.Read(&bodyBits)

	if _h1, _h2 := murmur3.Sum128(bodyBits); _h1 != h1 || _h2 != h2 {
		return nil, brokenFileError
	}

	d.indexes = indexes.Load(indexesBits)
	if filter, err := bloom.Load(bloomBits); err == nil {
		d.filter = filter
	}

	return &d, nil
}

type readerHelper struct {
	r io.Reader
}

func (rh readerHelper) Read(data interface{}) error {
	return binary.Read(rh.r, byteOrder, data)
}