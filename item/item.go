package item

import (
	"bytes"
	"encoding/binary"
	"io"
)

type status uint8

const (
	Deleted status = 1
)

type Item struct {
	Key string
	Value []byte
	Status status
}

func (i Item) Len() int {
	return len(i.Key) + len(i.Value) + 1
}

func (i Item) Bytes() []byte {
	b := bytes.NewBuffer(nil)

	binary.Write(b, binary.LittleEndian, len(i.Key))
	binary.Write(b, binary.LittleEndian, len(i.Value))
	binary.Write(b, binary.LittleEndian, i.Status)
	binary.Write(b, binary.LittleEndian, []byte(i.Key))
	binary.Write(b, binary.LittleEndian, i.Value)

	return b.Bytes()
}

func Load(r io.Reader) Item {
	var (
		key []byte
		keyLen int
		value []byte
		valueLen int
		s uint8
	)

	binary.Read(r, binary.LittleEndian, &keyLen)
	binary.Read(r, binary.LittleEndian, &valueLen)
	binary.Read(r, binary.LittleEndian, &s)

	key = make([]byte, keyLen)
	value = make([]byte, valueLen)
	binary.Read(r, binary.LittleEndian, &key)
	binary.Read(r, binary.LittleEndian, &value)

	return Item{Key: string(key), Value: value, Status: status(s)}
}