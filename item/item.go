package item

import (
	"bytes"
	"encoding/binary"
)

type status uint8

const (
	Deleted status = iota
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
	binary.Write(b, binary.LittleEndian, i)

	return b.Bytes()
}