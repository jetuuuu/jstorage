package disktable

import (
	"bytes"
	"encoding/binary"
)

type item struct {
	Key string
	Value []byte
}

func (i item) Len() int {
	return len(i.Key) + len(i.Value)
}

func (i item) Bytes() []byte {
	b := bytes.NewBuffer(nil)
	binary.Write(b, binary.LittleEndian, i)

	return b.Bytes()
}