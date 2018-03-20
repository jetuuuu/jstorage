package bitset

import (
	"sync/atomic"
	"encoding/binary"
	"bytes"
)

const (
	Zero uint32 = 0
	One uint32 = 1
)

type BitSet interface {
	Set(pos uint64, val uint32)
	IsOne(pos uint64) bool
	IsZero(pos uint64) bool
	Bytes() []byte
}

type bitset struct {
	bits []uint32
	size int
}

func New(size int) BitSet {
	b := bitset{bits: make([]uint32, size), size: size}
	return b
}

func Load(b []byte) (BitSet, error) {
	var (
		size int32
		bits []uint32
	)
	r := bytes.NewReader(b)
	err := binary.Read(r, binary.LittleEndian, &size)
	if err == nil {
		bits = make([]uint32, size)
		err = binary.Read(r, binary.LittleEndian, bits)
	}

	return bitset{bits: bits, size: int(size)}, err
}

func (b bitset) Set(pos uint64, val uint32) {
	atomic.StoreUint32(&b.bits[pos], val)
}

func (b bitset) IsOne(pos uint64) bool {
	val := atomic.LoadUint32(&b.bits[pos])
	return val  == One
}

func (b bitset) IsZero(pos uint64) bool {
	val := atomic.LoadUint32(&b.bits[pos])
	return val  == Zero
}

func (b bitset) Bytes() []byte {
	ret := make([]uint32, b.size)
	for i := 0; i < b.size; i++ {
		ret[i] = atomic.LoadUint32(&b.bits[i])
	}

	blob := bytes.NewBuffer(nil)
	binary.Write(blob, binary.LittleEndian, int32(b.size))
	binary.Write(blob, binary.LittleEndian, ret)

	return blob.Bytes()
}
