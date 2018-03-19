package bitset

import "sync/atomic"

const (
	Zero uint32 = 0
	One uint32 = 1
)

type BitSet interface {
	Set(pos uint64, val uint32)
	IsOne(pos uint64) bool
	IsZero(pos uint64) bool
}

type bitset struct {
	bits []uint32
}

func New(size int) BitSet {
	b := bitset{bits: make([]uint32, size)}
	return b
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

func (b bitset) String() string {
	ret := ""
	size := len(b.bits)
	for i := 0; i < size; i++ {
		if b.IsZero(uint64(i)) {
			ret += "0"
		} else {
			ret += "1"
		}
	}

	return ret
}