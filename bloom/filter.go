package bloom

import (
	"github.com/spaolacci/murmur3"
	"github.com/jetuuuu/jstorage/bitset"
	"github.com/jetuuuu/jstorage/utils"
	"math/rand"
	"math"
	"hash"
)

var (
	ln2InSquare = math.Ln2 * math.Ln2
)

type BloomFilter interface {
	Add(val []byte)
	Test(val []byte) bool
}

type bloomFilter struct {
	m uint64
	k int
	bits bitset.BitSet
	hashers []hash.Hash64
}

func New(count uint) BloomFilter {
	m, k := optimalMAndK(count, 0.0001)
	hashers := make([]hash.Hash64, k)
	for i := 0; i < k; i++ {
		hashers[i] = newHasher()
	}

	return bloomFilter{m:uint64(m), k:k, bits: bitset.New(m), hashers:hashers}
}

func (bf bloomFilter) Add(val []byte) {
	for _, hasher := range bf.hashers {
		hasher.Reset()
		hasher.Write(val)
		bf.bits.Set(hasher.Sum64() % bf.m, bitset.One)
	}
}

func (bf bloomFilter) Test(val []byte) bool {
	for _, hasher := range bf.hashers {
		hasher.Reset()
		hasher.Write(val)
		if bf.bits.IsZero(hasher.Sum64() % bf.m) {
			return false
		}
	}

	return true
}

func optimalMAndK(maxCount uint, errorProb float64) (int, int) {
	m := - (float64(maxCount) * math.Log(errorProb))/ln2InSquare
	k := (m/float64(maxCount)) * math.Ln2

	return utils.Round(m), utils.Round(k)
}

func newHasher() hash.Hash64 {
	return murmur3.New64WithSeed(rand.Uint32())
}
