package bloom

import (
	"github.com/spaolacci/murmur3"
	"github.com/jetuuuu/jstorage/bitset"
	"github.com/jetuuuu/jstorage/utils"
	"math/rand"
	"math"
	"hash"
	"encoding/binary"
	"bytes"
)

var (
	ln2InSquare = math.Ln2 * math.Ln2
	byteOrder = binary.LittleEndian
)

type BloomFilter interface {
	Add(val []byte)
	Test(val []byte) bool
	Bytes() []byte
	Size() uint64
}

type bloomFilter struct {
	m uint64
	k int
	bits bitset.BitSet
	hashers []hash.Hash64
	seeds []uint32
}

func New(count uint) BloomFilter {
	m, k := optimalMAndK(count, 0.0001)
	hashers := make([]hash.Hash64, k)
	seeds := make([]uint32, k)
	for i := 0; i < k; i++ {
		seeds[i] = rand.Uint32()
		hashers[i] = murmur3.New64WithSeed(seeds[i])
	}

	return bloomFilter{m:uint64(m), k:k, bits: bitset.New(m), hashers:hashers, seeds: seeds}
}


func Load(b []byte) (BloomFilter, error) {
	var (
		err error
		k int32
		seeds []uint32
		bits []byte
	)
	r := bytes.NewReader(b)
	err = binary.Read(r, byteOrder, &k)
	if err != nil {
		return nil, err
	}

	seeds = make([]uint32, k)
	err = binary.Read(r, byteOrder, &seeds)
	if err != nil {
		return nil, err
	}

	err = binary.Read(r, byteOrder, &bits)
	if err !=nil {
		return nil, err
	}

	set, err := bitset.Load(bits)
	if err != nil {
		return nil, err
	}

	hashers := make([]hash.Hash64, len(seeds))
	for i, seed := range seeds {
		hashers[i] = murmur3.New64WithSeed(seed)
	}

	return bloomFilter{bits: set, seeds: seeds, hashers:hashers}, nil
}

func (bf bloomFilter) Size() uint64 {
	return bf.m
}

func (bf bloomFilter) Bytes() []byte {
	b := bytes.NewBuffer(nil)

	binary.Write(b, byteOrder, int32(bf.k))
	binary.Write(b, byteOrder, bf.seeds)
	binary.Write(b, byteOrder, bf.bits.Bytes())

	return b.Bytes()
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
