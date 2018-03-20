package bitset

import (
	"testing"
	"fmt"
	"bytes"
)

func TestBitset_Set(t *testing.T) {
	bitset := New(2)
	bitset.Set(0, Zero)
	bitset.Set(1, One)

	if bitset.IsZero(1) {
		t.Fatal("Must be One")
	}

	if bitset.IsOne(0) {
		t.Fatal("Must be Zero")
	}

	s := fmt.Sprintf("%s", bitset)
	if s != "01" {
		t.Fatal("Must be 01, but: " + s)
	}
}

func TestLoad(t *testing.T) {
	bitset := New(3)
	bitset.Set(0, One)
	bitset.Set(1, Zero)
	bitset.Set(2, One)

	newBitSet, err := Load(bitset.Bytes())
	if err != nil {
		t.Fatalf("Err is not nil but %s", err.Error())
	}

	if !newBitSet.IsOne(0) {
		t.Fatal("[position 0] Value is not 1 but 0")
	}

	if !newBitSet.IsZero(1) {
		t.Fatal("[position 1] Value is not 0 but 1")
	}

	if !newBitSet.IsOne(2) {
		t.Fatal("[position 2] Value is not 1 but 0")
	}
}

func TestBitset_Bytes(t *testing.T) {
	bitset := New(3)
	bitset.Set(0, One)
	bitset.Set(1, Zero)
	bitset.Set(2, One)

	b := bitset.Bytes()
	fmt.Println(b)
	if !bytes.Equal(b, []byte{3, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0}) {
		t.Fatal("Bytes is not valid")
	}
}

func BenchmarkBitset_Set(b *testing.B) {
	bitset := New(100000)
	for i := 0; i < 100000; i++ {
		bitset.Set(uint64(i), uint32(i))
	}
}