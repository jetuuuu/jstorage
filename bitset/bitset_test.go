package bitset

import (
	"testing"
	"fmt"
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
