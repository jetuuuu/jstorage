package bloom

import (
	"testing"
	"strconv"
)

func TestBloomFilter_AddAndTest(t *testing.T) {
	b := New(300)
	for i := 0; i < 1000; i++ {
		key := "test" + strconv.Itoa(i)
		b.Add([]byte(key))
		if !b.Test([]byte(key)) {
			t.Fatalf("%s must be \"maybe true\" not false", key)
		}
	}
}