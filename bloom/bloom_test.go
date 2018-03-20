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

func BenchmarkBloomFilter_Add(b *testing.B) {
	count := 1000000
	keys := make([]string, count)
	for i := 0; i < count; i++ {
		keys[i] = "test" + strconv.Itoa(i)
	}
	b.ResetTimer()
	f := New(30000)

	for i := 0; i < count; i++ {
		f.Add([]byte(keys[i]))
	}
}

func BenchmarkBloomFilter_Test(b *testing.B) {
	count := 1000000
	keys := make([]string, count)
	for i := 0; i < count; i++ {
		keys[i] = "test" + strconv.Itoa(i)
	}

	f := New(30000)

	for i := 0; i < count; i++ {
		f.Add([]byte(keys[i]))
	}

	b.ResetTimer()

	for i := 0; i < count; i++ {
		f.Test([]byte(keys[i]))
	}
}