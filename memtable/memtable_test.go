package memtable

import (
	"testing"
	"strconv"
)

func BenchmarkMemTable_Set(b *testing.B) {
	m := New()

	count := 100000
	keys := make([]string, count)
	values := make([][]byte, count)
	for i := 0; i < count; i++ {
		keys[i] = "test_" + strconv.Itoa(i)
		values[i] = []byte(keys[i])
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < count; i++ {
		m.Set(keys[i], values[i])
	}
}
