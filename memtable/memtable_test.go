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

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for i := 0; i < count; i++ {
				m.Set(keys[i], values[i])
			}
		}
	})
}


func BenchmarkMemTable_Get(b *testing.B) {
	m := New()

	count := 100000
	keys := make([]string, count)
	values := make([][]byte, count)
	for i := 0; i < count; i++ {
		keys[i] = "test_" + strconv.Itoa(i)
		values[i] = []byte(keys[i])
	}

	for i := 0; i < count; i++ {
		m.Set(keys[i], values[i])
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for i := 0; i < count; i++ {
				m.Get(keys[i])
			}
		}
	})
}