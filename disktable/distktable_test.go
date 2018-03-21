package disktable

import (
	"testing"
	"github.com/jetuuuu/jstorage/bloom"
	"github.com/jetuuuu/jstorage/item"
	"encoding/binary"
	"os"
)

func TestDiskTable_Flush(t *testing.T) {
	keys := []string{
		"key1", "key2", "key3", "key4",
		"key10", "key20", "key30", "key40",
	}

	filter := bloom.New(uint(len(keys)))
	for _, k := range keys {
		filter.Add([]byte(k))
	}

	dt := New(filter)
	for _, k := range keys {
		dt.Write(item.Item{Key: k, Value: []byte(k)})
	}

	to := "disktable_test.db"
	err := dt.Flush(to)
	if err != nil {
		t.Errorf("Error must be nil but %v", err)
		os.Remove(to)
	}

	f, err := os.Open(to)
	close := func() {
		f.Close()
		os.Remove(to)
	}
	if err != nil {
		t.Errorf("Error must be nil but %v", err)
		close()
	}

	var (
		h1, h2 uint64
	)
	binary.Read(f, binary.LittleEndian, &h1)
	binary.Read(f, binary.LittleEndian, &h2)

	if h1 == 0 && h2 == 0 {
		t.Errorf("h1 nad h2 are zero")
		close()
	}

	close()
}
