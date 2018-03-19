package sstable

type item struct {
	Key string
	Value []byte
}

func (i item) Len() int {
	return len(i.Key) + len(i.Value)
}