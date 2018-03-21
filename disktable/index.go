package disktable


type indexes map[string]int

func (i indexes) Bytes() []byte {
	return nil
}

func (i indexes) Set(key string, offset int) {
	i[key] = offset
}

func (i indexes) Get(key string) (int, bool) {
	offset, ok := i[key]
	return offset, ok
}