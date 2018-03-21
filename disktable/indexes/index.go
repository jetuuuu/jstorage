package indexes


type Indexes map[string]int

func (i Indexes) Bytes() []byte {
	return nil
}

func (i Indexes) Set(key string, offset int) {
	i[key] = offset
}

func (i Indexes) Get(key string) (int, bool) {
	offset, ok := i[key]
	return offset, ok
}

func Load(b []byte) Indexes {
	panic("not implemented")
	return Indexes{}
}