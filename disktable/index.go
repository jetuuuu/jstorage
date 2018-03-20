package disktable


type indexes map[string]int

func (i indexes) Bytes() []byte {
	return nil
}

func (i indexes) Set(key string, offset int) {
	i[key] = offset
}