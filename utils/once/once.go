package once

import "sync/atomic"

type Once struct {
	f uint32
}

func New() *Once {
	return &Once{}
}

func (o *Once) Do(f func()) {
	if atomic.LoadUint32(&o.f) == 1 {
		return
	}

	if atomic.CompareAndSwapUint32(&o.f, 0, 1) {
		f()
	}
}
