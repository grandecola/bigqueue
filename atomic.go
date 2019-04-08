package bigqueue

import (
	"sync/atomic"
)

// atomicInt64 provides atomic access to a int64 value
type atomicInt64 struct {
	val int64
}

func newAtomicInt64(initial int64) *atomicInt64 {
	return &atomicInt64{val: initial}
}

func (i *atomicInt64) load() int64 {
	return atomic.LoadInt64(&i.val)
}

func (i *atomicInt64) store(newval int64) {
	atomic.StoreInt64(&i.val, newval)
}

func (i *atomicInt64) add(incr int64) {
	atomic.AddInt64(&i.val, incr)
}
