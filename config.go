package bigqueue

import (
	"errors"
	"os"
	"time"
)

const (
	cDefaultArenaSize = 128 * 1024 * 1024

	// tail, head and a buffer arena, hence 3.
	cMinMaxInMemArenas = 3

	// values chosen arbitrarily
	cDefaultMutOps      = 1000000
	cDefaultflushPeriod = time.Second
)

var (
	// ErrTooSmallArenaSize is returned when arena size is smaller than OS page size.
	ErrTooSmallArenaSize = errors.New("too small arena size")
	// ErrTooFewInMemArenas is returned when number of arenas allowed in memory < 3.
	ErrTooFewInMemArenas = errors.New("too few in memory arenas")
)

// bqConfig stores all the configuration related to bigqueue.
type bqConfig struct {
	arenaSize      int
	maxInMemArenas int
	flushMutOps    int64
	flushPeriod    time.Duration
}

// Option is function type that takes a bqConfig object
// and sets various config parameters in the object.
type Option func(*bqConfig) error

// newConfig creates an object of bqConfig with default parameter values.
func newConfig() *bqConfig {
	return &bqConfig{
		arenaSize:      cDefaultArenaSize,
		maxInMemArenas: cMinMaxInMemArenas,
		flushMutOps:    cDefaultMutOps,
		flushPeriod:    cDefaultflushPeriod,
	}
}

// SetArenaSize returns an Option closure that sets the arena size.
func SetArenaSize(arenaSize int) Option {
	return func(c *bqConfig) error {
		if arenaSize < os.Getpagesize() {
			return ErrTooSmallArenaSize
		}

		c.arenaSize = arenaSize
		return nil
	}
}

// SetMaxInMemArenas returns an Option closure that sets maximum number of
// Arenas that could reside in memory (RAM) at any time.  By default, all the
// arenas reside in memory and Operating System takes care of memory by paging
// in and out the pages from disk. A recommended value of maximum arenas that
// should be in memory should be chosen such that:
//    maxInMemArenas > (# of consumers) * 2 + 1
func SetMaxInMemArenas(maxInMemArenas int) Option {
	return func(c *bqConfig) error {
		if maxInMemArenas != 0 && maxInMemArenas < cMinMaxInMemArenas {
			return ErrTooFewInMemArenas
		}

		c.maxInMemArenas = maxInMemArenas
		return nil
	}
}

// SetPeriodicFlushOps returns an Option that sets the number of
// mutate operations (enqueue/dequeue) after which the queue's in-memory
// changes will be flushed to disk. This is a best effort flush and number
// of mutate operations is checked upon an enqueue/dequeue. If the value
// is set to <= 0, no periodic flush will be performed.
//
// For durability this value should be low.
// For performance this value should be high.
func SetPeriodicFlushOps(flushMutOps int64) Option {
	return func(c *bqConfig) error {
		c.flushMutOps = flushMutOps
		return nil
	}
}

// SetPeriodicFlushDuration returns an Option that sets a periodic
// flush every given duration after which the queue's in-memory
// changes will be flushed to disk. This is a best effort flush and
// elapsed time is checked upon an enqueue/dequeue only. If the value
// is set to <= 0, no periodic flush will be performed.
//
// For durability this value should be low.
// For performance this value should be high.
func SetPeriodicFlushDuration(flushPeriod time.Duration) Option {
	// TODO: in future we should do a timely flush from a background scheduled goroutine.
	return func(c *bqConfig) error {
		c.flushPeriod = flushPeriod
		return nil
	}
}
