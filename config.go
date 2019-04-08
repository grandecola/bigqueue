package bigqueue

import (
	"errors"
	"os"
	"time"

	"github.com/jonboulle/clockwork"
)

const (
	cDefaultArenaSize = 128 * 1024 * 1024

	// tail, head and a buffer arena, hence 3
	cMinMaxInMemArenas = 3

	// values chosen arbitrarily
	cDefaultMutOps      = 1000
	cDefaultflushPeriod = time.Minute
)

var (
	// ErrTooSmallArenaSize is returned when arena size is smaller than OS page size
	ErrTooSmallArenaSize = errors.New("too small arena size")
	// ErrTooFewInMemArenas is returned when number of arenas allowed in memory < 3
	ErrTooFewInMemArenas = errors.New("too few in memory arenas")
	// ErrMustBeGreaterThanZero is returned when a config value has non-positive value
	ErrMustBeGreaterThanZero = errors.New("must be greater than zero")
	// singleton instance of real clock.
	// TODO: not needed once https://github.com/jonboulle/clockwork/pull/14 is merged
	realClock = clockwork.NewRealClock()
)

// bqConfig stores all the configuration related to bigqueue
type bqConfig struct {
	arenaSize      int
	maxInMemArenas int
	flushMutOps    int64
	flushPeriod    int64
	clock          clockwork.Clock
}

// Option is function type that takes a bqConfig object
// and sets various config parameters in the object
type Option func(*bqConfig) error

// newConfig creates an object of bqConfig with default parameter values
func newConfig() *bqConfig {
	return &bqConfig{
		arenaSize:      cDefaultArenaSize,
		maxInMemArenas: cMinMaxInMemArenas,
		flushMutOps:    cDefaultMutOps,
		flushPeriod:    cDefaultflushPeriod.Nanoseconds(),
		clock:          realClock,
	}
}

// SetArenaSize returns an Option closure that sets the arena size
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
// in and out the pages from disk.
// A recommended value of maximum arenas that should be in memory
// is chosen such that -
//  maxInMemArenas > 2 + (maximum message size / arena size)
//  maxInMemArenas < (total available system memory - 1GB) / arena size
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
// of mutate operations is checked upon an enqueue/dequeue.
//
// For durability this value should be low.
// For performance this value should be high.
func SetPeriodicFlushOps(flushMutOps int64) Option {
	return func(c *bqConfig) error {
		if flushMutOps < 1 {
			return ErrMustBeGreaterThanZero
		}

		c.flushMutOps = flushMutOps
		return nil
	}
}

// SetPeriodicFlushDuration returns an Option that sets a periodic
// flush every given duration after which the queue's in-memory changes
// will be flushed to disk. This is a best effort flush and elapsed time is
// checked upon an enqueue/dequeue only.
//
// For durability this value should be low.
// For performance this value should be high.
func SetPeriodicFlushDuration(flushPeriod time.Duration) Option {
	// TODO: in future we should do a timely flush from a background scheduled goroutine
	return func(c *bqConfig) error {
		if flushPeriod < 1 {
			return ErrMustBeGreaterThanZero
		}

		c.flushPeriod = flushPeriod.Nanoseconds()
		return nil
	}
}

// setClock returns an Option closure to set a custom clock.
// Currently, this function is not exported and only used for unit testing.
func setClock(clock clockwork.Clock) Option {
	return func(c *bqConfig) error {
		c.clock = clock
		return nil
	}
}
