package bigqueue

import (
	"errors"
	"os"
)

const (
	cDefaultArenaSize = 128 * 1024 * 1024

	// tail, head and a buffer arena, hence 3
	cMinMaxInMemArenas = 3
)

var (
	// ErrTooSmallArenaSize is returned when arena size is smaller than OS page size
	ErrTooSmallArenaSize = errors.New("too small arena size")
	// ErrTooFewInMemArenas is returned when number of arenas allowed in memory < 3
	ErrTooFewInMemArenas = errors.New("too few in memory arenas")
)

// bqConfig stores all the configuration related to bigqueue
type bqConfig struct {
	arenaSize      int
	maxInMemArenas int
}

// Option is function type that takes a bqConfig object
// and sets various config parameters in the object
type Option func(*bqConfig) error

// newBQConfig creates an object of bqConfig with default parameter values
func newBQConfig() *bqConfig {
	return &bqConfig{
		arenaSize:      cDefaultArenaSize,
		maxInMemArenas: cMinMaxInMemArenas,
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
