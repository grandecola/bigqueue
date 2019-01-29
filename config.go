package bigqueue

import (
	"errors"
	"os"
)

const (
	cDefaultArenaSize = 128 * 1024 * 1024
)

var (
	// ErrTooSmallArenaSize is returned when arena size is smaller than OS page size
	ErrTooSmallArenaSize = errors.New("too small arena size")
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
		maxInMemArenas: 0,
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

// SetMaxInMemArenas returns an Option closure that sets maximum
// number of Arenas that could reside in memory (RAM) at any time.
// By default, all the arenas reside in memory and Operating System
// takes care of memory by paging in and out the pages from disk.
func SetMaxInMemArenas(maxInMemArenas int) Option {
	return func(c *bqConfig) error {
		c.maxInMemArenas = maxInMemArenas
		return nil
	}
}
