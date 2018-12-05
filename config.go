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
	arenaSize int
}

// Option is function type that takes a bqConfig object
// and sets various config parameters in the object
type Option func(*bqConfig) error

// newBQConfig creates an object of bqConfig with default paramtere values
func newBQConfig() *bqConfig {
	return &bqConfig{
		arenaSize: cDefaultArenaSize,
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
