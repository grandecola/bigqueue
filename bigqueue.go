package bigqueue

import (
	"errors"
	"fmt"
	"os"
	"path"
)

const (
	cArenaFileFmt     = "arena_%d.dat"
	cDefaultArenaSize = 128 * 1024 * 1024
)

var (
	// ErrTooSmallArenaSize is returned when arena size is smaller than OS page size
	ErrTooSmallArenaSize = errors.New("too small arena size")
	// ErrInvalidArenaSize is returned when persisted arena size
	// doesn't match with desired arena size
	ErrInvalidArenaSize = errors.New("mismatch in arena size")
)

// Option is function type that takes a BigQueue object
// and sets various config properties of the object
type Option func(*BigQueue) error

// BigQueue implements IBigQueue interface
type BigQueue struct {
	dir       string
	index     *queueIndex
	arenaList []*arena
	arenaSize int
}

// SetArenaSize returns an Option clojure that sets the arena size
func SetArenaSize(arenaSize int) Option {
	return func(bq *BigQueue) error {
		if arenaSize < os.Getpagesize() {
			return ErrTooSmallArenaSize
		}

		bq.arenaSize = arenaSize
		return nil
	}
}

// NewBigQueue constructs an instance of *BigQueue
func NewBigQueue(dir string, opts ...Option) (IBigQueue, error) {
	complete := false

	// create queue index
	index, err := newQueueIndex(dir)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !complete {
			index.close()
		}
	}()

	// create BigQueue
	bq := &BigQueue{
		index:     index,
		arenaList: make([]*arena, 0),
		dir:       dir,
		arenaSize: cDefaultArenaSize,
	}
	defer func() {
		if !complete {
			bq.Close()
		}
	}()

	// set configuration
	for _, opt := range opts {
		if err := opt(bq); err != nil {
			return nil, err
		}
	}

	// ensure that the arena size, if queue had existed, matches with the given arena size
	existingSize := index.getArenaSize()
	if existingSize == 0 {
		index.putArenaSize(bq.arenaSize)
	} else if existingSize != bq.arenaSize {
		return nil, ErrInvalidArenaSize
	}

	// initialize all the Arenas
	headAid, _ := index.getHead()
	tailAid, _ := index.getTail()
	for i := headAid; i <= tailAid; i++ {
		if err := bq.addNewArena(i); err != nil {
			return nil, err
		}
	}

	complete = true
	return bq, nil
}

// IsEmpty returns true when queue is empty
func (bq *BigQueue) IsEmpty() bool {
	headAid, headOffset := bq.index.getHead()
	tailAid, tailOffset := bq.index.getTail()
	return headAid == tailAid && headOffset == tailOffset
}

// Close will unmap all arenas
func (bq *BigQueue) Close() {
	bq.index.close()
	for _, arena := range bq.arenaList {
		arena.Unmap()
	}
}

// addNewArena creates arena with given arena id and adds it to BigQueue arenaList
func (bq *BigQueue) addNewArena(aid int) error {
	file := path.Join(bq.dir, fmt.Sprintf(cArenaFileFmt, aid))
	arena, err := newArena(file, bq.arenaSize)
	if err != nil {
		return err
	}

	bq.arenaList = append(bq.arenaList, arena)
	return nil
}
