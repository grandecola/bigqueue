package bigqueue

import (
	"errors"
)

var (
	// ErrInvalidArenaSize is returned when persisted arena size
	// doesn't match with desired arena size
	ErrInvalidArenaSize = errors.New("mismatch in arena size")
)

// Queue provides an interface to big, fast and persistent queue
type Queue interface {
	IsEmpty() bool
	Dequeue() error
	Close() error

	Peek() ([]byte, error)
	Enqueue([]byte) error
	PeekString() (string, error)
	EnqueueString(string) error
}

// MmapQueue implements Queue interface
type MmapQueue struct {
	conf  *bqConfig
	am    *arenaManager
	index *queueIndex
}

// NewMmapQueue constructs a new persistent queue
func NewMmapQueue(dir string, opts ...Option) (Queue, error) {
	complete := false

	// setup configuration
	conf := newConfig()
	for _, opt := range opts {
		if err := opt(conf); err != nil {
			return nil, err
		}
	}

	// create queue index
	index, err := newQueueIndex(dir)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !complete {
			_ = index.close()
		}
	}()

	// create arena manager
	am, err := newArenaManager(dir, conf, index)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !complete {
			_ = am.close()
		}
	}()

	// ensure that the arena size, if queue had existed,
	// matches with the given arena size
	existingSize := index.getArenaSize()
	if existingSize == 0 {
		index.putArenaSize(conf.arenaSize)
	} else if existingSize != conf.arenaSize {
		return nil, ErrInvalidArenaSize
	}

	complete = true
	return &MmapQueue{
		conf:  conf,
		am:    am,
		index: index,
	}, nil
}

// IsEmpty returns true when queue is empty
func (q *MmapQueue) IsEmpty() bool {
	headAid, headOffset := q.index.getHead()
	tailAid, tailOffset := q.index.getTail()
	return headAid == tailAid && headOffset == tailOffset
}

// Close will close index and arena manager
func (q *MmapQueue) Close() error {
	var retErr error
	if err := q.index.close(); err != nil {
		retErr = err
	}

	if err := q.am.close(); err != nil {
		retErr = err
	}

	return retErr
}
