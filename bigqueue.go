package bigqueue

import (
        "sync"
	"errors"
)

var (
	// ErrInvalidArenaSize is returned when persisted arena size
	// doesn't match with desired arena size
	ErrInvalidArenaSize = errors.New("mismatch in arena size")
)

// IBigQueue provides an interface to big, fast and persistent queue
type IBigQueue interface {
	IsEmpty() bool
	Peek() ([]byte, error)
        PeekAndDequeue() ([]byte, error)
	Enqueue(elem []byte) error
	Dequeue() error
	Close() error
}

// BigQueue implements IBigQueue interface
type BigQueue struct {
	conf  *bqConfig
	am    *arenaManager
	index *queueIndex
        hLock sync.RWMutex
        tLock sync.Mutex
}

// NewBigQueue constructs an instance of *BigQueue
func NewBigQueue(dir string, opts ...Option) (*BigQueue, error) {
	complete := false

	// setup configuration
	conf := newBQConfig()
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
			index.close()
		}
	}()

	// create arena manager
	am, err := newArenaManager(dir, conf, index)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !complete {
			am.close()
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
	return &BigQueue{
		conf:  conf,
		am:    am,
		index: index,
	}, nil
}

// IsEmpty returns true when queue is empty
func (bq *BigQueue) IsEmpty() bool {
	headAid, headOffset := bq.index.getHead()
	tailAid, tailOffset := bq.index.getTail()
	return headAid == tailAid && headOffset == tailOffset
}

// Close will close index and arena manager
func (bq *BigQueue) Close() error {
	var retErr error
	if err := bq.index.close(); err != nil {
		retErr = err
	}

	if err := bq.am.close(); err != nil {
		retErr = err
	}

	return retErr
}
