package bigqueue

import (
	"errors"
	"sync"
	"time"
)

var (
	// ErrInvalidArenaSize is returned when persisted arena size
	// doesn't match with desired arena size
	ErrInvalidArenaSize = errors.New("mismatch in arena size")
)

// Queue provides an interface to big, fast and persistent queue
type Queue interface {
	IsEmpty() bool
	Flush() error
	Close() error

	Enqueue([]byte) error
	EnqueueString(string) error
	Dequeue() error
	Peek() ([]byte, error)
	PeekString() (string, error)
}

// MmapQueue implements Queue interface
type MmapQueue struct {
	conf  *bqConfig
	index *queueIndex
	am    *arenaManager

	// using atomic to update these below
	mutOps    *atomicInt64
	flushChan chan struct{}
	done      chan struct{}
	quit      chan struct{}

	// The order of locks: hLock > tLock > am.Lock
	// protects head
	hLock sync.RWMutex
	// protects tail
	tLock sync.RWMutex
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

	bq := &MmapQueue{
		conf:      conf,
		am:        am,
		index:     index,
		mutOps:    newAtomicInt64(0),
		flushChan: make(chan struct{}, 100),
		done:      make(chan struct{}),
		quit:      make(chan struct{}),
	}

	// setup background thread to flush arenas periodically
	if err := bq.setupBgFlush(); err != nil {
		return nil, err
	}

	complete = true
	return bq, nil
}

// IsEmpty returns true when queue is empty
func (q *MmapQueue) IsEmpty() bool {
	q.hLock.RLock()
	defer q.hLock.RUnlock()

	q.tLock.RLock()
	defer q.tLock.RUnlock()

	return q.isEmpty()
}

// Flush syncs the in memory content of bigqueue to disk
// A read lock ensures that there is no writer which is what we want
func (q *MmapQueue) Flush() error {
	// we are locking arena manager first here which is fine because we
	// unlock arena manager before proceeding to acquiring more locks
	if err := q.am.flush(); err != nil {
		return err
	}

	q.hLock.RLock()
	defer q.hLock.RUnlock()

	q.tLock.RLock()
	defer q.tLock.RUnlock()

	if err := q.index.flush(); err != nil {
		return err
	}

	q.mutOps.store(0)
	return nil
}

// Close will close index and arena manager
func (q *MmapQueue) Close() error {
	q.hLock.Lock()
	defer q.hLock.Unlock()

	q.tLock.Lock()
	defer q.tLock.Unlock()

	// wait for quitting the background routine
	q.quit <- struct{}{}
	<-q.done

	var retErr error
	if err := q.am.close(); err != nil {
		retErr = err
	}

	if err := q.index.close(); err != nil {
		retErr = err
	}

	return retErr
}

// isEmpty is not thread safe and should be called only after acquiring necessary locks
func (q *MmapQueue) isEmpty() bool {
	headAid, headOffset := q.index.getHead()
	tailAid, tailOffset := q.index.getTail()
	return headAid == tailAid && headOffset == tailOffset
}

// setupBgFlush sets up background go routine to periodically flush arenas
func (q *MmapQueue) setupBgFlush() error {
	t := &time.Timer{
		C: make(chan time.Time),
	}
	if q.conf.flushPeriod != 0 {
		t = time.NewTimer(time.Duration(q.conf.flushPeriod))
	}

	go func() {
		var drainFlag bool

		for {
			if q.conf.flushPeriod != 0 {
				if !drainFlag && !t.Stop() {
					<-t.C
				}

				t.Reset(time.Duration(q.conf.flushPeriod))
				drainFlag = false
			}

			select {
			case <-q.quit:
				defer func() { q.done <- struct{}{} }()
				return
			case <-q.flushChan:
				if q.mutOps.load() >= q.conf.flushMutOps {
					_ = q.Flush()
				}
			case <-t.C:
				drainFlag = true
				_ = q.Flush()
			}
		}
	}()

	return nil
}
