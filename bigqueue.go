package bigqueue

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	cInt64Size       = 8
	cFilePerm        = 0744
	cDefaultConsumer = "__default__"
)

var (
	// ErrInvalidArenaSize is returned when persisted arena size
	// doesn't match with desired arena size.
	ErrInvalidArenaSize = errors.New("mismatch in arena size")
	// ErrDifferentQueues is returned when caller wants to copy
	// offsets from a consumer from a different queue.
	ErrDifferentQueues = errors.New("consumers from different queues")
)

// MmapQueue implements Queue interface.
type MmapQueue struct {
	conf      *bqConfig
	am        *arenaManager
	md        *metadata
	dc        int64 // default consumer
	mutOps    int64
	lastFlush time.Time

	lock  sync.Mutex // protects bigqueue
	drain chan struct{}
	quit  chan struct{}
	wg    sync.WaitGroup
}

// NewMmapQueue constructs a new persistent queue.
func NewMmapQueue(dir string, opts ...Option) (*MmapQueue, error) {
	complete := false

	// setup configuration
	conf := newConfig()
	for _, opt := range opts {
		if err := opt(conf); err != nil {
			return nil, err
		}
	}

	// create queue metadata
	md, err := newMetadata(dir, conf.arenaSize)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !complete {
			_ = md.close()
		}
	}()

	// create arena manager
	am, err := newArenaManager(dir, conf, md)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !complete {
			_ = am.close()
		}
	}()

	// ensure that the arena size, if queue had existed,
	// matches with the given arena size.
	existingSize := md.getArenaSize()
	if existingSize == 0 {
		md.putArenaSize(conf.arenaSize)
	} else if existingSize != conf.arenaSize {
		return nil, ErrInvalidArenaSize
	}

	dc, err := md.getConsumer(cDefaultConsumer)
	if err != nil {
		return nil, fmt.Errorf("error in adding default consumer :: %w", err)
	}

	bq := &MmapQueue{
		conf:  conf,
		am:    am,
		md:    md,
		dc:    dc,
		drain: make(chan struct{}, 1),
		quit:  make(chan struct{}),
	}
	go bq.periodicFlush()

	complete = true
	return bq, nil
}

// NewConsumer creates a new consumer or finds an existing one with same name.
func (q *MmapQueue) NewConsumer(name string) (*Consumer, error) {
	q.lock.Lock()
	defer q.lock.Unlock()

	base, err := q.md.getConsumer(name)
	if err != nil {
		return nil, err
	}

	return &Consumer{mq: q, base: base}, nil
}

// FromConsumer creates a new consumer or finds an existing one with same name.
// It also copies the offsets from the given consumer to this consumer.
func (q *MmapQueue) FromConsumer(name string, from *Consumer) (*Consumer, error) {
	if q != from.mq {
		return nil, ErrDifferentQueues
	}

	q.lock.Lock()
	defer q.lock.Unlock()

	base, err := q.md.getConsumer(name)
	if err != nil {
		return nil, err
	}

	// update offsets to given consumer
	aid, pos := q.md.getConsumerHead(from.base)
	q.md.putConsumerHead(base, aid, pos)

	return &Consumer{mq: q, base: base}, nil
}

// Close will close metadata and arena manager.
func (q *MmapQueue) Close() error {
	q.lock.Lock()
	defer q.lock.Unlock()

	// wait for background go routines to finish
	close(q.quit)
	q.wg.Wait()

	var retErr error
	if err := q.md.close(); err != nil {
		retErr = err
	}

	if err := q.am.close(); err != nil {
		retErr = err
	}

	return retErr
}

// Flush syncs the in memory content of bigqueue to disk.
func (q *MmapQueue) Flush() error {
	q.lock.Lock()
	defer q.lock.Unlock()

	if err := q.am.flush(); err != nil {
		return err
	}

	if err := q.md.flush(); err != nil {
		return err
	}

	q.mutOps = 0
	q.lastFlush = time.Now()
	return nil
}

func (q *MmapQueue) incrMutOps() {
	if q.conf.flushMutOps <= 0 {
		return
	}

	q.mutOps++
	if q.mutOps >= q.conf.flushMutOps {
		select {
		case q.drain <- struct{}{}:
		default:
		}
	}
}

// setupFlush sets up background go routine to periodically flush data.
func (q *MmapQueue) periodicFlush() {
	timer := &time.Timer{C: make(chan time.Time)}
	if q.conf.flushPeriod != 0 {
		timer = time.NewTimer(time.Duration(q.conf.flushPeriod))
	}

	var drainFlag bool
	for {
		if q.conf.flushPeriod != 0 {
			if !drainFlag && !timer.Stop() {
				<-timer.C
			}

			timer.Reset(time.Duration(q.conf.flushPeriod))
			drainFlag = false
		}

		select {
		case <-q.quit:
			return
		case <-q.drain:
			_ = q.Flush()
		case <-timer.C:
			drainFlag = true
			_ = q.Flush()
		}
	}
}
