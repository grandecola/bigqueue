package bigqueue

import (
	"errors"
	"fmt"
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
)

// MmapQueue implements Queue interface.
type MmapQueue struct {
	conf      *bqConfig
	am        *arenaManager
	md        *metadata
	dc        int64 // default consumer
	mutOps    int64
	lastFlush time.Time
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

	complete = true
	return &MmapQueue{
		conf: conf,
		am:   am,
		md:   md,
		dc:   dc,
	}, nil
}

// NewConsumer creates a new consumer or finds an existing one with same name.
func (q *MmapQueue) NewConsumer(name string) (*Consumer, error) {
	base, err := q.md.getConsumer(name)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		mq:   q,
		name: name,
		base: base,
	}, nil
}

// Close will close metadata and arena manager.
func (q *MmapQueue) Close() error {
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

func (q *MmapQueue) flushPeriodic() error {
	enoughOps := false
	if q.conf.flushMutOps != 0 {
		enoughOps = q.mutOps >= q.conf.flushMutOps
	}

	enoughTime := false
	if q.conf.flushPeriod != 0 {
		enoughTime = time.Since(q.lastFlush) >= q.conf.flushPeriod
	}

	if enoughOps || enoughTime {
		return q.Flush()
	}

	return nil
}
