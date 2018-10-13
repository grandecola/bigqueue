package bigqueue

import (
	"errors"
	"fmt"
	"path"
)

const (
	cDataFileFmt  = "data_%d.dat"
	cDataFileSize = 128 * 1024 * 1024
)

var (
	// ErrEmptyQueue is returned when peek/dequeue is performed on an empty queue
	ErrEmptyQueue = errors.New("queue is empty")
)

// BigQueue implements IBigQueue interface
type BigQueue struct {
	index     *QueueIndex
	arenaList []*Arena
	dir       string
}

// NewBigQueue constructs an instance of *BigQueue
func NewBigQueue(dir string) (*BigQueue, error) {
	index, err := NewQueueIndex(dir)
	if err != nil {
		return nil, err
	}

	headAid, _ := index.GetHead()
	tailAid, _ := index.GetTail()
	arenaList := make([]*Arena, 0)

	bq := &BigQueue{
		index:     index,
		arenaList: arenaList,
		dir:       dir,
	}

	for i := headAid; i <= tailAid; i++ {
		if err := bq.getNewArena(i); err != nil {
			return nil, err
		}
	}

	return bq, nil
}

// IsEmpty returns true when queue is empty
func (bq *BigQueue) IsEmpty() bool {
	headAid, headOffset := bq.index.GetHead()
	tailAid, tailOffset := bq.index.GetTail()

	return headAid == tailAid && headOffset == tailOffset
}

// getNewArena creates arena with aid and adds it to big queue arenaList
func (bq *BigQueue) getNewArena(aid int) error {
	file := path.Join(bq.dir, fmt.Sprintf(cDataFileFmt, aid))
	arena, err := NewArena(file, cDataFileSize)
	if err != nil {
		return err
	}
	bq.arenaList = append(bq.arenaList, arena)
	return nil
}

// Close will unmap all arenas
func (bq *BigQueue) Close() {
	for _, arena := range bq.arenaList {
		arena.Unmap()
	}
}
