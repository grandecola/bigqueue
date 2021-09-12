package bigqueue

import (
	"errors"
	"strings"
)

var (
	// ErrEmptyQueue is returned when dequeueAppend is performed on an empty queue.
	ErrEmptyQueue = errors.New("queue is empty")
)

// IsEmpty returns true when queue is empty for the default consumer.
func (q *MmapQueue) IsEmpty() bool {
	return q.isEmpty(q.dc)
}

func (q *MmapQueue) isEmpty(base int64) bool {
	q.lock.Lock()
	defer q.lock.Unlock()

	return q.isEmptyNoLock(base)
}

func (q *MmapQueue) isEmptyNoLock(base int64) bool {
	headAid, headOffset := q.md.getConsumerHead(base)
	tailAid, tailOffset := q.md.getTail()
	return headAid == tailAid && headOffset == tailOffset
}

// Dequeue removes an element from the queue and returns it.
// This function uses the default consumer to consume from the queue.
func (q *MmapQueue) Dequeue() ([]byte, error) {
	return q.dequeueAppend(nil, q.dc)
}

// DequeueAppend removes an element from the queue and appends it to data.
// This function uses the default consumer to consume from the queue.
func (q *MmapQueue) DequeueAppend(data []byte) ([]byte, error) {
	return q.dequeueAppend(data, q.dc)
}

func (q *MmapQueue) dequeueAppend(r []byte, base int64) ([]byte, error) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.isEmptyNoLock(base) {
		return nil, ErrEmptyQueue
	}

	// read head
	aid, offset := q.md.getConsumerHead(base)

	// read length
	aid, offset, length, err := q.readLength(aid, offset)
	if err != nil {
		return nil, err
	}

	if cap(r) < length {
		r = r[:cap(r)]
		r = append(r, make([]byte, length-len(r))...)
	} else {
		r = r[:length]
	}

	// read message
	aid, offset, err = q.processBytes(readAt, r, aid, offset, length)
	if err != nil {
		return nil, err
	}

	// update head
	q.md.putConsumerHead(base, aid, offset)
	q.incrMutOps()

	return r, nil
}

// DequeueString removes a string element from the queue and returns it.
// This function uses the default consumer to consume from the queue.
func (q *MmapQueue) DequeueString() (string, error) {
	return q.dequeueString(q.dc)
}

func (q *MmapQueue) dequeueString(base int64) (string, error) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.isEmptyNoLock(base) {
		return "", ErrEmptyQueue
	}

	// read head
	aid, offset := q.md.getConsumerHead(base)

	// read length
	aid, offset, length, err := q.readLength(aid, offset)
	if err != nil {
		return "", err
	}

	// read message
	var r strings.Builder
	r.Grow(length)
	aid, offset, err = q.processString(readStringAt, &r, "", aid, offset, length)
	if err != nil {
		return "", err
	}

	// update head
	q.md.putConsumerHead(base, aid, offset)
	q.incrMutOps()

	return r.String(), nil
}

// readLength reads length of the message.
// length is always written in 1 arena, it is never broken across arenas.
func (q *MmapQueue) readLength(aid, offset int) (int, int, int, error) {
	// check if length is present in same arena, if not get next arena.
	// If length is stored in next arena, get next aid with 0 offset value.
	if offset+cInt64Size > q.conf.arenaSize {
		aid, offset = aid+1, 0
	}

	// read length
	aa, err := q.am.getArena(aid)
	if err != nil {
		return 0, 0, 0, err
	}
	length := int(aa.ReadUint64At(int64(offset)))

	// update offset, if offset is equal to arena size,
	// reset arena to next aid and offset to 0
	offset += cInt64Size
	if offset == q.conf.arenaSize {
		aid, offset = aid+1, 0
	}

	return aid, offset, length, nil
}

