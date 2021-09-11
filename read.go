package bigqueue

import (
	"errors"
)

var (
	// ErrEmptyQueue is returned when dequeue is performed on an empty queue.
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
	return q.dequeue(q.dc)
}

// DequeueAppend removes an element from the queue and appends it to data.
// This function uses the default consumer to consume from the queue.
func (q *MmapQueue) DequeueAppend(data []byte) ([]byte, error) {
	return q.dequeueAppend(data, q.dc)
}

func (q *MmapQueue) dequeue(base int64) ([]byte, error) {
	return q.dequeueAppend(nil, base)
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
	newAid, newOffset, length, err := q.readLength(aid, offset)
	if err != nil {
		return nil, err
	}
	aid, offset = newAid, newOffset

	// read message
	if cap(r) < length {
		r = r[:cap(r)]
		r = append(r, make([]byte, length-len(r))...)
	} else {
		r = r[:length]
	}

	aid, offset, err = q.readBytes(r, aid, offset, length)
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
	s, err := q.Dequeue()
	return b2s(s), err
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

// readBytes reads length bytes from arena aid starting at offset.
func (q *MmapQueue) readBytes(r []byte, aid, offset, length int) (int, int, error) {
	counter := 0
	for {
		aa, err := q.am.getArena(aid)
		if err != nil {
			return 0, 0, err
		}

		bytesRead, _ := aa.ReadAt(r[counter:], int64(offset))
		counter += bytesRead
		offset += bytesRead

		// if offset is equal to arena size, reset arena to next aid and offset to 0.
		if offset == q.conf.arenaSize {
			aid, offset = aid+1, 0
		}

		// check if all bytes are read
		if counter == length {
			break
		}
	}

	return aid, offset, nil
}
