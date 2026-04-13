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

// BacklogBytes returns the number of bytes in the queue that have not yet been
// consumed by the default consumer. The value includes the 8-byte length prefix
// stored before each message payload. Returns ErrInvalidQueueState if the queue
// metadata is inconsistent (tail behind head).
func (q *MmapQueue) BacklogBytes() (int64, error) {
	return q.backlogBytes(q.dc)
}

func (q *MmapQueue) backlogBytes(base int64) (int64, error) {
	q.lock.Lock()
	defer q.lock.Unlock()

	return q.backlogBytesNoLock(base)
}

func (q *MmapQueue) backlogBytesNoLock(base int64) (int64, error) {
	headAid, headOffset := q.md.getConsumerHead(base)
	tailAid, tailOffset := q.md.getTail()

	n := int64(tailAid-headAid)*int64(q.conf.arenaSize) + int64(tailOffset-headOffset)
	if n < 0 {
		return 0, ErrInvalidQueueState
	}
	return n, nil
}

// Dequeue removes an element from the queue and returns it.
// This function uses the default consumer to consume from the queue.
func (q *MmapQueue) Dequeue() ([]byte, error) {
	return q.dequeue(q.dc)
}

func (q *MmapQueue) dequeue(base int64) ([]byte, error) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if err := q.dequeueReader(&q.br, base); err != nil {
		q.br.b = nil
		return nil, err
	}
	r := q.br.b
	q.br.b = nil
	return r, nil
}

// DequeueWithTag removes an element from the queue and returns the message and its
// tag ([]byte). The message was expected to be enqueued via EnqueueWithTag.
// This function uses the default consumer to consume from the queue.
func (q *MmapQueue) DequeueWithTag() ([]byte, []byte, error) {
	return q.dequeueWithTag(q.dc)
}

func (q *MmapQueue) dequeueWithTag(base int64) ([]byte, []byte, error) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if err := q.dequeueReader(&q.br, base); err != nil {
		q.br.b = nil
		return nil, nil, err
	}
	r := q.br.b
	q.br.b = nil

	if len(r) < 1 {
		return nil, nil, ErrInvalidTaggedMessage
	}
	tagLen := int(r[0])
	if len(r) < 1+tagLen {
		return nil, nil, ErrInvalidTaggedMessage
	}
	tag := r[1 : 1+tagLen]
	data := r[1+tagLen:]
	return data, tag, nil
}

// DequeueString removes a string element from the queue and returns it.
// This function uses the default consumer to consume from the queue.
func (q *MmapQueue) DequeueString() (string, error) {
	return q.dequeueString(q.dc)
}

func (q *MmapQueue) dequeueString(base int64) (string, error) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if err := q.dequeueReader(&q.sr, base); err != nil {
		q.sr.sb.Reset()
		return "", err
	}
	r := q.sr.sb.String()
	q.sr.sb.Reset()
	return r, nil
}

// dequeue reads one element of the queue into given reader.
// It takes care of reading the element that is spread across multiple arenas.
func (q *MmapQueue) dequeueReader(r reader, base int64) error {
	if q.isEmptyNoLock(base) {
		return ErrEmptyQueue
	}

	// read head
	aid, offset := q.md.getConsumerHead(base)

	// read length
	newAid, newOffset, length, err := q.readLength(aid, offset)
	if err != nil {
		return err
	}
	aid, offset = newAid, newOffset

	// read message
	r.grow(length)
	aid, offset, err = q.readBytes(r, aid, offset, length)
	if err != nil {
		return err
	}

	// update head
	q.md.putConsumerHead(base, aid, offset)
	q.incrMutOps()

	return nil
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
func (q *MmapQueue) readBytes(r reader, aid, offset, length int) (int, int, error) {
	counter := 0
	for {
		aa, err := q.am.getArena(aid)
		if err != nil {
			return 0, 0, err
		}

		bytesRead := r.readFrom(aa, offset, counter)
		counter += bytesRead
		offset += bytesRead

		// if offset is equal to arena size, reset arena to next aid and offset to 0.
		if offset == q.conf.arenaSize {
			aid, offset = aid+1, 0
		}

		// check if all bytes are read
		if counter >= length {
			break
		}
	}

	return aid, offset, nil
}
