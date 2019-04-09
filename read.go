package bigqueue

import (
	"errors"
	"strings"
)

const (
	cInt64Size = 8
)

var (
	// ErrEmptyQueue is returned when peek/dequeue is performed on an empty queue
	ErrEmptyQueue = errors.New("queue is empty")
)

// Peek returns the head (slice of bytes) of the queue
func (q *MmapQueue) Peek() ([]byte, error) {
	br := &bytesReader{}
	if err := q.peek(br); err != nil {
		return nil, err
	}

	return br.b, nil
}

// PeekString returns the head (string) of the queue
func (q *MmapQueue) PeekString() (string, error) {
	sr := &stringReader{sb: &strings.Builder{}}
	if err := q.peek(sr); err != nil {
		return "", err
	}

	return sr.sb.String(), nil
}

// Dequeue removes an element from the queue
func (q *MmapQueue) Dequeue() error {
	q.hLock.Lock()
	defer q.hLock.Unlock()

	q.tLock.RLock()
	emptyQueue := q.isEmpty()
	q.tLock.RUnlock()
	if emptyQueue {
		return ErrEmptyQueue
	}

	// read index
	aid, offset := q.index.getHead()

	// read length
	newAid, newOffset, length, err := q.readLength(aid, offset)
	if err != nil {
		return err
	}
	aid, offset = newAid, newOffset

	// calculate the start point for next element
	aid += (offset + length) / q.conf.arenaSize
	offset = (offset + length) % q.conf.arenaSize
	q.index.putHead(aid, offset)

	// increase number of mutation operations
	q.mutOps.add(1)
	if q.conf.flushMutOps != 0 && q.mutOps.load() >= q.conf.flushMutOps && len(q.flushChan) == 0 {
		q.flushChan <- struct{}{}
	}

	return nil
}

// reader knows how to read data from arena
type reader interface {
	// grow grows reader's capacity, if necessary, to guarantee space for
	// another n bytes. After grow(n), at least n bytes can be written to reader
	// without another allocation. If n is negative, grow panics.
	grow(n int)

	// readFrom copies data from arena starting at given offset. Because the data
	// may be spread over multiple arenas, an index into the data is provided so
	// the data is copied to, starting at given index.
	readFrom(aa *arena, offset, index int) int
}

// bytesReader holds a slice of bytes to hold the data, read from arena
type bytesReader struct {
	b []byte
}

func (br *bytesReader) grow(n int) {
	if n < 0 {
		panic("bigqueue.reader.grow: negative count")
	}

	temp := make([]byte, n)
	if br.b != nil {
		_ = copy(temp, br.b)
	}

	br.b = temp
}

func (br *bytesReader) readFrom(aa *arena, offset, index int) int {
	n, _ := aa.ReadAt(br.b[index:], int64(offset))
	return n
}

// stringReader holds a string builder to hold the data read from arena(s)
type stringReader struct {
	sb *strings.Builder
}

func (sr *stringReader) grow(n int) {
	sr.sb.Grow(n)
}

func (sr *stringReader) readFrom(aa *arena, offset, index int) int {
	if sr.sb.Len() != index {
		panic(errShouldNotReach)
	}

	return aa.ReadStringAt(sr.sb, int64(offset))
}

// peek reads one element of the queue into given reader.
// It takes care of reading the element that is spread acorss multiple arenas.
func (q *MmapQueue) peek(r reader) error {
	q.hLock.Lock()
	defer q.hLock.Unlock()

	q.tLock.RLock()
	emptyQueue := q.isEmpty()
	q.tLock.RUnlock()
	if emptyQueue {
		return ErrEmptyQueue
	}

	// read index
	aid, offset := q.index.getHead()

	// read length
	newAid, newOffset, length, err := q.readLength(aid, offset)
	if err != nil {
		return err
	}
	aid, offset = newAid, newOffset

	// read message
	r.grow(length)
	if err := q.readBytes(r, aid, offset, length); err != nil {
		return err
	}

	return nil
}

// readLength reads length of the message.
// length is always written in 1 arena, it is never broken across arenas.
func (q *MmapQueue) readLength(aid, offset int) (int, int, int, error) {
	// check if length is present in same arena, if not get next arena.
	// If length is stored in next arena, get next aid with 0 offset value
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

// readBytes reads length bytes from arena aid starting at offset
func (q *MmapQueue) readBytes(r reader, aid, offset, length int) error {
	counter := 0
	for {
		aa, err := q.am.getArena(aid)
		if err != nil {
			return err
		}

		bytesRead := r.readFrom(aa, offset, counter)
		counter += bytesRead
		offset += bytesRead

		// if offset is equal to arena size, reset arena to next aid and offset to 0
		if offset == q.conf.arenaSize {
			aid, offset = aid+1, 0
		}

		// check if all bytes are read
		if counter == length {
			break
		}
	}

	return nil
}
