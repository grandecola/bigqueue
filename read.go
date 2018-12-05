package bigqueue

import (
	"errors"
)

const (
	cInt64Size = 8
)

var (
	// ErrEmptyQueue is returned when peek/dequeue is performed on an empty queue
	ErrEmptyQueue = errors.New("queue is empty")
)

// Peek returns the head of the queue
func (bq *BigQueue) Peek() ([]byte, error) {
	if bq.IsEmpty() {
		return nil, ErrEmptyQueue
	}

	// read index
	aid, offset := bq.index.getHead()

	// read length
	var length int
	aid, offset, length = bq.readLength(aid, offset)

	// read message
	message, err := bq.readBytes(aid, offset, length)
	if err != nil {
		return nil, err
	}

	return message, nil
}

// Dequeue removes an element from the queue
func (bq *BigQueue) Dequeue() error {
	if bq.IsEmpty() {
		return ErrEmptyQueue
	}

	// read index
	aid, offset := bq.index.getHead()

	// read length
	var length int
	aid, offset, length = bq.readLength(aid, offset)

	// calculate the start point for next element
	aid += (offset + length) / bq.conf.arenaSize
	offset = (offset + length) % bq.conf.arenaSize
	bq.index.putHead(aid, offset)

	return nil
}

// readLength reads length of the message
func (bq *BigQueue) readLength(aid, offset int) (int, int, int) {
	// check if length is present in same arena, if not get next arena.
	// If length is stored in next arena, get next aid with 0 offset value
	if offset+cInt64Size > bq.conf.arenaSize {
		aid, offset = aid+1, 0
	}

	// read length
	length := int(bq.am.getArena(aid).ReadUint64(offset))

	// update offset, if offset is equal to arena size,
	// reset arena to next aid and offset to 0
	offset += cInt64Size
	if offset == bq.conf.arenaSize {
		aid, offset = aid+1, 0
	}

	return aid, offset, length
}

// readBytes reads length bytes from arena aid starting at offset
func (bq *BigQueue) readBytes(aid, offset, length int) ([]byte, error) {
	byteSlice := make([]byte, length)

	counter := 0
	for {
		bytesRead, err := bq.am.getArena(aid).Read(byteSlice[counter:], offset)
		if err != nil {
			return nil, err
		}
		counter += bytesRead
		offset += bytesRead

		// if offset is equal to arena size, reset arena to next aid and offset to 0
		if offset == bq.conf.arenaSize {
			aid, offset = aid+1, 0
		}

		// check if all bytes are read
		if counter == length {
			break
		}
	}

	return byteSlice, nil
}
