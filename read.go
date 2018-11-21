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
	return bq.getQueueHead(false)
}

// Dequeue removes an element from the queue
func (bq *BigQueue) Dequeue() ([]byte, error) {
	return bq.getQueueHead(true)
}

// getQueueHead gets the head of the queue and deletes the head if dequeue is true
func (bq *BigQueue) getQueueHead(dequeue bool) ([]byte, error) {
	if bq.IsEmpty() {
		return nil, ErrEmptyQueue
	}

	// read index
	aid, offset := bq.index.getHead()

	// read length
	var length int
	aid, offset, length = bq.readLength(aid, offset)

	// read message
	aid, offset, message, err := bq.readBytes(aid, offset, length)
	if err != nil {
		return nil, err
	}

	// update head
	if dequeue {
		bq.index.putHead(aid, offset)
	}

	return message, nil
}

// readLength reads length of the message
func (bq *BigQueue) readLength(aid, offset int) (int, int, int) {
	// check if length is present in same arena, if not get next arena.
	// If length is stored in next arena, get next aid with 0 offset value
	if offset+cInt64Size > bq.arenaSize {
		aid, offset = aid+1, 0
	}

	// read length
	length := int(bq.arenaList[aid].ReadUint64(offset))

	// update offset, if offset is equal to arena size,
	// reset arena to next aid and offset to 0
	offset += cInt64Size
	if offset == bq.arenaSize {
		aid, offset = aid+1, 0
	}

	return aid, offset, length
}

// readBytes reads length bytes from arena aid starting at offset
func (bq *BigQueue) readBytes(aid, offset, length int) (int, int, []byte, error) {
	byteSlice := make([]byte, length)

	counter := 0
	for {
		bytesRead, err := bq.arenaList[aid].Read(byteSlice[counter:], offset)
		if err != nil {
			return 0, 0, nil, err
		}
		counter += bytesRead
		offset += bytesRead

		// if offset is equal to arena size, reset arena to next aid and offset to 0
		if offset == bq.arenaSize {
			aid, offset = aid+1, 0
		}

		// check if all bytes are read
		if counter == length {
			break
		}
	}

	return aid, offset, byteSlice, nil
}
