package bigqueue

import (
	"fmt"
	"path"
)

const (
	cDataFileFmt  = "data_%d.dat"
	cDataFileSize = 128 * 1024 * 1024
)

// BigQueue implements bigqueue interface using Go slices
type BigQueue struct {
	index     *QueueIndex
	arenaList []*Arena
	dir       string
}

// NewBigQueue constructs an instance of bigqueue
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

// Peek returns the head of the queue
func (bq *BigQueue) Peek() ([]byte, error) {
	aid, offset := bq.index.GetHead()

	// read length
	aid, offset, lenBytes, err := bq.readBytes(aid, offset, 8)
	if err != nil {
		return nil, err
	}
	length := int(BytesToUint64(lenBytes))

	// read message
	aid, offset, message, err := bq.readBytes(aid, offset, length)
	if err != nil {
		return message, err
	}

	return message, err
}

// Enqueue adds a new element to the tail of the queue
func (bq *BigQueue) Enqueue(message []byte) error {
	aid, offset := bq.index.GetTail()

	// write length
	var err error
	length := uint64(len(message))
	aid, offset, err = bq.writeBytes(aid, offset, Uint64ToBytes(length))
	if err != nil {
		return err
	}

	// write message
	aid, offset, err = bq.writeBytes(aid, offset, message)
	if err != nil {
		return err
	}

	// update tail
	bq.index.UpdateTail(aid, offset)

	return nil
}

// Dequeue removes an element from the queue and returns it
func (bq *BigQueue) Dequeue() ([]byte, error) {
	aid, offset := bq.index.GetHead()

	// read length
	aid, offset, lenBytes, err := bq.readBytes(aid, offset, 8)
	if err != nil {
		return nil, err
	}
	length := int(BytesToUint64(lenBytes))

	// read message
	aid, offset, message, err := bq.readBytes(aid, offset, length)
	if err != nil {
		return message, err
	}

	// update head
	bq.index.UpdateHead(aid, offset)

	return message, nil
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

// readBytes reads length bytes from arena aid starting at offset, if length
// is bigger than areana size, it will read remaing bytes from next arena
func (bq *BigQueue) readBytes(aid, offset, length int) (int, int, []byte, error) {
	initialOffset := offset
	byteSlice := make([]byte, length)
	counter := 0

	for {
		bytesRead, err := bq.arenaList[aid].Read(byteSlice[counter:], offset)
		if err != nil {
			return 0, 0, nil, err
		}
		counter += bytesRead

		// check if read all bytes
		if counter < length {
			aid++
			offset = 0
		} else {
			// if read bytes were from initial arena and did not include last offset
			if initialOffset+length < cDataFileSize {
				offset += length

				// if read bytes were from initial arena and included last offset
			} else if initialOffset+length == cDataFileSize {
				aid++
				offset = 0

				// read bytes were from more than one arena
			} else {
				offset = bytesRead
			}
			break
		}
	}

	return aid, offset, byteSlice, nil
}

// writeBytes writes byteSlice in arena with aid starting at offset, if byteSlice size
// is greater than arena size the it creates at new arena to write remaining part
func (bq *BigQueue) writeBytes(aid, offset int, byteSlice []byte) (int, int, error) {
	initialOffset := offset
	length := len(byteSlice)
	counter := 0

	for {
		bytesWritten, err := bq.arenaList[aid].Write(byteSlice[counter:], offset)
		if err != nil {
			return 0, 0, err
		}
		counter += bytesWritten

		// check if all bytes are written
		if counter < length {
			if err = bq.getNewArena(aid + 1); err != nil {
				return 0, 0, err
			}
			aid++
			offset = 0
		} else {
			// if byteSlice got fit in first arena and did not occupy last offset
			if initialOffset+length < cDataFileSize {
				offset += length

				// if byteSlice got fit in first arena and occupied last offset
			} else if initialOffset+length == cDataFileSize {
				if err = bq.getNewArena(aid + 1); err != nil {
					return 0, 0, err
				}
				aid++
				offset = 0

				// if byteSlice took more than one arena
			} else {
				offset = bytesWritten
			}
			break
		}
	}

	return aid, offset, nil
}

// Flush will unmap all arenas
func (bq *BigQueue) Flush() {
	for _, arena := range bq.arenaList {
		arena.Unmap()
	}
}
