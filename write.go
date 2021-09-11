package bigqueue

import "github.com/grandecola/mmap"

// Enqueue adds a new slice of byte element to the tail of the queue.
func (q *MmapQueue) Enqueue(message []byte) error {
	q.lock.Lock()
	defer q.lock.Unlock()

	return q.enqueue(message)
}

func (q *MmapQueue) enqueue(w []byte) error {
	var err error
	aid, offset := q.md.getTail()
	aid, offset, err = q.writeLength(aid, offset, uint64(len(w)))
	if err != nil {
		return err
	}

	aid, offset, err = q.processBytes(writeAt, w, aid, offset, len(w))
	if err != nil {
		return err
	}

	q.md.putTail(aid, offset)
	q.incrMutOps()
	return err
}

// EnqueueString adds a new string element to the tail of the queue.
func (q *MmapQueue) EnqueueString(message string) error {
	return q.Enqueue(s2b(message))
}

// writeLength writes the length into tail arena. Note that length is
// always written in 1 arena, it is never broken across arenas.
func (q *MmapQueue) writeLength(aid, offset int, length uint64) (int, int, error) {
	if offset+cInt64Size > q.conf.arenaSize {
		aid, offset = aid+1, 0
	}

	aa, err := q.am.getArena(aid)
	if err != nil {
		return 0, 0, err
	}
	aa.WriteUint64At(length, int64(offset))

	offset += cInt64Size
	if offset == q.conf.arenaSize {
		aid, offset = aid+1, 0
	}

	return aid, offset, nil
}

// processBytes executes function on byteSlice in arena(s) with aid starting at offset.
func (q *MmapQueue) processBytes(f func(*mmap.File, []byte, int64) (int, error), w []byte, aid, offset, length int) (int, int, error) {
	counter := 0
	for {
		aa, err := q.am.getArena(aid)
		if err != nil {
			return 0, 0, err
		}

		bytesProcessed, _ := f(aa, w[counter:], int64(offset))
		counter += bytesProcessed
		offset += bytesProcessed

		if offset == q.conf.arenaSize {
			aid, offset = aid+1, 0
		}

		// check if all bytes are written
		if counter == length {
			break
		}
	}

	return aid, offset, nil
}

func writeAt(aa *mmap.File, data []byte, offset int64) (int, error) {
	return aa.WriteAt(data, offset)
}

func readAt(aa *mmap.File, data []byte, offset int64) (int, error) {
	return aa.ReadAt(data, offset)
}
