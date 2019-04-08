package bigqueue

// Enqueue adds a new slice of byte element to the tail of the queue
func (q *MmapQueue) Enqueue(message []byte) error {
	return q.enqueue(&bytesWriter{b: message})
}

// EnqueueString adds a new string element to the tail of the queue
func (q *MmapQueue) EnqueueString(message string) error {
	return q.enqueue(&stringWriter{s: message})
}

// writer knows how to copy data of given length to arena
type writer interface {
	// returns the length of the data that writer holds
	len() int

	// writes the data to arena starting at given offset. It is possible that the
	// whole data that writer holds may not fit in the given arena. Hence, an index
	// into the data is provided. The data is copied starting from index until either
	// no more data is left, or no space is left in the given arena to write more data.
	writeTo(aa *arena, offset, index int) int
}

// bytesWriter holds a slice of bytes
type bytesWriter struct {
	b []byte
}

func (bw *bytesWriter) len() int {
	return len(bw.b)
}

func (bw *bytesWriter) writeTo(aa *arena, offset, index int) int {
	n, _ := aa.WriteAt(bw.b[index:], int64(offset))
	return n
}

// stringWriter holds a string that can be written into arenas
type stringWriter struct {
	s string
}

func (sw *stringWriter) len() int {
	return len(sw.s)
}

func (sw *stringWriter) writeTo(aa *arena, offset, index int) int {
	return aa.WriteStringAt(sw.s[index:], int64(offset))
}

// enqueue writes the data hold by the given writer. It first writes the length
// of the data, then the data itself. It is possible that the whole data may not
// fit into one arena. This function takes care of spreading the data across
// multiple arenas when necessary.
func (q *MmapQueue) enqueue(w writer) error {
	complete := true
	q.tLock.Lock()
	defer func() {
		if !complete {
			q.tLock.Unlock()
		}
	}()

	aid, offset := q.index.getTail()
	newAid, newOffset, err := q.writeLength(aid, offset, uint64(w.len()))
	if err != nil {
		return err
	}
	aid, offset = newAid, newOffset

	// write data
	aid, offset, err = q.writeBytes(w, aid, offset)
	if err != nil {
		return err
	}
	// update tail
	q.index.putTail(aid, offset)
	q.mutOps.add(1)

	complete = true
	q.tLock.Unlock()
	return q.flushPeriodic()
}

// writeLength writes the length into tail arena. Note that length is
// always written in 1 arena, it is never broken across arenas.
func (q *MmapQueue) writeLength(aid, offset int, length uint64) (int, int, error) {
	// ensure that new arena is available if needed
	if offset+cInt64Size > q.conf.arenaSize {
		aid, offset = aid+1, 0
	}

	aa, err := q.am.getArena(aid)
	if err != nil {
		return 0, 0, err
	}
	aa.WriteUint64At(length, int64(offset))
	aa.dirty.store(1)

	// update offset now
	offset += cInt64Size
	if offset == q.conf.arenaSize {
		aid, offset = aid+1, 0
	}

	return aid, offset, nil
}

// writeBytes writes byteSlice in arena(s) with aid starting at offset
func (q *MmapQueue) writeBytes(w writer, aid, offset int) (
	int, int, error) {

	length := w.len()
	counter := 0
	for {
		aa, err := q.am.getArena(aid)
		if err != nil {
			return 0, 0, err
		}

		bytesWritten := w.writeTo(aa, offset, counter)
		counter += bytesWritten
		offset += bytesWritten
		aa.dirty.store(1)

		// ensure the next arena is available if needed
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
