package bigqueue

// Enqueue adds a new element to the tail of the queue
func (bq *BigQueue) Enqueue(message []byte) error {
        bq.tLock.Lock()
	aid, offset := bq.index.getTail()

	newAid, newOffset, err := bq.writeLength(aid, offset, uint64(len(message)))
	if err != nil {
                bq.tLock.Unlock()
		return err
	}
	aid, offset = newAid, newOffset

	// write message
	aid, offset, err = bq.writeBytes(aid, offset, message)
	if err != nil {
                bq.tLock.Unlock()
		return err
	}

	// update tail
	bq.index.putTail(aid, offset)

        bq.tLock.Unlock()
	return nil
}

// writeLength writes the length into tail arena. Note that length is
// always written in 1 arena, it is never broken across arenas.
func (bq *BigQueue) writeLength(aid, offset int, length uint64) (int, int, error) {
	// ensure that new arena is available if needed
	if offset+cInt64Size > bq.conf.arenaSize {
		aid, offset = aid+1, 0
	}

	aa, err := bq.am.getArena(aid)
	if err != nil {
		return 0, 0, err
	}
	aa.WriteUint64At(length, int64(offset))

	// update offset now
	offset += cInt64Size
	if offset == bq.conf.arenaSize {
		aid, offset = aid+1, 0
	}

	return aid, offset, nil
}

// writeBytes writes byteSlice in arena with aid starting at offset
func (bq *BigQueue) writeBytes(aid, offset int, byteSlice []byte) (
	int, int, error) {

	length := len(byteSlice)
	counter := 0
	for {
		aa, err := bq.am.getArena(aid)
		if err != nil {
			return 0, 0, err
		}

		bytesWritten, err := aa.WriteAt(byteSlice[counter:], int64(offset))
		if err != nil {
			return 0, 0, err
		}
		counter += bytesWritten
		offset += bytesWritten

		// ensure the next arena is available if needed
		if offset == bq.conf.arenaSize {
			aid, offset = aid+1, 0
		}

		// check if all bytes are written
		if counter == length {
			break
		}
	}

	return aid, offset, nil
}
