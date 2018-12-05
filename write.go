package bigqueue

// Enqueue adds a new element to the tail of the queue
func (bq *BigQueue) Enqueue(message []byte) error {
	aid, offset := bq.index.getTail()

	// write length
	var err error
	aid, offset, err = bq.writeLength(aid, offset, uint64(len(message)))
	if err != nil {
		return err
	}

	// write message
	aid, offset, err = bq.writeBytes(aid, offset, message)
	if err != nil {
		return err
	}

	// update tail
	bq.index.putTail(aid, offset)

	return nil
}

func (bq *BigQueue) writeLength(aid, offset int, length uint64) (int, int, error) {
	// ensure that new arena is available if needed
	if cInt64Size+offset >= bq.conf.arenaSize {
		if err := bq.am.addNewArena(aid + 1); err != nil {
			return 0, 0, err
		}
	}

	// check if length can be fit into same arena, if not, get new arena
	if cInt64Size+offset <= bq.conf.arenaSize {
		bq.am.getArena(aid).WriteUint64(offset, length)
	} else {
		aid, offset = aid+1, 0
		bq.am.getArena(aid).WriteUint64(offset, length)
	}

	// update offset now
	offset += cInt64Size
	if offset == bq.conf.arenaSize {
		aid, offset = aid+1, 0
	}

	return aid, offset, nil
}

// writeBytes writes byteSlice in arena with aid starting at offset
func (bq *BigQueue) writeBytes(aid, offset int, byteSlice []byte) (int, int, error) {
	length := len(byteSlice)

	counter := 0
	for {
		bytesWritten, err := bq.am.getArena(aid).Write(byteSlice[counter:], offset)
		if err != nil {
			return 0, 0, err
		}
		counter += bytesWritten
		offset += bytesWritten

		// ensure the next arena is available if needed
		if offset == bq.conf.arenaSize {
			if err = bq.am.addNewArena(aid + 1); err != nil {
				return 0, 0, err
			}

			aid, offset = aid+1, 0
		}

		// check if all bytes are written
		if counter == length {
			break
		}
	}

	return aid, offset, nil
}
