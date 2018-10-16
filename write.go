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
	// check if length can be fit into same arena, if not, get new arena
	if cInt64Size+offset <= bq.arenaSize {
		bq.arenaList[aid].WriteUint64(offset, length)
	} else {
		aid, offset = aid+1, 0
		if err := bq.addNewArena(aid); err != nil {
			return 0, 0, err
		}

		bq.arenaList[aid].WriteUint64(offset, length)
	}

	// update offset now
	offset += cInt64Size

	return aid, offset, nil
}

// writeBytes writes byteSlice in arena with aid starting at offset, if byteSlice size
// is greater than arena size then it calls writeBytesToMultipleArenas
func (bq *BigQueue) writeBytes(aid, offset int, byteSlice []byte) (int, int, error) {
	length := len(byteSlice)

	// check if slice can be fit into same aid
	if bq.arenaSize-offset >= length {
		if _, err := bq.arenaList[aid].Write(byteSlice, offset); err != nil {
			return 0, 0, err
		}
		offset += len(byteSlice)
	} else {
		var err error
		aid, offset, err = bq.writeBytesToMultipleArenas(aid, offset, byteSlice)
		if err != nil {
			return 0, 0, err
		}
	}

	return aid, offset, nil
}

// writeBytesToMultipleArenas is called if byteSlice cannot be fit into single arena
func (bq *BigQueue) writeBytesToMultipleArenas(aid, offset int, byteSlice []byte) (
	int, int, error) {

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
			if err = bq.addNewArena(aid + 1); err != nil {
				return 0, 0, err
			}
			aid, offset = aid+1, 0
		} else {
			offset = bytesWritten
			break
		}
	}

	return aid, offset, nil
}
