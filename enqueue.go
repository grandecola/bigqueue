package bigqueue

// Enqueue adds a new element to the tail of the queue
func (bq *BigQueue) Enqueue(message []byte) error {
	aid, offset := bq.index.GetTail()

	// write length
	var err error
	length := uint64(len(message))
	// check if length can be fit into same arena, if not get new arena
	if cDataFileSize-offset >= cInt64Size {
		bq.arenaList[aid].WriteUint64(offset, length)
	} else {
		aid++
		offset = 0
		if err = bq.getNewArena(aid); err != nil {
			return err
		}
		bq.arenaList[aid].WriteUint64(offset, length)
	}
	offset += cInt64Size

	// check if this arena is full, yes get new arena
	if offset >= cDataFileSize {
		aid++
		offset = 0
		if err = bq.getNewArena(aid); err != nil {
			return err
		}
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

// writeBytes writes byteSlice in arena with aid starting at offset, if byteSlice size
// is greater than arena size then it calls writeBytesToMultipleArenas
func (bq *BigQueue) writeBytes(aid, offset int, byteSlice []byte) (int, int, error) {
	length := len(byteSlice)

	// check if slice can be fit into same aid
	if cDataFileSize-offset >= length {
		if _, err := bq.arenaList[aid].Write(byteSlice, offset); err != nil {
			return 0, 0, err
		}
		offset += len(byteSlice)
	} else {
		var err error
		if aid, offset, err = bq.writeBytesToMultipleArenas(aid, offset, byteSlice); err != nil {
			return 0, 0, err
		}
	}

	// check if arena is full, if yes get new
	if offset == cDataFileSize {
		aid++
		offset = 0
		if err := bq.getNewArena(aid); err != nil {
			return 0, 0, err
		}
	}

	return aid, offset, nil
}

// writeBytesToMultipleArenas is called if byteSlice cannot be fit into single Arena
func (bq *BigQueue) writeBytesToMultipleArenas(aid, offset int, byteSlice []byte) (int, int, error) {
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
			offset = bytesWritten
			break
		}
	}

	return aid, offset, nil
}
