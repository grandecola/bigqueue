package bigqueue

const (
	cInt64Size = 8
)

// Dequeue removes an element from the queue and returns it
func (bq *BigQueue) Dequeue() ([]byte, error) {
	if bq.IsEmpty() {
		return nil, ErrEmptyQueue
	}

	aid, offset := bq.index.GetHead()

	// read length
	aid, offset = bq.getArenaAndOffsetForLength(aid, offset)
	length := int(bq.arenaList[aid].ReadUint64(offset))
	offset += cInt64Size
	aid, offset = bq.getNextAidAndOffsetIfFull(aid, offset)

	// read message
	aid, offset, message, err := bq.readBytes(aid, offset, length)
	if err != nil {
		return message, err
	}

	// update head
	bq.index.UpdateHead(aid, offset)

	return message, nil
}

// readBytes reads length bytes from arena aid starting at offset, if length
// is bigger than arena size, it calls readBytesFromMultipleArenas
func (bq *BigQueue) readBytes(aid, offset, length int) (int, int, []byte, error) {
	byteSlice := make([]byte, length)

	// check if length can be read from same arena
	if cDataFileSize-offset >= length {
		if _, err := bq.arenaList[aid].Read(byteSlice, offset); err != nil {
			return 0, 0, nil, err
		}
		offset += length
	} else {
		var err error
		if aid, offset, byteSlice, err = bq.readBytesFromMultipleArenas(aid, offset, length); err != nil {
			return 0, 0, nil, err
		}
	}

	if offset == cDataFileSize {
		aid, offset = aid+1, 0
	}

	return aid, offset, byteSlice, nil
}

// readBytesFromMultipleArenas is called when length to be read is greater than Arena size
func (bq *BigQueue) readBytesFromMultipleArenas(aid, offset, length int) (int, int, []byte, error) {
	byteSlice := make([]byte, length)

	counter := 0
	for {
		bytesRead, err := bq.arenaList[aid].Read(byteSlice[counter:], offset)
		if err != nil {
			return 0, 0, nil, err
		}
		counter += bytesRead

		if counter < length {
			aid, offset = aid+1, 0
		} else {
			offset = bytesRead
			break
		}
	}

	return aid, offset, byteSlice, nil
}
