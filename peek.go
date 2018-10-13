package bigqueue

// Peek returns the head of the queue
func (bq *BigQueue) Peek() ([]byte, error) {
	if bq.IsEmpty() {
		return nil, ErrEmptyQueue
	}

	aid, offset := bq.index.GetHead()

	// read length
	var length int
	aid, offset, length = bq.readLength(aid, offset)

	// read message
	_, _, message, err := bq.readBytes(aid, offset, length)
	if err != nil {
		return message, err
	}

	return message, nil
}

// getArenaAndOffsetForLength returns arena id and offset for length of message.
// If length is stored in aid, it returns aid and offset. If length is stored in
// next arena if return next aid with 0 offset value
func (bq *BigQueue) getArenaAndOffsetForLength(aid, offset int) (int, int) {
	// check if length is present in same arena, if not get next arena
	if cDataFileSize-offset < cInt64Size {
		aid, offset = aid+1, 0
	}

	return aid, offset
}

// getNextAidAndOffsetIfFull checks if arena is full. If yes it returns next aid and 0 offset
// else it returns same aid and offset
func (bq *BigQueue) getNextAidAndOffsetIfFull(aid, offset int) (int, int) {
	if offset == cDataFileSize {
		aid, offset = aid+1, 0
	}
	return aid, offset
}

// readLength reads length of the message
func (bq *BigQueue) readLength(aid, offset int) (int, int, int) {
	aid, offset = bq.getArenaAndOffsetForLength(aid, offset)
	length := int(bq.arenaList[aid].ReadUint64(offset))
	offset += cInt64Size
	aid, offset = bq.getNextAidAndOffsetIfFull(aid, offset)

	return aid, offset, length
}
