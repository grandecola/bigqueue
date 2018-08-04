package bigqueue

import (
	"path"
	"syscall"
)

const (
	cIndexFileName = "index.dat"
	cIndexFileSize = 4 * 8
	cFlushFlags    = syscall.SYS_SYNC
)

// QueueIndex stores head and tail for a BigQueue in an arena
type QueueIndex struct {
	indexFile  string
	indexArena *Arena
}

// NewQueueIndex creates/reads index for a BigQueue
func NewQueueIndex(dataDir string) (*QueueIndex, error) {
	indexFile := path.Join(dataDir, cIndexFileName)
	indexArena, err := NewArena(indexFile, cIndexFileSize)
	if err != nil {
		return nil, err
	}

	return &QueueIndex{
		indexFile:  indexFile,
		indexArena: indexArena,
	}, nil
}

// GetHead reads the value of head of the queue from the index arena.
// Head of a BigQueue can be identified using -
//   1. Arena ID
//   2. Position (offset) in the arena
//
//   <------- head aid ------> <------- head pos ------>
//  +------------+------------+------------+------------+
//  | byte 01-03 | byte 04-07 | byte 08-11 | byte 12-15 |
//  +------------+------------+------------+------------+
//
func (i *QueueIndex) GetHead() (int, int) {
	aid := i.indexArena.ReadUint64(0)
	pos := i.indexArena.ReadUint64(8)
	return int(aid), int(pos)
}

// UpdateHead writes the value of head in the index arena
func (i *QueueIndex) UpdateHead(aid, pos int) {
	i.indexArena.WriteUint64(0, uint64(aid))
	i.indexArena.WriteUint64(8, uint64(pos))
}

// GetTail reads the values of tail of the queue from the index arena.
// Tail of a BigQueue, similar to head, can be identified using -
//   1. Arena ID
//   2. Position (offset) in the arena
//
//   <------- tail aid ------> <------- tail pos ------>
//  +------------+------------+------------+------------+
//  | byte 16-19 | byte 20-23 | byte 24-27 | byte 28-31 |
//  +------------+------------+------------+------------+
//
func (i *QueueIndex) GetTail() (int, int) {
	aid := i.indexArena.ReadUint64(16)
	pos := i.indexArena.ReadUint64(24)
	return int(aid), int(pos)
}

// UpdateTail writes the value of tail in the index arena
func (i *QueueIndex) UpdateTail(aid, pos int) {
	i.indexArena.WriteUint64(16, uint64(aid))
	i.indexArena.WriteUint64(24, uint64(pos))
}

// Flush writes the memory state of the index arena on to disk
func (i *QueueIndex) Flush() {
	i.indexArena.Flush(cFlushFlags)
}
