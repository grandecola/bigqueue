package bigqueue

import (
	"path"
	"syscall"
)

const (
	cIndexFileName = "index.dat"
	cIndexFileSize = 5 * 8
)

// queueIndex stores head, tail and config parameters for a BigQueue in an arena
type queueIndex struct {
	indexFile  string
	indexArena *arena
}

// newQueueIndex creates/reads index for a BigQueue
func newQueueIndex(dataDir string) (*queueIndex, error) {
	indexFile := path.Join(dataDir, cIndexFileName)
	indexArena, err := newArena(indexFile, cIndexFileSize)
	if err != nil {
		return nil, err
	}

	return &queueIndex{
		indexFile:  indexFile,
		indexArena: indexArena,
	}, nil
}

// getHead reads the value of head of the queue from the index arena.
// Head of a BigQueue can be identified using -
//   1. arena ID
//   2. Position (offset) in the arena
//
//   <------- head aid ------> <------- head pos ------>
//  +------------+------------+------------+------------+
//  | byte 00-03 | byte 04-07 | byte 08-11 | byte 12-15 |
//  +------------+------------+------------+------------+
//
func (i *queueIndex) getHead() (int, int) {
	aid := i.indexArena.ReadUint64At(0)
	pos := i.indexArena.ReadUint64At(8)
	return int(aid), int(pos)
}

// putHead writes the value of head in the index arena
func (i *queueIndex) putHead(aid, pos int) {
	i.indexArena.WriteUint64At(uint64(aid), 0)
	i.indexArena.WriteUint64At(uint64(pos), 8)
	i.indexArena.dirty.store(1)
}

// getTail reads the values of tail of the queue from the index arena.
// Tail of a BigQueue, similar to head, can be identified using -
//   1. arena ID
//   2. Position (offset) in the arena
//
//   <------- tail aid ------> <------- tail pos ------>
//  +------------+------------+------------+------------+
//  | byte 16-19 | byte 20-23 | byte 24-27 | byte 28-31 |
//  +------------+------------+------------+------------+
//
func (i *queueIndex) getTail() (int, int) {
	aid := i.indexArena.ReadUint64At(16)
	pos := i.indexArena.ReadUint64At(24)
	return int(aid), int(pos)
}

// putTail writes the value of tail in the index arena
func (i *queueIndex) putTail(aid, pos int) {
	i.indexArena.WriteUint64At(uint64(aid), 16)
	i.indexArena.WriteUint64At(uint64(pos), 24)
	i.indexArena.dirty.store(1)
}

// getArenaSize reads the value of arena size from index
//
//   <------ arena size ----->
//  +------------+------------+
//  | byte 32-35 | byte 36-39 |
//  +------------+------------+
//
func (i *queueIndex) getArenaSize() int {
	return int(i.indexArena.ReadUint64At(32))
}

// putArenaSize writes the value of arena size in the index arena
func (i *queueIndex) putArenaSize(arenaSize int) {
	i.indexArena.WriteUint64At(uint64(arenaSize), 32)
	i.indexArena.dirty.store(1)
}

// flush writes the memory state of the index arena on to disk
func (i *queueIndex) flush() error {
	if i.indexArena.dirty.load() == 1 {
		if err := i.indexArena.Flush(syscall.MS_SYNC); err != nil {
			return err
		}

		i.indexArena.dirty.store(0)
	}

	return nil
}

// close releases all the resources currently used by the index
func (i *queueIndex) close() error {
	return i.indexArena.Unmap()
}
