package bigqueue

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/grandecola/mmap"
)

const (
	cMetadataVersion  = 1
	cMetadataFileName = "metadata.dat"

	// size of file without any consumer information.
	cMetadataSize = 56
)

var (
	// ErrIncompatibleVersion is returned when file format is older/newer.
	ErrIncompatibleVersion = errors.New("incompatible format of the code and data")
)

// metadata stores head, tail and config parameters for a bigqueue.
type metadata struct {
	aa   *mmap.File
	co   map[string]int64
	file string
	size int64
}

// newMetadata creates/reads metadata file for a bigqueue.
func newMetadata(dataDir string, arenaSize int) (*metadata, error) {
	metaPath := filepath.Join(dataDir, cMetadataFileName)
	info, err := os.Stat(metaPath)
	switch {
	case err != nil && !os.IsNotExist(err):
		return nil, fmt.Errorf("error in reading metadata file :: %w", err)
	case err != nil && os.IsNotExist(err):
		return createFile(metaPath)
	default:
		return loadFile(metaPath, info.Size())
	}
}

// loadFile loads the file and builds the struct for metadata.
func loadFile(metaPath string, size int64) (*metadata, error) {
	aa, err := newArena(metaPath, int(size))
	if err != nil {
		return nil, fmt.Errorf("error in creating arena for metadata file :: %w", err)
	}

	md := &metadata{
		aa:   aa,
		co:   make(map[string]int64),
		file: metaPath,
		size: size,
	}
	if md.getVersion() != cMetadataVersion {
		return nil, ErrIncompatibleVersion
	}

	base := int64(cMetadataSize)
	for i := 0; i < md.getNumConsumers(); i++ {
		name := md.getConsumerName(base)
		md.co[name] = base
		base += int64(len(name)) + 24
	}

	return md, nil
}

// createFile creates a new metadata file.
func createFile(metaPath string) (*metadata, error) {
	aa, err := newArena(metaPath, cMetadataSize)
	if err != nil {
		return nil, fmt.Errorf("error in creating arena for metadata file :: %w", err)
	}

	md := &metadata{
		aa:   aa,
		co:   make(map[string]int64),
		file: metaPath,
		size: cMetadataSize,
	}
	md.putVersion()

	return md, nil
}

// getVersion reads the value of data format version.
//
//   <-------- version ------->
//  +------------+------------+
//  | byte 00-03 | byte 04-07 |
//  +------------+------------+
//
func (m *metadata) getVersion() int {
	return int(m.aa.ReadUint64At(0))
}

// putVersion writes the metadata format version.
func (m *metadata) putVersion() {
	m.aa.WriteUint64At(cMetadataVersion, 0)
}

// getHead reads the value of head of the queue from the metadata.
// head points to the first element that is still not deleted yet.
// Head of a bigqueue can be identified using:
//   1. Arena ID
//   2. Position (offset) in the arena
//
//   <------- head aid ------> <------- head pos ------>
//  +------------+------------+------------+------------+
//  | byte 08-11 | byte 12-15 | byte 16-19 | byte 20-23 |
//  +------------+------------+------------+------------+
//
func (m *metadata) getHead() (int, int) {
	return int(m.aa.ReadUint64At(8)), int(m.aa.ReadUint64At(16))
}

// putHead stores the value of head in the metadata.
// func (m *metadata) putHead(aid, pos int) {
// 	m.aa.WriteUint64At(uint64(aid), 8)
// 	m.aa.WriteUint64At(uint64(pos), 16)
// }

// getTail reads the values of tail of the queue from the metadata arena.
// Tail of a bigqueue, similar to head, can be identified using:
//   1. Arena ID
//   2. Position (offset) in the arena
//
//   <------- tail aid ------> <------- tail pos ------>
//  +------------+------------+------------+------------+
//  | byte 24-27 | byte 28-31 | byte 32-35 | byte 36-39 |
//  +------------+------------+------------+------------+
func (m *metadata) getTail() (int, int) {
	return int(m.aa.ReadUint64At(24)), int(m.aa.ReadUint64At(32))
}

// putTail stores the value of tail in the metadata arena.
func (m *metadata) putTail(aid, pos int) {
	m.aa.WriteUint64At(uint64(aid), 24)
	m.aa.WriteUint64At(uint64(pos), 32)
}

// getArenaSize reads the value of arena size from metadata file.
//
//   <------ arena size ----->
//  +------------+------------+
//  | byte 40-43 | byte 44-47 |
//  +------------+------------+
//
func (m *metadata) getArenaSize() int {
	return int(m.aa.ReadUint64At(40))
}

// putArenaSize stores the value of arena size in the metadata.
func (m *metadata) putArenaSize(size int) {
	m.aa.WriteUint64At(uint64(size), 40)
}

// getNumConsumers reads the value of # of consumers from metadata file.
//
//   <---- # of consumers --->
//  +------------+------------+
//  | byte 48-51 | byte 52-55 |
//  +------------+------------+
//
func (m *metadata) getNumConsumers() int {
	return int(m.aa.ReadUint64At(48))
}

// putNumConsumers stores the value of # of consumers in the metadata.
func (m *metadata) putNumConsumers(size int) {
	m.aa.WriteUint64At(uint64(size), 48)
}

/*
 * Now, we store all the conusmer information in the metadata file.
 * We store 4 things for a given consumer (in this order) -
 *   1. Consumer name length (8 bytes)
 *   2. Head arena id (8 bytes)
 *   3. Head position in the arena (8 bytes)
 *   4. Name of the consumer (length)
 */

// getConsumerLength reads the length of the consumer name for
// the consumer stored at a given base offset in metadata file.
//
//   <------------- consumer length ----------->
//  +--------------------+----------------------+
//  | byte base - base+3 | byte base+4 - base+7 |
//  +--------------------+----------------------+
//
func (m *metadata) getConsumerLength(base int64) int {
	return int(m.aa.ReadUint64At(base))
}

// putConsumerLength writes the length of the consumer name into the metadata file.
func (m *metadata) putConsumerLength(base int64, length int) {
	m.aa.WriteUint64At(uint64(length), base)
}

// getConsumerHead reads the head position (aid+offset) for
// the consumer stored at a given base offset in metadata file.
//
//   <-------------- consumer head AID -------------> <-------------- consumer head pos -------------->
//  +-----------------------+------------------------+------------------------+------------------------+
//  | byte base+8 - base+11 | byte base+12 - base+15 | byte base+16 - base+19 | byte base+20 - base+23 |
//  +-----------------------+------------------------+------------------------+------------------------+
//
func (m *metadata) getConsumerHead(base int64) (int, int) {
	return int(m.aa.ReadUint64At(base + 8)), int(m.aa.ReadUint64At(base + 16))
}

// putConsumerHead writes the head position of the consumer into the metadata file.
func (m *metadata) putConsumerHead(base int64, aid, pos int) {
	m.aa.WriteUint64At(uint64(aid), base+8)
	m.aa.WriteUint64At(uint64(pos), base+16)
}

// getConsumerName reads the name of the consumer stored at a given offset in metadata.
func (m *metadata) getConsumerName(base int64) string {
	sb := &strings.Builder{}
	sb.Grow(m.getConsumerLength(base))
	_ = m.aa.ReadStringAt(sb, base+24)
	return sb.String()
}

// putConsumerName writes the name of the consumer in the metadata file.
// name is stored at offset 24 until it can be fully stored in the file.
func (m *metadata) putConsumerName(base int64, name string) {
	m.aa.WriteStringAt(name, base+24)
}

// putConsumer writes the consumer in the metadata file.
func (m *metadata) putConsumer(base int64, name string) {
	m.putConsumerLength(base, len(name))
	aid, offset := m.getHead()
	m.putConsumerHead(base, aid, offset)
	m.putConsumerName(base, name)
	m.putNumConsumers(m.getNumConsumers() + 1)
	m.co[name] = base
}

// getConsumer either finds an existing consumer with the given name or initializes
// a new consumer with the given name and stores it into the metadata file.
func (m *metadata) getConsumer(name string) (int64, error) {
	if b, ok := m.co[name]; ok {
		return b, nil
	}

	oldsize := m.size
	newsize := m.size + 24 + int64(len(name))
	if err := m.extendFile(newsize); err != nil {
		return 0, err
	}

	m.size = newsize
	m.putConsumer(oldsize, name)
	return oldsize, nil
}

// flush writes the memory state of the metadata arena on to disk.
func (m *metadata) flush() error {
	return m.aa.Flush(syscall.MS_SYNC)
}

// close releases all the resources currently used by the metadata.
func (m *metadata) close() error {
	if err := m.flush(); err != nil {
		return err
	}

	return m.aa.Unmap()
}

// extendFile extends the metadata file to given size.
func (m *metadata) extendFile(size int64) error {
	if err := m.close(); err != nil {
		return err
	}

	// extend the file
	if err := os.Truncate(m.file, size); err != nil {
		return fmt.Errorf("error in extending metadata file :: %w", err)
	}

	// remap the arena with bigger size
	aa, err := newArena(m.file, int(size))
	if err != nil {
		return fmt.Errorf("error in remapping metadata file :: %w", err)
	}

	m.aa = aa
	return nil
}
