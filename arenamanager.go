package bigqueue

import (
	"fmt"
	"path"
	"strconv"
	"syscall"

	"github.com/grandecola/mmap"
)

const (
	cArenaFileSuffix = "_arena.dat"
)

// arenaManager manages all the arenas for a bigqueue
type arenaManager struct {
	dir       string
	conf      *bqConfig
	md        *metadata
	baseAid   int
	arenas    []*mmap.File
	inMem     int
	filePath  []byte
}

// newArenaManager returns a pointer to new arenaManager.
func newArenaManager(dir string, conf *bqConfig, md *metadata) (*arenaManager, error) {
	headAid, _ := md.getHead()
	tailAid, _ := md.getTail()

	numArenas := tailAid + 1 - headAid
	arenas := make([]*mmap.File, numArenas)
	am := &arenaManager{
		dir:     path.Clean(dir),
		conf:    conf,
		md:      md,
		baseAid: headAid,
		arenas:  arenas,
	}

	// we load the tail arena into memory
	if err := am.loadArena(tailAid); err != nil {
		return nil, err
	}

	return am, nil
}

// getArena returns arena for a given arena ID
func (m *arenaManager) getArena(aid int) (*mmap.File, error) {
	relAid := aid - m.baseAid
	if relAid == len(m.arenas) {
		m.arenas = append(m.arenas, nil)
	}
	aa := m.arenas[relAid]
	if aa != nil {
		return aa, nil
	}

	// before we get a new arena into memory, we need to ensure that after fetching
	// a new arena into memory, we do not cross the provided memory limit.
	if err := m.ensureEnoughMem(); err != nil {
		return nil, err
	}

	// now, get arena into memory
	if err := m.loadArena(aid); err != nil {
		return nil, err
	}

	return m.arenas[aid], nil
}

// ensureEnoughMem ensures that at least 1 new arena can be brought into memory.
func (m *arenaManager) ensureEnoughMem() error {
	// if no limit on # of arenas, no need for eviction.
	if m.conf.maxInMemArenas == 0 {
		return nil
	}

	// Check whether an eviction is needed to begin with.
	if m.inMem < m.conf.maxInMemArenas {
		return nil
	}

	// Start evicting from the arena just before the last arena that we have.
	// If message size > arena size, last arena may not always be the tail arena.
	// We always ensure that head and tail arenas are not evicted from memory.
	// Simply iterate from the last arena until enough memory is
	// available for a new arena to be loaded into memory
	tailAid, _ := m.md.getTail()
	curAid := m.baseAid + len(m.arenas)
	for m.conf.maxInMemArenas-m.inMem <= 0 {
		curAid--

		if curAid < 0 {
			panic("not enough memory to hold arenas in memory")
		}

		if curAid == tailAid {
			continue
		}

		if err := m.unloadArena(curAid); err != nil {
			return err
		}
	}

	return nil
}

// loadArena will fetch the arena into memory.
func (m *arenaManager) loadArena(aid int) error {
	if m.arenas[aid-m.baseAid] != nil {
		return nil
	}

	m.filePath = append(m.filePath[:0], m.dir...)
	m.filePath = append(m.filePath, '/')
	m.filePath = strconv.AppendInt(m.filePath, int64(aid), 10)
	m.filePath = append(m.filePath, cArenaFileSuffix...)
	aa, err := newArena(string(m.filePath), m.conf.arenaSize)
	if err != nil {
		return err
	}

	m.inMem++
	m.arenas[aid-m.baseAid] = aa
	return nil
}

// unloadArena will remove the arena from memory.
func (m *arenaManager) unloadArena(aid int) error {
	if m.arenas[aid-m.baseAid] == nil {
		return nil
	}

	if err := m.arenas[aid-m.baseAid].Unmap(); err != nil {
		return fmt.Errorf("error in unmap :: %w", err)
	}

	m.inMem--
	m.arenas[aid-m.baseAid] = nil
	return nil
}

func (m *arenaManager) flush() error {
	for _, aa := range m.arenas {
		if aa == nil {
			continue
		}

		if err := aa.Flush(syscall.MS_SYNC); err != nil {
			return fmt.Errorf("error in flushing arena file :: %w", err)
		}
	}

	if err := m.md.flush(); err != nil {
		return err
	}

	return nil
}

// close unmaps all the arenas managed by arenaManager.
func (m *arenaManager) close() error {
	var retErr error
	for _, aa := range m.arenas {
		if aa == nil {
			continue
		}

		if err := aa.Unmap(); err != nil {
			retErr = err
		}
	}

	if retErr != nil {
		return fmt.Errorf("error in unmap :: %w", retErr)
	}

	return nil
}
