package bigqueue

import (
	"fmt"
	"path"
	"syscall"

	"github.com/grandecola/mmap"
)

const (
	cArenaFileFmt = "arena_%d.dat"
)

// arenaManager manages all the arenas for a bigqueue
type arenaManager struct {
	dir    string
	conf   *bqConfig
	md     *metadata
	arenas map[int]*mmap.File
	maxAid int
}

// newArenaManager returns a pointer to new arenaManager.
func newArenaManager(dir string, conf *bqConfig, md *metadata) (*arenaManager, error) {
	am := &arenaManager{
		dir:    dir,
		conf:   conf,
		md:     md,
		arenas: make(map[int]*mmap.File),
	}

	// we load the tail arena into memory
	tailAid, _ := md.getTail()
	if err := am.loadArena(tailAid); err != nil {
		return nil, err
	}

	return am, nil
}

// getArena returns arena for a given arena ID
func (m *arenaManager) getArena(aid int) (*mmap.File, error) {
	// check if arena is already into memory.
	if aa, ok := m.arenas[aid]; ok {
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
	if len(m.arenas) < m.conf.maxInMemArenas {
		return nil
	}

	// Start evicting from the arena just before the last arena that we have.
	// If message size > arena size, last arena may not always be the tail arena.
	// We always ensure that head and tail arenas are not evicted from memory.
	// Simply iterate from the last arena until enough memory is
	// available for a new arena to be loaded into memory
	tailAid, _ := m.md.getTail()
	curAid := m.maxAid
	for m.conf.maxInMemArenas-len(m.arenas) <= 0 {
		curAid--

		// TODO: may not want to remove arenas that have consumer heads.
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
	if _, ok := m.arenas[aid]; ok {
		return nil
	}

	if aid > m.maxAid {
		m.maxAid = aid
	}

	filePath := path.Join(m.dir, fmt.Sprintf(cArenaFileFmt, aid))
	aa, err := newArena(filePath, m.conf.arenaSize)
	if err != nil {
		return err
	}

	m.arenas[aid] = aa
	return nil
}

// unloadArena will remove the arena from memory.
func (m *arenaManager) unloadArena(aid int) error {
	aa, ok := m.arenas[aid]
	if !ok {
		return nil
	}

	if err := aa.Unmap(); err != nil {
		return fmt.Errorf("error in unmap :: %w", err)
	}

	delete(m.arenas, aid)
	return nil
}

func (m *arenaManager) flush() error {
	for _, aa := range m.arenas {
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
		if err := aa.Unmap(); err != nil {
			retErr = err
		}
	}

	if retErr != nil {
		return fmt.Errorf("error in unmap :: %w", retErr)
	}

	return nil
}
