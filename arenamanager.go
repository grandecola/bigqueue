package bigqueue

import (
	"fmt"
	"os"
	"path"
	"syscall"

	"github.com/grandecola/mmap"
)

const (
	cArenaFileFmt = "arena_%d.dat"
)

// arenaManager manages all the arenas for a bigqueue
type arenaManager struct {
	dir         string
	conf        *bqConfig
	md          *metadata
	baseAid     int
	arenaList   []*mmap.File
	inMemArenas int
}

// newArenaManager returns a pointer to new arenaManager.
func newArenaManager(dir string, conf *bqConfig, md *metadata) (
	*arenaManager, error) {

	headAid, _ := md.getHead()
	tailAid, _ := md.getTail()
	numArenas := tailAid + 1 - headAid
	arenaList := make([]*mmap.File, numArenas)
	am := &arenaManager{
		dir:       dir,
		conf:      conf,
		md:        md,
		baseAid:   headAid,
		arenaList: arenaList,
	}

	// we load the tail arenas into memory
	if err := am.loadArena(tailAid); err != nil {
		return nil, err
	}

	return am, nil
}

// getArena returns arena for a given arena ID
func (m *arenaManager) getArena(aid int) (*mmap.File, error) {
	// ensure that arenaList is long enough
	relAid := aid - m.baseAid
	if relAid > len(m.arenaList) {
		panic("invalid invariant: AID <= tail AID")
	} else if relAid == len(m.arenaList) {
		m.arenaList = append(m.arenaList, nil)
	}

	// check if arena is already into memory.
	aa := m.arenaList[relAid]
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

	return m.arenaList[relAid], nil
}

// ensureEnoughMem ensures that at least 1 new arena can be brought into memory.
// TODO: shrink arenaList
func (m *arenaManager) ensureEnoughMem() error {
	// Check whether head has moved and arenas can be unmapped
	// Remove all such arenas from memory, irrespectively.
	headAid, _ := m.md.getHead()
	for aid := m.baseAid; aid < headAid; aid++ {
		if err := m.unloadArena(aid); err != nil {
			return err
		}

		// disk garbage collection
		if err := m.delArenaFile(aid); err != nil {
			return err
		}
	}

	// if no limit on # of arenas, no need for eviction.
	if m.conf.maxInMemArenas == 0 {
		return nil
	}

	// Check whether an eviction is needed to begin with.
	if m.inMemArenas < m.conf.maxInMemArenas {
		return nil
	}

	// Start evicting from the arena just before the last arena that we have.
	// If message size > arena size, last arena may not always be the tail arena.
	// We always ensure that head and tail arenas are not evicted from memory.
	// Assuming m.conf.maxInMemArenas >= 3.
	// Simply iterate from the last arena until enough memory is
	// available for a new arena to be loaded into memory
	tailAid, _ := m.md.getTail()
	curAid := m.baseAid + len(m.arenaList)
	for m.conf.maxInMemArenas-m.inMemArenas <= 0 {
		curAid--

		if curAid < 0 {
			panic("invalid invariant: aid cannot be negative")
		}

		if curAid == tailAid || curAid == headAid {
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
	if m.arenaList[aid-m.baseAid] != nil {
		return nil
	}

	filePath := path.Join(m.dir, fmt.Sprintf(cArenaFileFmt, aid))
	aa, err := newArena(filePath, m.conf.arenaSize)
	if err != nil {
		return err
	}

	m.inMemArenas++
	m.arenaList[aid-m.baseAid] = aa
	return nil
}

// unloadArena will remove the arena from memory.
func (m *arenaManager) unloadArena(aid int) error {
	if m.arenaList[aid-m.baseAid] == nil {
		return nil
	}

	if err := m.arenaList[aid-m.baseAid].Unmap(); err != nil {
		return fmt.Errorf("error in unmap :: %w", err)
	}

	m.inMemArenas--
	m.arenaList[aid-m.baseAid] = nil
	return nil
}

// delArenaFile deletes the backed file for given arena with
// arena id: aid. If file doesn't exist, the error is ignored.
func (m *arenaManager) delArenaFile(aid int) error {
	filePath := path.Join(m.dir, fmt.Sprintf(cArenaFileFmt, aid))
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("error in deleting arena file :: %w", err)
	}

	return nil
}

func (m *arenaManager) flush() error {
	for _, aa := range m.arenaList {
		// arena could be nil when it is unloaded from memory.
		// see ensureEnoughMem()
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

	for i := 0; i < len(m.arenaList); i++ {
		aa := m.arenaList[i]
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
