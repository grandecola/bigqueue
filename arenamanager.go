package bigqueue

import (
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"syscall"
)

const (
	cArenaFileFmt = "arena_%d.dat"
)

var (
	// errShouldNotReach is returned when an invariant is not true anymore
	errShouldNotReach = errors.New("SHOULD NOT REACH HERE")
)

// arenaManager manages all the arenas for a bigqueue
type arenaManager struct {
	sync.RWMutex

	dir         string
	conf        *bqConfig
	index       *queueIndex
	baseAid     int
	inMemArenas int
	arenaList   []*arena
}

// newArenaManager returns a pointer to new arenaManager
func newArenaManager(dir string, conf *bqConfig, index *queueIndex) (
	*arenaManager, error) {

	headAid, _ := index.getHead()
	tailAid, _ := index.getTail()

	numArenas := tailAid + 1 - headAid
	arenaList := make([]*arena, numArenas)
	am := &arenaManager{
		dir:       dir,
		conf:      conf,
		index:     index,
		baseAid:   headAid,
		arenaList: arenaList,
	}

	// we load the tail and the head Arenas into memory
	if err := am.loadArenaIntoMemory(headAid); err != nil {
		return nil, err
	}

	if err := am.loadArenaIntoMemory(tailAid); err != nil {
		return nil, err
	}

	return am, nil
}

// getArena returns arena for a given arena ID
func (m *arenaManager) getArena(aid int) (*arena, error) {
	m.Lock()
	defer m.Unlock()

	// ensure that arenaList is long enough
	relAid := aid - m.baseAid
	if relAid > len(m.arenaList) {
		panic(errShouldNotReach)
	} else if relAid == len(m.arenaList) {
		m.arenaList = append(m.arenaList, nil)
	}

	// check if arena is already into memory
	aa := m.arenaList[relAid]
	if aa != nil {
		return aa, nil
	}

	// before we get a new arena into memory, we need to ensure that after fetching
	// a new arena into memory, we do not cross the provided memory limit
	if err := m.ensureEnoughMem(); err != nil {
		return nil, err
	}

	// otherwise, get arena into memory
	if err := m.loadArenaIntoMemory(aid); err != nil {
		return nil, err
	}

	return m.arenaList[relAid], nil
}

func (m *arenaManager) flush() error {
	m.RLock()
	defer m.RUnlock()

	for _, arena := range m.arenaList {
		if arena != nil && arena.dirty.load() == 1 {
			if err := arena.Flush(syscall.MS_SYNC); err != nil {
				return err
			}

			arena.dirty.store(0)
		}
	}

	return nil
}

// close unmaps all the arenas managed by arenaManager
func (m *arenaManager) close() error {
	m.Lock()
	defer m.Unlock()

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

	return retErr
}

// ensureEnoughMem ensures that at least 1 new arena can be brought into memory
// TODO: shrink arenaList
func (m *arenaManager) ensureEnoughMem() error {
	// Check whether head has moved and arenas can be unmpped
	// Remove all such arenas from memory, irrespectively
	headAid, _ := m.index.getHead()
	for aid := m.baseAid; aid < headAid; aid++ {
		if err := m.unloadArenaFromMemory(aid); err != nil {
			return err
		}

		// disk garbage collection
		if err := m.deleteArenaBackedFile(aid); err != nil {
			return err
		}
	}

	// if no limit on # of arenas, no need for eviction
	if m.conf.maxInMemArenas == 0 {
		return nil
	}

	// Check whether an eviction is needed to begin with
	if m.inMemArenas < m.conf.maxInMemArenas {
		return nil
	}

	// Start evicting from the arena just before the last arena that we have.
	// If message size > arena size, last arena may not always be the tail arena.
	// We always ensure that head and tail arenas are not evicted from memory.
	// Assuming m.conf.maxInMemArenas >= 3.
	// Simply iterate from the last arena until enough memory is
	// available for a new arena to be loaded into memory
	tailAid, _ := m.index.getTail()
	curAid := m.baseAid + len(m.arenaList)
	for m.conf.maxInMemArenas-m.inMemArenas <= 0 {
		curAid--

		if curAid < 0 {
			return errShouldNotReach
		}

		if curAid == tailAid || curAid == headAid {
			continue
		}

		if err := m.unloadArenaFromMemory(curAid); err != nil {
			return err
		}
	}

	return nil
}

// loadArenaIntoMemory will fetch the arena into memory
func (m *arenaManager) loadArenaIntoMemory(aid int) error {
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

// unloadArenaFromMemory will remove the arena from memory
func (m *arenaManager) unloadArenaFromMemory(aid int) error {
	if m.arenaList[aid-m.baseAid] == nil {
		return nil
	}

	if err := m.arenaList[aid-m.baseAid].Unmap(); err != nil {
		return err
	}

	m.inMemArenas--
	m.arenaList[aid-m.baseAid] = nil
	return nil
}

// deleteArenaBackedFile deletes the backed file for given arena with
// arena id: aid. If file doesn't exist, the error is ignored.
func (m *arenaManager) deleteArenaBackedFile(aid int) error {
	filePath := path.Join(m.dir, fmt.Sprintf(cArenaFileFmt, aid))
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}
