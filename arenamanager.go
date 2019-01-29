package bigqueue

import (
	"errors"
	"fmt"
	"path"
)

const (
	cArenaFileFmt = "arena_%d.dat"
)

var (
	// errShouldNotReach is returned when code reaches unexpected places
	errShouldNotReach = errors.New("SHOULD NOT REACH HERE")
)

// arenaManager manages all the arenas for a bigqueue
type arenaManager struct {
	dir       string
	conf      *bqConfig
	baseAid   int
	arenaList []*arena
}

// newArenaManager returns a pointer to new arenaManager
func newArenaManager(dir string, conf *bqConfig, headAid, tailAid int) (
	*arenaManager, error) {

	numArenas := tailAid + 1 - headAid
	arenaList := make([]*arena, numArenas)
	am := &arenaManager{
		dir:       dir,
		conf:      conf,
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

	// otherwise, get arena into memory
	if err := m.loadArenaIntoMemory(aid); err != nil {
		return nil, err
	}

	return m.arenaList[relAid], nil
}

// loadArenaIntoMemory will fetch the arena into memory
func (m *arenaManager) loadArenaIntoMemory(aid int) error {
	filePath := path.Join(m.dir, fmt.Sprintf(cArenaFileFmt, aid))
	aa, err := newArena(filePath, m.conf.arenaSize)
	if err != nil {
		return err
	}

	m.arenaList[aid-m.baseAid] = aa
	return nil
}

// close unmaps all the arenas managed by arenaManager
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

	return retErr
}
