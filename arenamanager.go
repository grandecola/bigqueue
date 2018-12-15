package bigqueue

import (
	"fmt"
	"path"
)

const (
	cArenaFileFmt = "arena_%d.dat"
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

	am := &arenaManager{
		dir:       dir,
		conf:      conf,
		baseAid:   headAid,
		arenaList: make([]*arena, 0),
	}

	// setup arenas
	for i := headAid; i <= tailAid; i++ {
		if err := am.addNewArena(i); err != nil {
			am.close()
			return nil, err
		}
	}

	return am, nil
}

// getArena returns arena for a given arena ID
func (m *arenaManager) getArena(aid int) *arena {
	return m.arenaList[aid-m.baseAid]
}

// addNewArena creates arena with given arena id and adds it to arenaList
func (m *arenaManager) addNewArena(aid int) error {
	file := path.Join(m.dir, fmt.Sprintf(cArenaFileFmt, aid))
	a, err := newArena(file, m.conf.arenaSize)
	if err != nil {
		return err
	}

	m.arenaList = append(m.arenaList, a)
	return nil
}

// close unmaps all the arenas managed by arenaManager
func (m *arenaManager) close() error {
	var retErr error
	for _, a := range m.arenaList {
		if err := a.Unmap(); err != nil {
			retErr = err
		}
	}

	return retErr
}
