package bigqueue

import (
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
	"time"
)

func TestNewArenaNoDir(t *testing.T) {
	arena, err := newArena(fmt.Sprintf("%d/temp.dat", time.Now().UnixNano()), 100)
	if arena != nil || err == nil || !strings.Contains(err.Error(), "no such file or directory") {
		t.Errorf("unexpected return for newArena :: %v", err)
	}
}

func TestNewArenaNoFile(t *testing.T) {
	arenaSize := 100
	fileName := path.Join(os.TempDir(), "temp.dat")
	defer os.Remove(fileName)

	arena, err := newArena(fileName, arenaSize)
	if err != nil {
		t.Errorf("error in creating new arena: %v", err)
	}
	defer arena.Unmap()

	// ensure arena struct stores correct size
	if arena.size != arenaSize {
		t.Errorf("arena size do not match, expected: %v, actual: %v", arenaSize, arena.size)
	}

	// ensure underlined file is of correct size
	info, err := os.Stat(fileName)
	if err != nil {
		t.Errorf("error in getting stats for file: %v", err)
	}
	if int(info.Size()) != arenaSize {
		t.Errorf("arena file size do not match, expected: %v, actual: %v", arenaSize, info.Size())
	}
}

func TestNewArenaLargerFile(t *testing.T) {
	arenaSize := 100
	fileName := path.Join(os.TempDir(), "temp.dat")
	defer os.Remove(fileName)

	if _, err := os.Create(fileName); err != nil {
		t.Errorf("error in creating file: %v", err)
	}
	if err := os.Truncate(fileName, int64(arenaSize*2)); err != nil {
		t.Errorf("error in truncating file: %v", err)
	}

	arena, err := newArena(fileName, arenaSize)
	if err != nil {
		t.Errorf("error in creating new arena: %v", err)
	}
	defer arena.Unmap()

	// ensure arena struct stores correct size
	if arena.size != arenaSize {
		t.Errorf("arena size do not match, expected: %v, actual: %v", arenaSize, arena.size)
	}

	// ensure underlined file is still of original size
	info, err := os.Stat(fileName)
	if err != nil {
		t.Errorf("error in getting stats for file: %v", err)
	}
	if int(info.Size()) != arenaSize*2 {
		t.Errorf("file size is changed, expected: %v, actual: %v", 2*arenaSize, info.Size())
	}
}
