package bigqueue

import (
	"fmt"
	"os"
	"path"
	"testing"
	"time"
)

func TestNewArenaNoDir(t *testing.T) {
	arena, err := newArena(fmt.Sprintf("%d/temp.dat", time.Now().UnixNano()), 100)
	if arena != nil || err == nil || os.IsExist(err) {
		t.Fatalf("unexpected return for newArena :: %v", err)
	}
}

func TestNewArenaNoFile(t *testing.T) {
	arenaSize := 100
	fileName := path.Join(os.TempDir(), "temp.dat")
	defer os.Remove(fileName)

	arena, err := newArena(fileName, arenaSize)
	if err != nil {
		t.Fatalf("error in creating new arena: %v", err)
	}
	defer arena.Unmap()

	// ensure arena struct stores correct size
	if arena.size != arenaSize {
		t.Fatalf("arena size do not match, exp: %v, actual: %v", arenaSize, arena.size)
	}

	// ensure underlined file is of correct size
	info, err := os.Stat(fileName)
	if err != nil {
		t.Fatalf("error in getting stats for file: %v", err)
	}
	if int(info.Size()) != arenaSize {
		t.Fatalf("file size do not match, exp: %v, actual: %v", arenaSize, info.Size())
	}
}

func TestNewArenaLargerFile(t *testing.T) {
	arenaSize := 100
	fileName := path.Join(os.TempDir(), "temp.dat")
	defer os.Remove(fileName)

	// setup an arena file
	if _, err := os.Create(fileName); err != nil {
		t.Fatalf("error in creating file: %v", err)
	}
	if err := os.Truncate(fileName, int64(arenaSize*2)); err != nil {
		t.Fatalf("error in truncating file: %v", err)
	}

	// creating new arena
	arena, err := newArena(fileName, arenaSize)
	if err != nil {
		t.Fatalf("error in creating new arena: %v", err)
	}
	defer arena.Unmap()

	// ensure arena struct stores correct size
	if arena.size != arenaSize {
		t.Fatalf("arena size do not match, exp: %v, actual: %v", arenaSize, arena.size)
	}

	// ensure underlined file is still of original size
	info, err := os.Stat(fileName)
	if err != nil {
		t.Fatalf("error in getting stats for file: %v", err)
	}
	if int(info.Size()) != arenaSize*2 {
		t.Fatalf("file size is changed, exp: %v, actual: %v", 2*arenaSize, info.Size())
	}
}

func TestNewArenaNoFolder(t *testing.T) {
	arenaSize := 100
	arena, err := newArena("1/2/3/4/5/6/arena.dat", arenaSize)
	if !os.IsNotExist(err) || arena != nil {
		t.Fatalf("expected file not exists error, returned: %v", err)
	}
}
