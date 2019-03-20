package bigqueue

import (
	"fmt"
	"os"
	"path"
	"testing"
	"time"
)

func TestNewArenaNoDir(t *testing.T) {
	aa, err := newArena(fmt.Sprintf("%d/temp.dat", time.Now().UnixNano()), 100)
	if aa != nil || err == nil || os.IsExist(err) {
		t.Fatalf("unexpected return for newArena :: %v", err)
	}
}

func TestNewArenaNoReadPerm(t *testing.T) {
	fileName := path.Join(os.TempDir(), "temp.dat")
	defer func() {
		if err := os.Remove(fileName); err != nil {
			t.Fatalf("error in deleting file: %v :: %v", fileName, err)
		}
	}()

	if _, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 000); err != nil {
		t.Fatalf("unable to create file :: %v", err)
	}

	aa, err := newArena("/temp.dat", 100)
	if aa != nil || err == nil || !os.IsPermission(err) {
		t.Fatalf("unexpected return for newArena :: %v", err)
	}
}

func TestNewArenaNoFile(t *testing.T) {
	arenaSize := 100
	fileName := path.Join(os.TempDir(), "temp.dat")
	defer func() {
		if err := os.Remove(fileName); err != nil {
			t.Fatalf("error in deleting file: %v :: %v", fileName, err)
		}
	}()

	aa, err := newArena(fileName, arenaSize)
	if err != nil {
		t.Fatalf("error in creating new arena: %v", err)
	}
	defer func() {
		errUnmap := aa.Unmap()
		if errUnmap != nil {
			t.Fatalf("error occurred while unmapping: %v", errUnmap)
		}
	}()

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
	defer func() {
		if err := os.Remove(fileName); err != nil {
			t.Fatalf("error in deleting file: %v :: %v", fileName, err)
		}
	}()

	// setup an arena file
	if _, err := os.Create(fileName); err != nil {
		t.Fatalf("error in creating file: %v", err)
	}
	if err := os.Truncate(fileName, int64(arenaSize*2)); err != nil {
		t.Fatalf("error in truncating file: %v", err)
	}

	// creating new arena
	aa, err := newArena(fileName, arenaSize)
	if err != nil {
		t.Fatalf("error in creating new arena: %v", err)
	}
	defer func() {
		errUnmap := aa.Unmap()
		if errUnmap != nil {
			t.Fatalf("error occurred while unmapping: %v", errUnmap)
		}
	}()

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
	aa, err := newArena("1/2/3/4/5/6/aa.dat", arenaSize)
	if !os.IsNotExist(err) || aa != nil {
		t.Fatalf("expected file not exists error, returned: %v", err)
	}
}
