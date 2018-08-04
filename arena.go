package bigqueue

import (
	"os"
	"syscall"

	"github.com/grandecola/mmap"
)

const (
	cFilePerm = 0744
)

// Arena is an abstraction for a memory mapped file of a given size
type Arena struct {
	*mmap.Mmap
	size int
	file string
}

// NewArena returns pointer to an Arena. It takes a file location and mmaps it.
// If file location does not exist, it creates file of given size.
func NewArena(file string, size int) (*Arena, error) {
	fd, err := openOrCreateFile(file, size)
	if err != nil {
		return nil, err
	}

	m, err := mmap.NewSharedFileMmap(fd, 0, size, syscall.PROT_READ|syscall.PROT_WRITE)
	if err != nil {
		return nil, err
	}

	return &Arena{
		Mmap: m,
		size: size,
		file: file,
	}, nil
}

func openOrCreateFile(file string, size int) (*os.File, error) {
	if _, err := os.Stat(file); err == nil {
		// open file
		fd, err := os.OpenFile(file, os.O_RDWR, cFilePerm)
		if err != nil {
			return nil, err
		}

		return fd, nil
	} else if os.IsNotExist(err) {
		// create an empty file
		fd, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, cFilePerm)
		if err != nil {
			return nil, err
		}

		// truncate the file to required size
		if err := os.Truncate(file, int64(size)); err != nil {
			return nil, err
		}

		return fd, nil
	} else {
		return nil, err
	}
}
