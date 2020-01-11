package bigqueue

import (
	"fmt"
	"os"
	"syscall"

	"github.com/grandecola/mmap"
)

// newArena returns pointer to a mapped file. It takes a file location and mmaps it.
// If file location does not exist, it creates a file of given size.
func newArena(file string, size int) (*mmap.File, error) {
	fd, err := openOrCreateFile(file, size)
	if err != nil {
		return nil, err
	}

	m, err := mmap.NewSharedFileMmap(fd, 0, size, syscall.PROT_READ|syscall.PROT_WRITE)
	if err != nil {
		return nil, fmt.Errorf("error in mmaping a file :: %w", err)
	}

	// We can close the file descriptor here.
	if err := fd.Close(); err != nil {
		return nil, fmt.Errorf("error in closing the fd :: %w", err)
	}

	return m, nil
}

// openOrCreateFile opens the file if it exists,
// otherwise creates a new file of a given size.
func openOrCreateFile(file string, size int) (*os.File, error) {
	if _, errExist := os.Stat(file); errExist == nil {
		fd, err := os.OpenFile(file, os.O_RDWR, cFilePerm)
		if err != nil {
			return nil, fmt.Errorf("error in reading arena file :: %w", err)
		}

		return fd, nil
	} else if os.IsNotExist(errExist) {
		// create an empty file
		fd, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, cFilePerm)
		if err != nil {
			return nil, fmt.Errorf("error in creating empty file :: %w", err)
		}

		// truncate the file to required size
		if err := os.Truncate(file, int64(size)); err != nil {
			return nil, fmt.Errorf("error in truncating file :: %w", err)
		}

		return fd, nil
	} else {
		return nil, fmt.Errorf("error in finding info for arena file :: %w", errExist)
	}
}
