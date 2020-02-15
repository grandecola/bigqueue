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
	fd, err := openOrCreateFile(file, int64(size))
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
func openOrCreateFile(file string, size int64) (*os.File, error) {
	fd, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, cFilePerm)
	if err != nil {
		return nil, fmt.Errorf("error in creating/opening file :: %w", err)
	}

	info, err := fd.Stat()
	if err != nil {
		return nil, fmt.Errorf("error in finding info for file :: %w", err)
	}

	if info.Size() < size {
		if err := fd.Truncate(size); err != nil {
			return nil, fmt.Errorf("error in truncating file :: %w", err)
		}
	}

	return fd, nil
}
