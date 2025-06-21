package bigqueue

import (
	"strings"

	"github.com/grandecola/mmap"
)

// reader knows how to read data from arena.
type reader interface {
	// grow grows reader's capacity, if necessary, to guarantee space for
	// another n bytes. After grow(n), at least n bytes can be written to reader
	// without another allocation. If n is negative, grow panics.
	grow(n int)

	// readFrom copies data from arena starting at given offset. Because the data
	// may be spread over multiple arenas, an index into the data is provided so
	// the data is copied starting at given index.
	readFrom(aa *mmap.File, offset, index int) int
}

// bytesReader holds a slice of bytes to hold the data.
type bytesReader struct {
	b []byte
}

// grow expands the capacity of bytesReader by n bytes.
func (br *bytesReader) grow(n int) {
	if n < 0 {
		panic("bigqueue.reader.grow: negative count")
	}

	temp := make([]byte, n+len(br.b))
	if br.b != nil {
		_ = copy(temp, br.b)
	}
	br.b = temp
}

// readFrom reads the arena at offset and copies the data at index.
func (br *bytesReader) readFrom(aa *mmap.File, offset, index int) int {
	n, _ := aa.ReadAt(br.b[index:], int64(offset))
	return n
}

// stringReader holds a string builder to hold the data read from arena(s).
type stringReader struct {
	sb   strings.Builder
	ecap int
}

// grow expands the capacity of the string builder by n bytes.
func (sr *stringReader) grow(n int) {
	sr.ecap = n + sr.sb.Len()
	sr.sb.Grow(sr.ecap - sr.sb.Cap())
}

// readFrom reads data from arena starting at offset and stores it at provided index.
func (sr *stringReader) readFrom(aa *mmap.File, offset, _ int) int {
	return aa.ReadStringAt(&sr.sb, int64(offset), int64(sr.ecap-sr.sb.Len()))
}
