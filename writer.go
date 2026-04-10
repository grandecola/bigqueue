package bigqueue

import (
	"github.com/grandecola/mmap"
)

// writer knows how to copy data of given length to arena.
type writer interface {
	// len returns the length of the data that writer holds.
	len() int

	// writeTo writes the data to arena starting at given offset. It is possible that the
	// whole data that writer holds may not fit in the given arena. Hence, an index
	// into the data is provided. The data is copied starting from index until either
	// no more data is left, or no space is left in the given arena to write more data.
	writeTo(aa *mmap.File, offset, index int) int
}

// bytesWriter holds a slice of bytes and satisfies the bigqueue.writer interface.
type bytesWriter struct {
	b []byte
}

// len returns the length of data that bytesWriter holds.
func (bw *bytesWriter) len() int {
	return len(bw.b)
}

// writeTo writes data that it holds from index to end of
// the data or arena, into the arena starting at the offset.
func (bw *bytesWriter) writeTo(aa *mmap.File, offset, index int) int {
	n, _ := aa.WriteAt(bw.b[index:], int64(offset))
	return n
}

// taggedBytesWriter holds a tag ([]byte) and a data slice and satisfies the bigqueue.writer
// interface. The wire layout written per message is:
//
//	[1-byte tag-length | tag bytes... | data bytes...]
//
// This allows DequeueWithTag to recover both the tag and the original payload without
// allocating any extra memory.
type taggedBytesWriter struct {
	tag []byte
	b   []byte
}

// len returns the total number of bytes that taggedBytesWriter will write
// (1-byte tag-length + tag + data).
func (tw *taggedBytesWriter) len() int {
	return 1 + len(tw.tag) + len(tw.b)
}

// writeTo writes the tag-length byte, tag bytes, and data bytes into the arena starting
// at offset. index is the total number of bytes already written (across previous arenas).
// Each call writes at most one logical segment (tag-len byte, tag bytes, or data bytes)
// so the writeBytes loop can handle arena-boundary transitions between segments.
// It returns the number of bytes written in this call.
func (tw *taggedBytesWriter) writeTo(aa *mmap.File, offset, index int) int {
	tagLen := len(tw.tag)

	// Phase 0: write 1-byte tag-length prefix (overall index 0).
	if index == 0 {
		tagLenBuf := [1]byte{byte(tagLen)}
		n, _ := aa.WriteAt(tagLenBuf[:], int64(offset))
		return n
	}

	// Phase 1: write tag bytes (overall indices 1 .. tagLen).
	if index < 1+tagLen {
		tagStart := index - 1
		n, _ := aa.WriteAt(tw.tag[tagStart:], int64(offset))
		return n
	}

	// Phase 2: write data bytes (overall indices 1+tagLen ..).
	dataStart := index - 1 - tagLen
	n, _ := aa.WriteAt(tw.b[dataStart:], int64(offset))
	return n
}

// stringWriter holds a string and satisfies bigqueue.writer interface.
type stringWriter struct {
	s string
}

// len returns the length of string that stringWriter holds.
func (sw *stringWriter) len() int {
	return len(sw.s)
}

// writeTo writes the string starting from index into arena
// starting at offset until either arena lasts or string lasts.
func (sw *stringWriter) writeTo(aa *mmap.File, offset, index int) int {
	return aa.WriteStringAt(sw.s[index:], int64(offset))
}
