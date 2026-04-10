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

// taggedBytesWriter holds a tag byte and a slice of bytes and satisfies the bigqueue.writer
// interface. The tag byte is written before the data, allowing consumers to identify message
// types during dequeue without parsing the full payload.
type taggedBytesWriter struct {
	tagBuf [1]byte
	b      []byte
}

// len returns the length of data that taggedBytesWriter holds (tag + data).
func (tw *taggedBytesWriter) len() int {
	return 1 + len(tw.b)
}

// writeTo writes the tag byte followed by the data into the arena starting at offset.
// Because the data may span multiple arenas, index tracks the total bytes written so far.
func (tw *taggedBytesWriter) writeTo(aa *mmap.File, offset, index int) int {
	if index == 0 {
		n, _ := aa.WriteAt(tw.tagBuf[:], int64(offset))
		if n < 1 {
			return 0
		}
		n2, _ := aa.WriteAt(tw.b, int64(offset+1))
		return 1 + n2
	}
	// Tag byte already written; continue writing remaining data bytes.
	dataIndex := index - 1
	n, _ := aa.WriteAt(tw.b[dataIndex:], int64(offset))
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
