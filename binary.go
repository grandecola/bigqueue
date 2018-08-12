package bigqueue

import "encoding/binary"

// BytesToUint64 converts byte slice to unit64
func BytesToUint64(byteSlice []byte) uint64 {
	return binary.LittleEndian.Uint64(byteSlice)
}

// Uint64ToBytes converts uint64 to bytes
func Uint64ToBytes(num uint64) []byte {
	byteSlice := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteSlice, num)
	return byteSlice
}
