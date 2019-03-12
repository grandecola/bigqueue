package bigqueue

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndex(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	qi, err := newQueueIndex(testDir)
	if err != nil {
		t.Fatal("error in creating new queue index ::", err)
	}

	var aid, offset int
	aid, offset = qi.getHead()
	assert.Equal(t, 0, aid)
	assert.Equal(t, 0, offset)

	aid, offset = qi.getTail()
	assert.Equal(t, 0, aid)
	assert.Equal(t, 0, offset)

	qi.putHead(0, 8)
	aid, offset = qi.getHead()
	assert.Equal(t, 0, aid)
	assert.Equal(t, 8, offset)

	qi.putHead(7, 98)
	aid, offset = qi.getHead()
	assert.Equal(t, 7, aid)
	assert.Equal(t, 98, offset)

	aid, offset = qi.getTail()
	assert.Equal(t, 0, aid)
	assert.Equal(t, 0, offset)

	qi.putTail(9, 127*1024*1024)
	aid, offset = qi.getTail()
	assert.Equal(t, 9, aid)
	assert.Equal(t, 127*1024*1024, offset)

	arenaSize := 8 * 1024 * 1024
	qi.putArenaSize(arenaSize)
	assert.Equal(t, qi.getArenaSize(), arenaSize)

	if errFlush := qi.flush(); errFlush != nil {
		t.Fatalf("error in calling flush: %v", errFlush)
	}
	indexFile := path.Join(testDir, cIndexFileName)
	fd, err := os.Open(indexFile)
	if err != nil {
		t.Fatal("error in opening index file ::", err)
	}
	data, err := ioutil.ReadAll(fd)
	if err != nil {
		t.Fatal("error in reading index file ::", err)
	}
	expected := []byte{0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0xf0, 0x07, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00}
	if !bytes.Equal(expected, data) {
		t.Fatal("index file has unexpected content")
	}
}
