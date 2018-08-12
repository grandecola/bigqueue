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
	testDir := fmt.Sprintf(testPath, rand.Intn(1000))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	qi, err := NewQueueIndex(testDir)
	if err != nil {
		t.Error("error in creating new queue index ::", err)
	}

	var aid, offset int
	aid, offset = qi.GetHead()
	assert.Equal(t, 0, aid)
	assert.Equal(t, 0, offset)

	aid, offset = qi.GetTail()
	assert.Equal(t, 0, aid)
	assert.Equal(t, 0, offset)

	qi.UpdateHead(0, 8)
	aid, offset = qi.GetHead()
	assert.Equal(t, 0, aid)
	assert.Equal(t, 8, offset)

	qi.UpdateHead(7, 98)
	aid, offset = qi.GetHead()
	assert.Equal(t, 7, aid)
	assert.Equal(t, 98, offset)

	aid, offset = qi.GetTail()
	assert.Equal(t, 0, aid)
	assert.Equal(t, 0, offset)

	qi.UpdateTail(9, 127*1024*1024)
	aid, offset = qi.GetTail()
	assert.Equal(t, 9, aid)
	assert.Equal(t, 127*1024*1024, offset)

	qi.Flush()
	indexFile := path.Join(testDir, cIndexFileName)
	fd, err := os.Open(indexFile)
	if err != nil {
		t.Error("error in opening index file ::", err)
	}
	data, err := ioutil.ReadAll(fd)
	if err != nil {
		t.Error("error in reading index file ::", err)
	}
	expected := []byte{0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0xf0, 0x07, 0x00, 0x00, 0x00, 0x00}
	if !bytes.Equal(expected, data) {
		t.Errorf("index file has unexpected content")
	}
}
