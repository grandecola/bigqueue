package bigqueue

import (
	"os"
	"path/filepath"
	"testing"
)

func TestArenaFileNotDeletedWhenDataNotConsumed(t *testing.T) {
	dir := "testdata_arena_not_deleted"
	defer os.RemoveAll(dir)

	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("failed to create test directory: %v", err)
	}

	bq, err := NewMmapQueue(dir)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer bq.Close()

	msg := []byte("hello")
	err = bq.Enqueue(msg)
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	arenaFile := filepath.Join(dir, "0"+cArenaFileSuffix)
	if _, err := os.Stat(arenaFile); os.IsNotExist(err) {
		t.Errorf("arena file should not be deleted when data not consumed")
	}
}
