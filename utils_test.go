package bigqueue

import (
	"os"
	"testing"
)

func createTestDir(t *testing.T, testDir string) {
	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		if err := os.Mkdir(testDir, cFilePerm); err != nil {
			t.Fatalf("unable to create test dir: %v", err)
		}
	}
}

func deleteTestDir(t *testing.T, testDir string) {
	if err := os.RemoveAll(testDir); err != nil {
		t.Fatalf("unable to delete test dir: %v", err)
	}
}
