package bigqueue

import (
	"os"
	"path"
	"testing"
)

var (
	testPath = path.Join(os.TempDir(), "testdir_%d")
)

func createTestDir(t *testing.T, testDir string) {
	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		if err := os.Mkdir(testDir, cFilePerm); err != nil {
			t.Errorf("unable to create test dir: %s", err)
		}
	}
}

func deleteTestDir(t *testing.T, testDir string) {
	if err := os.RemoveAll(testDir); err != nil {
		t.Errorf("unable to delete test dir: %s", err)
	}
}
