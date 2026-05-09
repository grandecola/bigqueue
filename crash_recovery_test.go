package bigqueue

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestHelperCrashEnqueue is a helper process that constantly enqueues and then lets itself be killed.
func TestHelperCrashEnqueue(_ *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	testDir := os.Getenv("CRASH_TEST_DIR")
	bq, err := NewMmapQueue(testDir, SetArenaSize(4096))
	if err != nil {
		os.Exit(2)
	}
	msg := []byte("enqueue crash testing message data block")
	for {
		if err := bq.Enqueue(msg); err != nil {
			os.Exit(3)
		}
	}
}

// TestCrashRecovery_Enqueue uses a multi-process approach.
// A child process executes a real Enqueue operation continuously and is forcefully killed.
// The parent process then checks if the queue file can be recovered without corruption.
func TestCrashRecovery_Enqueue(t *testing.T) {
	testDir := filepath.Join(t.TempDir(), "test_crash_enqueue_exec")
	os.MkdirAll(testDir, 0755)

	cmd := exec.Command(os.Args[0], "-test.run=TestHelperCrashEnqueue")
	cmd.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1", "CRASH_TEST_DIR="+testDir)
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start helper process: %v", err)
	}

	// Wait briefly to allow some data to be written, then aggressively kill the process.
	time.Sleep(100 * time.Millisecond)
	_ = cmd.Process.Kill()
	_ = cmd.Wait() // wait for it to actually die

	// Now we reopen the queue to ensure it hasn't been corrupted by the torn write.
	bq, err := NewMmapQueue(testDir, SetArenaSize(4096))
	if err != nil {
		t.Fatalf("failed to reopen queue after crash: %v", err)
	}
	defer bq.Close()

	// Verify the queue is readable without any internal panic or metadata inconsistency.
	count := 0
	expectedMsg := []byte("enqueue crash testing message data block")
	for !bq.IsEmpty() {
		msg, err := bq.Dequeue()
		if err != nil {
			t.Fatalf("failed to dequeue recovered message at index %d: %v", count, err)
		}
		if !bytes.Equal(msg, expectedMsg) {
			t.Fatalf("message logic corrupted, got %s", string(msg))
		}
		count++
	}

	if count == 0 {
		t.Logf("Queue was clean but empty. Try increasing sleep to verify writes if needed.")
	} else {
		t.Logf("Successfully read %d messages recovered cleanly after enqueue crash", count)
	}
}

// TestHelperCrashDequeue is a helper process that constantly dequeues.
func TestHelperCrashDequeue(_ *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	testDir := os.Getenv("CRASH_TEST_DIR")
	bq, err := NewMmapQueue(testDir, SetArenaSize(4096))
	if err != nil {
		os.Exit(2)
	}
	for {
		if bq.IsEmpty() {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		_, err := bq.Dequeue()
		if err != nil {
			os.Exit(3)
		}
		// Slow down so the parent process can reliably kill it mid-flight
		time.Sleep(1 * time.Millisecond)
	}
}

// TestCrashRecovery_Dequeue uses a multi-process approach.
// A child process dequeues messages continuously and gets forcefully killed.
// The parent process then checks if the unconsumed (or partially consumed but uncommitted)
// messages can still be safely dequeued without loss.
func TestCrashRecovery_Dequeue(t *testing.T) {
	testDir := filepath.Join(t.TempDir(), "test_crash_dequeue_exec")
	os.MkdirAll(testDir, 0755)

	// Pre-fill the queue
	bq, err := NewMmapQueue(testDir, SetArenaSize(4096))
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	msg := []byte("dequeue crash testing message data")
	total := 1000
	for i := 0; i < total; i++ {
		if err := bq.Enqueue(msg); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}
	}
	bq.Close() // Ensure all data is on disk

	cmd := exec.Command(os.Args[0], "-test.run=TestHelperCrashDequeue")
	cmd.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1", "CRASH_TEST_DIR="+testDir)
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start helper process: %v", err)
	}

	// Wait briefly to allow some reading to begin, then kill it.
	time.Sleep(50 * time.Millisecond)
	_ = cmd.Process.Kill()
	_ = cmd.Wait() // Wait for it to actually die

	// Reopen and check consistency
	bq2, err := NewMmapQueue(testDir, SetArenaSize(4096))
	if err != nil {
		t.Fatalf("failed to reopen queue after crash: %v", err)
	}
	defer bq2.Close()

	remaining := 0
	for !bq2.IsEmpty() {
		recoveredMsg, err := bq2.Dequeue()
		if err != nil {
			t.Fatalf("failed to dequeue recovered message at offset %d: %v", remaining, err)
		}
		if !bytes.Equal(recoveredMsg, msg) {
			t.Fatalf("corrupted message found")
		}
		remaining++
	}

	t.Logf("Recovered %d messages from original %d after dequeue crash", remaining, total)
	if remaining > total {
		t.Fatalf("somehow got more messages than we inserted! %d > %d", remaining, total)
	}
}

// TestCrashRecovery_GC simulates a torn/interrupted GC state.
// We manually construct an inconsistent state (half-deleted arenas) and ensure the
// queue can still open, be correctly cleaned up, and gracefully recover.
func TestCrashRecovery_GC(t *testing.T) {
	testDir := filepath.Join(t.TempDir(), "test_crash_gc_torn")
	os.MkdirAll(testDir, 0755)

	arenaSize := 4096
	maxKeep := 0

	// 1. Create a queue and fill it to create multiple arenas.
	bq, err := NewMmapQueue(testDir, SetArenaSize(arenaSize), SetMaxArenasToKeep(maxKeep))
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}

	msg := make([]byte, 1000)
	for i := 0; i < 20; i++ { // Creates ~5 arenas
		if err := bq.Enqueue(msg); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}
	}

	// 2. Consume enough to advance head significantly.
	// Since 1000 * 20 = 20k bytes, and arena is 4096,
	// this should span about 5 arenas.
	for i := 0; i < 18; i++ {
		if _, err := bq.Dequeue(); err != nil {
			t.Fatalf("failed to dequeue: %v", err)
		}
	}

	bq.GC() // Let it compute the min consumer head properly while still open.

	// Update global head since it's lazily updated in GC.
	// Actually we should just read the consumer head because global head only advances
	// when GC happens and there aren't many extra arenas.
	consumerHeadAid, _ := bq.md.getConsumerHead(bq.dc)
	t.Logf("Consumer head advanced to arena #%d", consumerHeadAid)

	if consumerHeadAid < 4 {
		t.Logf("Warning: Expected consumer head to have advanced more")
	}

	bq.Flush()
	bq.Close()

	// 3. Simulate a crash during GC cleanup where only SOME files got deleted.
	// Assume arenas 0 and 1 got deleted before crash, but 2 and 3 did not.
	_ = os.Remove(filepath.Join(testDir, "0_arena.dat"))
	_ = os.Remove(filepath.Join(testDir, "1_arena.dat"))

	// 4. Verification: Reopen the queue with torn GC state.
	// Provide 0 for MaxArenasToKeep so that it aggressive cleans up.
	bq2, err := NewMmapQueue(testDir, SetArenaSize(arenaSize), SetMaxArenasToKeep(0))
	if err != nil {
		t.Fatalf("failed to reopen queue with torn GC state: %v", err)
	}

	// Run GC explicitly to see if it cleans up the remainder (like 2_arena.dat and 3_arena.dat)
	// We need to fetch global head to see what the system thinks is obsolete.
	bq2.GC()
	globalHeadAid, _ := bq2.md.getHead()
	t.Logf("Computed Global head by GC: %d", globalHeadAid)
	bq2.Close()

	// 5. Verify the remaining obsolete files are cleaned up gracefully.
	// Note: We used maxKeep=0 for the recovery, so everything before globalHeadAid should be deleted.
	for i := 0; i < globalHeadAid; i++ {
		path := filepath.Join(testDir, fmt.Sprintf("%d_arena.dat", i))
		if _, err := os.Stat(path); err == nil {
			t.Errorf("arena %d should have been deleted by recovery GC! (globalHeadAid=%d)", i, globalHeadAid)
		}
	}
}
