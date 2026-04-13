package bigqueue

import (
	"math/rand"
	"os"
	"testing"
)

// TestBacklogBytesEmptyQueue verifies that a newly created queue has 0 backlog bytes.
func TestBacklogBytesEmptyQueue(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir)
	if err != nil {
		t.Fatalf("unable to create bigqueue: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error closing bigqueue: %v", err)
		}
	}()

	n, err := bq.BacklogBytes()
	if err != nil {
		t.Fatalf("BacklogBytes returned error: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 backlog bytes on empty queue, got %d", n)
	}
}

// TestBacklogBytesEnqueueDequeue verifies that BacklogBytes tracks bytes correctly
// across enqueue and dequeue operations on the default consumer.
func TestBacklogBytesEnqueueDequeue(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir)
	if err != nil {
		t.Fatalf("unable to create bigqueue: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error closing bigqueue: %v", err)
		}
	}()

	// Enqueue three messages with known payload sizes.
	messages := [][]byte{
		[]byte("hello"),      // 5 bytes payload → 8+5 = 13 bytes stored
		[]byte("world!!!!!"), // 10 bytes payload → 8+10 = 18 bytes stored
		[]byte("go"),         // 2 bytes payload → 8+2 = 10 bytes stored
	}

	var expectedBacklog int64
	for _, msg := range messages {
		if err := bq.Enqueue(msg); err != nil {
			t.Fatalf("Enqueue failed: %v", err)
		}
		expectedBacklog += int64(cInt64Size + len(msg))

		n, err := bq.BacklogBytes()
		if err != nil {
			t.Fatalf("BacklogBytes returned error: %v", err)
		}
		if n != expectedBacklog {
			t.Fatalf("after enqueue: expected %d backlog bytes, got %d", expectedBacklog, n)
		}
	}

	// Dequeue one by one; backlog should decrease after each dequeue.
	for _, msg := range messages {
		expectedBacklog -= int64(cInt64Size + len(msg))
		if _, err := bq.Dequeue(); err != nil {
			t.Fatalf("Dequeue failed: %v", err)
		}

		n, err := bq.BacklogBytes()
		if err != nil {
			t.Fatalf("BacklogBytes returned error: %v", err)
		}
		if n != expectedBacklog {
			t.Fatalf("after dequeue: expected %d backlog bytes, got %d", expectedBacklog, n)
		}
	}

	// Queue is empty; BacklogBytes must be 0.
	n, err := bq.BacklogBytes()
	if err != nil {
		t.Fatalf("BacklogBytes returned error: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 backlog bytes after all dequeues, got %d", n)
	}
}

// TestBacklogBytesNamedConsumer verifies that BacklogBytes works correctly for
// named consumers, and that a new consumer starts at the current head position.
func TestBacklogBytesNamedConsumer(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir)
	if err != nil {
		t.Fatalf("unable to create bigqueue: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error closing bigqueue: %v", err)
		}
	}()

	msg1 := []byte("first")
	msg2 := []byte("second")

	if err := bq.Enqueue(msg1); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	if err := bq.Enqueue(msg2); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// A new named consumer starts at the same head position as the default
	// consumer (the beginning), so it should see the full backlog.
	c, err := bq.NewConsumer("myConsumer")
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}

	totalBytes := int64(cInt64Size+len(msg1)) + int64(cInt64Size+len(msg2))
	n, err := c.BacklogBytes()
	if err != nil {
		t.Fatalf("Consumer.BacklogBytes returned error: %v", err)
	}
	if n != totalBytes {
		t.Fatalf("expected consumer backlog %d, got %d", totalBytes, n)
	}

	// Dequeue the first message via the named consumer.
	if _, err := c.Dequeue(); err != nil {
		t.Fatalf("Consumer.Dequeue failed: %v", err)
	}

	expectedAfterDequeue := int64(cInt64Size + len(msg2))
	n, err = c.BacklogBytes()
	if err != nil {
		t.Fatalf("Consumer.BacklogBytes returned error: %v", err)
	}
	if n != expectedAfterDequeue {
		t.Fatalf("expected consumer backlog %d after dequeue, got %d", expectedAfterDequeue, n)
	}

	// The default consumer should still see the full backlog (independent head).
	n, err = bq.BacklogBytes()
	if err != nil {
		t.Fatalf("BacklogBytes returned error: %v", err)
	}
	if n != totalBytes {
		t.Fatalf("expected default consumer backlog %d, got %d", totalBytes, n)
	}
}

// TestBacklogBytesFromConsumer verifies that FromConsumer copies offsets correctly.
func TestBacklogBytesFromConsumer(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir)
	if err != nil {
		t.Fatalf("unable to create bigqueue: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error closing bigqueue: %v", err)
		}
	}()

	msg1 := []byte("aaa")
	msg2 := []byte("bbbbb")

	if err := bq.Enqueue(msg1); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	if err := bq.Enqueue(msg2); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Consume the first message with the default consumer.
	if _, err := bq.Dequeue(); err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	// Create a new consumer copied from the default consumer's current position.
	dc := &Consumer{mq: bq, base: bq.dc}
	copied, err := bq.FromConsumer("copied", dc)
	if err != nil {
		t.Fatalf("FromConsumer failed: %v", err)
	}

	// The copied consumer should only see msg2 remaining.
	expected := int64(cInt64Size + len(msg2))
	n, err := copied.BacklogBytes()
	if err != nil {
		t.Fatalf("copied.BacklogBytes returned error: %v", err)
	}
	if n != expected {
		t.Fatalf("expected copied consumer backlog %d, got %d", expected, n)
	}
}

// TestBacklogBytesArenaBoundary verifies that BacklogBytes is correct when head
// and tail span multiple arenas (small arena size forces many arena crossings).
func TestBacklogBytesArenaBoundary(t *testing.T) {
	t.Parallel()

	arenaSize := os.Getpagesize()
	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir, SetArenaSize(arenaSize),
		SetPeriodicFlushOps(0), SetPeriodicFlushDuration(0))
	if err != nil {
		t.Fatalf("unable to create bigqueue: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error closing bigqueue: %v", err)
		}
	}()

	// Use a message size that is just under the arena size so that we cross
	// arena boundaries after a few enqueues.
	payload := make([]byte, arenaSize/2)
	const numMsgs = 20

	var expectedBacklog int64
	for i := range numMsgs {
		if err := bq.Enqueue(payload); err != nil {
			t.Fatalf("Enqueue %d failed: %v", i, err)
		}
		expectedBacklog += int64(cInt64Size + len(payload))

		n, err := bq.BacklogBytes()
		if err != nil {
			t.Fatalf("BacklogBytes after enqueue %d: %v", i, err)
		}
		if n != expectedBacklog {
			t.Fatalf("enqueue %d: expected backlog %d, got %d", i, expectedBacklog, n)
		}
	}

	for i := range numMsgs {
		expectedBacklog -= int64(cInt64Size + len(payload))
		if _, err := bq.Dequeue(); err != nil {
			t.Fatalf("Dequeue %d failed: %v", i, err)
		}

		n, err := bq.BacklogBytes()
		if err != nil {
			t.Fatalf("BacklogBytes after dequeue %d: %v", i, err)
		}
		if n != expectedBacklog {
			t.Fatalf("dequeue %d: expected backlog %d, got %d", i, expectedBacklog, n)
		}
	}
}

// TestBacklogBytesStress enqueues and dequeues a large number of messages with
// varying sizes under a small arena size and validates that BacklogBytes is always
// non-negative, monotonically increasing with enqueues, and decreasing with
// dequeues. When testing.Short() is set, the iteration count is reduced.
func TestBacklogBytesStress(t *testing.T) {
	t.Parallel()

	arenaSize := os.Getpagesize() * 4
	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir,
		SetArenaSize(arenaSize),
		SetPeriodicFlushOps(0),
		SetPeriodicFlushDuration(0),
	)
	if err != nil {
		t.Fatalf("unable to create bigqueue: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error closing bigqueue: %v", err)
		}
	}()

	totalMessages := 100_000
	if testing.Short() {
		totalMessages = 1_000
	}

	rng := rand.New(rand.NewSource(42))

	prevBacklog, err := bq.BacklogBytes()
	if err != nil {
		t.Fatalf("initial BacklogBytes: %v", err)
	}
	if prevBacklog != 0 {
		t.Fatalf("expected 0 initial backlog, got %d", prevBacklog)
	}

	for i := range totalMessages {
		// Vary payload size: small, medium, and near-arena-size messages.
		var payloadLen int
		switch i % 3 {
		case 0:
			payloadLen = rng.Intn(64) + 1 // 1–64 bytes
		case 1:
			payloadLen = rng.Intn(arenaSize/4) + 1 // 1–arenaSize/4 bytes
		case 2:
			payloadLen = arenaSize - cInt64Size - 1 // just under a full arena
		}

		payload := make([]byte, payloadLen)
		if err := bq.Enqueue(payload); err != nil {
			t.Fatalf("Enqueue %d failed: %v", i, err)
		}

		// Validate periodically: BacklogBytes must be non-negative and
		// must not decrease after an enqueue.
		if i%1000 == 0 || i == totalMessages-1 {
			n, err := bq.BacklogBytes()
			if err != nil {
				t.Fatalf("BacklogBytes at enqueue %d: %v", i, err)
			}
			if n < 0 {
				t.Fatalf("BacklogBytes returned negative value %d at enqueue %d", n, i)
			}
			if n < prevBacklog {
				t.Fatalf("enqueue %d: BacklogBytes decreased from %d to %d", i, prevBacklog, n)
			}
			prevBacklog = n
		}
	}

	// Dequeue all messages and verify backlog never increases and stays non-negative.
	for i := range totalMessages {
		beforeDequeue, err := bq.BacklogBytes()
		if err != nil {
			t.Fatalf("BacklogBytes before dequeue %d: %v", i, err)
		}

		if _, err := bq.Dequeue(); err != nil {
			t.Fatalf("Dequeue %d failed: %v", i, err)
		}

		afterDequeue, err := bq.BacklogBytes()
		if err != nil {
			t.Fatalf("BacklogBytes after dequeue %d: %v", i, err)
		}
		if afterDequeue > beforeDequeue {
			t.Fatalf("dequeue %d: BacklogBytes increased from %d to %d", i, beforeDequeue, afterDequeue)
		}
		if afterDequeue < 0 {
			t.Fatalf("dequeue %d: BacklogBytes is negative: %d", i, afterDequeue)
		}
	}

	// Queue should be empty with 0 backlog.
	n, err := bq.BacklogBytes()
	if err != nil {
		t.Fatalf("final BacklogBytes: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 backlog after full drain, got %d", n)
	}
}
