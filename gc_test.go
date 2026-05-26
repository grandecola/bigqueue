package bigqueue

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestArenaGC(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "test_bigqueue_gc")
	os.RemoveAll(testDir)
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("failed to create test dir: %v", err)
	}
	defer os.RemoveAll(testDir)

	arenaSize := 4096
	maxKeep := 1

	bq, err := NewMmapQueue(testDir, SetArenaSize(arenaSize), SetMaxArenasToKeep(maxKeep))
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}

	// 1. Fill several arenas
	msg := make([]byte, 1024)
	for i := 0; i < 10; i++ {
		if err := bq.Enqueue(msg); err != nil {
			t.Fatalf("failed to enqueue: %v", i)
		}
	}

	tailAid, _ := bq.md.getTail()
	if tailAid < 2 {
		t.Fatalf("tailAid too small: %v", tailAid)
	}

	// 2. Consume to advance head
	consumer, _ := bq.NewConsumer("__default__")
	for i := 0; i < 8; i++ {
		_, err := consumer.Dequeue()
		if err != nil {
			t.Fatalf("failed to dequeue: %v", err)
		}
	}

	if err := bq.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	headAid, _ := bq.md.getConsumerHead(bq.dc)
	bq.Close()

	// 3. Reopen to trigger GC
	bq, err = NewMmapQueue(testDir, SetArenaSize(arenaSize), SetMaxArenasToKeep(maxKeep))
	if err != nil {
		t.Fatalf("failed to reopen queue: %v", err)
	}
	bq.Close()

	limitAid := headAid - maxKeep

	// Arenas < limitAid should be deleted.
	for i := 0; i < limitAid; i++ {
		path := filepath.Join(testDir, fmt.Sprintf("%d_arena.dat", i))
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("arena %d should have been deleted (headAid=%d, limitAid=%d)", i, headAid, limitAid)
		}
	}

	// Arenas >= limitAid should still exist.
	for i := limitAid; i <= tailAid; i++ {
		path := filepath.Join(testDir, fmt.Sprintf("%d_arena.dat", i))
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("arena %d should exist", i)
		}
	}
}

func TestArenaGC_MultipleArenasAndGC(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "test_bigqueue_gc_multi_arenas")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	arenaSize := 4096 // OS Page size (minimum allowed)
	maxKeep := 1      // Keep only 1 arena before the current head

	bq, err := NewMmapQueue(testDir, SetArenaSize(arenaSize), SetMaxArenasToKeep(maxKeep))
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}

	// 1. Enqueue enough messages to create multiple arena files.
	// 4096 byte arena. Message size will be half-ish to ensure split.
	msg := make([]byte, 2000)
	// 2000 + 8 = 2008 bytes. 2 messages per arena.
	for i := 0; i < 20; i++ {
		if err := bq.Enqueue(msg); err != nil {
			t.Fatalf("failed to enqueue msg %d: %v", i, err)
		}
	}

	// Verify we have multiple arenas (approx 10 arenas)
	tailAid, _ := bq.md.getTail()
	if tailAid < 9 {
		t.Fatalf("expected tailAid to be at least 9, got %d", tailAid)
	}

	// 2. Consume some messages to move the head of the default consumer.
	// Move it to arena 6.
	c, _ := bq.NewConsumer("__default__")
	for i := 0; i < 14; i++ { // 14 msgs = 7 arenas approx
		if _, err := c.Dequeue(); err != nil {
			t.Fatalf("failed to dequeue msg %d: %v", i, err)
		}
	}
	bq.Flush()

	headAid, _ := bq.md.getConsumerHead(c.base)
	if headAid < 6 {
		t.Fatalf("expected headAid to be at least 6, got %d", headAid)
	}

	// 3. Trigger GC.
	// Since maxKeep is 1, it should keep arena 'headAid' and 'headAid-1'.
	bq.GC()

	// 4. Verify deletions
	limitAid := headAid - maxKeep
	for i := 0; i < limitAid; i++ {
		path := filepath.Join(testDir, fmt.Sprintf("%d_arena.dat", i))
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("arena %d should have been deleted", i)
		}
	}

	// 5. Verify survivors
	for i := limitAid; i <= int(tailAid); i++ {
		path := filepath.Join(testDir, fmt.Sprintf("%d_arena.dat", i))
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("arena %d should still exist", i)
		}
	}

	// 6. Test: New consumer created AFTER GC.
	// It should NOT be able to see or read deleted data (arenas 0-4).
	// Its head AID should be global head (which was updated to 6).
	cNew, err := bq.NewConsumer("after_gc_consumer")
	if err != nil {
		t.Fatalf("failed to create consumer after GC: %v", err)
	}

	newAid, _ := bq.md.getConsumerHead(cNew.base)
	if newAid != headAid {
		t.Errorf("new consumer should start at global head %d, but got %d", headAid, newAid)
	}

	// 7. Test: Existing consumer (the default one) after GC.
	// We want to see if bq.am.baseAid not being updated causes issues.
	// c is already at arena 6. Let's try to dequeue more.
	// Enqueue one more message to be sure there's something to read.
	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("failed to enqueue extra msg: %v", err)
	}

	data, err := c.Dequeue()
	if err != nil {
		t.Fatalf("existing consumer failed to dequeue after GC: %v", err)
	}
	if len(data) != 2000 {
		t.Errorf("expected 2000 bytes, got %d", len(data))
	}

	// 8. Test: New consumer dequeue.
	data, err = cNew.Dequeue()
	if err != nil {
		t.Fatalf("new consumer failed to dequeue: %v", err)
	}
	if len(data) != 2000 {
		t.Errorf("expected 2000 bytes, got %d", len(data))
	}

	bq.Close()
}

func TestArenaGC_MultipleConsumers(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "test_bigqueue_gc_multi")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	arenaSize := 4096
	maxKeep := 1

	bq, err := NewMmapQueue(testDir, SetArenaSize(arenaSize), SetMaxArenasToKeep(maxKeep))
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}

	msg := make([]byte, 1024)
	for i := 0; i < 20; i++ {
		bq.Enqueue(msg)
	}

	c1, _ := bq.NewConsumer("c1")
	c2, _ := bq.NewConsumer("c2")

	for i := 0; i < 10; i++ {
		c1.Dequeue()
	}
	for i := 0; i < 4; i++ {
		c2.Dequeue()
	}

	bq.Flush()

	head1, _ := bq.md.getConsumerHead(c1.base)
	head2, _ := bq.md.getConsumerHead(c2.base)

	minHead := head1
	if head2 < minHead {
		minHead = head2
	}

	// Trigger GC via new arena
	for i := 0; i < 10; i++ {
		bq.Enqueue(msg)
	}

	limitAid := minHead - maxKeep
	for i := 0; i < limitAid; i++ {
		path := filepath.Join(testDir, fmt.Sprintf("%d_arena.dat", i))
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("arena %d should have been deleted (head1=%d, head2=%d)", i, head1, head2)
		}
	}

	path := filepath.Join(testDir, fmt.Sprintf("%d_arena.dat", limitAid))
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Errorf("arena %d should exist", limitAid)
	}

	bq.Close()
}
func TestArenaGC_NewConsumerAfterGC(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "test_bigqueue_gc_new_consumer")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	arenaSize := 4096
	maxKeep := 1 // Keep 1 arena before minHead

	bq, err := NewMmapQueue(testDir, SetArenaSize(arenaSize), SetMaxArenasToKeep(maxKeep))
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}

	// 1. Fill 5 arenas
	msg := make([]byte, 1024)
	for i := 0; i < 15; i++ {
		bq.Enqueue(msg)
	}

	// 2. Advance default consumer to arena 2 or more
	c, _ := bq.NewConsumer("__default__")
	// 4096 / (1024 + 8) = 3.96 -> 3 messages per arena.
	// To reach arena 3, we need 3 * 3 = 9 messages.
	// But let's just check what we get.
	for i := 0; i < 12; i++ {
		c.Dequeue()
	}
	bq.Flush()

	headAid, headOff := bq.md.getConsumerHead(c.base)
	if headAid < 2 {
		t.Fatalf("headAid should be at least 2, got %d", headAid)
	}

	// 3. Trigger GC
	bq.GC()

	// 4. Create new consumer.
	cNew, err := bq.NewConsumer("newbie")
	if err != nil {
		t.Fatalf("failed to create newbie: %v", err)
	}
	newAid, newOff := bq.md.getConsumerHead(cNew.base)
	if newAid != headAid || newOff != headOff {
		t.Errorf("new consumer should start at global head (%d, %d), but got (%d, %d)", headAid, headOff, newAid, newOff)
	}

	bq.Close()
}

func TestArenaGC_OffsetPrecision(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "test_bigqueue_gc_offset")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	arenaSize := 4096
	bq, err := NewMmapQueue(testDir, SetArenaSize(arenaSize), SetMaxArenasToKeep(1))
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}

	msg := make([]byte, 100)
	for i := 0; i < 5; i++ {
		bq.Enqueue(msg)
	}

	c1, _ := bq.NewConsumer("c1")
	c2, _ := bq.NewConsumer("c2")

	// c1 and c2 consume, but we MUST also advance the default consumer
	// because GC considers all consumers.
	dc, _ := bq.NewConsumer("__default__")
	dc.Dequeue() // moves it to same as c1

	// c1 consumes 1 msg
	c1.Dequeue()
	// c2 consumes 2 msgs
	c2.Dequeue()
	c2.Dequeue()

	// Both are in arena 0
	aid1, off1 := bq.md.getConsumerHead(c1.base)
	aid2, off2 := bq.md.getConsumerHead(c2.base)

	if aid1 != aid2 {
		t.Fatalf("both consumers should be in same arena for this test, got %d and %d", aid1, aid2)
	}
	if off2 <= off1 {
		t.Fatalf("c2 offset should be greater than c1, got %d and %d", off2, off1)
	}

	// Trigger GC
	bq.GC()

	// Global head should be equal to c1 (the laggard)
	globalAid, globalOff := bq.md.getHead()
	if globalAid != aid1 || globalOff != off1 {
		t.Errorf("global head should match c1 (min offset), expected (%d,%d), got (%d,%d)", aid1, off1, globalAid, globalOff)
	}

	// Now create a new consumer and ensure it picks up global head
	c3, _ := bq.NewConsumer("c3")
	aid3, off3 := bq.md.getConsumerHead(c3.base)
	if aid3 != aid1 || off3 != off1 {
		t.Errorf("new consumer should match min office, expected (%d,%d), got (%d,%d)", aid1, off1, aid3, off3)
	}

	bq.Close()
}
