package bigqueue

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/grandecola/mmap"
	"github.com/jonboulle/clockwork"
)

func TestSetFlushIntervalMutateOpsValidation(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	_, err := NewMmapQueue(testDir, SetFlushIntervalMutateOps(0))
	if err != ErrMustBeGreaterThanZero {
		t.Fatalf("expected error ErrMustBeGreaterThanZero, got: %v", err)
	}
}

func TestSetFlushElapsedDurationValidation(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	_, err := NewMmapQueue(testDir, SetFlushElapsedDuration(-1))
	if err != ErrMustBeGreaterThanZero {
		t.Fatalf("expected error ErrMustBeGreaterThanZero, got: %v", err)
	}
}

func TestSetFlushIntervalMutateOps(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	arenaSize := os.Getpagesize()
	bq, err := NewMmapQueue(testDir, SetFlushIntervalMutateOps(4), SetArenaSize(arenaSize),
		SetMaxInMemArenas(3))
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer bq.Close()
	setupFlushCountFile(bq, t)

	// expectedIndex = true because of the putSize for a newly created queue
	checkDirtiness(bq, t, true, [3]bool{false, false, false})
	checkTimesCalled(bq, t, 0, [3]int{0, 0, 0})

	msg := bytes.Repeat([]byte("a"), arenaSize)
	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	checkDirtiness(bq, t, true, [3]bool{true, true, false})
	checkTimesCalled(bq, t, 0, [3]int{0, 0, 0})

	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	checkDirtiness(bq, t, true, [3]bool{true, true, true})
	checkTimesCalled(bq, t, 0, [3]int{0, 0, 0})

	if err := bq.Dequeue(); err != nil {
		t.Fatalf("dequeue failed :: %v", err)
	}

	checkDirtiness(bq, t, true, [3]bool{true, true, true})
	checkTimesCalled(bq, t, 0, [3]int{0, 0, 0})
	checkMutateOps(bq, t, 3)

	if err := bq.Dequeue(); err != nil {
		t.Fatalf("dequeue failed :: %v", err)
	}

	checkDirtiness(bq, t, false, [3]bool{false, false, false})
	checkTimesCalled(bq, t, 1, [3]int{1, 1, 1})
	checkMutateOps(bq, t, 0)
}

func TestSetFlushElapsedDuration(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	flushElapsedDuration := time.Second
	arenaSize := os.Getpagesize()
	clock := clockwork.NewFakeClock()

	bq, err := NewMmapQueue(testDir, SetFlushElapsedDuration(flushElapsedDuration),
		SetArenaSize(arenaSize), SetFlushIntervalMutateOps(math.MaxInt64),
		SetMaxInMemArenas(3), setClock(clock))
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer bq.Close()

	setupFlushCountFile(bq, t)

	checkDirtiness(bq, t, true, [3]bool{false, false, false})
	checkTimesCalled(bq, t, 0, [3]int{0, 0, 0})

	msg := []byte("a")
	for i := 0; i < arenaSize/cInt64Size; i++ { // a lot of writes, but still no flush
		if err := bq.Enqueue(msg); err != nil {
			t.Fatalf("enqueue failed :: %v", err)
		}
	}

	checkDirtiness(bq, t, true, [3]bool{true, true, false})
	checkTimesCalled(bq, t, 0, [3]int{0, 0, 0})

	clock.Advance(flushElapsedDuration)

	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	checkDirtiness(bq, t, false, [3]bool{false, false, false})
	checkTimesCalled(bq, t, 1, [3]int{1, 1, 0})

	clock.Advance(time.Second / 2)

	for i := 0; i < arenaSize/cInt64Size; i++ {
		if err := bq.Enqueue(msg); err != nil {
			t.Fatalf("enqueue failed :: %v", err)
		}
	}

	checkDirtiness(bq, t, true, [3]bool{false, true, true})
	checkTimesCalled(bq, t, 1, [3]int{1, 1, 0})

	clock.Advance(time.Second)

	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	checkDirtiness(bq, t, false, [3]bool{false, false, false})
	checkTimesCalled(bq, t, 2, [3]int{1, 2, 1})
}

func TestResetOfBothFlushIntervalStates(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	flushElapsedDuration := time.Second
	clock := clockwork.NewFakeClock()

	bq, err := NewMmapQueue(testDir, SetFlushElapsedDuration(flushElapsedDuration),
		SetFlushIntervalMutateOps(2),
		setClock(clock))

	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer bq.Close()

	msg := []byte("a")
	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	checkMutateOps(bq, t, 1)
	clock.Advance(flushElapsedDuration) // time interval reached, flush happens, resets both prevFlushTime, mutateOps

	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	checkMutateOps(bq, t, 0)
	prevFlushTime := (bq.(*MmapQueue)).prevFlushTime
	if prevFlushTime != clock.Now() {
		t.Fatalf("expected prevFlushTime: %v to be reset to clock's now: %v", prevFlushTime, clock.Now())
	}

	clock.Advance(flushElapsedDuration / 2) // time moves ahead

	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}
	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	// flush after 2 mutateOps, causes mutateOps reset and time reset to Now()

	checkMutateOps(bq, t, 0)
	prevFlushTime = (bq.(*MmapQueue)).prevFlushTime
	if prevFlushTime != clock.Now() {
		t.Fatalf("expected prevFlushTime: %v to be reset to clock's now: %v", prevFlushTime, clock.Now())
	}

}

type flushCountingFile struct {
	mmap.File
	timesCalled int
}

func (m *flushCountingFile) Flush(flags int) error {
	m.timesCalled++
	return m.File.Flush(flags)
}

func setupFlushCountFile(q Queue, t *testing.T) {
	bq := q.(*MmapQueue)
	for i := 0; i < bq.conf.maxInMemArenas; i++ {
		arena, err := bq.am.getArena(i)
		if err != nil {
			t.Fatalf("test setup failed, could not load arena %d because: %v", i, err)
		}
		arena.File = &flushCountingFile{arena.File, 0}
	}
	bq.index.indexArena.File = &flushCountingFile{bq.index.indexArena.File, 0}
}

func checkMutateOps(q Queue, t *testing.T, expected int64) {
	bq := q.(*MmapQueue)
	if bq.mutateOpsSinceFlush != expected {
		t.Fatalf("expected mutateOpsSinceFlush %d, got %d", expected, bq.mutateOpsSinceFlush)
	}
}

// expects first three arenas to be in memory
func checkTimesCalled(q Queue, t *testing.T, indexTimesCalled int, timesCalled [3]int) {
	bq := q.(*MmapQueue)
	for i, expected := range timesCalled {
		arena := bq.am.arenaList[i]
		switch m := arena.File.(type) {
		case *flushCountingFile:
			if m.timesCalled != expected {
				t.Fatalf("arena %d mmap flushed %d times, expected %d", i, m.timesCalled, 1)
			}
		default:
			t.Fatalf("expected arena %d mmap to be of type *flushCountingFile, but is %T", i, m)
		}
	}

	switch m := bq.index.indexArena.File.(type) {
	case *flushCountingFile:
		if m.timesCalled != indexTimesCalled {
			t.Fatalf("index arena mmap flushed %d times, expected %d", m.timesCalled, 1)
		}
	default:
		t.Fatalf("index arena mmap expected to be of type *flushCountingFile, but is %T", m)
	}
}

// expects first three arenas to be in memory
func checkDirtiness(q Queue, t *testing.T, expectedIndex bool, expectedArenas [3]bool) {
	bq := q.(*MmapQueue)
	if bq.index.indexArena.dirty != expectedIndex {
		t.Fatalf("dirty flag for index expected %v, got %v", expectedIndex, bq.index.indexArena.dirty)
	}

	for i, expected := range expectedArenas {
		arena := bq.am.arenaList[i]
		if arena == nil {
			t.Fatalf("aid %d is nil, expected it to be in memory", i)
		}
		if arena.dirty != expected {
			t.Fatalf("dirty flag for arena %d expected %v, got %v", i, expected, arena.dirty)
		}
	}
}
