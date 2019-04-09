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

func TestSetPeriodicFlushOpsFail(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%v", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	_, err := NewMmapQueue(testDir, SetPeriodicFlushOps(0))
	if err != ErrMustBeGreaterThanZero {
		t.Fatalf("expected error ErrMustBeGreaterThanZero, got: %v", err)
	}
}

func TestSetPeriodicFlushDurationFail(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%v", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	_, err := NewMmapQueue(testDir, SetPeriodicFlushDuration(0))
	if err != ErrMustBeGreaterThanZero {
		t.Fatalf("expected error ErrMustBeGreaterThanZero, got: %v", err)
	}
}

func TestPeriodicFlush(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%v", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, err := NewMmapQueue(testDir, SetPeriodicFlushDuration(4))
	if err != nil {
		t.Fatalf("error in NewMmapQueue :: %v", err)
	}

	for i := 0; i < 4; i++ {
		if err := bq.EnqueueString("string"); err != nil {
			t.Fatalf("error in EnqueueString :: %v", err)
		}
	}

	time.Sleep(time.Millisecond * 100)
	checkMutOps(t, bq, 0)
}

func TestSetPeriodicFlushOps(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%v", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	arenaSize := os.Getpagesize()
	bq, err := NewMmapQueue(testDir, SetPeriodicFlushOps(4),
		SetArenaSize(arenaSize), SetMaxInMemArenas(3))
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer bq.Close()

	setupFlushCountFile(bq, t)
	checkDirtiness(t, bq, 1, [3]int64{0, 0, 0})
	checkTimesCalled(t, bq, 0, [3]int{0, 0, 0})

	msg := bytes.Repeat([]byte("a"), arenaSize)
	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}
	checkDirtiness(t, bq, 1, [3]int64{1, 1, 0})
	checkTimesCalled(t, bq, 0, [3]int{0, 0, 0})

	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}
	checkDirtiness(t, bq, 1, [3]int64{1, 1, 1})
	checkTimesCalled(t, bq, 0, [3]int{0, 0, 0})

	if err := bq.Dequeue(); err != nil {
		t.Fatalf("dequeue failed :: %v", err)
	}
	checkDirtiness(t, bq, 1, [3]int64{1, 1, 1})
	checkTimesCalled(t, bq, 0, [3]int{0, 0, 0})
	checkMutOps(t, bq, 3)

	if err := bq.Dequeue(); err != nil {
		t.Fatalf("dequeue failed :: %v", err)
	}
	checkDirtiness(t, bq, 0, [3]int64{0, 0, 0})
	checkTimesCalled(t, bq, 1, [3]int{1, 1, 1})
	checkMutOps(t, bq, 0)
}

func TestSetPeriodicFlushDuration(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%v", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	flushPeriod := time.Second
	arenaSize := os.Getpagesize()
	clock := clockwork.NewFakeClock()

	bq, err := NewMmapQueue(testDir, SetPeriodicFlushDuration(flushPeriod),
		SetArenaSize(arenaSize), SetPeriodicFlushOps(math.MaxInt64), SetMaxInMemArenas(3))
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer bq.Close()

	setupFlushCountFile(bq, t)
	checkDirtiness(t, bq, 1, [3]int64{0, 0, 0})
	checkTimesCalled(t, bq, 0, [3]int{0, 0, 0})

	msg := []byte("a")
	for i := 0; i < arenaSize/cInt64Size; i++ { // a lot of writes, but still no flush
		if err := bq.Enqueue(msg); err != nil {
			t.Fatalf("enqueue failed :: %v", err)
		}
	}
	checkDirtiness(t, bq, 1, [3]int64{1, 1, 0})
	checkTimesCalled(t, bq, 0, [3]int{0, 0, 0})

	// advance clock by the flush period
	clock.Advance(flushPeriod)

	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}
	checkDirtiness(t, bq, 0, [3]int64{0, 0, 0})
	checkTimesCalled(t, bq, 1, [3]int{1, 1, 0})

	// advance clock by half of the flush period
	clock.Advance(flushPeriod / 2)
	for i := 0; i < arenaSize/cInt64Size; i++ {
		if err := bq.Enqueue(msg); err != nil {
			t.Fatalf("enqueue failed :: %v", err)
		}
	}
	checkDirtiness(t, bq, 1, [3]int64{0, 1, 1})
	checkTimesCalled(t, bq, 1, [3]int{1, 1, 0})

	// advance clock by flush period
	clock.Advance(flushPeriod)

	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}
	checkDirtiness(t, bq, 0, [3]int64{0, 0, 0})
	checkTimesCalled(t, bq, 2, [3]int{1, 2, 1})
}

func TestResetFlushStates(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%v", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	flushPeriod := time.Second
	clock := clockwork.NewFakeClock()

	bq, err := NewMmapQueue(testDir, SetPeriodicFlushDuration(flushPeriod),
		SetPeriodicFlushOps(2))
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer bq.Close()

	msg := []byte("a")
	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}
	checkMutOps(t, bq, 1)

	// time interval reached, flush happens, resets both lastFlush, mutOps
	clock.Advance(flushPeriod)

	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}
	checkMutOps(t, bq, 0)

	// time moves ahead by half of flush period
	clock.Advance(flushPeriod / 2)

	// flush after 2 mutOps, causes mutOps reset and time reset to Now()
	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}
	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}
	checkMutOps(t, bq, 0)

	// move time ahead and mutate some ops
	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}
	checkMutOps(t, bq, 1)

	bq.Flush()
	checkMutOps(t, bq, 0)
}

func checkMutOps(t *testing.T, q Queue, expected int64) {
	bq := q.(*MmapQueue)
	if bq.mutOps.load() != expected {
		t.Fatalf("expected mutOps %v, got %v", expected, bq.mutOps)
	}
}

// expects first three arenas to be in memory
func checkDirtiness(t *testing.T, q Queue, expectedIndex int64, expectedArenas [3]int64) {
	bq := q.(*MmapQueue)
	isDirty := bq.index.indexArena.dirty.load()
	if isDirty != expectedIndex {
		t.Fatalf("dirty flag for index expected %v, got %v", expectedIndex, isDirty)
	}

	for i, expected := range expectedArenas {
		arena, err := bq.am.getArena(i)
		if err != nil {
			t.Fatalf("test setup failed, could not load arena %v because: %v", i, err)
		}
		if arena == nil {
			t.Fatalf("aid %v is nil, expected it to be in memory", i)
		}

		if arena.dirty.load() != expected {
			t.Fatalf("dirty flag for arena %v expected %v, got %v", i, expected, arena.dirty.load())
		}
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

	bq.hLock.Lock()
	bq.tLock.Lock()
	bq.index.indexArena.File = &flushCountingFile{bq.index.indexArena.File, 0}
	bq.hLock.Unlock()
	bq.tLock.Unlock()

	for i := 0; i < bq.conf.maxInMemArenas; i++ {
		arena, err := bq.am.getArena(i)
		if err != nil {
			t.Fatalf("test setup failed, could not load arena %v because: %v", i, err)
		}

		bq.am.Lock()
		arena.File = &flushCountingFile{arena.File, 0}
		bq.am.Unlock()
	}
}

// assumes first three arenas to be in memory
func checkTimesCalled(t *testing.T, q Queue, indexTimesCalled int, timesCalled [3]int) {
	bq := q.(*MmapQueue)
	for i, expected := range timesCalled {
		arena, err := bq.am.getArena(i)
		if err != nil {
			t.Fatalf("test setup failed, could not load arena %v because: %v", i, err)
		}

		switch m := arena.File.(type) {
		case *flushCountingFile:
			if m.timesCalled != expected {
				t.Fatalf("arena %v mmap flushed %v times, expected %v", i, m.timesCalled, 1)
			}
		default:
			t.Fatalf("expected arena %v mmap to be of type *flushCountingFile, but is %T", i, m)
		}
	}

	switch m := bq.index.indexArena.File.(type) {
	case *flushCountingFile:
		if m.timesCalled != indexTimesCalled {
			t.Fatalf("index arena mmap flushed %v times, expected %v", m.timesCalled, 1)
		}
	default:
		t.Fatalf("index arena mmap expected to be of type *flushCountingFile, but is %T", m)
	}
}
