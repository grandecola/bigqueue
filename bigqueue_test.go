package bigqueue

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func checkInMemArenaInvariant(t *testing.T, bq *MmapQueue) {
	t.Helper()
	if bq.am.inMem > bq.conf.maxInMemArenas {
		t.Fatalf("# of in memory arenas should not be more than %v, actual: %v",
			bq.conf.maxInMemArenas, len(bq.am.arenas))
	}
}

func TestIsEmpty(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir + "/")
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	if !bq.IsEmpty() {
		t.Fatalf("BigQueue length should be 0")
	}

	msg := []byte("abcdefgh")
	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("unable to enqueue message :: %v", err)
	}

	if bq.IsEmpty() {
		t.Fatalf("IsEmpty should return false after enqueue")
	}

	if _, err := bq.Dequeue(); err != nil {
		t.Fatalf("unable to dequeue message :: %v", err)
	}

	if !bq.IsEmpty() {
		t.Fatalf("BigQueue length should be 0")
	}
}

func TestDequeue(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir)
	if err != nil {
		t.Fatalf("unable to get BigQueue :: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	if msg, err := bq.Dequeue(); err != ErrEmptyQueue || msg != nil {
		t.Fatalf("Dequeue should return empty queue error, returned: %v", err)
	}

	msg := []byte("abcdefghij")
	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	if headMsg, err := bq.Dequeue(); err != nil {
		t.Fatalf("Dequeue failed :: %v", err)
	} else if !bytes.Equal(msg, headMsg) {
		t.Fatalf("messages don't match :: expected %s, actual: %s", string(msg), string(headMsg))
	}
}

func TestEnqueueSmallMessage(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir)
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	msg := []byte("abcdefghij")
	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	if bq.IsEmpty() {
		t.Fatalf("BigQueue should not be empty")
	}

	if poppedMsg, err := bq.Dequeue(); err != nil {
		t.Fatalf("unable to dequeue :: %v", err)
	} else if !bytes.Equal(msg, poppedMsg) {
		t.Fatalf("unequal messages, eq: %s, dq: %s", string(msg), string(poppedMsg))
	}
}

func TestEnqueueLargeMessage(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir)
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	msg := make([]byte, 0)
	for range cDefaultArenaSize - 8 {
		m := []byte("a")
		msg = append(msg, m...)
	}
	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	if deQueuedMsg, err := bq.Dequeue(); err != nil {
		t.Fatalf("Dequeue failed :: %v", err)
	} else if !bytes.Equal(deQueuedMsg, msg) {
		t.Fatalf("dequeued and enqueued messages are not equal")
	}

	if !bq.IsEmpty() {
		t.Fatalf("IsEmpty should return true")
	}
}

func TestEnqueueOverlapLength(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir)
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	msg1 := make([]byte, 0)
	for range cDefaultArenaSize - 12 {
		m := []byte("a")
		msg1 = append(msg1, m...)
	}
	if err := bq.Enqueue(msg1); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	msg2 := make([]byte, 0)
	for range cDefaultArenaSize - 4 {
		m := []byte("a")
		msg2 = append(msg2, m...)
	}
	if err := bq.Enqueue(msg2); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	if dequeueMsg, err := bq.Dequeue(); err != nil {
		t.Fatalf("Dequeue failed :: %v", err)
	} else if !bytes.Equal(dequeueMsg, msg1) {
		t.Fatalf("dequeued and enqeued messages are not equal")
	}

	if dequeueMsg, err := bq.Dequeue(); err != nil {
		t.Fatalf("Dequeue failed :: %v", err)
	} else if !bytes.Equal(dequeueMsg, msg2) {
		t.Fatalf("dequeued and enqeued messages are not equal")
	}

	if !bq.IsEmpty() {
		t.Fatalf("queue should be empty")
	}
}

func TestEnqueueLargeNumberOfMessages(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir)
	if err != nil {
		t.Fatalf("unable to get BigQueue: %s", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	numMessages := 10
	lengths := make([]int, 0)
	alphabets := "abcdefghijklmnopqrstuvwxyz"
	for range numMessages {
		msgLen := rand.Intn(cDefaultArenaSize) + cDefaultArenaSize
		lengths = append(lengths, msgLen)
		msg := make([]byte, 0)
		for {
			if msgLen > len(alphabets) {
				msg = append(msg, []byte(alphabets)...)
				msgLen -= len(alphabets)
			} else {
				msg = append(msg, []byte(alphabets[0:msgLen])...)
				break
			}
		}
		if err := bq.Enqueue(msg); err != nil {
			t.Fatalf("unable to enqueue message :: %v", err)
		}
	}

	for i := range numMessages {
		if msg, err := bq.Dequeue(); err != nil {
			t.Fatalf("unable to dequeue message :: %v", err)
		} else if len(msg) != lengths[i] {
			t.Fatalf("enqueued and dequeued lengths don't match for msg no %d", i)
		}
	}

	if !bq.IsEmpty() {
		t.Fatalf("queue should be empty")
	}
}

func TestEnqueueZeroLengthMessage(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir)
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	emptyMsg := make([]byte, 0)
	if err := bq.Enqueue(emptyMsg); err != nil {
		t.Fatalf("unable to enqueue empty message :: %v", err)
	}

	if bq.IsEmpty() {
		t.Fatalf("IsEmpty should return false if empty message is present in queue")
	}

	if deQueuedMsg, err := bq.Dequeue(); err != nil {
		t.Fatalf("unable to dequeue empty message")
	} else if !bytes.Equal(deQueuedMsg, emptyMsg) {
		t.Fatalf("dequeued and enqueued messages are not equal")
	}

	if !bq.IsEmpty() {
		t.Fatalf("queue should be empty now")
	}
}

func TestEnqueueWhenMessageLengthFits(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	arenaSize := 4 * 1024
	bq, err := NewMmapQueue(testDir, SetArenaSize(arenaSize))
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	msg1 := bytes.Repeat([]byte("a"), arenaSize-16)
	if err := bq.Enqueue(msg1); err != nil {
		t.Fatalf("unable to enqueue msg1: %s", err)
	}

	msg2 := bytes.Repeat([]byte("b"), 3*arenaSize)
	if err := bq.Enqueue(msg2); err != nil {
		t.Fatalf("unable to enqueue msg2: %s", err)
	}

	if _, err := bq.Dequeue(); err != nil {
		t.Fatalf("unable to dequeue msg1: %s", err)
	}
	if _, err := bq.Dequeue(); err != nil {
		t.Fatalf("unable to dequeue msg2: %s", err)
	}
}

func TestReadWriteCornerCases(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	arenaSize := 8 * 1024
	bq, err := NewMmapQueue(testDir, SetArenaSize(arenaSize))
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}

	for i := 1; i < 10; i++ {
		msgLength := i*arenaSize/2 - 8
		if i == 5 {
			msgLength -= 8
		}
		msg := bytes.Repeat([]byte("a"), msgLength)
		if err := bq.Enqueue(msg); err != nil {
			t.Fatalf("enqueue failed :: %v", err)
		}

		if bq.IsEmpty() {
			t.Fatalf("BigQueue should not be empty")
		}

		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
		if bqTemp, err := NewMmapQueue(testDir, SetArenaSize(arenaSize)); err != nil {
			t.Fatalf("unable to get BigQueue: %v", err)
		} else {
			bq = bqTemp
		}

		if poppedMsg, err := bq.Dequeue(); err != nil {
			t.Fatalf("unable to dequeue :: %v", err)
		} else if !bytes.Equal(msg, poppedMsg) {
			t.Fatalf("unequal messages, eq: %s, dq: %s", string(msg), string(poppedMsg))
		}

		if !bq.IsEmpty() {
			t.Fatalf("BigQueue should be empty")
		}
	}

	if err := bq.Close(); err != nil {
		t.Fatalf("error in closing bigqueue :: %v", err)
	}
}

func TestArenaSize(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir, SetArenaSize(8*1024))
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	msg := []byte("abcdefghij")
	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	if bq.IsEmpty() {
		t.Fatalf("BigQueue should not be empty")
	}

	if poppedMsg, err := bq.Dequeue(); err != nil {
		t.Fatalf("unable to dequeue :: %v", err)
	} else if !bytes.Equal(msg, poppedMsg) {
		t.Fatalf("unequal length, eq: %s, dq: %s", string(msg), string(poppedMsg))
	}
}

func TestArenaSize2(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	arenaSize := os.Getpagesize() * 2
	bq, err := NewMmapQueue(testDir, SetArenaSize(arenaSize))
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	msg := []byte("abcdefghij")
	for range arenaSize / len(msg) * 4 {
		if err := bq.Enqueue(msg); err != nil {
			t.Fatalf("enqueue failed :: %v", err)
		}
	}

	if bq.IsEmpty() {
		t.Fatalf("BigQueue should not be empty")
	}

	for range arenaSize / len(msg) * 4 {
		if poppedMsg, err := bq.Dequeue(); err != nil {
			t.Fatalf("unable to dequeue :: %v", err)
		} else if !bytes.Equal(msg, poppedMsg) {
			t.Fatalf("unequal messages, eq: %s, dq: %s", string(msg), string(poppedMsg))
		}
	}
}

func TestArenaSize3(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	arenaSize := os.Getpagesize()
	bq, err := NewMmapQueue(testDir, SetArenaSize(arenaSize))
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	msg := []byte("abcdefgh")
	for range arenaSize / len(msg) * 4 {
		if err := bq.Enqueue(msg); err != nil {
			t.Fatalf("enqueue failed :: %v", err)
		}
	}

	if bq.IsEmpty() {
		t.Fatalf("BigQueue should not be empty")
	}

	for range arenaSize / len(msg) * 4 {
		if poppedMsg, err := bq.Dequeue(); err != nil {
			t.Fatalf("unable to dequeue :: %v", err)
		} else if !bytes.Equal(msg, poppedMsg) {
			t.Fatalf("unequal length, eq: %s, dq: %s", string(msg), string(poppedMsg))
		}
	}
}

func TestArenaSizeFail(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	_, err := NewMmapQueue(testDir, SetArenaSize(os.Getpagesize()/2))
	if err != ErrTooSmallArenaSize {
		t.Fatalf("expected error: %v, got: %v", ErrTooSmallArenaSize, err)
	}
}

func TestArenaSizeFail2(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir, SetArenaSize(8*1024))
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}

	msg := []byte("abcdefghij")
	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	if err := bq.Close(); err != nil {
		t.Fatalf("error in closing bigqueue :: %v", err)
	}
	if _, err := NewMmapQueue(testDir, SetArenaSize(6*1024)); err != ErrInvalidArenaSize {
		t.Fatalf("expected invalid arena size error, got: %v", err)
	}
}

func TestArenaSizeNotMultiple(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir, SetArenaSize(5732))
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}

	msg := []byte("abcdefghij")
	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	if err := bq.Close(); err != nil {
		t.Fatalf("error in closing bigqueue :: %v", err)
	}
	if tempBq, err := NewMmapQueue(testDir, SetArenaSize(5732)); err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	} else {
		bq = tempBq
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	if poppedMsg, err := bq.Dequeue(); err != nil {
		t.Fatalf("unable to dequeue :: %v", err)
	} else if !bytes.Equal(msg, poppedMsg) {
		t.Fatalf("unequal messages, eq: %s, dq: %s", string(msg), string(poppedMsg))
	}
}

func TestNewBigqueueNoFolder(t *testing.T) {
	t.Parallel()

	bq, err := NewMmapQueue("1/2/3/4/5/6")
	if !os.IsNotExist(errors.Unwrap(errors.Unwrap(err))) || bq != nil {
		t.Fatalf("expected file not exists error, returned: %v", err)
	}
}

func TestNewBigqueueTooLargeArena(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	arenaSize := math.MaxInt64
	bq, err := NewMmapQueue(testDir, SetArenaSize(arenaSize))
	if bq != nil || !strings.Contains(err.Error(), "file too large") &&
		!strings.Contains(err.Error(), "no space left on device") {

		t.Fatalf("expected file too large, returned: %v", err)
	}
}

func TestLimitedMemoryErr(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	arenaSize := os.Getpagesize() * 2
	bq, err := NewMmapQueue(testDir, SetArenaSize(arenaSize), SetMaxInMemArenas(1))
	if err != ErrTooFewInMemArenas || bq != nil {
		t.Fatalf("expected too few in mem arenas error, returned: %v", err)
	}
}

func TestLimitedMemoryNoErr(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	arenaSize := os.Getpagesize() * 2
	bq, err := NewMmapQueue(testDir, SetArenaSize(arenaSize), SetMaxInMemArenas(0))
	if err != nil || bq == nil {
		t.Fatalf("expected no error, returned: %v", err)
	}
	if err := bq.Close(); err != nil {
		t.Fatalf("error in closing bigqueue :: %v", err)
	}
}

func runTestLimitedMemory(t *testing.T, messageSize, arenaSize, maxInMemArenas int) {
	t.Helper()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir, SetArenaSize(arenaSize),
		SetMaxInMemArenas(maxInMemArenas))
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	msg := bytes.Repeat([]byte("a"), messageSize)
	for range 11 {
		if err := bq.Enqueue(msg); err != nil {
			t.Fatalf("enqueue failed :: %v", err)
		}

		checkInMemArenaInvariant(t, bq)
	}

	for range 5 {
		if _, err := bq.Dequeue(); err != nil {
			t.Fatalf("dequeue failed :: %v", err)
		}

		checkInMemArenaInvariant(t, bq)
	}

	for range 5 {
		if err := bq.Enqueue(msg); err != nil {
			t.Fatalf("enqueue failed :: %v", err)
		}

		checkInMemArenaInvariant(t, bq)
	}

	if bq.IsEmpty() {
		t.Fatalf("BigQueue should not be empty")
	}

	if err := bq.Close(); err != nil {
		t.Fatalf("error in closing bigqueue :: %v", err)
	}
	if bqTemp, err := NewMmapQueue(testDir, SetArenaSize(arenaSize),
		SetMaxInMemArenas(maxInMemArenas)); err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	} else {
		bq = bqTemp
	}

	for range 7 {
		if poppedMsg, err := bq.Dequeue(); err != nil {
			t.Fatalf("unable to dequeue :: %v", err)
		} else if !bytes.Equal(msg, poppedMsg) {
			t.Fatalf("unequal messages, eq: %s, dq: %s", string(msg), string(poppedMsg))
		}
		checkInMemArenaInvariant(t, bq)

		if err := bq.Enqueue(msg); err != nil {
			t.Fatalf("enqueue failed :: %v", err)
		}
		checkInMemArenaInvariant(t, bq)
	}

	for range 11 {
		if _, err := bq.Dequeue(); err != nil {
			t.Fatalf("dequeue failed :: %v", err)
		}

		checkInMemArenaInvariant(t, bq)
	}
}

func TestLimitedMemorySmallMessage(t *testing.T) {
	t.Parallel()
	arenaSize := os.Getpagesize() * 2
	runTestLimitedMemory(t, arenaSize-16, arenaSize, 3)
}

func TestLimitedMemoryLargeMessage(t *testing.T) {
	t.Parallel()
	arenaSize := os.Getpagesize() * 2
	runTestLimitedMemory(t, arenaSize*4, arenaSize, 3)
}

func TestLimitedMemoryHugeMessage1(t *testing.T) {
	t.Parallel()
	arenaSize := os.Getpagesize() * 2
	runTestLimitedMemory(t, arenaSize*7-8, arenaSize, 5)
}

func TestLimitedMemoryHugeMessage2(t *testing.T) {
	t.Parallel()
	arenaSize := os.Getpagesize() * 2
	runTestLimitedMemory(t, arenaSize*7, arenaSize, 5)
}

func TestLimitedMemoryExactMessage1(t *testing.T) {
	t.Parallel()
	arenaSize := os.Getpagesize() * 2
	runTestLimitedMemory(t, arenaSize*3-8, arenaSize, 5)
}

func TestLimitedMemoryExactMessage2(t *testing.T) {
	t.Parallel()
	arenaSize := os.Getpagesize() * 2
	runTestLimitedMemory(t, arenaSize-8, arenaSize, 3)
}

func TestReadWriteString(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir, SetArenaSize(8*1024))
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	// message larger than arena size, coinciding arena end
	msg := strings.Repeat("a", 2*8*1024-8)
	if err := bq.EnqueueString(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	if bq.IsEmpty() {
		t.Fatalf("BigQueue should not be empty")
	}

	if poppedMsg, err := bq.DequeueString(); err != nil {
		t.Fatalf("unable to dequeue :: %v", err)
	} else if msg != poppedMsg {
		t.Fatalf("unequal messages, eq: %s, dq: %s", msg, poppedMsg)
	}

	if err := bq.Close(); err != nil {
		t.Fatalf("error in closing bigqueue :: %v", err)
	}

	if bqTemp, err := NewMmapQueue(testDir, SetArenaSize(8*1024)); err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	} else {
		bq = bqTemp
	}

	// enqueue empty string
	if err := bq.EnqueueString(""); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	if poppedMsg, err := bq.DequeueString(); err != nil {
		t.Fatalf("unable to dequeue :: %v", err)
	} else if poppedMsg != "" {
		t.Fatalf("unequal messages, eq: <>, dq: %s", poppedMsg)
	}

	// enqueue small string
	smallStr := "bigqueue is awesome"
	if err := bq.EnqueueString(smallStr); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	if poppedMsg, err := bq.DequeueString(); err != nil {
		t.Fatalf("unable to dequeue :: %v", err)
	} else if smallStr != poppedMsg {
		t.Fatalf("unequal messages, eq: <>, dq: %s", poppedMsg)
	}
}

func TestConsumerSmallMessage(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir)
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	msg := []byte("test message")
	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	if bq.IsEmpty() {
		t.Fatalf("BigQueue should not be empty")
	}

	// enqueue message using default consumer
	if poppedMsg, err := bq.Dequeue(); err != nil {
		t.Fatalf("unable to dequeue :: %v", err)
	} else if !bytes.Equal(msg, poppedMsg) {
		t.Fatalf("unequal messages, eq: %s, dq: %s", string(msg), string(poppedMsg))
	}

	c, err := bq.NewConsumer("consumer")
	if err != nil {
		t.Fatalf("error in creating a consumer :: %v", err)
	}

	if c.IsEmpty() {
		t.Fatalf("BigQueue should not be empty for consumer")
	}

	if poppedMsg, err := c.Dequeue(); err != nil {
		t.Fatalf("unable to dequeue from consumer :: %v", err)
	} else if !bytes.Equal(msg, poppedMsg) {
		t.Fatalf("unequal messages, eq: %s, dq: %s", string(msg), string(poppedMsg))
	}
}

func TestConsumerReadWriteCornerCases(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	arenaSize := 8 * 1024
	bq, err := NewMmapQueue(testDir, SetArenaSize(arenaSize))
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}

	for i := 1; i < 10; i++ {
		msgLength := i*arenaSize/2 - 8
		if i == 5 {
			msgLength -= 8
		}
		msg := bytes.Repeat([]byte("a"), msgLength)
		if err := bq.Enqueue(msg); err != nil {
			t.Fatalf("enqueue failed :: %v", err)
		}

		if bq.IsEmpty() {
			t.Fatalf("BigQueue should not be empty")
		}

		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
		if bqTemp, err := NewMmapQueue(testDir, SetArenaSize(arenaSize)); err != nil {
			t.Fatalf("unable to get BigQueue: %v", err)
		} else {
			bq = bqTemp
		}

		if poppedMsg, err := bq.Dequeue(); err != nil {
			t.Fatalf("unable to dequeue :: %v", err)
		} else if !bytes.Equal(msg, poppedMsg) {
			t.Fatalf("unequal messages, eq: %s, dq: %s", string(msg), string(poppedMsg))
		}

		if !bq.IsEmpty() {
			t.Fatalf("BigQueue should be empty")
		}

		cur, err := bq.NewConsumer("consumer" + strconv.FormatInt(int64(i), 10))
		if err != nil {
			t.Fatalf("error in creating a consumer :: %v", err)
		}

		if cur.IsEmpty() {
			t.Fatalf("BigQueue should not be empty for consumer")
		}

		for j := 1; j <= i-1; j++ {
			if _, err := cur.Dequeue(); err != nil {
				t.Fatalf("unable to dequeue from consumer :: %v", err)
			}
		}

		for j := 1; j <= i; j++ {
			c, err := bq.NewConsumer("consumer" + strconv.FormatInt(int64(j), 10))
			if err != nil {
				t.Fatalf("error in creating a consumer :: %v", err)
			}

			if poppedMsg, err := c.DequeueString(); err != nil {
				t.Fatalf("unable to dequeue from consumer :: %v", err)
			} else if string(msg) != poppedMsg {
				t.Fatalf("unequal messages, eq: %s, dq: %s", string(msg), poppedMsg)
			}

			if !c.IsEmpty() {
				t.Fatalf("BigQueue should be empty")
			}
		}
	}

	if err := bq.Close(); err != nil {
		t.Fatalf("error in closing bigqueue :: %v", err)
	}
}

func TestCopyConsumerReadWriteCornerCases(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	arenaSize := 8 * 1024
	bq, err := NewMmapQueue(testDir, SetArenaSize(arenaSize))
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}

	for i := 1; i < 10; i++ {
		msgLength := i*arenaSize/2 - 8
		if i == 5 {
			msgLength -= 8
		}
		msg := bytes.Repeat([]byte("a"), msgLength)
		if err := bq.Enqueue(msg); err != nil {
			t.Fatalf("enqueue failed :: %v", err)
		}

		if bq.IsEmpty() {
			t.Fatalf("BigQueue should not be empty")
		}

		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
		if bqTemp, err := NewMmapQueue(testDir, SetArenaSize(arenaSize)); err != nil {
			t.Fatalf("unable to get BigQueue: %v", err)
		} else {
			bq = bqTemp
		}

		if poppedMsg, err := bq.Dequeue(); err != nil {
			t.Fatalf("unable to dequeue :: %v", err)
		} else if !bytes.Equal(msg, poppedMsg) {
			t.Fatalf("unequal messages, eq: %s, dq: %s", string(msg), string(poppedMsg))
		}

		if !bq.IsEmpty() {
			t.Fatalf("BigQueue should be empty")
		}

		lastC, err := bq.NewConsumer("consumer" + strconv.FormatInt(int64(i-1), 10))
		if err != nil {
			t.Fatalf("error in creating a consumer :: %v", err)
		}
		cur, err := bq.FromConsumer("consumer"+strconv.FormatInt(int64(i), 10), lastC)
		if err != nil {
			t.Fatalf("error in creating a consumer :: %v", err)
		}

		if cur.IsEmpty() {
			t.Fatalf("BigQueue should not be empty for consumer")
		}

		for j := 1; j <= i; j++ {
			c, err := bq.NewConsumer("consumer" + strconv.FormatInt(int64(j), 10))
			if err != nil {
				t.Fatalf("error in creating a consumer :: %v", err)
			}

			if poppedMsg, err := c.DequeueString(); err != nil {
				t.Fatalf("unable to dequeue from consumer :: %v", err)
			} else if string(msg) != poppedMsg {
				t.Fatalf("unequal messages, eq: %s, dq: %s", string(msg), poppedMsg)
			}

			if !c.IsEmpty() {
				t.Fatalf("BigQueue should be empty")
			}
		}
	}

	if err := bq.Close(); err != nil {
		t.Fatalf("error in closing bigqueue :: %v", err)
	}
}

func TestConsumersFromDifferentQueuesErr(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir)
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}

	c1, err := bq.NewConsumer("consumer1")
	if err != nil {
		t.Fatalf("error in creating a consumer :: %v", err)
	}

	if err := bq.Close(); err != nil {
		t.Fatalf("error in closing bigqueue :: %v", err)
	}

	if bqTemp, err := NewMmapQueue(testDir); err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	} else {
		bq = bqTemp
	}

	if _, err := bq.FromConsumer("consumer1", c1); err != ErrDifferentQueues {
		t.Fatalf("expected consumers from different queues error, returned: %v", err)
	}
}

func TestManyConsumers(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	arenaSize := 8 * 1024
	bq, err := NewMmapQueue(testDir, SetArenaSize(arenaSize))
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}

	for i := range 200 {
		_, err := bq.NewConsumer("consumer" + strconv.FormatInt(int64(i), 10))
		if err != nil {
			t.Fatalf("error in creating a consumer :: %v", err)
		}
	}

	if err := bq.Close(); err != nil {
		t.Fatalf("error in closing bigqueue :: %v", err)
	}
	if bqTemp, err := NewMmapQueue(testDir, SetArenaSize(arenaSize)); err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	} else {
		bq = bqTemp
	}

	for i := range 200 {
		_, err := bq.NewConsumer("consumer" + strconv.FormatInt(int64(i), 10))
		if err != nil {
			t.Fatalf("error in creating a consumer :: %v", err)
		}
	}

	num := bq.md.getNumConsumers()
	if num != 201 {
		t.Fatalf("number of consumers do not match, exp: 3001, actual %v", num)
	}

	if err := bq.Close(); err != nil {
		t.Fatalf("error in closing bigqueue :: %v", err)
	}
}

func TestFlush(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir, SetPeriodicFlushOps(3), SetPeriodicFlushDuration(time.Second))
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()
}

func TestParallel(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir, SetArenaSize(8*1024),
		SetPeriodicFlushDuration(time.Millisecond*10), SetPeriodicFlushOps(10))
	if err != nil {
		t.Fatalf("unable to create a bigqueue: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	// we have 11 API functions that we will call in parallel
	// and let the race detector catch if there is a race condition
	N := 2000
	errChan := make(chan error, 30000)
	var wg, rwg sync.WaitGroup
	defer wg.Wait()

	isEmptyFunc := func() {
		defer wg.Done()
		var emptyCount int64
		var nonEmptyCount int64
		for range N {
			if bq.IsEmpty() {
				emptyCount++
			} else {
				nonEmptyCount++
			}
		}
	}

	flushFunc := func() {
		defer wg.Done()
		for range N {
			if err := bq.Flush(); err != nil {
				errChan <- fmt.Errorf("error while Flush :: %v", err)
				return
			}
		}
	}

	enqueueFunc := func() {
		defer wg.Done()
		for range N {
			if err := bq.Enqueue([]byte("elem")); err != nil {
				errChan <- fmt.Errorf("error while Enqueue :: %v", err)
				return
			}
		}
	}

	enqueueStringFunc := func() {
		defer wg.Done()
		for range N {
			if err := bq.EnqueueString("elem"); err != nil {
				errChan <- fmt.Errorf("error while Enqueue :: %v", err)
				return
			}
		}
	}

	dequeueFunc := func() {
		defer wg.Done()
		for range N {
			if elem, err := bq.Dequeue(); err == ErrEmptyQueue {
				continue
			} else if err != nil {
				errChan <- fmt.Errorf("error while Dequeue :: %v", err)
				return
			} else if !bytes.Equal(elem, []byte("elem")) {
				errChan <- fmt.Errorf("invalid value, exp: elem, actual: %v", string(elem))
				return
			}
		}
	}

	dequeueStringFunc := func() {
		defer wg.Done()
		for range N {
			if elem, err := bq.DequeueString(); err == ErrEmptyQueue {
				continue
			} else if err != nil {
				errChan <- fmt.Errorf("error while Dequeue :: %v", err)
				return
			} else if elem != "elem" {
				errChan <- fmt.Errorf("invalid value, exp: elem, actual: %v", elem)
				return
			}
		}
	}

	newConsumerFunc := func() {
		defer wg.Done()
		for i := range N {
			c, err := bq.NewConsumer("existing")
			if err != nil {
				errChan <- fmt.Errorf("error while NewConsumer :: %v", err)
				return
			}

			if _, err := bq.FromConsumer("new"+strconv.Itoa(i), c); err != nil {
				errChan <- fmt.Errorf("error while FromConsumer :: %v", err)
				return
			}
		}
	}

	c, err := bq.NewConsumer("existing")
	if err != nil {
		t.Fatalf("error while NewConsumer :: %v", err)
	}
	consumerIsEmptyFunc := func() {
		defer wg.Done()
		var emptyCount int64
		var nonEmptyCount int64
		for range N {
			if c.IsEmpty() {
				emptyCount++
			} else {
				nonEmptyCount++
			}
		}
	}

	consumerDequeueFunc := func() {
		defer wg.Done()
		for range N {
			if elem, err := c.Dequeue(); err == ErrEmptyQueue {
				continue
			} else if err != nil {
				errChan <- fmt.Errorf("error while Dequeue :: %v", err)
				return
			} else if !bytes.Equal(elem, []byte("elem")) {
				errChan <- fmt.Errorf("invalid value, exp: elem, actual: %v", string(elem))
				return
			}
		}
	}

	consumerDequeueStringFunc := func() {
		defer wg.Done()
		for range N {
			if elem, err := c.DequeueString(); err == ErrEmptyQueue {
				continue
			} else if err != nil {
				errChan <- fmt.Errorf("error while Dequeue :: %v", err)
				return
			} else if elem != "elem" {
				errChan <- fmt.Errorf("invalid value, exp: elem, actual: %v", elem)
				return
			}
		}
	}

	fail := false
	rwg.Add(1)
	go func() {
		rwg.Done()
		for err := range errChan {
			t.Log(err)
			fail = true
		}
	}()

	wg.Add(20)
	go isEmptyFunc()
	go isEmptyFunc()
	go flushFunc()
	go flushFunc()
	go enqueueFunc()
	go enqueueFunc()
	go enqueueStringFunc()
	go enqueueStringFunc()
	go dequeueFunc()
	go dequeueFunc()
	go dequeueStringFunc()
	go dequeueStringFunc()
	go newConsumerFunc()
	go newConsumerFunc()
	go consumerIsEmptyFunc()
	go consumerIsEmptyFunc()
	go consumerDequeueFunc()
	go consumerDequeueFunc()
	go consumerDequeueStringFunc()
	go consumerDequeueStringFunc()
	wg.Wait()

	close(errChan)
	rwg.Wait()
	if fail {
		t.FailNow()
	}
}

func TestEnqueueWithTag(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir)
	if err != nil {
		t.Fatalf("unable to get BigQueue :: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	msg := []byte("hello world")
	var tag byte = 42
	if err := bq.EnqueueWithTag(msg, tag); err != nil {
		t.Fatalf("EnqueueWithTag failed :: %v", err)
	}

	gotMsg, gotTag, err := bq.DequeueWithTag()
	if err != nil {
		t.Fatalf("DequeueWithTag failed :: %v", err)
	}
	if gotTag != tag {
		t.Fatalf("tag mismatch: expected %d, got %d", tag, gotTag)
	}
	if !bytes.Equal(gotMsg, msg) {
		t.Fatalf("message mismatch: expected %s, got %s", string(msg), string(gotMsg))
	}
}

func TestDequeueWithTagEmptyQueue(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir)
	if err != nil {
		t.Fatalf("unable to get BigQueue :: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	if msg, tag, err := bq.DequeueWithTag(); err != ErrEmptyQueue || msg != nil || tag != 0 {
		t.Fatalf("DequeueWithTag on empty queue should return ErrEmptyQueue, got err: %v, msg: %v, tag: %v", err, msg, tag)
	}
}

func TestEnqueueWithTagMultipleMessages(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir)
	if err != nil {
		t.Fatalf("unable to get BigQueue :: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	type entry struct {
		msg []byte
		tag byte
	}
	entries := []entry{
		{[]byte("first"), 1},
		{[]byte("second"), 2},
		{[]byte("third"), 3},
		{[]byte(""), 255},
	}

	for _, e := range entries {
		if err := bq.EnqueueWithTag(e.msg, e.tag); err != nil {
			t.Fatalf("EnqueueWithTag failed :: %v", err)
		}
	}

	for _, e := range entries {
		gotMsg, gotTag, err := bq.DequeueWithTag()
		if err != nil {
			t.Fatalf("DequeueWithTag failed :: %v", err)
		}
		if gotTag != e.tag {
			t.Fatalf("tag mismatch: expected %d, got %d", e.tag, gotTag)
		}
		if !bytes.Equal(gotMsg, e.msg) {
			t.Fatalf("message mismatch: expected %s, got %s", string(e.msg), string(gotMsg))
		}
	}

	if !bq.IsEmpty() {
		t.Fatalf("queue should be empty")
	}
}

func TestEnqueueWithTagLargeMessage(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir)
	if err != nil {
		t.Fatalf("unable to get BigQueue :: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	// message large enough to span multiple arenas
	msg := bytes.Repeat([]byte("a"), cDefaultArenaSize+100)
	var tag byte = 77
	if err := bq.EnqueueWithTag(msg, tag); err != nil {
		t.Fatalf("EnqueueWithTag failed :: %v", err)
	}

	gotMsg, gotTag, err := bq.DequeueWithTag()
	if err != nil {
		t.Fatalf("DequeueWithTag failed :: %v", err)
	}
	if gotTag != tag {
		t.Fatalf("tag mismatch: expected %d, got %d", tag, gotTag)
	}
	if !bytes.Equal(gotMsg, msg) {
		t.Fatalf("message content mismatch for large message")
	}
}

func TestEnqueueWithTagConsumer(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir)
	if err != nil {
		t.Fatalf("unable to get BigQueue :: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	c, err := bq.NewConsumer("tagged-consumer")
	if err != nil {
		t.Fatalf("unable to create consumer :: %v", err)
	}

	msg := []byte("tagged payload")
	var tag byte = 99
	if err := bq.EnqueueWithTag(msg, tag); err != nil {
		t.Fatalf("EnqueueWithTag failed :: %v", err)
	}

	gotMsg, gotTag, err := c.DequeueWithTag()
	if err != nil {
		t.Fatalf("consumer DequeueWithTag failed :: %v", err)
	}
	if gotTag != tag {
		t.Fatalf("tag mismatch: expected %d, got %d", tag, gotTag)
	}
	if !bytes.Equal(gotMsg, msg) {
		t.Fatalf("message mismatch: expected %s, got %s", string(msg), string(gotMsg))
	}
}

// TestEnqueueWithTagBoundaryValues verifies that tag values 0 and 255 (boundary bytes) are
// preserved correctly.
func TestEnqueueWithTagBoundaryValues(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir)
	if err != nil {
		t.Fatalf("unable to get BigQueue :: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	type entry struct {
		msg []byte
		tag byte
	}
	entries := []entry{
		{[]byte("tag-zero"), 0},
		{[]byte("tag-max"), 255},
	}

	for _, e := range entries {
		if err := bq.EnqueueWithTag(e.msg, e.tag); err != nil {
			t.Fatalf("EnqueueWithTag failed :: %v", err)
		}
	}

	for _, e := range entries {
		gotMsg, gotTag, err := bq.DequeueWithTag()
		if err != nil {
			t.Fatalf("DequeueWithTag failed :: %v", err)
		}
		if gotTag != e.tag {
			t.Fatalf("tag mismatch: expected %d, got %d", e.tag, gotTag)
		}
		if !bytes.Equal(gotMsg, e.msg) {
			t.Fatalf("message mismatch: expected %s, got %s", string(e.msg), string(gotMsg))
		}
	}
}

// TestEnqueueWithTagNilMessage verifies that a nil message with a tag is handled correctly.
func TestEnqueueWithTagNilMessage(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir)
	if err != nil {
		t.Fatalf("unable to get BigQueue :: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	var tag byte = 7
	if err := bq.EnqueueWithTag(nil, tag); err != nil {
		t.Fatalf("EnqueueWithTag with nil message failed :: %v", err)
	}

	gotMsg, gotTag, err := bq.DequeueWithTag()
	if err != nil {
		t.Fatalf("DequeueWithTag failed :: %v", err)
	}
	if gotTag != tag {
		t.Fatalf("tag mismatch: expected %d, got %d", tag, gotTag)
	}
	if len(gotMsg) != 0 {
		t.Fatalf("expected empty message, got length %d", len(gotMsg))
	}
}

// TestEnqueueWithTagArenaOverlap verifies correct behaviour when the tag+data straddles
// an arena boundary (the tag byte ends up in one arena and the data starts in the next).
func TestEnqueueWithTagArenaOverlap(t *testing.T) {
	t.Parallel()

	arenaSize := 4 * 1024 // 4 KB
	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir, SetArenaSize(arenaSize))
	if err != nil {
		t.Fatalf("unable to get BigQueue :: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	// Fill the queue so the next enqueue's tag byte will land at the very end of an
	// arena. arenaSize-8 bytes of data + 8 bytes length header = exactly one arena.
	// The next EnqueueWithTag will write its 8-byte length at offset 0 of arena 1,
	// pushing the tag+data to start exactly at offset 8, which is still in arena 1
	// unless we craft the sizes more carefully. We instead use arenaSize-9 so the
	// next write's length header fills the remaining 9 bytes (8 length + 1 tag) and
	// the tag sits at the arena boundary.
	filler := bytes.Repeat([]byte("x"), arenaSize-9)
	if err := bq.Enqueue(filler); err != nil {
		t.Fatalf("Enqueue filler failed :: %v", err)
	}

	msg := []byte("boundary-test")
	var tag byte = 13
	if err := bq.EnqueueWithTag(msg, tag); err != nil {
		t.Fatalf("EnqueueWithTag failed :: %v", err)
	}

	// drain the filler
	if _, err := bq.Dequeue(); err != nil {
		t.Fatalf("Dequeue filler failed :: %v", err)
	}

	gotMsg, gotTag, err := bq.DequeueWithTag()
	if err != nil {
		t.Fatalf("DequeueWithTag failed :: %v", err)
	}
	if gotTag != tag {
		t.Fatalf("tag mismatch: expected %d, got %d", tag, gotTag)
	}
	if !bytes.Equal(gotMsg, msg) {
		t.Fatalf("message mismatch: expected %s, got %s", string(msg), string(gotMsg))
	}
}

// TestEnqueueWithTagInterleavedWithEnqueue verifies that regular Enqueue/Dequeue messages
// and tagged messages do not interfere with each other when interleaved.
func TestEnqueueWithTagInterleavedWithEnqueue(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir)
	if err != nil {
		t.Fatalf("unable to get BigQueue :: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	plain := []byte("plain-message")
	tagged := []byte("tagged-message")
	var tag byte = 50

	if err := bq.Enqueue(plain); err != nil {
		t.Fatalf("Enqueue failed :: %v", err)
	}
	if err := bq.EnqueueWithTag(tagged, tag); err != nil {
		t.Fatalf("EnqueueWithTag failed :: %v", err)
	}
	if err := bq.Enqueue(plain); err != nil {
		t.Fatalf("Enqueue failed :: %v", err)
	}

	// dequeue plain
	if gotMsg, err := bq.Dequeue(); err != nil {
		t.Fatalf("Dequeue failed :: %v", err)
	} else if !bytes.Equal(gotMsg, plain) {
		t.Fatalf("plain message mismatch: expected %s, got %s", string(plain), string(gotMsg))
	}

	// dequeue tagged
	gotMsg, gotTag, err := bq.DequeueWithTag()
	if err != nil {
		t.Fatalf("DequeueWithTag failed :: %v", err)
	}
	if gotTag != tag {
		t.Fatalf("tag mismatch: expected %d, got %d", tag, gotTag)
	}
	if !bytes.Equal(gotMsg, tagged) {
		t.Fatalf("tagged message mismatch: expected %s, got %s", string(tagged), string(gotMsg))
	}

	// dequeue plain again
	if gotMsg, err := bq.Dequeue(); err != nil {
		t.Fatalf("Dequeue failed :: %v", err)
	} else if !bytes.Equal(gotMsg, plain) {
		t.Fatalf("plain message mismatch: expected %s, got %s", string(plain), string(gotMsg))
	}

	if !bq.IsEmpty() {
		t.Fatalf("queue should be empty")
	}
}

// TestEnqueueWithTagMultipleConsumers verifies that multiple independent consumers each
// receive the correct tag and payload from the same tagged messages.
func TestEnqueueWithTagMultipleConsumers(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir)
	if err != nil {
		t.Fatalf("unable to get BigQueue :: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error in closing bigqueue :: %v", err)
		}
	}()

	c1, err := bq.NewConsumer("consumer-1")
	if err != nil {
		t.Fatalf("unable to create consumer-1 :: %v", err)
	}
	c2, err := bq.NewConsumer("consumer-2")
	if err != nil {
		t.Fatalf("unable to create consumer-2 :: %v", err)
	}

	type entry struct {
		msg []byte
		tag byte
	}
	entries := []entry{
		{[]byte("msg-a"), 10},
		{[]byte("msg-b"), 20},
		{[]byte("msg-c"), 30},
	}

	for _, e := range entries {
		if err := bq.EnqueueWithTag(e.msg, e.tag); err != nil {
			t.Fatalf("EnqueueWithTag failed :: %v", err)
		}
	}

	for _, consumer := range []*Consumer{c1, c2} {
		for _, e := range entries {
			gotMsg, gotTag, err := consumer.DequeueWithTag()
			if err != nil {
				t.Fatalf("consumer DequeueWithTag failed :: %v", err)
			}
			if gotTag != e.tag {
				t.Fatalf("tag mismatch: expected %d, got %d", e.tag, gotTag)
			}
			if !bytes.Equal(gotMsg, e.msg) {
				t.Fatalf("message mismatch: expected %s, got %s", string(e.msg), string(gotMsg))
			}
		}
	}
}

// TestEnqueueWithTagPersistence verifies that tagged messages survive a queue close/reopen
// cycle.
func TestEnqueueWithTagPersistence(t *testing.T) {
	t.Parallel()

	testDir := t.TempDir()

	msg := []byte("persisted payload")
	var tag byte = 88

	// write
	bq, err := NewMmapQueue(testDir)
	if err != nil {
		t.Fatalf("unable to create BigQueue :: %v", err)
	}
	if err := bq.EnqueueWithTag(msg, tag); err != nil {
		t.Fatalf("EnqueueWithTag failed :: %v", err)
	}
	if err := bq.Close(); err != nil {
		t.Fatalf("error closing bigqueue :: %v", err)
	}

	// reopen and read
	bq2, err := NewMmapQueue(testDir)
	if err != nil {
		t.Fatalf("unable to reopen BigQueue :: %v", err)
	}
	defer func() {
		if err := bq2.Close(); err != nil {
			t.Fatalf("error closing bigqueue :: %v", err)
		}
	}()

	gotMsg, gotTag, err := bq2.DequeueWithTag()
	if err != nil {
		t.Fatalf("DequeueWithTag after reopen failed :: %v", err)
	}
	if gotTag != tag {
		t.Fatalf("tag mismatch after reopen: expected %d, got %d", tag, gotTag)
	}
	if !bytes.Equal(gotMsg, msg) {
		t.Fatalf("message mismatch after reopen: expected %s, got %s", string(msg), string(gotMsg))
	}
}

// TestEnqueueWithTagRandomPayloads verifies that EnqueueWithTag and DequeueWithTag preserve
// exact byte-for-byte data and FIFO order for a wide variety of payload types: random numeric
// bytes, non-UTF-8 binary sequences, Chinese, Korean, and Japanese encoded text, mixed
// multibyte payloads, and randomly chosen tag values.
func TestEnqueueWithTagRandomPayloads(t *testing.T) {
	t.Parallel()

	// Use a fixed seed for reproducibility while covering a broad character space.
	rng := rand.New(rand.NewSource(42))

	type entry struct {
		payload []byte
		tag     byte
		label   string
	}

	// Helper: random bytes of the given length that are not necessarily valid UTF-8.
	randomBytes := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(rng.Intn(256))
		}
		return b
	}

	entries := []entry{
		// Numeric (ASCII digit sequences)
		{[]byte("1234567890"), 0x01, "numeric-ascii"},
		{[]byte("9876543210987654321"), 0x02, "long-numeric-ascii"},
		// Non-UTF-8 binary sequences (high bytes, invalid lead bytes)
		{[]byte{0xFF, 0xFE, 0x00, 0xD8, 0x00}, 0x10, "non-utf8-bom-like"},
		{[]byte{0x80, 0x81, 0x82, 0xFE, 0xFF}, 0x11, "non-utf8-high-bytes"},
		{randomBytes(16), 0x12, "non-utf8-random-16"},
		{randomBytes(64), 0x13, "non-utf8-random-64"},
		// Chinese (Simplified & Traditional)
		{[]byte("你好世界"), 0x20, "chinese-simplified"},
		{[]byte("繁體中文測試"), 0x21, "chinese-traditional"},
		{[]byte("中华人民共和国"), 0x22, "chinese-long"},
		// Korean
		{[]byte("안녕하세요"), 0x30, "korean-hello"},
		{[]byte("대한민국"), 0x31, "korean-country"},
		{[]byte("가나다라마바사아자차카타파하"), 0x32, "korean-alphabet"},
		// Japanese
		{[]byte("こんにちは"), 0x40, "japanese-hiragana"},
		{[]byte("コンニチハ"), 0x41, "japanese-katakana"},
		{[]byte("日本語テスト"), 0x42, "japanese-mixed"},
		{[]byte("漢字テスト"), 0x43, "japanese-kanji"},
		// Mixed multibyte in one payload
		{[]byte("hello 世界 안녕 こんにちは 🌏"), 0x50, "mixed-multilingual"},
		// Boundary tag values with multibyte payload
		{[]byte("境界値テスト"), 0x00, "tag-zero-multibyte"},
		{[]byte("边界值测试"), 0xFF, "tag-max-multibyte"},
		// Random tag values with random binary payloads
		{randomBytes(128), byte(rng.Intn(256)), "random-128"},
		{randomBytes(512), byte(rng.Intn(256)), "random-512"},
	}

	testDir := t.TempDir()
	bq, err := NewMmapQueue(testDir)
	if err != nil {
		t.Fatalf("unable to create BigQueue :: %v", err)
	}
	defer func() {
		if err := bq.Close(); err != nil {
			t.Fatalf("error closing bigqueue :: %v", err)
		}
	}()

	// Enqueue all entries in order.
	for _, e := range entries {
		if err := bq.EnqueueWithTag(e.payload, e.tag); err != nil {
			t.Fatalf("[%s] EnqueueWithTag failed :: %v", e.label, err)
		}
	}

	// Dequeue all entries and verify exact byte equality and FIFO order.
	for i, e := range entries {
		gotMsg, gotTag, err := bq.DequeueWithTag()
		if err != nil {
			t.Fatalf("[%d/%s] DequeueWithTag failed :: %v", i, e.label, err)
		}
		if gotTag != e.tag {
			t.Fatalf("[%d/%s] tag mismatch: want 0x%02x, got 0x%02x", i, e.label, e.tag, gotTag)
		}
		if !bytes.Equal(gotMsg, e.payload) {
			t.Fatalf("[%d/%s] payload mismatch: want %q (%d bytes), got %q (%d bytes)",
				i, e.label, e.payload, len(e.payload), gotMsg, len(gotMsg))
		}
	}

	if !bq.IsEmpty() {
		t.Fatalf("queue should be empty after dequeuing all entries")
	}
}
