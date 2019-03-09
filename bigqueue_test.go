package bigqueue

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path"
	"strings"
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

func TestIsEmpty(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, err := NewBigQueue(testDir + "/")
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer bq.Close()

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

	if err := bq.Dequeue(); err != nil {
		t.Fatalf("unable to dequeue message :: %v", err)
	}

	if !bq.IsEmpty() {
		t.Fatalf("BigQueue length should be 0")
	}
}

func TestPeek(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, err := NewBigQueue(testDir)
	if err != nil {
		t.Fatalf("unable to get BigQueue :: %v", err)
	}
	defer bq.Close()

	if msg, err := bq.Peek(); err != ErrEmptyQueue || msg != nil {
		t.Fatalf("peek should return empty queue error, returned: %v", err)
	}

	if err := bq.Dequeue(); err != ErrEmptyQueue {
		t.Fatalf("dequeue should return empty queue error, returned: %v", err)
	}

	msg := []byte("abcdefghij")
	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	if headMsg, err := bq.Peek(); err != nil {
		t.Fatalf("peek failed :: %v", err)
	} else if !bytes.Equal(msg, headMsg) {
		t.Fatalf("messages don't match :: expected %s, actual: %s", string(msg), string(headMsg))
	}
}

func TestEnqueueSmallMessage(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, err := NewBigQueue(testDir)
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer bq.Close()

	msg := []byte("abcdefghij")
	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	if bq.IsEmpty() {
		t.Fatalf("BigQueue should not be empty")
	}

	if poppedMsg, err := bq.Peek(); err != nil {
		t.Fatalf("unable to peek :: %v", err)
	} else if !bytes.Equal(msg, poppedMsg) {
		t.Fatalf("unequal messages, eq: %s, dq: %s", string(msg), string(poppedMsg))
	}

	if err := bq.Dequeue(); err != nil {
		t.Fatalf("unable to dequeue :: %v", err)
	}
}

func TestEnqueueLargeMessage(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, err := NewBigQueue(testDir)
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer bq.Close()

	msg := make([]byte, 0)
	for i := 0; i < cDefaultArenaSize-8; i++ {
		m := []byte("a")
		msg = append(msg, m...)
	}
	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	if deQueuedMsg, err := bq.Peek(); err != nil {
		t.Fatalf("peek failed :: %v", err)
	} else if !bytes.Equal(deQueuedMsg, msg) {
		t.Fatalf("dequeued and enqueued messages are not equal")
	}

	if err := bq.Dequeue(); err != nil {
		t.Fatalf("dequeue failed :: %v", err)
	}

	if !bq.IsEmpty() {
		t.Fatalf("IsEmpty should return true")
	}
}

func TestEnqueueOverlapLength(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, err := NewBigQueue(testDir)
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer bq.Close()

	msg1 := make([]byte, 0)
	for i := 0; i < cDefaultArenaSize-12; i++ {
		m := []byte("a")
		msg1 = append(msg1, m...)
	}
	if err := bq.Enqueue(msg1); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	msg2 := make([]byte, 0)
	for i := 0; i < cDefaultArenaSize-4; i++ {
		m := []byte("a")
		msg2 = append(msg2, m...)
	}
	if err := bq.Enqueue(msg2); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	if dequeueMsg, err := bq.Peek(); err != nil {
		t.Fatalf("peek failed :: %v", err)
	} else if !bytes.Equal(dequeueMsg, msg1) {
		t.Fatalf("dequeued and enqeued messages are not equal")
	}
	if err := bq.Dequeue(); err != nil {
		t.Fatalf("dequeue failed :: %v", err)
	}

	if dequeueMsg, err := bq.Peek(); err != nil {
		t.Fatalf("peek failed :: %v", err)
	} else if !bytes.Equal(dequeueMsg, msg2) {
		t.Fatalf("dequeued and enqeued messages are not equal")
	}
	if err := bq.Dequeue(); err != nil {
		t.Fatalf("dequeue failed :: %v", err)
	}

	if !bq.IsEmpty() {
		t.Fatalf("queue should be empty")
	}
}

func TestEnqueueLargeNumberOfMessages(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, err := NewBigQueue(testDir)
	if err != nil {
		t.Fatalf("unable to get BigQueue: %s", err)
	}
	defer bq.Close()

	numMessages := 10
	lengths := make([]int, 0)
	alphabets := "abcdefghijklmnopqrstuvwxyz"
	for i := 0; i < numMessages; i++ {
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
			t.Fatalf("uanble to enqueue message :: %v", err)
		}
	}

	for i := 0; i < numMessages; i++ {
		if msg, err := bq.Peek(); err != nil {
			t.Fatalf("uanble to peek message :: %v", err)
		} else if len(msg) != lengths[i] {
			t.Fatalf("enqueued and dequeued lengths don't match for msg no %d", i)
		}
		if err := bq.Dequeue(); err != nil {
			t.Fatalf("uanble to dequeue message :: %v", err)
		}
	}

	if !bq.IsEmpty() {
		t.Fatalf("queue should be empty")
	}
}

func TestEnqueueZeroLengthMessage(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, err := NewBigQueue(testDir)
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer bq.Close()

	emptyMsg := make([]byte, 0)
	if err := bq.Enqueue(emptyMsg); err != nil {
		t.Fatalf("unable to enqueue empty message :: %v", err)
	}

	if bq.IsEmpty() {
		t.Fatalf("IsEmpty should return false if empty message is present in queue")
	}

	if deQueuedMsg, err := bq.Peek(); err != nil {
		t.Fatalf("unable to peek empty message")
	} else if !bytes.Equal(deQueuedMsg, emptyMsg) {
		t.Fatalf("dequeued and enqueued messages are not equal")
	}
	if err := bq.Dequeue(); err != nil {
		t.Fatalf("unable to dequeue empty message")
	}

	if !bq.IsEmpty() {
		t.Fatalf("queue should be empty now")
	}
}

func TestEnqueueWhenMessageLengthFits(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	arenaSize := 4 * 1024
	bq, err := NewBigQueue(testDir, SetArenaSize(arenaSize))
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer bq.Close()

	msg1 := bytes.Repeat([]byte("a"), arenaSize-16)
	if err := bq.Enqueue(msg1); err != nil {
		t.Fatalf("unable to enqueue msg1: %s", err)
	}

	msg2 := bytes.Repeat([]byte("b"), 3*arenaSize)
	if err := bq.Enqueue(msg2); err != nil {
		t.Fatalf("unable to enqueue msg2: %s", err)
	}

	if err := bq.Dequeue(); err != nil {
		t.Fatalf("unable to dequeue msg1: %s", err)
	}

	if err := bq.Dequeue(); err != nil {
		t.Fatalf("unable to dequeue msg2: %s", err)
	}
}

func TestReadWriteCornerCases(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	arenaSize := 8 * 1024
	bq, err := NewBigQueue(testDir, SetArenaSize(arenaSize))
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

		bq.Close()
		if bqTemp, err := NewBigQueue(testDir, SetArenaSize(arenaSize)); err != nil {
			t.Fatalf("unable to get BigQueue: %v", err)
		} else {
			bq = bqTemp
		}

		if poppedMsg, err := bq.Peek(); err != nil {
			t.Fatalf("unable to peek :: %v", err)
		} else if !bytes.Equal(msg, poppedMsg) {
			t.Fatalf("unequal messages, eq: %s, dq: %s", string(msg), string(poppedMsg))
		}

		if err := bq.Dequeue(); err != nil {
			t.Fatalf("unable to dequeue :: %v", err)
		}

		if !bq.IsEmpty() {
			t.Fatalf("BigQueue should be empty")
		}
	}

	bq.Close()
}

func TestArenaSize(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, err := NewBigQueue(testDir, SetArenaSize(8*1024))
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer bq.Close()

	msg := []byte("abcdefghij")
	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	if bq.IsEmpty() {
		t.Fatalf("BigQueue should not be empty")
	}

	if poppedMsg, err := bq.Peek(); err != nil {
		t.Fatalf("unable to peek :: %v", err)
	} else if !bytes.Equal(msg, poppedMsg) {
		t.Fatalf("unequal length, eq: %s, dq: %s", string(msg), string(poppedMsg))
	}

	if poppedMsg, err := bq.Peek(); err != nil {
		t.Fatalf("unable to peek :: %v", err)
	} else if !bytes.Equal(msg, poppedMsg) {
		t.Fatalf("unequal messages, eq: %s, dq: %s", string(msg), string(poppedMsg))
	}

	if err := bq.Dequeue(); err != nil {
		t.Fatalf("unable to dequeue :: %v", err)
	}
}

func TestArenaSize2(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	arenaSize := os.Getpagesize() * 2
	bq, err := NewBigQueue(testDir, SetArenaSize(arenaSize))
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer bq.Close()

	msg := []byte("abcdefghij")
	for i := 0; i < arenaSize/len(msg)*4; i++ {
		if err := bq.Enqueue(msg); err != nil {
			t.Fatalf("enqueue failed :: %v", err)
		}
	}

	if bq.IsEmpty() {
		t.Fatalf("BigQueue should not be empty")
	}

	for i := 0; i < arenaSize/len(msg)*4; i++ {
		if poppedMsg, err := bq.Peek(); err != nil {
			t.Fatalf("unable to peek :: %v", err)
		} else if !bytes.Equal(msg, poppedMsg) {
			t.Fatalf("unequal messages, eq: %s, dq: %s", string(msg), string(poppedMsg))
		}

		if poppedMsg, err := bq.Peek(); err != nil {
			t.Fatalf("unable to peek :: %v", err)
		} else if !bytes.Equal(msg, poppedMsg) {
			t.Fatalf("unequal length, eq: %s, dq: %s", string(msg), string(poppedMsg))
		}

		if err := bq.Dequeue(); err != nil {
			t.Fatalf("unable to dequeue :: %v", err)
		}
	}
}

func TestArenaSize3(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	arenaSize := os.Getpagesize()
	bq, err := NewBigQueue(testDir, SetArenaSize(arenaSize))
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}
	defer bq.Close()

	msg := []byte("abcdefgh")
	for i := 0; i < arenaSize/len(msg)*4; i++ {
		if err := bq.Enqueue(msg); err != nil {
			t.Fatalf("enqueue failed :: %v", err)
		}
	}

	if bq.IsEmpty() {
		t.Fatalf("BigQueue should not be empty")
	}

	for i := 0; i < arenaSize/len(msg)*4; i++ {
		if poppedMsg, err := bq.Peek(); err != nil {
			t.Fatalf("unable to peek :: %v", err)
		} else if !bytes.Equal(msg, poppedMsg) {
			t.Fatalf("unequal length, eq: %s, dq: %s", string(msg), string(poppedMsg))
		}

		if poppedMsg, err := bq.Peek(); err != nil {
			t.Fatalf("unable to peek :: %v", err)
		} else if !bytes.Equal(msg, poppedMsg) {
			t.Fatalf("unequal length, eq: %s, dq: %s", string(msg), string(poppedMsg))
		}

		if err := bq.Dequeue(); err != nil {
			t.Fatalf("unable to dequeue :: %v", err)
		}
	}
}

func TestArenaSizeFail(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	_, err := NewBigQueue(testDir, SetArenaSize(os.Getpagesize()/2))
	if err != ErrTooSmallArenaSize {
		t.Fatalf("expected error: %v, got: %v", ErrTooSmallArenaSize, err)
	}
}

func TestArenaSizeFail2(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, err := NewBigQueue(testDir, SetArenaSize(8*1024))
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}

	msg := []byte("abcdefghij")
	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	bq.Close()
	if _, err := NewBigQueue(testDir, SetArenaSize(6*1024)); err != ErrInvalidArenaSize {
		t.Fatalf("expected invalid arena size error, got: %v", err)
	}
}

func TestArenaSizeNotMultiple(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, err := NewBigQueue(testDir, SetArenaSize(5732))
	if err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	}

	msg := []byte("abcdefghij")
	if err := bq.Enqueue(msg); err != nil {
		t.Fatalf("enqueue failed :: %v", err)
	}

	bq.Close()
	if tempBq, err := NewBigQueue(testDir, SetArenaSize(5732)); err != nil {
		t.Fatalf("unable to get BigQueue: %v", err)
	} else {
		bq = tempBq
	}
	defer bq.Close()

	if poppedMsg, err := bq.Peek(); err != nil {
		t.Fatalf("unable to peek :: %v", err)
	} else if !bytes.Equal(msg, poppedMsg) {
		t.Fatalf("unequal messages, eq: %s, dq: %s", string(msg), string(poppedMsg))
	}
}

func TestNewBigqueueNoFolder(t *testing.T) {
	bq, err := NewBigQueue("1/2/3/4/5/6")
	if !os.IsNotExist(err) || bq != nil {
		t.Fatalf("expected file not exists error, returned: %v", err)
	}
}

func TestNewBigqueueTooLargeArena(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	arenaSize := math.MaxInt64
	bq, err := NewBigQueue(testDir, SetArenaSize(arenaSize))
	if bq != nil || !(strings.Contains(err.Error(), "file too large") ||
		strings.Contains(err.Error(), "no space left on device")) {

		t.Fatalf("expected file too large, returned: %v", err)
	}
}
