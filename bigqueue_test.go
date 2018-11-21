package bigqueue

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path"
	"testing"
)

func TestIsEmpty(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, errQ := NewBigQueue(testDir)
	if errQ != nil {
		t.Errorf("unable to get BigQueue: %v", errQ)
	}

	if bq.IsEmpty() == false {
		t.Errorf("BigQueue length should be 0")
	}

	msg := []byte("abcdefgh")
	if err := bq.Enqueue(msg); err != nil {
		t.Errorf("unable to enqueue message :: %v", err)
	}

	if bq.IsEmpty() == true {
		t.Errorf("IsEmpty should return false after enqueue")
	}

	if err := bq.Dequeue(); err != nil {
		t.Errorf("unable to dequeue message :: %v", err)
	}

	if bq.IsEmpty() == false {
		t.Errorf("BigQueue length should be 0")
	}
}

func TestPeek(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, errQ := NewBigQueue(testDir)
	if errQ != nil {
		t.Errorf("unable to get BigQueue :: %v", errQ)
	}

	msg := []byte("abcdefghij")
	if err := bq.Enqueue(msg); err != nil {
		t.Errorf("enqueue failed :: %v", err)
	}

	headMsg, err := bq.Peek()
	if err != nil {
		t.Errorf("peek failed :: %v", err)
	}

	if !bytes.Equal(msg, headMsg) {
		t.Errorf("lengths don't match :: expected %s, actual: %s", string(msg), string(headMsg))
	}
}

func TestEnqueueSmallMessage(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, errQ := NewBigQueue(testDir)
	if errQ != nil {
		t.Errorf("unable to get BigQueue: %v", errQ)
	}

	msg := []byte("abcdefghij")
	if err := bq.Enqueue(msg); err != nil {
		t.Errorf("enqueue failed :: %v", err)
	}

	if bq.IsEmpty() == true {
		t.Errorf("BigQueue should not be empty")
	}

	poppedMsg, err := bq.Peek()
	if err != nil {
		t.Errorf("unable to peek :: %v", err)
	}

	if err := bq.Dequeue(); err != nil {
		t.Errorf("unable to dequeue :: %v", err)
	}

	if !bytes.Equal(msg, poppedMsg) {
		t.Errorf("unequal length, eq: %s, dq: %s", string(msg), string(poppedMsg))
	}
}

func TestEnqueueLargeMessage(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, errQ := NewBigQueue(testDir)
	if errQ != nil {
		t.Errorf("unable to get BigQueue: %v", errQ)
	}

	msg := make([]byte, 0)
	for i := 0; i < cDefaultArenaSize-8; i++ {
		m := []byte("a")
		msg = append(msg, m...)
	}
	if err := bq.Enqueue(msg); err != nil {
		t.Errorf("enqueue failed :: %v", err)
	}

	deQueuedMsg, err := bq.Peek()
	if err != nil {
		t.Errorf("peek failed :: %v", err)
	}

	if err := bq.Dequeue(); err != nil {
		t.Errorf("dequeue failed :: %v", err)
	}

	if !bytes.Equal(deQueuedMsg, msg) {
		t.Errorf("dequeued and enqueued messages are not equal")
	}

	if bq.IsEmpty() == false {
		t.Errorf("IsEmpty should return true")
	}
}

func TestEnqueueOverlapLength(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, errQ := NewBigQueue(testDir)
	if errQ != nil {
		t.Errorf("unable to get BigQueue: %v", errQ)
	}

	msg1 := make([]byte, 0)
	for i := 0; i < cDefaultArenaSize-12; i++ {
		m := []byte("a")
		msg1 = append(msg1, m...)
	}
	if err := bq.Enqueue(msg1); err != nil {
		t.Errorf("enqueue failed :: %v", err)
	}

	msg2 := make([]byte, 0)
	for i := 0; i < cDefaultArenaSize-4; i++ {
		m := []byte("a")
		msg2 = append(msg2, m...)
	}
	if err := bq.Enqueue(msg2); err != nil {
		t.Errorf("enqueue failed :: %v", err)
	}

	dequeueMsg1, err := bq.Peek()
	if err != nil {
		t.Errorf("peek failed :: %v", err)
	}
	if err := bq.Dequeue(); err != nil {
		t.Errorf("dequeue failed :: %v", err)
	}
	if bytes.Compare(dequeueMsg1, msg1) != 0 {
		t.Errorf("dequeued and enqeued messages are not equal")
	}

	dequeueMsg2, err := bq.Peek()
	if err != nil {
		t.Errorf("peek failed :: %v", err)
	}
	if err := bq.Dequeue(); err != nil {
		t.Errorf("dequeue failed :: %v", err)
	}
	if !bytes.Equal(dequeueMsg2, msg2) {
		t.Errorf("dequeued and enqeued messages are not equal")
	}

	if bq.IsEmpty() == false {
		t.Errorf("queue should be empty")
	}
}

func TestEnqueueLargeNumberOfMessages(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, err := NewBigQueue(testDir)
	if err != nil {
		t.Errorf("unable to get BigQueue: %s", err)
	}

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
			t.Errorf("uanble to enqueue message :: %v", err)
		}
	}

	for i := 0; i < numMessages; i++ {
		msg, err := bq.Peek()
		if err != nil {
			t.Errorf("uanble to peek message :: %v", err)
		}
		if err := bq.Dequeue(); err != nil {
			t.Errorf("uanble to dequeue message :: %v", err)
		}

		if len(msg) != lengths[i] {
			t.Errorf("enqueued and dequeued lengths don't match for msg no %d", i)
		}
	}

	if !bq.IsEmpty() {
		t.Errorf("queue should be empty")
	}
}

func TestEnqueueZeroLengthMessage(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, errQ := NewBigQueue(testDir)
	if errQ != nil {
		t.Errorf("unable to get BigQueue: %v", errQ)
	}

	emptyMsg := make([]byte, 0)
	if err := bq.Enqueue(emptyMsg); err != nil {
		t.Errorf("unable to enqueue empty message :: %v", err)
	}

	if bq.IsEmpty() {
		t.Errorf("IsEmpty should return false if empty message is present in queue")
	}

	deQueuedMsg, err := bq.Peek()
	if err != nil {
		t.Errorf("unable to peek empty message")
	}
	if err := bq.Dequeue(); err != nil {
		t.Errorf("unable to dequeue empty message")
	}

	if !bytes.Equal(deQueuedMsg, emptyMsg) {
		t.Errorf("dequeued and enqueued messages are not equal")
	}

	if !bq.IsEmpty() {
		t.Errorf("queue should be empty now")
	}
}

func TestEnqueueWhenMessageLengthFits(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	arenaSize := 4 * 1024
	bq, errQ := NewBigQueue(testDir, SetArenaSize(arenaSize))
	if errQ != nil {
		t.Errorf("unable to get BigQueue: %v", errQ)
	}

	msg1 := bytes.Repeat([]byte("a"), arenaSize-16)
	if err := bq.Enqueue(msg1); err != nil {
		t.Errorf("unable to enqueue msg1: %s", err)
	}

	msg2 := bytes.Repeat([]byte("b"), 3*arenaSize)
	if err := bq.Enqueue(msg2); err != nil {
		t.Errorf("unable to enqueue msg2: %s", err)
	}

	if err := bq.Dequeue(); err != nil {
		t.Errorf("unable to dequeue msg1: %s", err)
	}

	if err := bq.Dequeue(); err != nil {
		t.Errorf("unable to dequeue msg2: %s", err)
	}
}

func TestArenaSize(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, errQ := NewBigQueue(testDir, SetArenaSize(8*1024))
	if errQ != nil {
		t.Errorf("unable to get BigQueue: %v", errQ)
	}

	msg := []byte("abcdefghij")
	if err := bq.Enqueue(msg); err != nil {
		t.Errorf("enqueue failed :: %v", err)
	}

	if bq.IsEmpty() == true {
		t.Errorf("BigQueue should not be empty")
	}

	poppedMsg1, err := bq.Peek()
	if err != nil {
		t.Errorf("unable to peek :: %v", err)
	}

	if !bytes.Equal(msg, poppedMsg1) {
		t.Errorf("unequal length, eq: %s, dq: %s", string(msg), string(poppedMsg1))
	}

	poppedMsg2, err := bq.Peek()
	if err != nil {
		t.Errorf("unable to peek :: %v", err)
	}
	if err := bq.Dequeue(); err != nil {
		t.Errorf("unable to dequeue :: %v", err)
	}

	if !bytes.Equal(msg, poppedMsg2) {
		t.Errorf("unequal length, eq: %s, dq: %s", string(msg), string(poppedMsg2))
	}
}

func TestArenaSize2(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	arenaSize := os.Getpagesize() * 2
	bq, err := NewBigQueue(testDir, SetArenaSize(arenaSize))
	if err != nil {
		t.Errorf("unable to get BigQueue: %v", err)
	}

	msg := []byte("abcdefghij")
	for i := 0; i < arenaSize/len(msg)*4; i++ {
		if err := bq.Enqueue(msg); err != nil {
			t.Errorf("enqueue failed :: %v", err)
		}
	}

	if bq.IsEmpty() == true {
		t.Errorf("BigQueue should not be empty")
	}

	for i := 0; i < arenaSize/len(msg)*4; i++ {
		poppedMsg1, err := bq.Peek()
		if err != nil {
			t.Errorf("unable to peek :: %v", err)
		}

		if !bytes.Equal(msg, poppedMsg1) {
			t.Errorf("unequal length, eq: %s, dq: %s", string(msg), string(poppedMsg1))
		}

		poppedMsg2, err := bq.Peek()
		if err != nil {
			t.Errorf("unable to peek :: %v", err)
		}
		if err := bq.Dequeue(); err != nil {
			t.Errorf("unable to dequeue :: %v", err)
		}

		if !bytes.Equal(msg, poppedMsg2) {
			t.Errorf("unequal length, eq: %s, dq: %s", string(msg), string(poppedMsg2))
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
		t.Errorf("unable to get BigQueue: %v", err)
	}

	msg := []byte("abcdefgh")
	for i := 0; i < arenaSize/len(msg)*4; i++ {
		if err := bq.Enqueue(msg); err != nil {
			t.Errorf("enqueue failed :: %v", err)
		}
	}

	if bq.IsEmpty() == true {
		t.Errorf("BigQueue should not be empty")
	}

	for i := 0; i < arenaSize/len(msg)*4; i++ {
		poppedMsg1, err := bq.Peek()
		if err != nil {
			t.Errorf("unable to peek :: %v", err)
		}

		if !bytes.Equal(msg, poppedMsg1) {
			t.Errorf("unequal length, eq: %s, dq: %s", string(msg), string(poppedMsg1))
		}

		poppedMsg2, err := bq.Peek()
		if err != nil {
			t.Errorf("unable to peek :: %v", err)
		}
		if err := bq.Dequeue(); err != nil {
			t.Errorf("unable to dequeue :: %v", err)
		}

		if !bytes.Equal(msg, poppedMsg2) {
			t.Errorf("unequal length, eq: %s, dq: %s", string(msg), string(poppedMsg2))
		}
	}
}

func TestArenaSizeFail(t *testing.T) {
	testDir := path.Join(os.TempDir(), fmt.Sprintf("testdir_%d", rand.Intn(1000)))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	_, err := NewBigQueue(testDir, SetArenaSize(os.Getpagesize()/2))
	if err != ErrTooSmallArenaSize {
		t.Errorf("expected error: %v, got: %v", ErrTooSmallArenaSize, err)
	}
}
