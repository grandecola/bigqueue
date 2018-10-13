package bigqueue

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

func TestIsEmpty(t *testing.T) {
	testDir := fmt.Sprintf(testPath, rand.Intn(1000))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, err := NewBigQueue(testDir)
	if err != nil {
		t.Errorf("unable to get big queue: %s", err)
	}

	if bq.IsEmpty() == false {
		t.Errorf("big queue length should have been 0")
	}

	msg := []byte("abcdefgh")
	if err := bq.Enqueue(msg); err != nil {
		t.Errorf("unable to enque message err: %s", err)
	}

	if bq.IsEmpty() == true {
		t.Errorf("IsEmpty should not have returned true after enqueue")
	}

	if _, err = bq.Dequeue(); err != nil {
		t.Errorf("unable to dequeue message err: %s", err)
	}

	if bq.IsEmpty() == false {
		t.Errorf("big queue length should have been 0")
	}
}

func TestPeek(t *testing.T) {
	testDir := fmt.Sprintf(testPath, rand.Intn(1000))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, err := NewBigQueue(testDir)
	if err != nil {
		t.Errorf("unable to get big queue: %s", err)
	}

	msg := []byte("abcdefghij")
	if err := bq.Enqueue(msg); err != nil {
		t.Errorf("enqueue failed err: %s", err)
	}

	frontMsg, err := bq.Peek()
	if err != nil {
		t.Errorf("peek failed: %s", err)
	}

	if !bytes.Equal(msg, frontMsg) {
		t.Errorf("lengths does not match: %s, actual: %s", string(msg), string(frontMsg))
	}
}

func TestEnqueueSmallMessage(t *testing.T) {
	testDir := fmt.Sprintf(testPath, rand.Intn(1000))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, err := NewBigQueue(testDir)
	if err != nil {
		t.Errorf("unable to get big queue: %s", err)
	}

	msg := []byte("abcdefghij")
	if err := bq.Enqueue(msg); err != nil {
		t.Errorf("enqueue failed err: %s", err)
	}

	if bq.IsEmpty() == true {
		t.Errorf("queue should not be empty")
	}

	poppedMsg, err := bq.Dequeue()
	if err != nil {
		t.Errorf("unable to dequeue: %s", err)
	}

	if !bytes.Equal(msg, poppedMsg) {
		t.Errorf("lengths are not equal, eq: %s, dq: %s", string(msg), string(poppedMsg))
	}
}

func TestEnqueueLargeMessage(t *testing.T) {
	testDir := fmt.Sprintf(testPath, rand.Intn(1000))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, err := NewBigQueue(testDir)
	if err != nil {
		t.Errorf("unable to get big queue: %s", err)
	}

	msg := make([]byte, 0)
	for i := 0; i < cDataFileSize-8; i++ {
		m := []byte("a")
		msg = append(msg, m...)
	}
	if err := bq.Enqueue(msg); err != nil {
		t.Errorf("enqueue failed err: %s", err)
	}

	deQueuedMsg, err := bq.Dequeue()
	if err != nil {
		t.Errorf("deque failed with err: %s", err)
	}

	if !bytes.Equal(deQueuedMsg, msg) {
		t.Errorf("deQueuedMsg and enQueued msg are not equal")
	}

	if bq.IsEmpty() == false {
		t.Errorf("Is empty should have returned true")
	}
}

func TestEnqueueOverlapLength(t *testing.T) {
	testDir := fmt.Sprintf(testPath, rand.Intn(1000))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, err := NewBigQueue(testDir)
	if err != nil {
		t.Errorf("unable to get big queue: %s", err)
	}

	msg1 := make([]byte, 0)
	for i := 0; i < cDataFileSize-12; i++ {
		m := []byte("a")
		msg1 = append(msg1, m...)
	}
	if err := bq.Enqueue(msg1); err != nil {
		t.Errorf("enqueue failed err: %s", err)
	}

	msg2 := make([]byte, 0)
	for i := 0; i < cDataFileSize-4; i++ {
		m := []byte("a")
		msg2 = append(msg2, m...)
	}
	if err := bq.Enqueue(msg2); err != nil {
		t.Errorf("enqueue failed err: %s", err)
	}

	deQueuedMsg1, err := bq.Dequeue()
	if err != nil {
		t.Errorf("deQueuedMsg1 failed with err: %s", err)
	}
	if bytes.Compare(deQueuedMsg1, msg1) != 0 {
		t.Errorf("deQueuedMsg1 and enQeuedMsg1 are not equal")
	}

	deQueuedMsg2, err := bq.Dequeue()
	if err != nil {
		t.Errorf("deQueuedMsg2 failed with err: %s", err)
	}
	if !bytes.Equal(deQueuedMsg2, msg2) {
		t.Errorf("deQueuedMsg2 and enQueuedMsg2 are not equal")
	}

	if bq.IsEmpty() == false {
		t.Errorf("queue should have been empty")
	}
}

func TestEnqueueLargeNumberOfMessages(t *testing.T) {
	testDir := fmt.Sprintf(testPath, rand.Intn(1000))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, err := NewBigQueue(testDir)
	if err != nil {
		t.Errorf("unable to get big queue: %s", err)
	}

	noOfMsgToEnqueue := 10
	enQueuedLengths := make([]int, 0)

	alphabets := "abcdefghijklmnopqrstuvwxyz"
	for i := 0; i < noOfMsgToEnqueue; i++ {
		msgLen := rand.Intn(cDataFileSize) + cDataFileSize
		enQueuedLengths = append(enQueuedLengths, msgLen)
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
			t.Errorf("uanble to enqueue msg: %s", err)
		}
	}

	for i := 0; i < noOfMsgToEnqueue; i++ {
		msg, err := bq.Dequeue()
		if err != nil {
			t.Errorf("uanble to dequeue msg: %s", err)
		}

		if len(msg) != enQueuedLengths[i] {
			t.Errorf("enqueued and dequeued length does not match for msg no %d", i)
		}
	}

	if !bq.IsEmpty() {
		t.Errorf("queue should be empty after all messages are dequeued")
	}
}

func TestEnqueueZeroLengthMessage(t *testing.T) {
	testDir := fmt.Sprintf(testPath, rand.Intn(1000))
	createTestDir(t, testDir)
	defer deleteTestDir(t, testDir)

	bq, err := NewBigQueue(testDir)
	if err != nil {
		t.Errorf("unable to get big queue: %s", err)
	}

	emptyMsg := make([]byte, 0)
	if err := bq.Enqueue(emptyMsg); err != nil {
		t.Errorf("unable to enqueue empty msg: %s", err)
	}

	if bq.IsEmpty() {
		t.Errorf("IsEmpty should not return true if empty msg is present in queue")
	}

	deQueuedMsg, err := bq.Dequeue()
	if err != nil {
		t.Errorf("unable to dequeue empty msg")
	}

	if !bytes.Equal(deQueuedMsg, emptyMsg) {
		t.Errorf("dequeued and enqueued msg are not equal")
	}

	if !bq.IsEmpty() {
		t.Errorf("queue should be empty now")
	}
}
