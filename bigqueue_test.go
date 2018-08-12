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

	dequeuedMsg, err := bq.Dequeue()
	if err != nil {
		t.Errorf("deque failed with err: %s", err)
	}

	if len(dequeuedMsg) != len(msg) {
		t.Errorf("lengths does not match, actual: %d, expected: %d", len(dequeuedMsg), len(msg))
	}
}

func TestEnqueOverlapLength(t *testing.T) {
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

	dequeuedMsg1, err := bq.Dequeue()
	if err != nil {
		t.Errorf("dequedMsg1 failed with err: %s", err)
	}
	if len(dequeuedMsg1) != len(msg1) {
		t.Errorf("dequedMsg1 len does not match, actual: %d, expected: %d", len(dequeuedMsg1), len(msg1))
	}

	dequeuedMsg2, err := bq.Dequeue()
	if err != nil {
		t.Errorf("dequedMsg2 failed with err: %s", err)
	}
	if len(dequeuedMsg2) != len(msg2) {
		t.Errorf("dequedMsg2 len does not match, actual: %d, expected: %d", len(dequeuedMsg2), len(msg2))
	}
}
