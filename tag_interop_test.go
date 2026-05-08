package bigqueue

import (
	"bytes"
	"os"
	"testing"
)

func TestEnqueueWithTagDequeue(t *testing.T) {
	dir := "testdata_tag_interop"
	defer os.RemoveAll(dir)

	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	bq, err := NewMmapQueue(dir)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer bq.Close()

	payload := []byte("hello world")
	tag := []byte("tag")

	// 1. Write data using EnqueueWithTag
	if err := bq.EnqueueWithTag(payload, tag); err != nil {
		t.Fatalf("enqueue with tag failed: %v", err)
	}

	// 2. Read using Dequeue (normal dequeue)
	// Expectation: get the full raw data as [tagLen][tag...][payload...]
	allData, err := bq.Dequeue()
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}

	expectedPrefix := append([]byte{byte(len(tag))}, tag...)
	expectedAll := append(expectedPrefix, payload...)

	if !bytes.Equal(allData, expectedAll) {
		t.Errorf("data mismatch: expected %v, got %v", expectedAll, allData)
	}
}

func TestEnqueueDequeueWithTag(t *testing.T) {
	dir := "testdata_tag_interop_2"
	defer os.RemoveAll(dir)

	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	bq, err := NewMmapQueue(dir)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer bq.Close()

	// 1. Basic test with empty tag format
	payload := []byte("plain data")
	dataWithEmptyTag := append([]byte{0}, payload...)

	if err := bq.Enqueue(dataWithEmptyTag); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	gotPayload, gotTag, err := bq.DequeueWithTag()
	if err != nil {
		t.Fatalf("dequeue with tag failed: %v", err)
	}

	if len(gotTag) != 0 {
		t.Errorf("expected empty tag, got %v", gotTag)
	}

	if !bytes.Equal(gotPayload, payload) {
		t.Errorf("payload mismatch: expected %v, got %v", payload, gotPayload)
	}

	// 2. Stress test with 100,000 random messages (strings, numbers, special chars, non-UTF8)
	count := 100000
	payloads := make([][]byte, count)
	for i := 0; i < count; i++ {
		// Generate random payload length (1 to 100 bytes)
		pLen := (i % 100) + 1
		p := make([]byte, pLen)
		for j := 0; j < pLen; j++ {
			// Fill with various types of data
			switch j % 4 {
			case 0:
				p[j] = byte('A' + (j % 26)) // Letters
			case 1:
				p[j] = byte('0' + (j % 10)) // Numbers
			case 2:
				p[j] = "!@#$%^&*"[j%8] // Special chars
			case 3:
				p[j] = byte(0x80 + (j % 128)) // Non-UTF8
			}
		}
		payloads[i] = p

		// Prepend 0 byte to signify empty tag
		data := append([]byte{0}, p...)
		if err := bq.Enqueue(data); err != nil {
			t.Fatalf("failed to enqueue message %d: %v", i, err)
		}
	}

	// 3. Verify all messages
	for i := 0; i < count; i++ {
		dp, dt, err := bq.DequeueWithTag()
		if err != nil {
			t.Fatalf("failed to dequeue message %d: %v", i, err)
		}
		if len(dt) != 0 {
			t.Fatalf("expected empty tag at message %d, got %v", i, dt)
		}
		if !bytes.Equal(dp, payloads[i]) {
			t.Fatalf("data mismatch at message %d", i)
		}
	}
}
