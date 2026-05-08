package bigqueue

import (
	"fmt"
	"os"
	"testing"
)

func TestConsumerPersistence(t *testing.T) {
	dir := "testdata_consumer_persistence"
	defer os.RemoveAll(dir)

	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	// 1. Initial setup and write 1000 messages
	bq, err := NewMmapQueue(dir)
	if err != nil {
		t.Fatalf("failed to open queue: %v", err)
	}

	for i := 0; i < 1000; i++ {
		bq.Enqueue([]byte(fmt.Sprintf("msg-%04d", i)))
	}

	// 2. Create 100 consumers and each consume 500 messages
	consumerNames := make([]string, 100)
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("consumer-%d", i)
		consumerNames[i] = name
		c, _ := bq.NewConsumer(name)
		for j := 0; j < 500; j++ {
			msg, _ := c.Dequeue()
			expected := fmt.Sprintf("msg-%04d", j)
			if string(msg) != expected {
				t.Fatalf("consumer %s mismatch: exp %s, got %s", name, expected, string(msg))
			}
		}
	}
	bq.Close() // Simulator service stop

	// 3. Reopen queue and verify each consumer continues from index 500
	bq2, err := NewMmapQueue(dir)
	if err != nil {
		t.Fatalf("failed to reopen queue: %v", err)
	}
	defer bq2.Close()

	for _, name := range consumerNames {
		c, err := bq2.NewConsumer(name) // This should retrieve persisted offset
		if err != nil {
			t.Fatalf("failed to retrieve consumer %s: %v", name, err)
		}

		for j := 500; j < 1000; j++ {
			msg, err := c.Dequeue()
			if err != nil {
				t.Fatalf("consumer %s failed at msg %d: %v", name, j, err)
			}
			expected := fmt.Sprintf("msg-%04d", j)
			if string(msg) != expected {
				t.Fatalf("persisted consumer %s mismatch: exp %s, got %s", name, expected, string(msg))
			}
		}

		// Ensure no more data
		if !c.IsEmpty() {
			t.Errorf("expected consumer %s to be empty", name)
		}
	}
}
