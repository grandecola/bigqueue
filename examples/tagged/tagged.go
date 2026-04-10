package main

import (
	"bytes"
	"fmt"

	"github.com/grandecola/bigqueue"
)

// msgType constants used as multi-byte tags to identify message schemas.
var (
	tagOrder   = []byte("ORDER")
	tagPayment = []byte("PAYMENT")
	tagRefund  = []byte("REFUND")
)

func main() {
	bq, err := bigqueue.NewMmapQueue("bq")
	if err != nil {
		panic(err)
	}
	defer bq.Close()

	// Enqueue messages with different type tags.
	messages := []struct {
		tag     []byte
		payload []byte
	}{
		{tagOrder, []byte(`{"id":1,"item":"book"}`)},
		{tagPayment, []byte(`{"id":1,"amount":9.99}`)},
		{tagRefund, []byte(`{"id":1,"reason":"damaged"}`)},
	}

	for _, m := range messages {
		if err := bq.EnqueueWithTag(m.payload, m.tag); err != nil {
			panic(err)
		}
	}

	// Dequeue and route messages by tag — no payload parsing needed.
	for !bq.IsEmpty() {
		payload, tag, err := bq.DequeueWithTag()
		if err != nil {
			panic(err)
		}

		switch {
		case bytes.Equal(tag, tagOrder):
			fmt.Printf("[ORDER]   %s\n", payload)
		case bytes.Equal(tag, tagPayment):
			fmt.Printf("[PAYMENT] %s\n", payload)
		case bytes.Equal(tag, tagRefund):
			fmt.Printf("[REFUND]  %s\n", payload)
		default:
			fmt.Printf("[UNKNOWN tag=%x] %s\n", tag, payload)
		}
	}
}
