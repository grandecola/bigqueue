package main

import (
	"fmt"

	"github.com/grandecola/bigqueue"
)

func main() {
	bq, err := bigqueue.NewMmapQueue("bq")
	if err != nil {
		panic(err)
	}
	defer bq.Close()

	if err := bq.Enqueue([]byte("elem")); err != nil {
		panic(err)
	}

	if err := bq.EnqueueString("elem2"); err != nil {
		panic(err)
	}

	if bq.IsEmpty() {
		panic("queue cannot be empty")
	}

	if elem, err := bq.Dequeue(); err != nil {
		panic(err)
	} else {
		fmt.Println("expected: elem, dequeued:", string(elem))
	}

	if elem2, err := bq.DequeueString(); err != nil {
		panic(err)
	} else {
		fmt.Println("expected: elem2, dequeued:", elem2)
	}

	c1, err := bq.NewConsumer("consumer1")
	if err != nil {
		panic(err)
	}

	if c1.IsEmpty() {
		panic("consumer1: queue cannot be empty")
	}

	if elem, err := c1.Dequeue(); err != nil {
		panic(err)
	} else {
		fmt.Println("consumer1: expected: elem, dequeued:", string(elem))
	}

	c2, err := bq.FromConsumer("consumer2", c1)
	if err != nil {
		panic(err)
	}

	if c2.IsEmpty() {
		panic("consumer2: queue cannot be empty")
	}

	if elem2, err := c2.DequeueString(); err != nil {
		panic(err)
	} else {
		fmt.Println("consumer2: expected: elem2, dequeued:", elem2)
	}
}
