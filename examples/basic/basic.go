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
}
