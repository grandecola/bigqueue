package main

import (
	"fmt"

	"github.com/grandecola/bigqueue"
)

func main() {
	bq, err := bigqueue.NewBigQueue("bq")
	if err != nil {
		panic(err)
	}
	defer bq.Close()

	err = bq.Enqueue([]byte("elem"))
	if err != nil {
		panic(err)
	}

	if !bq.IsEmpty() {
		if elem, err := bq.Peek(); err != nil {
			panic(err)
		} else {
			fmt.Println("expected: elem, peeked:", string(elem))
		}

		if err := bq.Dequeue(); err != nil {
			panic(err)
		}
	}
}
