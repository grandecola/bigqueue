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
		elem, err := bq.Dequeue()
		if err != nil {
			panic(err)
		}
		fmt.Println("dequeue:", string(elem))
	}
}
