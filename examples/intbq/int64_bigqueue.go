package main

import (
	"fmt"
	"strconv"

	"github.com/grandecola/bigqueue"
)

// IBigQueue provides an interface to big, fast and persistent queue
type IInt64BigQueue interface {
	IsEmpty() bool
	Peek() (int64, error)
	Enqueue(elem int64) error
	Dequeue() error
	Close() error
}

// BigQueue implements IBigQueue interface
type Int64BigQueue struct {
	bq bigqueue.IBigQueue
}

// NewBigQueue constructs an instance of *BigQueue
func NewInt64BigQueue(dir string, opts ...bigqueue.Option) (IInt64BigQueue, error) {

	bq, err := bigqueue.NewBigQueue(dir, opts...)

	if err != nil {
		return nil, err
	}

	return &Int64BigQueue{
		bq: bq,
	}, nil
}

// Peek returns the head of the bigInt queue
func (ibq *Int64BigQueue) Peek() (int64, error) {
	data, err := ibq.bq.Peek()
	if err != nil {
		return 0, err
	}

	num, err := strconv.Atoi(string(data))
	if err != nil {
		return 0, err
	}

	return int64(num), err
}

// IsEmpty returns true when bigInt queue is empty
func (ibq *Int64BigQueue) IsEmpty() bool { 
	return ibq.bq.IsEmpty()
}

// Close will close index and arena manager
func (ibq *Int64BigQueue) Close() error {
	return ibq.bq.Close()
}

// Dequeue removes an element from the bigInt queue
func (ibq *Int64BigQueue) Dequeue() error {
	return ibq.bq.Dequeue()
}

// Enqueue adds a new element to the tail of the bigInt queue
func (ibq *Int64BigQueue) Enqueue(message int64) error {
	stringMsg := strconv.Itoa(int(message))
	byteArrayMsg := []byte (stringMsg)
	return ibq.bq.Enqueue(byteArrayMsg)
}

func main() {
	ibq, err := NewInt64BigQueue("ibq")
	if err != nil {
		panic(err)
	}
	defer ibq.Close()

	err = ibq.Enqueue(454)
	if err != nil {
		panic(err)
	}

	if !ibq.IsEmpty() {
		if elem, err := ibq.Peek(); err != nil {
			panic(err)
		} else {
			fmt.Println("expected: 454, peeked:", elem)
		}

		if err := ibq.Dequeue(); err != nil {
			panic(err)
		}
	}
}


