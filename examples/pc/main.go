package main

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"

	"github.com/grandecola/bigqueue"
)

var (
	maxEnqueue   = 1000
	enqueueCount = int64(0)
	dequeueCount = int64(0)
)

func main() {
	bq, err := bigqueue.NewMmapQueue("bq")
	if err != nil {
		panic(err)
	}
	defer bq.Close()

	var wg sync.WaitGroup
	wg.Add(11)
	go producer(bq, &wg)
	go producer(bq, &wg)
	go producer(bq, &wg)
	go producer(bq, &wg)
	go producer(bq, &wg)
	go producer(bq, &wg)
	go consumer(bq, &wg)
	go consumer(bq, &wg)
	go consumer(bq, &wg)
	go consumer(bq, &wg)
	go consumer(bq, &wg)
	wg.Wait()

	fmt.Println("queue is empty:", bq.IsEmpty())
	fmt.Println("total enqueue:", enqueueCount)
	fmt.Println("total dequeue:", dequeueCount)
}

func producer(bq bigqueue.Queue, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		c := int(atomic.AddInt64(&enqueueCount, 1))
		if c > maxEnqueue {
			atomic.AddInt64(&enqueueCount, -1)
			break
		}

		elem := rand.Intn(10000)
		if err := bq.EnqueueString(string(elem)); err != nil {
			panic(err)
		}
	}

}

func consumer(bq bigqueue.Queue, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		c := int(atomic.LoadInt64(&dequeueCount))
		if c >= maxEnqueue {
			break
		}

		if err := bq.Dequeue(); err == bigqueue.ErrEmptyQueue {
			continue
		} else if err != nil {
			panic(err)
		}

		atomic.AddInt64(&dequeueCount, 1)
	}
}
