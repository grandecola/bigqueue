package main

import (
    "fmt"
    "github.com/grandecola/bigqueue"
    "sync"
    "math/rand"
    "time"
    "sync/atomic"
)

func init() {
    rand.Seed(time.Now().Unix())
}

func sleep() {
    time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
}

var ops uint64
var total uint64 = 100
var arrInd uint64 = 0
var rwArray [201]int

func producer(bq *bigqueue.BigQueue, wg *sync.WaitGroup) {
    var i uint64
    for i = 0; i < total; i+= 1 {
        id := rand.Intn(100)
        err := bq.Enqueue([]byte(string(id)))
        ind := atomic.AddUint64(&arrInd, 1);
        rwArray[ind] = -1

        if err != nil {
            panic(err)
        }
    }
    wg.Done()
}

func consumer(bq *bigqueue.BigQueue, wg *sync.WaitGroup) {
    for ops != total {
        err := bq.Dequeue()
        if err != nil {
            continue
        }
        ind := atomic.AddUint64(&arrInd, 1);
        rwArray[ind] = +1
        atomic.AddUint64(&ops, 1)
    }
    wg.Done()
}

func main() {
    bq, err := bigqueue.NewBigQueue("bq")
    defer bq.Close()
    if err != nil {
        panic(err)
    }

    wg := sync.WaitGroup{}
    wg.Add(2)
    go producer(bq, &wg)
    go consumer(bq, &wg)

    wg.Wait()

    res := 0
    var i uint64
    for i = 1; i <= 2*total; i += 1 {
        res += rwArray[i]
        if res < 0 {
            fmt.Println(false)
        }
    }

    fmt.Println(bq.IsEmpty())
}
