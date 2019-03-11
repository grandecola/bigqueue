# bigqueue

Package bigqueue provides embedded, fast and persistent queue
written in pure Go using memory mapped file

Create or open a bigqueue:

```go
bq, err := bigqueue.NewBigQueue("path/to/queue")
defer bq.Close()
```

Bigqueue persists the data of the queue in multiple Arenas.
Each Arena is a file on disk that is mapped into memory (RAM)
using mmap syscall. Default size of each Arena is set to 128MB.
It is possible to create a bigqueue with custom Arena size:

```go
bq, err := bigqueue.NewBigQueue("path/to/queue", bigqueue.SetArenaSize(4*1024))
defer bq.Close()
```

Bigqueue also allows setting up the maximum possible memory that it
can use. By default, the maximum memory is set to [3 x Arena Size].

```go
bq, err := bigqueue.NewBigQueue("path/to/queue", bigqueue.SetArenaSize(4*1024), bigqueue.SetMaxInMemArenas(10))
defer bq.Close()
```

In this case, bigqueue will never allocate more memory than `4KB*10=40KB`. This
memory is above and beyond the memory used in buffers for copying data.

Write to bigqueue:

```go
err := bq.Enqueue([]byte("elem"))   // size = 1
```

Read from bigqueue:

```go
elem, err := bq.Peek()        // size = 1
err := bq.Dequeue()           // size = 0
```

Check whether bigqueue has non zero elements:

```go
isEmpty := bq.IsEmpty()```


---

Created by [goreadme](https://github.com/apps/goreadme)
