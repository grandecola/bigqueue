// Package bigqueue provides embedded, fast and persistent queue
// written in pure Go using memory mapped file
//
// Create or open a bigqueue:
//
//	bq, err := bigqueue.NewBigQueue("path/to/queue")
//	defer bq.Close()
//
// Bigqueue persists the data of the queue in multiple Arenas.
// Each Arena is a file on disk that is mapped into memory (RAM)
// using mmap syscall. Default size of each Arena is set to 128MB.
// It is possible to create a bigqueue with custom Arena size:
//
//	bq, err := bigqueue.NewBigQueue("path/to/queue", bigqueue.SetArenaSize(4*1024))
//	defer bq.Close()
//
// Write to bigqueue:
//
//	err := bq.Enqueue([]byte("elem"))   // size = 1
//
// Read from bigqueue:
//
//	elem, err := bq.Peek()              // size = 1
//	elem, err := bq.Dequeue()           // size = 0
//
// Check whether bigqueue has non zero elements:
//
//	isEmpty := bq.IsEmpty()
//
package bigqueue
