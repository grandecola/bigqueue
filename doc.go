// Package bigqueue provides embedded, fast and persistent queue
// written in pure Go using memory mapped file. bigqueue is
// currently not thread safe. To use bigqueue in parallel context,
// a Write lock needs to be acquired (even for Read APIs).
//
// Create or open a bigqueue:
//
//	bq, err := bigqueue.NewQueue("path/to/queue")
//	defer bq.Close()
//
// bigqueue persists the data of the queue in multiple Arenas.
// Each Arena is a file on disk that is mapped into memory (RAM)
// using mmap syscall. Default size of each Arena is set to 128MB.
// It is possible to create a bigqueue with custom Arena size:
//
//	bq, err := bigqueue.NewQueue("path/to/queue", bigqueue.SetArenaSize(4*1024))
//	defer bq.Close()
//
// bigqueue also allows setting up the maximum possible memory that it
// can use. By default, the maximum memory is set to [3 x Arena Size].
//
//	 bq, err := bigqueue.NewQueue("path/to/queue", bigqueue.SetArenaSize(4*1024),
//		     bigqueue.SetMaxInMemArenas(10))
//	 defer bq.Close()
//
// In this case, bigqueue will never allocate more memory than `4KB*10=40KB`. This
// memory is above and beyond the memory used in buffers for copying data.
//
// Bigqueue allows to set periodic flush based on either elapsed time or number
// of mutate (enqueue/dequeue) operations. Flush syncs the in memory changes of all
// memory mapped files with disk. *This is a best effort flush*. Elapsed time and
// number of mutate operations are only checked upon an enqueue/dequeue.
//
// This is how we can set these options:
//
//	bq, err := bigqueue.NewQueue("path/to/queue", bigqueue.SetPeriodicFlushOps(2))
//
// In this case, a flush is done after every two mutate operations.
//
//	bq, err := bigqueue.NewQueue("path/to/queue", bigqueue.SetPeriodicFlushDuration(time.Minute))
//
// In this case, a flush is done after one minute elapses and an Enqueue/Dequeue is called.
//
// Write to bigqueue:
//
//	err := bq.Enqueue([]byte("elem"))   // size = 1
//
// bigqueue allows writing string data directly, avoiding conversion to `[]byte`:
//
//	err := bq.EnqueueString("elem")   // size = 2
//
// Read from bigqueue:
//
//	elem, err := bq.Dequeue()
//
// we can also read string data from bigqueue:
//
//	elem, err := bq.DequeueString()
//
// Check whether bigqueue has non zero elements:
//
//	isEmpty := bq.IsEmpty()
//
// bigqueue allows reading data from bigqueue using consumers similar to Kafka. This allows
// multiple consumers from reading data at different offsets (not in thread safe manner yet).
// The offsets of each consumer are persisted on disk and can be retrieved by creating a
// consumer with the same name. Data will be read from the same offset where it was left off.
//
// We can create a new consumer as follows. The offsets of a new consumer are set at the
// start of the queue wherever the first non-deleted element is.
//
//	consumer, err := bq.NewConsumer("consumer")
//
// We can also copy an existing consumer. This will create a consumer that will have the
// same offsets into the queue as that of the existing consumer.
//
//	copyConsumer, err := bq.FromConsumer("copyConsumer", consumer)
//
// Now, read operations can be performed on the consumer:
//
//	isEmpty := consumer.IsEmpty()
//	elem, err := consumer.Dequeue()
//	elem, err := consumer.DequeueString()
package bigqueue
