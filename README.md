[![GoDoc](https://godoc.org/github.com/grandecola/bigqueue?status.svg)](https://godoc.org/github.com/grandecola/bigqueue) [![MIT license](http://img.shields.io/badge/license-MIT-brightgreen.svg)](http://opensource.org/licenses/MIT) [![Build Status](https://travis-ci.com/grandecola/bigqueue.svg?branch=master)](https://travis-ci.com/grandecola/bigqueue) [![codecov](https://codecov.io/gh/grandecola/bigqueue/branch/master/graph/badge.svg)](https://codecov.io/gh/grandecola/bigqueue)

 [![Go Report Card](https://goreportcard.com/badge/github.com/grandecola/bigqueue)](https://goreportcard.com/report/github.com/grandecola/bigqueue) [![golangci](https://golangci.com/badges/github.com/grandecola/bigqueue.svg)](https://golangci.com/r/github.com/grandecola/bigqueue) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/9933553bc3fb433d8d007cd917a64d90)](https://www.codacy.com/app/mangalaman93/bigqueue?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=grandecola/bigqueue&amp;utm_campaign=Badge_Grade) [![Maintainability](https://api.codeclimate.com/v1/badges/b3e1b2f184edd8150ddd/maintainability)](https://codeclimate.com/github/grandecola/bigqueue/maintainability) [![CodeFactor](https://www.codefactor.io/repository/github/grandecola/bigqueue/badge)](https://www.codefactor.io/repository/github/grandecola/bigqueue)

# bigqueue

`bigqueue` provides embedded, fast and persistent queue written in pure Go using
memory mapped (`mmap`) files. `bigqueue` is currently **not** thread safe. Check
out the roadmap for [v0.3.0](https://github.com/grandecola/bigqueue/milestone/4)
for more details on progress on thread safety. To use `bigqueue` in parallel
context, a **write** lock needs to be acquired (even for `Read` APIs).

## Installation
```
go get github.com/grandecola/bigqueue
```

## Requirements
* Only works for `linux` and `darwin` OS
* Only works on Little Endian architecture

## Usage

### Standard API
Create or open a bigqueue:
```go
bq, err := bigqueue.NewMmapQueue("path/to/queue")
defer bq.Close()
```

bigqueue persists the data of the queue in multiple Arenas.
Each Arena is a file on disk that is mapped into memory (RAM)
using mmap syscall. Default size of each Arena is set to 128MB.
It is possible to create a bigqueue with custom Arena size:
```go
bq, err := bigqueue.NewMmapQueue("path/to/queue", bigqueue.SetArenaSize(4*1024))
defer bq.Close()
```

Bigqueue also allows setting up the maximum possible memory that it
can use. By default, the maximum memory is set to [3 x Arena Size].
```go
bq, err := bigqueue.NewQueue("path/to/queue", bigqueue.SetArenaSize(4*1024),
	    bigqueue.SetMaxInMemArenas(10))
defer bq.Close()
```
In this case, bigqueue will never allocate more memory than `4KB*10=40KB`. This
memory is above and beyond the memory used in buffers for copying data.

Bigqueue allows to set periodic flush based on either elapsed time or number
of mutate (enqueue/dequeue) operations. Flush syncs the in memory changes of all
memory mapped files with disk. *This is a best effort flush*. Elapsed time and
number of mutate operations are only checked upon an enqueue/dequeue.

This is how we can set these options:
```go
bq, err := bigqueue.NewQueue("path/to/queue", bigqueue.SetPeriodicFlushOps(2))
```
In this case, a flush is done after every two mutate operations.

```go
bq, err := bigqueue.NewQueue("path/to/queue", bigqueue.SetPeriodicFlushDuration(time.Minute))
```
In this case, a flush is done after one minute elapses and an Enqueue/Dequeue is called.

Write to bigqueue:
```go
err := bq.Enqueue([]byte("elem"))
```

bigqueue allows writing string data directly, avoiding conversion to `[]byte`:
```go
err := bq.EnqueueString("elem")
```

Read from bigqueue:
```go
elem, err := bq.Dequeue()
```

we can also read string data from bigqueue:
```go
elem, err := bq.DequeueString()
```

Check whether bigqueue has non zero elements:
```go
isEmpty := bq.IsEmpty()
```

### Advanced API
bigqueue allows reading data from bigqueue using consumers similar to Kafka. This allows
multiple consumers from reading data at different offsets (not in thread safe manner yet).
The offsets of each consumer are persisted on disk and can be retrieved by creating a
consumer with the same name. Data will be read from the same offset where it was left off.

We can create a new consumer as follows. The offsets of a new consumer are set at the
start of the queue wherever the first non-deleted element is.
```go
consumer, err := bq.NewConsumer("consumer")
```

We can also copy an existing consumer. This will create a consumer that will have the
same offsets into the queue as that of the existing consumer.
```go
copyConsumer, err := bq.FromConsumer("copyConsumer", consumer)
```

Now, read operations can be performed on the consumer:
```go
isEmpty := consumer.IsEmpty()
elem, err := consumer.Dequeue()
elem, err := consumer.DequeueString()
```

## Benchmarks

Benchmarks are run on a Lenovo P52s laptop (i7-8550U, 8 core @1.80GHz, 15.4GB RAM)
having ubuntu 18.10, 64 bit machine.

Go version: 1.13

### NewMmapQueue
```go
BenchmarkNewMmapQueue/ArenaSize-4KB-8         	     259	   4336293 ns/op	    2578 B/op	      44 allocs/op
BenchmarkNewMmapQueue/ArenaSize-128KB-8       	     277	   4292180 ns/op	    2577 B/op	      44 allocs/op
BenchmarkNewMmapQueue/ArenaSize-4MB-8         	     282	   4279293 ns/op	    2575 B/op	      44 allocs/op
BenchmarkNewMmapQueue/ArenaSize-128MB-8       	     276	   4294212 ns/op	    2577 B/op	      44 allocs/op
```

### Enqueue
```go
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-8         	 1227482	       974 ns/op	      50 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-8         	 1227622	       990 ns/op	      50 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-8      	 1349326	       905 ns/op	      52 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-4KB/MaxMem-384KB-8       	  295298	      3629 ns/op	      49 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-4KB/MaxMem-1.25MB-8      	  335749	      3684 ns/op	      49 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-4KB/MaxMem-NoLimit-8     	  371170	      3407 ns/op	      51 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-128KB/MaxMem-12MB-8        	   13934	     82812 ns/op	      49 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-128KB/MaxMem-40MB-8        	   14103	     84175 ns/op	      49 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-128KB/MaxMem-NoLimit-8     	   15004	     86985 ns/op	      52 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-4MB/MaxMem-256MB-8       	     450	   2908083 ns/op	      50 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-4MB/MaxMem-1.25GB-8      	     474	   3051462 ns/op	      49 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-4MB/MaxMem-NoLimit-8     	     469	   2928673 ns/op	      51 B/op	       1 allocs/op
```

### EnqueueString
```go
BenchmarkEnqueueString/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-8   	 1143330	      1067 ns/op	      34 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-8   	 1118235	      1111 ns/op	      34 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-8         	 1267702	     29356 ns/op	      36 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-384KB-8          	  333758	      3695 ns/op	      33 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-1.25MB-8         	  324952	      3810 ns/op	      33 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-NoLimit-8        	  361842	     90321 ns/op	      35 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-12MB-8           	   13420	     94311 ns/op	      33 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-40MB-8           	   13555	     87892 ns/op	      33 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-NoLimit-8        	   14716	    269216 ns/op	      36 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-256MB-8          	     393	   3820592 ns/op	      33 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-1.25GB-8         	     463	   4252438 ns/op	      34 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-NoLimit-8        	     386	   4935426 ns/op	      34 B/op	       1 allocs/op
```

### Dequeue
```go
BenchmarkDequeue/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-8                  	 1000000	      6303 ns/op	     176 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-8                  	 1000000	      9283 ns/op	     176 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-8               	 6215215	       208 ns/op	     160 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-4KB/MaxMem-384KB-8                	  506739	      4813 ns/op	    4143 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-4KB/MaxMem-1.25MB-8               	  517282	      6274 ns/op	    4143 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-4KB/MaxMem-NoLimit-8              	  892928	      1341 ns/op	    4128 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-128KB/MaxMem-12MB-8                 	   25336	     46375 ns/op	  131127 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-128KB/MaxMem-40MB-8                 	   25876	     46788 ns/op	  131127 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-128KB/MaxMem-NoLimit-8              	   36745	     34488 ns/op	  131104 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-4MB/MaxMem-256MB-8                	     734	   1740006 ns/op	 4194386 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-4MB/MaxMem-1.25GB-8               	     931	   1591828 ns/op	 4194375 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-4MB/MaxMem-NoLimit-8              	     990	   1437580 ns/op	 4194336 B/op	       2 allocs/op
```

### DequeueString
```go
BenchmarkDequeueString/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-8            	 1000000	      6760 ns/op	     184 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-8            	 1000000	      9584 ns/op	     184 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-8         	 5069414	       247 ns/op	     168 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-384KB-8          	  505219	      4913 ns/op	    4151 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-1.25MB-8         	  499880	      6123 ns/op	    4151 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-NoLimit-8        	  816019	      1398 ns/op	    4136 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-12MB-8           	   25624	     45954 ns/op	  131135 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-40MB-8           	   25681	     45620 ns/op	  131135 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-NoLimit-8        	   36438	     34198 ns/op	  131112 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-256MB-8          	     708	   1688158 ns/op	 4194398 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-1.25GB-8         	     966	   2062903 ns/op	 4194384 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-NoLimit-8        	    1008	   1469626 ns/op	 4194344 B/op	       3 allocs/op
```

**Note:** Before running benchmarks `ulimit` and `vm.max_map_count` parameters should be adjusted using below commands:
```
ulimit -n 50000
echo 262144 > /proc/sys/vm/max_map_count
```
