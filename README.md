[![GoDoc](https://godoc.org/github.com/grandecola/bigqueue?status.svg)](https://godoc.org/github.com/grandecola/bigqueue) [![MIT license](http://img.shields.io/badge/license-MIT-brightgreen.svg)](http://opensource.org/licenses/MIT) [![Build Status](https://travis-ci.com/grandecola/bigqueue.svg?branch=master)](https://travis-ci.com/grandecola/bigqueue) [![codecov](https://codecov.io/gh/grandecola/bigqueue/branch/master/graph/badge.svg)](https://codecov.io/gh/grandecola/bigqueue)

 [![Go Report Card](https://goreportcard.com/badge/github.com/grandecola/bigqueue)](https://goreportcard.com/report/github.com/grandecola/bigqueue) [![golangci](https://golangci.com/badges/github.com/grandecola/bigqueue.svg)](https://golangci.com/r/github.com/grandecola/bigqueue) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/9933553bc3fb433d8d007cd917a64d90)](https://www.codacy.com/app/mangalaman93/bigqueue?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=grandecola/bigqueue&amp;utm_campaign=Badge_Grade) [![Maintainability](https://api.codeclimate.com/v1/badges/b3e1b2f184edd8150ddd/maintainability)](https://codeclimate.com/github/grandecola/bigqueue/maintainability) [![CodeFactor](https://www.codefactor.io/repository/github/grandecola/bigqueue/badge)](https://www.codefactor.io/repository/github/grandecola/bigqueue)

# bigqueue

`bigqueue` provides embedded, fast and persistent queue written in pure Go using
memory mapped (`mmap`) files. `bigqueue` is now *thread safe* as well.

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
memory mapped files with disk. *This is a best effort flush*.

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
BenchmarkNewMmapQueue/ArenaSize-4KB-8         	     268	   4358988 ns/op	    2878 B/op	      46 allocs/op
BenchmarkNewMmapQueue/ArenaSize-128KB-8       	     274	   4296694 ns/op	    2818 B/op	      46 allocs/op
BenchmarkNewMmapQueue/ArenaSize-4MB-8         	     271	   4261047 ns/op	    2893 B/op	      46 allocs/op
BenchmarkNewMmapQueue/ArenaSize-128MB-8       	     282	   4283085 ns/op	    2830 B/op	      46 allocs/op
```

### Enqueue
```go
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-8         	 1202368	       990 ns/op	      50 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-8         	 1192279	      1011 ns/op	      50 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-8      	 1292190	       945 ns/op	      53 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-4KB/MaxMem-384KB-8       	  312024	      3566 ns/op	      49 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-4KB/MaxMem-1.25MB-8      	  316080	      3560 ns/op	      49 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-4KB/MaxMem-NoLimit-8     	  344344	      3377 ns/op	      51 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-128KB/MaxMem-12MB-8        	   14205	     83075 ns/op	      49 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-128KB/MaxMem-40MB-8        	   14340	     84964 ns/op	      49 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-128KB/MaxMem-NoLimit-8     	   15315	     87320 ns/op	      52 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-4MB/MaxMem-256MB-8       	     478	   2755725 ns/op	      49 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-4MB/MaxMem-1.25GB-8      	     456	   2876194 ns/op	      50 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-4MB/MaxMem-NoLimit-8     	     488	   2655537 ns/op	      51 B/op	       1 allocs/op
```

### EnqueueString
```go
BenchmarkEnqueueString/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-8   	 1224559	      1007 ns/op	      34 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-8   	 1000000	      1055 ns/op	      34 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-8         	 1339819	     24896 ns/op	      36 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-384KB-8          	  299647	      3636 ns/op	      33 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-1.25MB-8         	  286297	      3721 ns/op	      33 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-NoLimit-8        	  373315	     91819 ns/op	      35 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-12MB-8           	   14222	     91579 ns/op	      33 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-40MB-8           	   14296	     93319 ns/op	      33 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-NoLimit-8        	   14955	    276195 ns/op	      36 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-256MB-8          	     438	   3639666 ns/op	      33 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-1.25GB-8         	     432	   4169120 ns/op	      33 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-NoLimit-8        	     435	   5480456 ns/op	      34 B/op	       1 allocs/op
```

### Dequeue
```go
BenchmarkDequeue/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-8                  	 1000000	      6271 ns/op	     176 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-8                  	 1000000	      9296 ns/op	     176 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-8               	 4987659	       261 ns/op	     160 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-4KB/MaxMem-384KB-8                	  504364	      4802 ns/op	    4143 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-4KB/MaxMem-1.25MB-8               	  505152	      6133 ns/op	    4143 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-4KB/MaxMem-NoLimit-8              	  863947	      1381 ns/op	    4128 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-128KB/MaxMem-12MB-8                 	   26614	     44979 ns/op	  131128 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-128KB/MaxMem-40MB-8                 	   26200	     45006 ns/op	  131128 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-128KB/MaxMem-NoLimit-8              	   37497	     33536 ns/op	  131104 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-4MB/MaxMem-256MB-8                	     657	   1895278 ns/op	 4194386 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-4MB/MaxMem-1.25GB-8               	     793	   2168856 ns/op	 4194371 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-4MB/MaxMem-NoLimit-8              	     796	   1832712 ns/op	 4194336 B/op	       2 allocs/op
```

### DequeueString
```go
BenchmarkDequeueString/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-8            	 1000000	      6953 ns/op	     184 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-8            	 1000000	      9790 ns/op	     184 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-8         	 4167858	       302 ns/op	     168 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-384KB-8          	  510627	      4860 ns/op	    4151 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-1.25MB-8         	  412483	      5385 ns/op	    4151 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-NoLimit-8        	  807202	      1471 ns/op	    4136 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-12MB-8           	   26528	     45981 ns/op	  131136 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-40MB-8           	   26294	     45813 ns/op	  131136 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-NoLimit-8        	   36961	     33995 ns/op	  131112 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-256MB-8          	     684	   1872227 ns/op	 4194395 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-1.25GB-8         	     849	   1841189 ns/op	 4194381 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-NoLimit-8        	     811	   1746608 ns/op	 4194344 B/op	       3 allocs/op
```

**Note:** Before running benchmarks `ulimit` and `vm.max_map_count` parameters should be adjusted using below commands:
```
ulimit -n 50000
echo 262144 > /proc/sys/vm/max_map_count
```
