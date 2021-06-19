[![PkgGoDev](https://pkg.go.dev/badge/github.com/grandecola/bigqueue)](https://pkg.go.dev/github.com/grandecola/bigqueue) [![MIT license](http://img.shields.io/badge/license-MIT-brightgreen.svg)](http://opensource.org/licenses/MIT) [![Build Status](https://travis-ci.com/grandecola/bigqueue.svg?branch=master)](https://travis-ci.com/grandecola/bigqueue) [![codecov](https://codecov.io/gh/grandecola/bigqueue/branch/master/graph/badge.svg)](https://codecov.io/gh/grandecola/bigqueue)

 [![Go Report Card](https://goreportcard.com/badge/github.com/grandecola/bigqueue)](https://goreportcard.com/report/github.com/grandecola/bigqueue) [![Codacy Badge](https://app.codacy.com/project/badge/Grade/f7a080f9ab2b4f7e9543b4eb8e404e2b)](https://www.codacy.com/gh/grandecola/bigqueue/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=grandecola/bigqueue&amp;utm_campaign=Badge_Grade) [![Maintainability](https://api.codeclimate.com/v1/badges/b3e1b2f184edd8150ddd/maintainability)](https://codeclimate.com/github/grandecola/bigqueue/maintainability) [![CodeFactor](https://www.codefactor.io/repository/github/grandecola/bigqueue/badge)](https://www.codefactor.io/repository/github/grandecola/bigqueue)

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

### Setup

```
goos: linux
goarch: amd64
cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
Go version: 1.16
```

### NewMmapQueue
```go
BenchmarkNewMmapQueue/ArenaSize-4KB-12         	     312	   3762844 ns/op	    2506 B/op	      36 allocs/op
BenchmarkNewMmapQueue/ArenaSize-128KB-12       	     310	   3897333 ns/op	    2506 B/op	      36 allocs/op
BenchmarkNewMmapQueue/ArenaSize-4MB-12         	     301	   4033893 ns/op	    2504 B/op	      36 allocs/op
BenchmarkNewMmapQueue/ArenaSize-128MB-12       	     309	   3954329 ns/op	    2504 B/op	      36 allocs/op
```

### Enqueue
```go
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-12         	 1021855	      1172 ns/op	      15 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-12         	  999122	      1178 ns/op	      15 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-12      	 1000000	      1027 ns/op	      20 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-4KB/MaxMem-384KB-12       	  258444	      4602 ns/op	      14 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-4KB/MaxMem-1.25MB-12      	  246780	      4610 ns/op	      14 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-4KB/MaxMem-NoLimit-12     	  271261	      4118 ns/op	      14 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-128KB/MaxMem-12MB-12        	   10000	    108440 ns/op	      14 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-128KB/MaxMem-40MB-12        	   10000	    108159 ns/op	      14 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-128KB/MaxMem-NoLimit-12     	   10000	    104991 ns/op	      14 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-4MB/MaxMem-256MB-12       	     330	   3619772 ns/op	      13 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-4MB/MaxMem-1.25GB-12      	     339	   3502254 ns/op	      13 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-4MB/MaxMem-NoLimit-12     	     336	   3478795 ns/op	      13 B/op	       0 allocs/op
```

### EnqueueString
```go
BenchmarkEnqueueString/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-12   	  843966	      1186 ns/op	      15 B/op	       0 allocs/op
BenchmarkEnqueueString/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-12   	 1000000	      1180 ns/op	      15 B/op	       0 allocs/op
BenchmarkEnqueueString/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-12         	 1000000	      1026 ns/op	      15 B/op	       0 allocs/op
BenchmarkEnqueueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-384KB-12          	  257824	      4642 ns/op	      14 B/op	       0 allocs/op
BenchmarkEnqueueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-1.25MB-12         	  256230	      4621 ns/op	      14 B/op	       0 allocs/op
BenchmarkEnqueueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-NoLimit-12        	  266560	      4101 ns/op	      14 B/op	       0 allocs/op
BenchmarkEnqueueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-12MB-12           	   10000	    107929 ns/op	      14 B/op	       0 allocs/op
BenchmarkEnqueueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-40MB-12           	   10000	    107948 ns/op	      14 B/op	       0 allocs/op
BenchmarkEnqueueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-NoLimit-12        	   11434	    103482 ns/op	      13 B/op	       0 allocs/op
BenchmarkEnqueueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-256MB-12          	     333	   3650641 ns/op	      13 B/op	       0 allocs/op
BenchmarkEnqueueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-1.25GB-12         	     339	   3559835 ns/op	      13 B/op	       0 allocs/op
BenchmarkEnqueueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-NoLimit-12        	     334	   3546090 ns/op	      13 B/op	       0 allocs/op
```

### Dequeue
```go
BenchmarkDequeue/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-12                  	 1000000	      3201 ns/op	     142 B/op	       1 allocs/op
BenchmarkDequeue/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-12                  	 1000000	      3187 ns/op	     142 B/op	       1 allocs/op
BenchmarkDequeue/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-12               	 6737412	       174.0 ns/op	     128 B/op	       1 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-4KB/MaxMem-384KB-12                	  502522	      3478 ns/op	    4109 B/op	       1 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-4KB/MaxMem-1.25MB-12               	  516555	      3509 ns/op	    4109 B/op	       1 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-4KB/MaxMem-NoLimit-12              	 1000000	      1156 ns/op	    4096 B/op	       1 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-128KB/MaxMem-12MB-12                 	   29844	     39677 ns/op	  131085 B/op	       1 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-128KB/MaxMem-40MB-12                 	   30626	     39388 ns/op	  131085 B/op	       1 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-128KB/MaxMem-NoLimit-12              	   45805	     26247 ns/op	  131072 B/op	       1 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-4MB/MaxMem-256MB-12                	    1005	   1241554 ns/op	 4194316 B/op	       1 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-4MB/MaxMem-1.25GB-12               	    1257	   1164477 ns/op	 4194314 B/op	       1 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-4MB/MaxMem-NoLimit-12              	    1260	    884842 ns/op	 4194304 B/op	       1 allocs/op
```

### DequeueString
```go
BenchmarkDequeueString/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-12            	 1000000	      3200 ns/op	     142 B/op	       1 allocs/op
BenchmarkDequeueString/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-12            	 1000000	      3206 ns/op	     142 B/op	       1 allocs/op
BenchmarkDequeueString/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-12         	 6239718	       188.8 ns/op	     128 B/op	       1 allocs/op
BenchmarkDequeueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-384KB-12          	  501561	      3511 ns/op	    4109 B/op	       1 allocs/op
BenchmarkDequeueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-1.25MB-12         	  507860	      3535 ns/op	    4109 B/op	       1 allocs/op
BenchmarkDequeueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-NoLimit-12        	 1000000	      1236 ns/op	    4096 B/op	       1 allocs/op
BenchmarkDequeueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-12MB-12           	   29692	     39532 ns/op	  131085 B/op	       1 allocs/op
BenchmarkDequeueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-40MB-12           	   30268	     39709 ns/op	  131085 B/op	       1 allocs/op
BenchmarkDequeueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-NoLimit-12        	   46911	     25956 ns/op	  131072 B/op	       1 allocs/op
BenchmarkDequeueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-256MB-12          	     968	   1254574 ns/op	 4194316 B/op	       1 allocs/op
BenchmarkDequeueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-1.25GB-12         	    1429	   1175763 ns/op	 4194314 B/op	       1 allocs/op
BenchmarkDequeueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-NoLimit-12        	    1364	    865977 ns/op	 4194304 B/op	       1 allocs/op$$
```

**Note:** Before running benchmarks `ulimit` and `vm.max_map_count` parameters should be adjusted using below commands:
```
ulimit -n 50000
echo 262144 > /proc/sys/vm/max_map_count
```
