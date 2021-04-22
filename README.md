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

Benchmarks are run on a Lenovo P52s laptop (i7-8550U, 8 core @1.80GHz, 15.4GB RAM)
having ubuntu 18.10, 64 bit machine.

Go version: 1.13

### NewMmapQueue
```go
BenchmarkNewMmapQueue/ArenaSize-4KB-8         	     279	   4206291 ns/op	    2401 B/op	      38 allocs/op
BenchmarkNewMmapQueue/ArenaSize-128KB-8       	     285	   4218564 ns/op	    2400 B/op	      38 allocs/op
BenchmarkNewMmapQueue/ArenaSize-4MB-8         	     288	   4251324 ns/op	    2401 B/op	      38 allocs/op
BenchmarkNewMmapQueue/ArenaSize-128MB-8       	     288	   4169841 ns/op	    2400 B/op	      38 allocs/op
```

### Enqueue
```go
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-8         	 1277236	       935 ns/op	      48 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-8         	 1268900	       968 ns/op	      48 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-8      	 1412449	       851 ns/op	      48 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-4KB/MaxMem-384KB-8       	  336560	      3584 ns/op	      47 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-4KB/MaxMem-1.25MB-8      	  335191	      3926 ns/op	      47 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-4KB/MaxMem-NoLimit-8     	  305390	      3354 ns/op	      47 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-128KB/MaxMem-12MB-8        	   13652	     86532 ns/op	      46 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-128KB/MaxMem-40MB-8        	   13773	     84258 ns/op	      46 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-128KB/MaxMem-NoLimit-8     	   13807	     89458 ns/op	      46 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-4MB/MaxMem-256MB-8       	     448	   2910430 ns/op	      46 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-4MB/MaxMem-1.25GB-8      	     442	   3123539 ns/op	      45 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-4MB/MaxMem-NoLimit-8     	     453	   3016637 ns/op	      46 B/op	       1 allocs/op
```

### EnqueueString
```go
BenchmarkEnqueueString/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-8   	 1274005	       963 ns/op	      32 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-8   	 1244082	       982 ns/op	      32 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-8         	 1432782	       887 ns/op	      32 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-384KB-8          	  300306	      3668 ns/op	      31 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-1.25MB-8         	  336058	      3684 ns/op	      31 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-NoLimit-8        	  365847	      3534 ns/op	      31 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-12MB-8           	   13741	     86820 ns/op	      30 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-40MB-8           	   13714	     86950 ns/op	      30 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-NoLimit-8        	   13804	     93003 ns/op	      30 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-256MB-8          	     417	   2893948 ns/op	      30 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-1.25GB-8         	     444	   3127065 ns/op	      29 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-NoLimit-8        	     429	   2910933 ns/op	      30 B/op	       1 allocs/op
```

### Dequeue
```go
BenchmarkDequeue/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-8                  	 1000000	      2901 ns/op	     175 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-8                  	 1000000	      2921 ns/op	     175 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-8               	 5159112	       246 ns/op	     160 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-4KB/MaxMem-384KB-8                	  488948	      3235 ns/op	    4142 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-4KB/MaxMem-1.25MB-8               	  524533	      3275 ns/op	    4142 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-4KB/MaxMem-NoLimit-8              	  851850	      1408 ns/op	    4128 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-128KB/MaxMem-12MB-8                 	   25760	     45141 ns/op	  131118 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-128KB/MaxMem-40MB-8                 	   26340	     44453 ns/op	  131118 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-128KB/MaxMem-NoLimit-8              	   36206	     40891 ns/op	  131104 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-4MB/MaxMem-256MB-8                	     633	   2284370 ns/op	 4194349 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-4MB/MaxMem-1.25GB-8               	     775	   1845506 ns/op	 4194345 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-4MB/MaxMem-NoLimit-8              	     808	   1930464 ns/op	 4194336 B/op	       2 allocs/op
```

### DequeueString
```go
BenchmarkDequeueString/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-8            	 1000000	      3065 ns/op	     183 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-8            	 1000000	      3045 ns/op	     183 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-8         	 4386606	       287 ns/op	     168 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-384KB-8          	  506248	      3375 ns/op	    4150 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-1.25MB-8         	  502797	      3352 ns/op	    4150 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-NoLimit-8        	  826635	      1391 ns/op	    4136 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-12MB-8           	   25773	     45963 ns/op	  131126 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-40MB-8           	   26059	     46397 ns/op	  131126 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-NoLimit-8        	   35088	     41857 ns/op	  131112 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-256MB-8          	     655	   1995248 ns/op	 4194357 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-1.25GB-8         	     786	   1864277 ns/op	 4194353 B/op	       3 allocs/op
BenchmarkDequeueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-NoLimit-8        	     668	   1973988 ns/op	 4194344 B/op	       3 allocs/op
```

**Note:** Before running benchmarks `ulimit` and `vm.max_map_count` parameters should be adjusted using below commands:
```
ulimit -n 50000
echo 262144 > /proc/sys/vm/max_map_count
```
