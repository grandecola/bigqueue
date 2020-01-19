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
BenchmarkNewMmapQueue/ArenaSize-4KB-8         	     279	   4457479 ns/op	    2429 B/op	      43 allocs/op
BenchmarkNewMmapQueue/ArenaSize-128KB-8       	     276	   4351779 ns/op	    2432 B/op	      43 allocs/op
BenchmarkNewMmapQueue/ArenaSize-4MB-8         	     272	   4315690 ns/op	    2429 B/op	      43 allocs/op
BenchmarkNewMmapQueue/ArenaSize-128MB-8       	     264	   4368081 ns/op	    2430 B/op	      43 allocs/op
```

### Enqueue
```go
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-8         	 1209154	       983 ns/op	      52 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-8         	 1232470	       993 ns/op	      52 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-128B/MaxMem-1GB-8          	 1340344	       916 ns/op	      52 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-8      	 1310674	       917 ns/op	      52 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-16KB/MaxMem-40KB-8         	   10000	    111495 ns/op	    2487 B/op	      49 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-16KB/MaxMem-1GB-8          	   12082	     99689 ns/op	    2463 B/op	      49 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-16KB/MaxMem-NoLimit-8      	   12202	     98691 ns/op	    2455 B/op	      49 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-1MB/MaxMem-1GB-8           	     192	   6331195 ns/op	  157534 B/op	    3073 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-1MB/MaxMem-NoLimit-8       	     186	   6323666 ns/op	  155235 B/op	    3073 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-128B/MaxMem-384KB-8      	 5513988	       219 ns/op	      32 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-128B/MaxMem-1.25MB-8     	 5216648	       221 ns/op	      32 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-128B/MaxMem-1GB-8        	 5916529	       205 ns/op	      32 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-128B/MaxMem-NoLimit-8    	 5902616	       204 ns/op	      32 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-16KB/MaxMem-384KB-8      	   82014	     14913 ns/op	     107 B/op	       2 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-16KB/MaxMem-1.25MB-8     	   83192	     14671 ns/op	     107 B/op	       2 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-16KB/MaxMem-1GB-8        	   93020	     13877 ns/op	     107 B/op	       2 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-16KB/MaxMem-NoLimit-8    	   91248	     13667 ns/op	     107 B/op	       2 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-1MB/MaxMem-384KB-8       	    1224	    942900 ns/op	    4893 B/op	      97 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-1MB/MaxMem-1.25MB-8      	    1251	    935153 ns/op	    4886 B/op	      97 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-1MB/MaxMem-1GB-8         	    1363	    890850 ns/op	    4861 B/op	      97 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-1MB/MaxMem-NoLimit-8     	    1350	    843793 ns/op	    4863 B/op	      97 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-128B/MaxMem-12MB-8         	 6291542	       185 ns/op	      32 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-128B/MaxMem-40MB-8         	 5953832	       187 ns/op	      32 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-128B/MaxMem-1GB-8          	 6454640	       179 ns/op	      32 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-128B/MaxMem-NoLimit-8      	 6096469	       179 ns/op	      32 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-16KB/MaxMem-12MB-8         	  101112	     11337 ns/op	      34 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-16KB/MaxMem-40MB-8         	  102528	     11392 ns/op	      34 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-16KB/MaxMem-1GB-8          	  111819	     11127 ns/op	      34 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-16KB/MaxMem-NoLimit-8      	  112003	     10867 ns/op	      34 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-1MB/MaxMem-12MB-8          	    1711	    700932 ns/op	     178 B/op	       4 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-1MB/MaxMem-40MB-8          	    1630	    735637 ns/op	     178 B/op	       4 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-1MB/MaxMem-1GB-8           	    1510	    683575 ns/op	     179 B/op	       4 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-1MB/MaxMem-NoLimit-8       	    1647	    751768 ns/op	     178 B/op	       4 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-128B/MaxMem-256MB-8      	 6570226	       183 ns/op	      32 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-128B/MaxMem-1.25GB-8     	 6649483	       181 ns/op	      32 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-128B/MaxMem-NoLimit-8    	 6607172	       179 ns/op	      32 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-16KB/MaxMem-256MB-8      	   93114	     11044 ns/op	      32 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-16KB/MaxMem-1.25GB-8     	  113638	     11125 ns/op	      32 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-16KB/MaxMem-NoLimit-8    	  112514	     10783 ns/op	      32 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-1MB/MaxMem-256MB-8       	    1702	    721401 ns/op	      36 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-1MB/MaxMem-1.25GB-8      	    1831	    767649 ns/op	      36 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-1MB/MaxMem-NoLimit-8     	    1670	    703520 ns/op	      36 B/op	       1 allocs/op
```

### Dequeue
```go
BenchmarkDequeue/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-8         	    1000	      1455 ns/op	      96 B/op	       3 allocs/op
BenchmarkDequeue/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-8         	    5000	      5540 ns/op	     467 B/op	      16 allocs/op
BenchmarkDequeue/ArenaSize-4KB/MessageSize-128B/MaxMem-1GB-8          	    5000	        58.7 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-8      	    5000	        67.3 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-4KB/MessageSize-16KB/MaxMem-40KB-8         	     100	    372351 ns/op	   32789 B/op	    1165 allocs/op
BenchmarkDequeue/ArenaSize-4KB/MessageSize-16KB/MaxMem-1GB-8          	    2000	       184 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-4KB/MessageSize-16KB/MaxMem-NoLimit-8      	    3000	       188 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-4KB/MessageSize-1MB/MaxMem-1GB-8           	     500	       501 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-4KB/MessageSize-1MB/MaxMem-NoLimit-8       	     500	       507 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-128B/MaxMem-384KB-8      	    5000	        99.7 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-128B/MaxMem-1.25MB-8     	    5000	        64.4 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-128B/MaxMem-1GB-8        	    5000	        75.9 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-128B/MaxMem-NoLimit-8    	    5000	        70.2 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-16KB/MaxMem-384KB-8      	     100	      5444 ns/op	     135 B/op	       4 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-16KB/MaxMem-1.25MB-8     	     100	      4769 ns/op	      52 B/op	       1 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-16KB/MaxMem-1GB-8        	    3000	       138 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-16KB/MaxMem-NoLimit-8    	    3000	       146 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-1MB/MaxMem-384KB-8       	     100	    795956 ns/op	   65072 B/op	    2322 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-1MB/MaxMem-1.25MB-8      	     100	    822750 ns/op	   65072 B/op	    2322 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-1MB/MaxMem-1GB-8         	    1000	       373 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-1MB/MaxMem-NoLimit-8     	    1000	       381 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-128B/MaxMem-12MB-8         	    5000	        65.2 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-128B/MaxMem-40MB-8         	    5000	        68.5 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-128B/MaxMem-1GB-8          	    5000	        60.3 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-128B/MaxMem-NoLimit-8      	    5000	        64.5 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-16KB/MaxMem-12MB-8         	    3000	      2669 ns/op	       3 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-16KB/MaxMem-40MB-8         	    5000	      2732 ns/op	       4 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-16KB/MaxMem-1GB-8          	    3000	       150 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-16KB/MaxMem-NoLimit-8      	    3000	       134 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-1MB/MaxMem-12MB-8          	     100	    122439 ns/op	     563 B/op	      18 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-1MB/MaxMem-40MB-8          	     100	    130529 ns/op	     480 B/op	      16 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-1MB/MaxMem-1GB-8           	    2000	    212331 ns/op	    7781 B/op	     277 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-1MB/MaxMem-NoLimit-8       	    2000	       219 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-128B/MaxMem-256MB-8      	    5000	        63.1 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-128B/MaxMem-1.25GB-8     	    5000	        62.9 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-128B/MaxMem-NoLimit-8    	    5000	        71.0 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-16KB/MaxMem-256MB-8      	    3000	       149 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-16KB/MaxMem-1.25GB-8     	    5000	       134 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-16KB/MaxMem-NoLimit-8    	    3000	       133 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-1MB/MaxMem-256MB-8       	    2000	    131369 ns/op	      10 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-1MB/MaxMem-1.25GB-8      	    2000	    115991 ns/op	       6 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-1MB/MaxMem-NoLimit-8     	    2000	       205 ns/op	       0 B/op	       0 allocs/op
```

**Note:** Before running benchmarks `ulimit` and `vm.max_map_count` parameters should be adjusted using below commands:
```
ulimit -n 50000
echo 262144 > /proc/sys/vm/max_map_count
```
