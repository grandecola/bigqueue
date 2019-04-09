[![GoDoc](https://godoc.org/github.com/grandecola/bigqueue?status.svg)](https://godoc.org/github.com/grandecola/bigqueue) [![MIT license](http://img.shields.io/badge/license-MIT-brightgreen.svg)](http://opensource.org/licenses/MIT) [![Build Status](https://travis-ci.com/grandecola/bigqueue.svg?branch=master)](https://travis-ci.com/grandecola/bigqueue) [![codecov](https://codecov.io/gh/grandecola/bigqueue/branch/master/graph/badge.svg)](https://codecov.io/gh/grandecola/bigqueue)

 [![Go Report Card](https://goreportcard.com/badge/github.com/grandecola/bigqueue)](https://goreportcard.com/report/github.com/grandecola/bigqueue) [![golangci](https://golangci.com/badges/github.com/grandecola/bigqueue.svg)](https://golangci.com/r/github.com/grandecola/bigqueue) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/9933553bc3fb433d8d007cd917a64d90)](https://www.codacy.com/app/mangalaman93/bigqueue?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=grandecola/bigqueue&amp;utm_campaign=Badge_Grade) [![Maintainability](https://api.codeclimate.com/v1/badges/b3e1b2f184edd8150ddd/maintainability)](https://codeclimate.com/github/grandecola/bigqueue/maintainability) [![CodeFactor](https://www.codefactor.io/repository/github/grandecola/bigqueue/badge)](https://www.codefactor.io/repository/github/grandecola/bigqueue)

# bigqueue

`bigqueue` provides embedded, fast and persistent queue written in pure Go using
memory mapped (`mmap`) files. `bigqueue` is currently **not** thread safe. Check
out the roadmap for [v0.3.0](https://github.com/grandecola/bigqueue/milestone/4)
for more details on progress on thread safety. To use `bigqueue` in parallel
context, a **Write** lock needs to be acquired (even for `Read` APIs).

## Installation
```
go get github.com/grandecola/bigqueue
```

## Requirements
* Only works for `linux` and `darwin` OS
* Only works on Little Endian architecture

## Usage
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
bq, err := bigqueue.NewMmapQueue("path/to/queue", bigqueue.SetArenaSize(4*1024), bigqueue.SetMaxInMemArenas(10))
defer bq.Close()
```
In this case, bigqueue will never allocate more memory than `4KB*10=40KB`. This
memory is above and beyond the memory used in buffers for copying data.

Bigqueue allows to set periodic flush based on either elapsed time or number
of mutate (enqueue/dequeue) operations. Flush syncs the in memory changes of all
memory mapped files with disk. *This is a best effort flush*.

This is how you can set these options -
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
err := bq.Enqueue([]byte("elem"))   // size = 1
```

bigqueue allows writing string data directly, avoiding conversion to `[]byte`:
```go
err := bq.EnqueueString("elem")   // size = 2
```

Read from bigqueue:
```go
elem, err := bq.Peek()        // size = 2
err := bq.Dequeue()           // size = 1
```

we can also read string data from bigqueue:
```go
elem, err := bq.PeekString()  // size = 1
err := bq.Dequeue()           // size = 0
```

Check whether bigqueue has non zero elements:
```go
isEmpty := bq.IsEmpty()
```

## Benchmarks

Benchmarks are run on a Lenovo P52s laptop (i7-8550U, 8 core @1.80GHz, 15.4GB RAM)
having ubuntu 18.10, 64 bit machine.

Go version: 1.12

### NewMmapQueue
```go
BenchmarkNewMmapQueue/ArenaSize-4KB-8         	   50000	     39909 ns/op	    1381 B/op	      30 allocs/op
BenchmarkNewMmapQueue/ArenaSize-128KB-8       	   30000	     40594 ns/op	    1381 B/op	      30 allocs/op
BenchmarkNewMmapQueue/ArenaSize-4MB-8         	   30000	     40160 ns/op	    1381 B/op	      30 allocs/op
BenchmarkNewMmapQueue/ArenaSize-128MB-8       	   30000	     40510 ns/op	    1381 B/op	      30 allocs/op
```

### Enqueue
```go
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-8         	 2000000	       827 ns/op	      21 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-8         	 2000000	       814 ns/op	      21 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-128B/MaxMem-1GB-8          	 2000000	       733 ns/op	      23 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-8      	 2000000	       742 ns/op	      21 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-16KB/MaxMem-40KB-8         	   20000	     93169 ns/op	    2586 B/op	      52 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-16KB/MaxMem-1GB-8          	   20000	     84426 ns/op	    2585 B/op	      52 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-16KB/MaxMem-NoLimit-8      	   20000	     81964 ns/op	    2585 B/op	      52 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-1MB/MaxMem-1GB-8           	     300	   5227199 ns/op	  165919 B/op	    3328 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-1MB/MaxMem-NoLimit-8       	     300	   5365171 ns/op	  165918 B/op	    3328 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-128B/MaxMem-384KB-8      	10000000	       153 ns/op	       0 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-128B/MaxMem-1.25MB-8     	10000000	       147 ns/op	       0 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-128B/MaxMem-1GB-8        	10000000	       132 ns/op	       0 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-128B/MaxMem-NoLimit-8    	10000000	       130 ns/op	       0 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-16KB/MaxMem-384KB-8      	  200000	     11989 ns/op	      80 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-16KB/MaxMem-1.25MB-8     	  100000	     11561 ns/op	      80 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-16KB/MaxMem-1GB-8        	  200000	     12661 ns/op	      80 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-16KB/MaxMem-NoLimit-8    	  200000	     12289 ns/op	      80 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-1MB/MaxMem-384KB-8       	    2000	    759625 ns/op	    5133 B/op	     104 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-1MB/MaxMem-1.25MB-8      	    2000	    760162 ns/op	    5133 B/op	     104 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-1MB/MaxMem-1GB-8         	    2000	    772780 ns/op	    5133 B/op	     104 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-1MB/MaxMem-NoLimit-8     	    2000	    731294 ns/op	    5133 B/op	     104 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-128B/MaxMem-12MB-8         	10000000	       113 ns/op	       0 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-128B/MaxMem-40MB-8         	20000000	       116 ns/op	       0 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-128B/MaxMem-1GB-8          	20000000	       132 ns/op	       0 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-128B/MaxMem-NoLimit-8      	20000000	       125 ns/op	       0 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-16KB/MaxMem-12MB-8         	  200000	      8446 ns/op	       2 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-16KB/MaxMem-40MB-8         	  200000	      8695 ns/op	       2 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-16KB/MaxMem-1GB-8          	  200000	      9203 ns/op	       2 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-16KB/MaxMem-NoLimit-8      	  200000	      9807 ns/op	       2 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-1MB/MaxMem-12MB-8          	    2000	    536200 ns/op	     154 B/op	       3 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-1MB/MaxMem-40MB-8          	    3000	    540404 ns/op	     155 B/op	       3 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-1MB/MaxMem-1GB-8           	    3000	    601541 ns/op	     155 B/op	       3 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-1MB/MaxMem-NoLimit-8       	    3000	    623102 ns/op	     155 B/op	       3 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-128B/MaxMem-256MB-8      	20000000	       121 ns/op	       0 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-128B/MaxMem-1.25GB-8     	20000000	       126 ns/op	       0 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-128B/MaxMem-NoLimit-8    	20000000	       128 ns/op	       0 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-16KB/MaxMem-256MB-8      	  200000	      8344 ns/op	       0 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-16KB/MaxMem-1.25GB-8     	  200000	      9063 ns/op	       0 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-16KB/MaxMem-NoLimit-8    	  200000	      9743 ns/op	       0 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-1MB/MaxMem-256MB-8       	    3000	    550256 ns/op	       4 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-1MB/MaxMem-1.25GB-8      	    3000	    611339 ns/op	       4 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-1MB/MaxMem-NoLimit-8     	    3000	    617378 ns/op	       4 B/op	       0 allocs/op

```

### Dequeue (-benchtime=200us)
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

### Peek
```go
BenchmarkPeek/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-8         	20000000	       117 ns/op	     128 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-8         	20000000	       113 ns/op	     128 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4KB/MessageSize-128B/MaxMem-1GB-8          	20000000	       109 ns/op	     128 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-8      	20000000	       136 ns/op	     128 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4KB/MessageSize-16KB/MaxMem-40KB-8         	  300000	      3862 ns/op	   16384 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4KB/MessageSize-16KB/MaxMem-1GB-8          	  500000	      3858 ns/op	   16384 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4KB/MessageSize-16KB/MaxMem-NoLimit-8      	  300000	      3878 ns/op	   16384 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4KB/MessageSize-1MB/MaxMem-1GB-8           	   10000	    169672 ns/op	 1048576 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4KB/MessageSize-1MB/MaxMem-NoLimit-8       	   10000	    175354 ns/op	 1048576 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128KB/MessageSize-128B/MaxMem-384KB-8      	20000000	       109 ns/op	     128 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128KB/MessageSize-128B/MaxMem-1.25MB-8     	10000000	       132 ns/op	     128 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128KB/MessageSize-128B/MaxMem-1GB-8        	10000000	       114 ns/op	     128 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128KB/MessageSize-128B/MaxMem-NoLimit-8    	10000000	       129 ns/op	     128 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128KB/MessageSize-16KB/MaxMem-384KB-8      	  500000	      2848 ns/op	   16384 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128KB/MessageSize-16KB/MaxMem-1.25MB-8     	  500000	      2859 ns/op	   16384 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128KB/MessageSize-16KB/MaxMem-1GB-8        	  500000	      2841 ns/op	   16384 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128KB/MessageSize-16KB/MaxMem-NoLimit-8    	  500000	      2937 ns/op	   16384 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128KB/MessageSize-1MB/MaxMem-384KB-8       	    3000	    364824 ns/op	 1052850 B/op	      81 allocs/op
BenchmarkPeek/ArenaSize-128KB/MessageSize-1MB/MaxMem-1.25MB-8      	   10000	    165552 ns/op	 1048577 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128KB/MessageSize-1MB/MaxMem-1GB-8         	    5000	    302818 ns/op	 1048576 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128KB/MessageSize-1MB/MaxMem-NoLimit-8     	    5000	    278720 ns/op	 1048576 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4MB/MessageSize-128B/MaxMem-12MB-8         	10000000	       100 ns/op	     128 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4MB/MessageSize-128B/MaxMem-40MB-8         	20000000	       128 ns/op	     128 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4MB/MessageSize-128B/MaxMem-1GB-8          	10000000	       142 ns/op	     128 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4MB/MessageSize-128B/MaxMem-NoLimit-8      	10000000	       125 ns/op	     128 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4MB/MessageSize-16KB/MaxMem-12MB-8         	  500000	      2505 ns/op	   16384 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4MB/MessageSize-16KB/MaxMem-40MB-8         	  500000	      2586 ns/op	   16384 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4MB/MessageSize-16KB/MaxMem-1GB-8          	  500000	      2771 ns/op	   16384 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4MB/MessageSize-16KB/MaxMem-NoLimit-8      	  500000	      2440 ns/op	   16384 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4MB/MessageSize-1MB/MaxMem-12MB-8          	    5000	    201685 ns/op	 1048581 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4MB/MessageSize-1MB/MaxMem-40MB-8          	   10000	    202935 ns/op	 1048585 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4MB/MessageSize-1MB/MaxMem-1GB-8           	   10000	    204652 ns/op	 1048585 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4MB/MessageSize-1MB/MaxMem-NoLimit-8       	   10000	    206010 ns/op	 1048585 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128MB/MessageSize-128B/MaxMem-256MB-8      	20000000	       121 ns/op	     128 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128MB/MessageSize-128B/MaxMem-1.25GB-8     	10000000	       157 ns/op	     128 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128MB/MessageSize-128B/MaxMem-NoLimit-8    	10000000	       117 ns/op	     128 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128MB/MessageSize-16KB/MaxMem-256MB-8      	 1000000	      2694 ns/op	   16384 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128MB/MessageSize-16KB/MaxMem-1.25GB-8     	  500000	      2400 ns/op	   16384 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128MB/MessageSize-16KB/MaxMem-NoLimit-8    	  500000	      2548 ns/op	   16384 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128MB/MessageSize-1MB/MaxMem-256MB-8       	   10000	    204232 ns/op	 1048583 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128MB/MessageSize-1MB/MaxMem-1.25GB-8      	   10000	    205590 ns/op	 1048585 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128MB/MessageSize-1MB/MaxMem-NoLimit-8     	   10000	    204979 ns/op	 1048585 B/op	       1 allocs/op
```

**Note:** Before running benchmarks `ulimit` and `vm.max_map_count` parameters should be adjusted using below commands:
```
ulimit -n 50000
echo 262144 > /proc/sys/vm/max_map_count
```
