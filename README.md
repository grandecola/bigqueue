# bigqueue [![Build Status](https://travis-ci.com/grandecola/bigqueue.svg?branch=master)](https://travis-ci.com/grandecola/bigqueue) [![Go Report Card](https://goreportcard.com/badge/github.com/grandecola/bigqueue)](https://goreportcard.com/report/github.com/grandecola/bigqueue) [![MIT license](http://img.shields.io/badge/license-MIT-brightgreen.svg)](http://opensource.org/licenses/MIT) [![GoDoc](https://godoc.org/github.com/grandecola/bigqueue?status.svg)](https://godoc.org/github.com/grandecola/bigqueue) [![codecov](https://codecov.io/gh/grandecola/bigqueue/branch/master/graph/badge.svg)](https://codecov.io/gh/grandecola/bigqueue)

`bigqueue` provides embedded, fast and persistent queue written
in pure Go using memory mapped (`mmap`) files.

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
isEmpty := bq.IsEmpty()
```

## Benchmarks

Benchmarks are run on AWS m4.xlarge (4 vCore, 16GB RAM) ubuntu 16.04 AMI, 64 bit machine.

Go version: 1.10.3

### NewBigQueue
```go
BenchmarkNewBigQueue/ArenaSize-4KB-4         	   30000	     52187 ns/op	    1227 B/op	      27 allocs/op
BenchmarkNewBigQueue/ArenaSize-128KB-4       	   30000	     51047 ns/op	    1227 B/op	      27 allocs/op
BenchmarkNewBigQueue/ArenaSize-4MB-4         	   30000	     50991 ns/op	    1227 B/op	      27 allocs/op
BenchmarkNewBigQueue/ArenaSize-128MB-4       	   30000	     50949 ns/op	    1227 B/op	      27 allocs/op
```

### Enqueue
```go
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-128B-4         	 2000000	       906 ns/op	      24 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-16KB-4         	   10000	    100219 ns/op	    2454 B/op	      52 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-1MB-4          	     200	   6386061 ns/op	  156937 B/op	    3328 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-128B-4       	10000000	       163 ns/op	       0 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-16KB-4       	  100000	     17110 ns/op	      76 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-1MB-4        	    2000	   1133015 ns/op	    4876 B/op	     104 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-128B-4         	10000000	       135 ns/op	       0 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-16KB-4         	  100000	     13185 ns/op	       2 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-1MB-4          	    2000	    913184 ns/op	     146 B/op	       3 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-128B-4       	10000000	       133 ns/op	       0 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-16KB-4       	  100000	     13080 ns/op	       0 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-1MB-4        	    2000	    913350 ns/op	       4 B/op	       0 allocs/op
```

### Dequeue (-benchtime=200us)
```go
BenchmarkDequeue/ArenaSize-4KB/MessageSize-128B-4         	    5000	        44.9 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-4KB/MessageSize-16KB-4         	    3000	       173 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-4KB/MessageSize-1MB-4          	    1000	       455 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-128B-4       	    5000	        66.9 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-16KB-4       	    2000	       111 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-1MB-4        	    1000	       291 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-128B-4         	    5000	        45.3 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-16KB-4         	    3000	       120 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-1MB-4          	    2000	       264 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-128B-4       	   10000	        45.1 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-16KB-4       	    3000	       114 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-1MB-4        	    2000	       253 ns/op	       0 B/op	       0 allocs/op
```

### Peek
```go
BenchmarkPeek/ArenaSize-4KB/MessageSize-128B-4         	20000000	       107 ns/op	     128 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4KB/MessageSize-16KB-4         	  300000	      4065 ns/op	   16384 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4KB/MessageSize-1MB-4          	   10000	    225151 ns/op	 1048576 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128KB/MessageSize-128B-4       	20000000	       107 ns/op	     128 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128KB/MessageSize-16KB-4       	  300000	      4149 ns/op	   16384 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128KB/MessageSize-1MB-4        	   10000	    219405 ns/op	 1048576 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4MB/MessageSize-128B-4         	20000000	       107 ns/op	     128 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4MB/MessageSize-16KB-4         	  300000	      4204 ns/op	   16384 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4MB/MessageSize-1MB-4          	    5000	    316045 ns/op	 1048576 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128MB/MessageSize-128B-4       	20000000	       107 ns/op	     128 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128MB/MessageSize-16KB-4       	  300000	      4185 ns/op	   16384 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128MB/MessageSize-1MB-4        	    5000	    314474 ns/op	 1048576 B/op	       1 allocs/op
```

**Note:** Before running benchmarks `ulimit` and `vm.max_map_count` parameters should be adjusted using below commands:
```
ulimit -n 50000
echo 262144 > /proc/sys/vm/max_map_count
```
