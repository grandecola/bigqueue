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
elem, err := bq.Peek()              // size = 1
elem, err := bq.Dequeue()           // size = 0
```

Check whether bigqueue has non zero elements:
```go
isEmpty := bq.IsEmpty()
```

## Benchmarks

Benchmarks were ran on AWS m4.xlarge(4 vCore, 16GB RAM) ubuntu machine.

Go version: 1.10.3
```go
BenchmarkNewBigQueue/ArenaSize-4KB-4         	               20000	     56287 ns/op	    1227 B/op	      27 allocs/op
BenchmarkNewBigQueue/ArenaSize-128KB-4       	               30000	     63173 ns/op	    1227 B/op	      27 allocs/op
BenchmarkNewBigQueue/ArenaSize-4MB-4         	               30000	     52720 ns/op	    1227 B/op	      27 allocs/op
BenchmarkNewBigQueue/ArenaSize-128MB-4       	               30000	     59261 ns/op	    1227 B/op	      27 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-128B-4         	 1000000	      1250 ns/op	      28 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-16KB-4         	   10000	    147465 ns/op	    2847 B/op	      64 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-1MB-4          	     200	   9454431 ns/op	  183445 B/op	    4103 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-128B-4       	10000000	       195 ns/op	       0 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-16KB-4       	  100000	     21245 ns/op	      88 B/op	       2 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-1MB-4        	    1000	   1345897 ns/op	    5624 B/op	     128 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-128B-4         	10000000	       135 ns/op	       0 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-16KB-4         	  100000	     13312 ns/op	       2 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-1MB-4          	    2000	    951234 ns/op	     174 B/op	       4 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-128B-4       	10000000	       136 ns/op	       0 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-16KB-4       	  100000	     13787 ns/op	       0 B/op	       0 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-1MB-4        	    2000	    939966 ns/op	       9 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-4KB/MessageSize-128B-4         	 3000000	      1060 ns/op	     131 B/op	       1 allocs/op
BenchmarkDequeue/ArenaSize-4KB/MessageSize-16KB-4         	   30000	    221819 ns/op	   16776 B/op	      13 allocs/op
BenchmarkDequeue/ArenaSize-4KB/MessageSize-1MB-4          	     500	  17043982 ns/op	 1073660 B/op	     771 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-128B-4       	10000000	       169 ns/op	     128 B/op	       1 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-16KB-4       	  100000	     11257 ns/op	   16396 B/op	       1 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-1MB-4        	    2000	    852711 ns/op	 1049363 B/op	      25 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-128B-4         	10000000	       144 ns/op	     128 B/op	       1 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-16KB-4         	  200000	      9962 ns/op	   16384 B/op	       1 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-1MB-4          	    3000	    598927 ns/op	 1048602 B/op	       1 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-128B-4       	10000000	       141 ns/op	     128 B/op	       1 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-16KB-4       	  200000	      9558 ns/op	   16384 B/op	       1 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-1MB-4        	    3000	    788294 ns/op	 1048579 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4KB/MessageSize-128B-4            	20000000	       109 ns/op	     128 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4KB/MessageSize-16KB-4            	  500000	      3719 ns/op	   16384 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4KB/MessageSize-1MB-4             	    5000	    269390 ns/op	 1048582 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128KB/MessageSize-128B-4          	20000000	       108 ns/op	     128 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128KB/MessageSize-16KB-4          	  500000	      3576 ns/op	   16384 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128KB/MessageSize-1MB-4           	    5000	    261303 ns/op	 1048577 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4MB/MessageSize-128B-4            	20000000	       108 ns/op	     128 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4MB/MessageSize-16KB-4            	  500000	      3543 ns/op	   16384 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-4MB/MessageSize-1MB-4             	    5000	    292992 ns/op	 1048577 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128MB/MessageSize-128B-4          	20000000	       108 ns/op	     128 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128MB/MessageSize-16KB-4          	  500000	      3583 ns/op	   16384 B/op	       1 allocs/op
BenchmarkPeek/ArenaSize-128MB/MessageSize-1MB-4           	    5000	    291712 ns/op	 1048577 B/op	       1 allocs/op
```

**Note:** Before running benchmarks ulimit and vm.max_map_count parameters should be adjusted 
using below commands:

```
ulimit -n 50000
echo 262144 > /proc/sys/vm/max_map_count
```
