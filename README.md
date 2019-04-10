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
```
BenchmarkNewMmapQueue/ArenaSize-4KB-8         	   50000	     39105 ns/op	    1952 B/op	      38 allocs/op
BenchmarkNewMmapQueue/ArenaSize-128KB-8       	   50000	     39966 ns/op	    1952 B/op	      38 allocs/op
BenchmarkNewMmapQueue/ArenaSize-4MB-8         	   30000	     44026 ns/op	    1952 B/op	      38 allocs/op
BenchmarkNewMmapQueue/ArenaSize-128MB-8       	   30000	     43401 ns/op	    1952 B/op	      38 allocs/op
```

### Enqueue
```
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-8         	 2000000	      1037 ns/op	      53 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-8         	 1000000	      1037 ns/op	      53 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-8      	 2000000	       992 ns/op	      55 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-4KB/MaxMem-384KB-8       	  500000	      3261 ns/op	      51 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-4KB/MaxMem-1.25MB-8      	  500000	      3268 ns/op	      51 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128KB/MessageSize-4KB/MaxMem-NoLimit-8     	  500000	      3101 ns/op	      51 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-128KB/MaxMem-12MB-8        	   20000	     68541 ns/op	      51 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-128KB/MaxMem-40MB-8        	   20000	     65840 ns/op	      51 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-4MB/MessageSize-128KB/MaxMem-NoLimit-8     	   20000	     74252 ns/op	      51 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-4MB/MaxMem-256MB-8       	    1000	   2292552 ns/op	      50 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-4MB/MaxMem-1.25GB-8      	    1000	   3069585 ns/op	      50 B/op	       1 allocs/op
BenchmarkEnqueue/ArenaSize-128MB/MessageSize-4MB/MaxMem-NoLimit-8     	    1000	   2582143 ns/op	      50 B/op	       1 allocs/op
```

### EnqueueString
```
BenchmarkEnqueueString/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-8   	 1000000	      1074 ns/op	      37 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-8   	 1000000	      1069 ns/op	      37 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-8         	 2000000	      1008 ns/op	      37 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-384KB-8          	  500000	      3196 ns/op	      35 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-1.25MB-8         	  500000	      3265 ns/op	      35 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-128KB/MessageSize-4KB/MaxMem-NoLimit-8        	  500000	      3100 ns/op	      35 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-12MB-8           	   20000	     69388 ns/op	      35 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-40MB-8           	   20000	     67953 ns/op	      35 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-4MB/MessageSize-128KB/MaxMem-NoLimit-8        	   20000	     74339 ns/op	      35 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-256MB-8          	    1000	   2305918 ns/op	      34 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-1.25GB-8         	    1000	   3408842 ns/op	      34 B/op	       1 allocs/op
BenchmarkEnqueueString/ArenaSize-128MB/MessageSize-4MB/MaxMem-NoLimit-8        	    1000	   2827411 ns/op	      34 B/op	       1 allocs/op
```

### Dequeue (-benchtime=200us)
```
BenchmarkDequeue/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-8         	     200	      2748 ns/op	      16 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-8         	     500	      3339 ns/op	      30 B/op	       1 allocs/op
BenchmarkDequeue/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-8      	     500	       738 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-4KB/MaxMem-384KB-8       	     100	      3158 ns/op	       6 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-4KB/MaxMem-1.25MB-8      	     500	      3784 ns/op	      25 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128KB/MessageSize-4KB/MaxMem-NoLimit-8     	    1000	       532 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-128KB/MaxMem-12MB-8        	     100	      8156 ns/op	       6 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-128KB/MaxMem-40MB-8        	    1000	     20373 ns/op	      77 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-4MB/MessageSize-128KB/MaxMem-NoLimit-8     	    1000	       324 ns/op	       0 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-4MB/MaxMem-256MB-8       	     100	    242637 ns/op	       6 B/op	       0 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-4MB/MaxMem-1.25GB-8      	    1000	    450719 ns/op	      77 B/op	       2 allocs/op
BenchmarkDequeue/ArenaSize-128MB/MessageSize-4MB/MaxMem-NoLimit-8     	    1000	       343 ns/op	       0 B/op	       0 allocs/op
```

### Peek
```
BenchmarkPeek/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-8         	 5000000	       328 ns/op	     160 B/op	       2 allocs/op
BenchmarkPeek/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-8         	 5000000	       352 ns/op	     160 B/op	       2 allocs/op
BenchmarkPeek/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-8      	 5000000	       330 ns/op	     160 B/op	       2 allocs/op
BenchmarkPeek/ArenaSize-128KB/MessageSize-4KB/MaxMem-384KB-8       	 2000000	       828 ns/op	    4128 B/op	       2 allocs/op
BenchmarkPeek/ArenaSize-128KB/MessageSize-4KB/MaxMem-1.25MB-8      	 2000000	       833 ns/op	    4128 B/op	       2 allocs/op
BenchmarkPeek/ArenaSize-128KB/MessageSize-4KB/MaxMem-NoLimit-8     	 2000000	       832 ns/op	    4128 B/op	       2 allocs/op
BenchmarkPeek/ArenaSize-4MB/MessageSize-128KB/MaxMem-12MB-8        	  100000	     14757 ns/op	  131104 B/op	       2 allocs/op
BenchmarkPeek/ArenaSize-4MB/MessageSize-128KB/MaxMem-40MB-8        	  100000	     15957 ns/op	  131104 B/op	       2 allocs/op
BenchmarkPeek/ArenaSize-4MB/MessageSize-128KB/MaxMem-NoLimit-8     	  100000	     19017 ns/op	  131104 B/op	       2 allocs/op
BenchmarkPeek/ArenaSize-128MB/MessageSize-4MB/MaxMem-256MB-8       	    2000	    840638 ns/op	 4194336 B/op	       2 allocs/op
BenchmarkPeek/ArenaSize-128MB/MessageSize-4MB/MaxMem-1.25GB-8      	    2000	    829111 ns/op	 4194338 B/op	       2 allocs/op
BenchmarkPeek/ArenaSize-128MB/MessageSize-4MB/MaxMem-NoLimit-8     	    2000	    832551 ns/op	 4194338 B/op	       2 allocs/op
```

### PeekString
```
BenchmarkPeekString/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-8   	 3000000	       378 ns/op	     168 B/op	       3 allocs/op
BenchmarkPeekString/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-8   	 5000000	       380 ns/op	     168 B/op	       3 allocs/op
BenchmarkPeekString/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-8         	 5000000	       379 ns/op	     168 B/op	       3 allocs/op
BenchmarkPeekString/ArenaSize-128KB/MessageSize-4KB/MaxMem-384KB-8          	 2000000	       891 ns/op	    4136 B/op	       3 allocs/op
BenchmarkPeekString/ArenaSize-128KB/MessageSize-4KB/MaxMem-1.25MB-8         	 2000000	       952 ns/op	    4136 B/op	       3 allocs/op
BenchmarkPeekString/ArenaSize-128KB/MessageSize-4KB/MaxMem-NoLimit-8        	 2000000	       902 ns/op	    4136 B/op	       3 allocs/op
BenchmarkPeekString/ArenaSize-4MB/MessageSize-128KB/MaxMem-12MB-8           	  100000	     16434 ns/op	  131112 B/op	       3 allocs/op
BenchmarkPeekString/ArenaSize-4MB/MessageSize-128KB/MaxMem-40MB-8           	  100000	     14933 ns/op	  131112 B/op	       3 allocs/op
BenchmarkPeekString/ArenaSize-4MB/MessageSize-128KB/MaxMem-NoLimit-8        	  100000	     18725 ns/op	  131112 B/op	       3 allocs/op
BenchmarkPeekString/ArenaSize-128MB/MessageSize-4MB/MaxMem-256MB-8          	    2000	    828577 ns/op	 4194344 B/op	       3 allocs/op
BenchmarkPeekString/ArenaSize-128MB/MessageSize-4MB/MaxMem-1.25GB-8         	    2000	    828485 ns/op	 4194344 B/op	       3 allocs/op
BenchmarkPeekString/ArenaSize-128MB/MessageSize-4MB/MaxMem-NoLimit-8        	    2000	    827706 ns/op	 4194344 B/op	       3 allocs/op
```

### Parallel Enqueue, Peek & Dequeue (50% read)
```
BenchmarkParallel/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-2         	  300000	    118959 ns/op	    9517 B/op	     338 allocs/op
BenchmarkParallel/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-4         	  200000	     81196 ns/op	    6221 B/op	     220 allocs/op
BenchmarkParallel/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-6         	  200000	     84231 ns/op	    6236 B/op	     221 allocs/op
BenchmarkParallel/ArenaSize-4KB/MessageSize-128B/MaxMem-12KB-8         	  200000	     84659 ns/op	    6267 B/op	     222 allocs/op

BenchmarkParallel/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-2         	  300000	    116938 ns/op	    9326 B/op	     331 allocs/op
BenchmarkParallel/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-4         	  200000	     80793 ns/op	    6175 B/op	     218 allocs/op
BenchmarkParallel/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-6         	  200000	     83311 ns/op	    6196 B/op	     219 allocs/op
BenchmarkParallel/ArenaSize-4KB/MessageSize-128B/MaxMem-40KB-8         	  200000	     83865 ns/op	    6215 B/op	     220 allocs/op

BenchmarkParallel/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-2      	  500000	    131874 ns/op	   10405 B/op	     370 allocs/op
BenchmarkParallel/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-4      	  300000	     80649 ns/op	    6224 B/op	     220 allocs/op
BenchmarkParallel/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-6      	  300000	     85044 ns/op	    6251 B/op	     221 allocs/op
BenchmarkParallel/ArenaSize-4KB/MessageSize-128B/MaxMem-NoLimit-8      	  300000	     83882 ns/op	    6257 B/op	     221 allocs/op

BenchmarkParallel/ArenaSize-128KB/MessageSize-4KB/MaxMem-384KB-2       	  200000	     73409 ns/op	    6876 B/op	     197 allocs/op
BenchmarkParallel/ArenaSize-128KB/MessageSize-4KB/MaxMem-384KB-4       	  200000	     74832 ns/op	    6921 B/op	     198 allocs/op
BenchmarkParallel/ArenaSize-128KB/MessageSize-4KB/MaxMem-384KB-6       	  200000	     77923 ns/op	    6901 B/op	     197 allocs/op
BenchmarkParallel/ArenaSize-128KB/MessageSize-4KB/MaxMem-384KB-8       	  200000	     78874 ns/op	    6956 B/op	     199 allocs/op

BenchmarkParallel/ArenaSize-128KB/MessageSize-4KB/MaxMem-1.25MB-2      	  200000	     72669 ns/op	    6833 B/op	     195 allocs/op
BenchmarkParallel/ArenaSize-128KB/MessageSize-4KB/MaxMem-1.25MB-4      	  200000	     76367 ns/op	    6836 B/op	     195 allocs/op
BenchmarkParallel/ArenaSize-128KB/MessageSize-4KB/MaxMem-1.25MB-6      	  200000	     77391 ns/op	    6830 B/op	     195 allocs/op
BenchmarkParallel/ArenaSize-128KB/MessageSize-4KB/MaxMem-1.25MB-8      	  200000	     77202 ns/op	    6824 B/op	     195 allocs/op

BenchmarkParallel/ArenaSize-128KB/MessageSize-4KB/MaxMem-NoLimit-2     	  200000	     50378 ns/op	    5039 B/op	     131 allocs/op
BenchmarkParallel/ArenaSize-128KB/MessageSize-4KB/MaxMem-NoLimit-4     	  200000	     53093 ns/op	    5047 B/op	     131 allocs/op
BenchmarkParallel/ArenaSize-128KB/MessageSize-4KB/MaxMem-NoLimit-6     	  200000	     53375 ns/op	    5054 B/op	     131 allocs/op
BenchmarkParallel/ArenaSize-128KB/MessageSize-4KB/MaxMem-NoLimit-8     	  200000	     53314 ns/op	    5045 B/op	     131 allocs/op

BenchmarkParallel/ArenaSize-4MB/MessageSize-128KB/MaxMem-12MB-2        	   20000	     93825 ns/op	   43838 B/op	      21 allocs/op
BenchmarkParallel/ArenaSize-4MB/MessageSize-128KB/MaxMem-12MB-4        	   20000	     98604 ns/op	   43839 B/op	      21 allocs/op
BenchmarkParallel/ArenaSize-4MB/MessageSize-128KB/MaxMem-12MB-6        	   20000	     97676 ns/op	   43923 B/op	      21 allocs/op
BenchmarkParallel/ArenaSize-4MB/MessageSize-128KB/MaxMem-12MB-8        	   20000	     97217 ns/op	   43905 B/op	      21 allocs/op

BenchmarkParallel/ArenaSize-4MB/MessageSize-128KB/MaxMem-40MB-2        	   20000	     92797 ns/op	   43854 B/op	      20 allocs/op
BenchmarkParallel/ArenaSize-4MB/MessageSize-128KB/MaxMem-40MB-4        	   20000	     95689 ns/op	   43878 B/op	      20 allocs/op
BenchmarkParallel/ArenaSize-4MB/MessageSize-128KB/MaxMem-40MB-6        	   20000	     96982 ns/op	   43894 B/op	      20 allocs/op
BenchmarkParallel/ArenaSize-4MB/MessageSize-128KB/MaxMem-40MB-8        	   20000	     96803 ns/op	   43776 B/op	      20 allocs/op

BenchmarkParallel/ArenaSize-4MB/MessageSize-128KB/MaxMem-NoLimit-2     	   20000	     85169 ns/op	   43940 B/op	      14 allocs/op
BenchmarkParallel/ArenaSize-4MB/MessageSize-128KB/MaxMem-NoLimit-4     	   20000	     88100 ns/op	   43759 B/op	      14 allocs/op
BenchmarkParallel/ArenaSize-4MB/MessageSize-128KB/MaxMem-NoLimit-6     	   20000	     90889 ns/op	   43839 B/op	      14 allocs/op
BenchmarkParallel/ArenaSize-4MB/MessageSize-128KB/MaxMem-NoLimit-8     	   20000	     89966 ns/op	   43711 B/op	      14 allocs/op

BenchmarkParallel/ArenaSize-128MB/MessageSize-4MB/MaxMem-256MB-2       	    1000	   2416649 ns/op	 1249987 B/op	       2 allocs/op
BenchmarkParallel/ArenaSize-128MB/MessageSize-4MB/MaxMem-256MB-4       	    1000	   2371849 ns/op	 1237424 B/op	       2 allocs/op
BenchmarkParallel/ArenaSize-128MB/MessageSize-4MB/MaxMem-256MB-6       	    1000	   2403440 ns/op	 1262595 B/op	       2 allocs/op
BenchmarkParallel/ArenaSize-128MB/MessageSize-4MB/MaxMem-256MB-8       	    1000	   2364809 ns/op	 1258415 B/op	       2 allocs/op

BenchmarkParallel/ArenaSize-128MB/MessageSize-4MB/MaxMem-1.25GB-2      	    1000	   2376265 ns/op	 1245779 B/op	       2 allocs/op
BenchmarkParallel/ArenaSize-128MB/MessageSize-4MB/MaxMem-1.25GB-4      	    1000	   2442414 ns/op	 1224824 B/op	       2 allocs/op
BenchmarkParallel/ArenaSize-128MB/MessageSize-4MB/MaxMem-1.25GB-6      	    1000	   2385776 ns/op	 1291940 B/op	       2 allocs/op
BenchmarkParallel/ArenaSize-128MB/MessageSize-4MB/MaxMem-1.25GB-8      	    1000	   2392604 ns/op	 1258388 B/op	       2 allocs/op

BenchmarkParallel/ArenaSize-128MB/MessageSize-4MB/MaxMem-NoLimit-2     	    1000	   2417605 ns/op	 1249971 B/op	       2 allocs/op
BenchmarkParallel/ArenaSize-128MB/MessageSize-4MB/MaxMem-NoLimit-4     	    1000	   2434296 ns/op	 1237405 B/op	       2 allocs/op
BenchmarkParallel/ArenaSize-128MB/MessageSize-4MB/MaxMem-NoLimit-6     	    1000	   2453080 ns/op	 1262580 B/op	       2 allocs/op
BenchmarkParallel/ArenaSize-128MB/MessageSize-4MB/MaxMem-NoLimit-8     	    1000	   2382040 ns/op	 1275170 B/op	       2 allocs/op
```

### Comparison Between EnqueueString vs Enqueue
```
BenchmarkStringDoubleCopy-8   	 5000000	       338 ns/op
BenchmarkStringNoCopy-8   	 5000000	       312 ns/op
```

**Note:** Before running benchmarks `ulimit` and `vm.max_map_count` parameters should be adjusted using below commands:
```
ulimit -n 50000
echo 262144 > /proc/sys/vm/max_map_count
```
