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
