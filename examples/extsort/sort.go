package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/grandecola/bigqueue"
)

var (
	fileIndexCount = 0
)

// TODO: close the queues to release resources
func externalSort(inputPath, tempPath, outputPath string, maxMemSortElem int) error {
	fmt.Println("divide step")
	queues, err := divide(inputPath, tempPath, maxMemSortElem)
	if err != nil {
		return fmt.Errorf("error in divide step :: %v", err)
	}

	fmt.Println("merge step")
	fq, err := merge(tempPath, queues)
	if err != nil {
		return fmt.Errorf("error in merge step :: %v", err)
	}

	// write the final output to file
	od, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("error in opening input file :: %v", err)
	}
	defer od.Close()

	w := bufio.NewWriter(od)
	for !fq.IsEmpty() {
		v, err := fq.Dequeue()
		if err != nil {
			return fmt.Errorf("unable to dequeue from bigqueue :: %v", err)
		}

		w.WriteString(string(v) + "\n")
	}

	return w.Flush()
}

func divide(inputPath, tempPath string, maxMemSortElem int) ([]*bigqueue.BigQueue, error) {
	queues := make([]*bigqueue.BigQueue, 0)

	// read all the data and put it in multiple queues
	// each queue can have maximum size of given max size
	fd, err := os.Open(inputPath)
	if err != nil {
		return nil, fmt.Errorf("error in opening input file :: %v", err)
	}
	defer fd.Close()

	elemCount := 0
	data := make([]int, 0, maxMemSortElem)
	r := bufio.NewReader(fd)
	for {
		// each line contains 1 element in the file
		str, err := r.ReadString('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("error in reading input file :: %v", err)
		}

		// convert the element into integer
		str = strings.TrimSpace(str)
		num, err := strconv.Atoi(str)
		if err != nil {
			return nil, fmt.Errorf("error in converting {%s} :: %v", str, err)
		}
		data = append(data, num)

		// check whether we have enough element to perform in memory sort
		elemCount++
		if elemCount < maxMemSortElem {
			continue
		}

		// if yes, add the sorted elements into the queue
		sort.Ints(data)
		bq, err := buildBigQueue(tempPath, data)
		if err != nil {
			return nil, fmt.Errorf("error in building bigqueue :: %v", err)
		}

		// add the queue in the list and truncate the slice that holds data
		queues = append(queues, bq)
		data = data[:0]
		elemCount = 0
	}

	if len(data) != 0 {
		sort.Ints(data)
		bq, err := buildBigQueue(tempPath, data)
		if err != nil {
			return nil, fmt.Errorf("error in building bigqueue :: %v", err)
		}

		queues = append(queues, bq)
	}

	return queues, nil
}

func merge(tempPath string, queues []*bigqueue.BigQueue) (*bigqueue.BigQueue, error) {
	currentQueues := queues
	nextQueues := make([]*bigqueue.BigQueue, 0)
	for iteration := 0; len(currentQueues) != 1; iteration++ {
		fmt.Printf("iteration %d, queue length %d\n", iteration, len(currentQueues))

		for i := 0; i < len(currentQueues); i += 2 {
			// if only one queue is left, just add this queue
			q1 := currentQueues[i]
			if i+1 >= len(currentQueues) {
				nextQueues = append(nextQueues, q1)
				continue
			}

			q2 := currentQueues[i+1]
			mq, err := bigqueue.NewBigQueue(getTempDir(tempPath))
			if err != nil {
				return nil, fmt.Errorf("unable to create bigqueue :: %v", err)
			}

			// add elements in sorted order
			for !q1.IsEmpty() && !q2.IsEmpty() {
				e1, err1 := q1.Peek()
				e2, err2 := q2.Peek()
				if err1 != nil || err2 != nil {
					return nil, fmt.Errorf("unable to dequeue :: %v :: %v", err1, err2)
				}

				num1, err1 := strconv.Atoi(string(e1))
				num2, err2 := strconv.Atoi(string(e2))
				if err1 != nil || err2 != nil {
					return nil, fmt.Errorf("error in conversion :: %v :: %v", err1, err2)
				}

				if num1 < num2 {
					q1.Dequeue()
					mq.Enqueue([]byte(strconv.Itoa(num1)))
				} else {
					q2.Dequeue()
					mq.Enqueue([]byte(strconv.Itoa(num2)))
				}
			}

			// add elements from the non-empty queue
			var lq *bigqueue.BigQueue
			if q1.IsEmpty() {
				lq = q1
			} else {
				lq = q2
			}
			for !lq.IsEmpty() {
				e1, err := lq.Dequeue()
				if err != nil {
					return nil, fmt.Errorf("unable to dequeue :: %v", err)
				}

				num, err := strconv.Atoi(string(e1))
				if err != nil {
					return nil, fmt.Errorf("unable to convert :: %v", err)
				}

				mq.Enqueue([]byte(strconv.Itoa(num)))
			}

			nextQueues = append(nextQueues, mq)
		}

		currentQueues = nextQueues
		nextQueues = make([]*bigqueue.BigQueue, 0)
	}

	return currentQueues[0], nil
}

func buildBigQueue(tempPath string, data []int) (*bigqueue.BigQueue, error) {
	bq, err := bigqueue.NewBigQueue(getTempDir(tempPath))
	if err != nil {
		return nil, fmt.Errorf("unable to init bigqueue :: %v", err)
	}

	for _, e := range data {
		bq.Enqueue([]byte(strconv.Itoa(e)))
	}

	return bq, nil
}

func getTempDir(tempPath string) string {
	queueDir := "q" + strconv.Itoa(fileIndexCount)
	fileIndexCount++

	queuePath := path.Join(tempPath, queueDir)
	if err := os.MkdirAll(queuePath, 0700); err != nil {
		panic(err)
	}

	return queuePath
}
