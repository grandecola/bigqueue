package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/grandecola/bigqueue"
)

var (
	bqFileIndexCount = 0
)

// ExternalSort perform external sort https://en.wikipedia.org/wiki/External_sorting
// The inputPath should be a path to a file containing integers in each line
// The outputPath is similarly formatted file with sorted integers
// The tempPath is used to write intermediate files
// maxMemSortSize is number of elements that can be sorted directly in memory
func ExternalSort(inputPath, tempPath, outputPath string, maxMemSortSize int) error {
	files, err := ioutil.ReadDir(tempPath)
	if err != nil {
		return fmt.Errorf("unable to read temp directory :: %v", err)
	}
	if len(files) != 0 {
		return fmt.Errorf("non-empty temp directory")
	}

	log.Println("starting divide step")
	iqs, err := divide(inputPath, tempPath, maxMemSortSize)
	if err != nil {
		return fmt.Errorf("error in divide step :: %v", err)
	}

	log.Println("starting merge step")
	oq, err := merge(tempPath, iqs)
	if err != nil {
		return fmt.Errorf("error in merge step :: %v", err)
	}

	if err := writeToFile(oq, outputPath); err != nil {
		return fmt.Errorf("error in writing output to file :: %v", err)
	}
	oq.Close()

	return nil
}

// divide step divides all the input data into sorted group of elements.
// Each group is persisted to disk using bigqueue interface.
func divide(inputPath, tempPath string, maxMemSortSize int) ([]bigqueue.IBigQueue, error) {
	log.Println("reading input file")
	queues := make([]bigqueue.IBigQueue, 0)

	// open input file
	fd, err := os.Open(inputPath)
	if err != nil {
		return nil, fmt.Errorf("error in opening input file :: %v", err)
	}
	defer fd.Close()
	reader := bufio.NewReader(fd)

	// read all the data from input file and divide it in multiple queues
	// such that each queue has data sorted and has maximum size of maxMemSortSize
	elemCount := 0
	data := make([]int, 0, maxMemSortSize)
	for {
		// each line contains 1 element in the file
		str, err := reader.ReadString('\n')
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
		elemCount++
		data = append(data, num)

		// check whether we have enough element to perform in memory sort
		if elemCount < maxMemSortSize {
			continue
		}

		// if yes, add the sorted elements into the queue
		sort.Ints(data)
		bq, err := buildBigQueue(tempPath, data)
		if err != nil {
			return nil, fmt.Errorf("error in building bigqueue :: %v", err)
		}

		// add the queue in the list and truncate the slice that holds data in memory
		queues = append(queues, bq)
		elemCount = 0
		data = data[:0]
	}

	// write the final list of elements to bigqueue
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

// merge step merges the sorted group of elements stored in bigqueue using bigqueue
// TODO: merge multiple queues together instead of just 2 queues
func merge(tempPath string, queues []bigqueue.IBigQueue) (bigqueue.IBigQueue, error) {
	currentQueues := queues
	nextQueues := make([]bigqueue.IBigQueue, 0)
	for iteration := 0; len(currentQueues) != 1; iteration++ {
		log.Printf("iteration %d, # queues %d\n", iteration, len(currentQueues))

		for i := 0; i < len(currentQueues); i += 2 {
			// if only one queue is left, just add this queue
			q1 := currentQueues[i]
			if i+1 >= len(currentQueues) {
				nextQueues = append(nextQueues, q1)
				continue
			}

			// otherwise, merge the two queues
			q2 := currentQueues[i+1]
			mq, err := mergeQueues(q1, q2, tempPath)
			if err != nil {
				return nil, fmt.Errorf("error in merging two queues :: %v", err)
			}
			q1.Close()
			q2.Close()

			nextQueues = append(nextQueues, mq)
		}

		currentQueues = nextQueues
		nextQueues = make([]bigqueue.IBigQueue, 0)
	}

	return currentQueues[0], nil
}

func buildBigQueue(tempPath string, data []int) (bigqueue.IBigQueue, error) {
	bq, err := bigqueue.NewBigQueue(getTempDir(tempPath), bigqueue.SetMaxInMemArenas(3))
	if err != nil {
		return nil, fmt.Errorf("unable to init bigqueue :: %v", err)
	}

	// write all the data to bigqueue
	for _, e := range data {
		if err := bq.Enqueue([]byte(strconv.Itoa(e))); err != nil {
			return nil, fmt.Errorf("unable to write to bigqueue :: %v", err)
		}
	}

	return bq, nil
}

func getTempDir(tempPath string) string {
	queueDir := "q" + strconv.Itoa(bqFileIndexCount)
	bqFileIndexCount++

	queuePath := path.Join(tempPath, queueDir)
	if err := os.MkdirAll(queuePath, 0700); err != nil {
		panic(err)
	}

	return queuePath
}

func mergeQueues(q1, q2 bigqueue.IBigQueue, tempPath string) (bigqueue.IBigQueue, error) {
	mq, err := bigqueue.NewBigQueue(getTempDir(tempPath), bigqueue.SetMaxInMemArenas(3))
	if err != nil {
		return nil, fmt.Errorf("unable to create bigqueue :: %v", err)
	}

	for !q1.IsEmpty() && !q2.IsEmpty() {
		e1, err1 := q1.Peek()
		e2, err2 := q2.Peek()
		if err1 != nil || err2 != nil {
			return nil, fmt.Errorf("unable to peek :: %v || %v", err1, err2)
		}

		num1, err1 := strconv.Atoi(string(e1))
		num2, err2 := strconv.Atoi(string(e2))
		if err1 != nil || err2 != nil {
			return nil, fmt.Errorf("error in conversion :: %v || %v", err1, err2)
		}

		if num1 < num2 {
			if err := q1.Dequeue(); err != nil {
				return nil, fmt.Errorf("unable to dequeue :: %v", err1)
			}

			mq.Enqueue([]byte(strconv.Itoa(num1)))
		} else {
			if err := q2.Dequeue(); err != nil {
				return nil, fmt.Errorf("unable to dequeue :: %v", err1)
			}

			mq.Enqueue([]byte(strconv.Itoa(num2)))
		}
	}

	// add elements from the non-empty queue
	var lq bigqueue.IBigQueue
	if q1.IsEmpty() {
		lq = q1
	} else {
		lq = q2
	}
	for !lq.IsEmpty() {
		e1, err := lq.Peek()
		if err != nil {
			return nil, fmt.Errorf("unable to peek :: %v", err)
		}
		mq.Enqueue(e1)

		if err := lq.Dequeue(); err != nil {
			return nil, fmt.Errorf("unable to dequeue :: %v", err)
		}
	}

	return mq, nil
}

func writeToFile(oq bigqueue.IBigQueue, outputPath string) error {
	// write the final output to file
	od, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("error in opening input file :: %v", err)
	}
	defer od.Close()

	w := bufio.NewWriter(od)
	for !oq.IsEmpty() {
		v, err := oq.Peek()
		if err != nil {
			return fmt.Errorf("unable to peek from bigqueue :: %v", err)
		}
		w.WriteString(string(v) + "\n")

		if err := oq.Dequeue(); err != nil {
			return fmt.Errorf("unable to dequeue from bigqueue :: %v", err)
		}
	}

	return w.Flush()
}
