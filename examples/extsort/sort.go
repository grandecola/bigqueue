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
func merge(tempPath string, queues []bigqueue.IBigQueue) (bigqueue.IBigQueue, error) {
	currentQueues := queues
	k := 4
	nextQueues := make([]bigqueue.IBigQueue, 0)
	for iteration := 0; len(currentQueues) != 1; iteration++ {
		log.Printf("iteration %d, # queues %d\n", iteration, len(currentQueues))

		for i := 0; i < len(currentQueues); i += k {
			// if only one queue is left, just add this queue
			if i+1 >= len(currentQueues) {
				nextQueues = append(nextQueues, currentQueues[i])
				continue
			}

			lastElem := i + k
			if lastElem > len(currentQueues) {
				lastElem = len(currentQueues) - 1
			}

			queueList := currentQueues[i:lastElem]
			mq, err := mergeQueues(queueList, tempPath)
			if err != nil {
				return nil, fmt.Errorf("error in merging two queues :: %v", err)
			}

			for j := i; j < lastElem; j += 1 {
				currentQueues[j].Close()
			}

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

type Pair struct {
	value, index int
}

func min(i, j Pair) Pair {
	if i.value < j.value {
		return i
	}
	return j
}

func mergeQueues(queueList []bigqueue.IBigQueue, tempPath string) (bigqueue.IBigQueue, error) {
	mq, err := bigqueue.NewBigQueue(getTempDir(tempPath), bigqueue.SetMaxInMemArenas(3))
	if err != nil {
		return nil, fmt.Errorf("unable to create bigqueue :: %v", err)
	}

	const max_value = 10000000000

	k := len(queueList)
	segTree := make([]Pair, 2*k)

	for i := 0; i < k; i += 1 {
		if queueList[i].IsEmpty() {
			segTree[i+k] = Pair{max_value, i}
			continue
		}

		val, err := queueList[i].Peek()
		if err != nil {
			return nil, fmt.Errorf("unable to peek :: %v", err)
		}
		num, err := strconv.Atoi(string(val))
		if err != nil {
			return nil, fmt.Errorf("error in conversion :: %v ", err)
		}

		segTree[i+k] = Pair{num, i}
	}

	for i := k - 1; i > 0; i -= 1 {
		segTree[i] = min(segTree[2*i], segTree[2*i+1])
	}

	empty := 0

	for empty < k {
		top := segTree[1]

		if err := queueList[top.index].Dequeue(); err != nil {
			return nil, fmt.Errorf("unable to dequeue :: %v", err)
		}

		mq.Enqueue([]byte(strconv.Itoa(top.value)))

		index := top.index + k

		if queueList[top.index].IsEmpty() {
			empty += 1
			segTree[index] = Pair{max_value, top.index}
		} else {
			val, err := queueList[top.index].Peek()
			if err != nil {
				return nil, fmt.Errorf("unable to peek :: %v", err)
			}

			num, err := strconv.Atoi(string(val))
			if err != nil {
				return nil, fmt.Errorf("error in conversion :: %v ", err)
			}
			segTree[index] = Pair{num, top.index}
		}

		for index != 1 {
			index = index / 2
			segTree[index] = min(segTree[index*2], segTree[index*2+1])
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
