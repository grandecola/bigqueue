package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	cMaxRandomNum = 1000000000
)

// tested with go run sort.go main.go --num 1000000000 --nummem 100000000 -t temp
func main() {
	var numDataElements, maxMemSortSize int
	var inputPath, tempPath, outputPath string
	flag.IntVar(&numDataElements, "num", 1000, "# elements to generate & sort")
	flag.IntVar(&maxMemSortSize, "nummem", 100, "# elements to sort in mem")
	flag.StringVar(&inputPath, "i", "input.dat", "input file (overwritten)")
	flag.StringVar(&tempPath, "t", "bq", "path to write intermediate data")
	flag.StringVar(&outputPath, "o", "output.dat", "output file (overwritten)")
	flag.Parse()

	// generate random data for testing
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		log.Println("generating random dataset")
		if err := generateData(numDataElements, cMaxRandomNum, inputPath); err != nil {
			panic(err)
		}
	}

	// sort the data
	if err := ExternalSort(inputPath, tempPath, outputPath, maxMemSortSize); err != nil {
		panic(err)
	}

	// validate the sorted data
	log.Println("validating sort")
	if err := validateSort(outputPath); err != nil {
		panic(err)
	}
}

// generateData generates random data and writes them to a file with newline delimiter
func generateData(size, maxRandomNum int, dataFilePath string) error {
	// open file
	fd, err := os.Create(dataFilePath)
	if err != nil {
		return fmt.Errorf("unable to open file :: %v", err)
	}
	defer fd.Close()
	writer := bufio.NewWriter(fd)

	// generate random numbers and write to file
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < size; i++ {
		num := (rand.Int() % (2 * maxRandomNum)) - maxRandomNum
		str := strconv.Itoa(num) + "\n"
		writer.WriteString(str)
	}

	return writer.Flush()
}

func validateSort(outputPath string) error {
	// open input file
	fd, err := os.Open(outputPath)
	if err != nil {
		return fmt.Errorf("error in opening input file :: %v", err)
	}
	defer fd.Close()
	reader := bufio.NewReader(fd)

	prevNum := 0
	firstIter := true
	for {
		// each line contains 1 element in the file
		str, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("error in reading input file :: %v", err)
		}

		// convert the element into integer
		str = strings.TrimSpace(str)
		num, err := strconv.Atoi(str)
		if err != nil {
			return fmt.Errorf("error in converting {%s} :: %v", str, err)
		}

		// check the order of prev and cur element
		if firstIter {
			firstIter = false
		} else if prevNum > num {
			return fmt.Errorf("incorrect sort")
		}

		prevNum = num
	}

	return nil
}
