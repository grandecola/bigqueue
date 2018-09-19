package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"
)

func main() {
	maxRandomNum := 10000
	numDataElements := 100000
	maxMemSortSize := 10000
	inputPath := "examples/extsort/input.dat"
	tempPath := "examples/extsort/bq"
	outputPath := "examples/extsort/output.dat"

	// generate random data for testing
	if err := generateData(numDataElements, maxRandomNum, inputPath); err != nil {
		panic(err)
	}

	// sort the data
	if err := externalSort(inputPath, tempPath, outputPath, maxMemSortSize); err != nil {
		panic(err)
	}
}

func generateData(size, maxRandomNum int, dataFilePath string) error {
	fd, err := os.Create(dataFilePath)
	if err != nil {
		return fmt.Errorf("unable to open file :: %v", err)
	}
	defer fd.Close()

	writer := bufio.NewWriter(fd)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < size; i++ {
		num := (rand.Int() % maxRandomNum) - maxRandomNum/2
		str := strconv.Itoa(num) + "\n"
		writer.WriteString(str)
	}

	return writer.Flush()
}
