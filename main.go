package main

import (
	"fmt"
	"math/rand"
	"os"

	"github.com/pkg/errors"
)

func main() {
	//number of partitions of input file
	numRuns := 10
	//total numbers in the input file
	totalNum := 100000

	inputFile := "input.txt"
	outputFile := "output.txt"

	//populate input file
	f, err := os.Create(inputFile)
	if err != nil {
		panic(err)
	}
	for i := 0; i < totalNum; i++ {
		fmt.Fprintln(f, rand.Intn(totalNum))
	}
	f.Close()

	if totalNum%numRuns > 0 {
		numRuns++
	}
	err = extMergeSort(inputFile, outputFile, numRuns, totalNum/numRuns)
	if err != nil {
		panic(err)
	}

	fmt.Println("SUCCESS!!!")
}

func extMergeSort(inputFile string, outputFile string, numRuns int, runSize int) error {
	err := createInitialRuns(inputFile, runSize, numRuns)
	if err != nil {
		return errors.Wrap(err, "create initial runs")
	}
	err = mergeRuns(outputFile, numRuns)
	if err != nil {
		return errors.Wrap(err, "merge runs")
	}
	return nil
}
