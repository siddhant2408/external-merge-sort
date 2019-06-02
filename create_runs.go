package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"

	"github.com/pkg/errors"
)

//return sorted run files
func createInitialRuns(inputFile string, runSize int, numRuns int) error {
	f, err := os.Open(inputFile)
	if err != nil {
		return errors.Wrap(err, "open input file")
	}
	defer f.Close()

	runFiles, err := getRunFilesArray(numRuns)
	if err != nil {
		return errors.Wrap(err, "get run files array")
	}

	err = populateRunFiles(f, runFiles, runSize)
	if err != nil {
		deleteRunFiles(runFiles)
		return errors.Wrap(err, "populate run files")
	}
	closeRunFiles(runFiles)

	return nil
}

func getRunFilesArray(numRuns int) ([]*os.File, error) {
	runFiles := make([]*os.File, 0)
	for i := 0; i < numRuns; i++ {
		runFileName := "temp" + strconv.Itoa(i) + ".txt"
		runFile, err := os.Create(runFileName)
		if err != nil {
			//delete already created run files
			deleteRunFiles(runFiles)
			return nil, errors.Wrap(err, "create file")
		}
		runFiles = append(runFiles, runFile)
	}
	return runFiles, nil
}

func populateRunFiles(inputFile *os.File, runFiles []*os.File, runSize int) error {
	scanner := bufio.NewScanner(inputFile)
	scanner.Split(bufio.ScanLines)
	moreInput := true
	curRunFileIndex := 0
	var wg sync.WaitGroup
	for moreInput {
		arr, isEOFReached, err := getInputFileBatch(scanner, runSize)
		if err != nil {
			return errors.Wrap(err, "get file contents")
		}
		//this can happen in two ways, either you encountered EOF at the start only
		//or the input stopped before reaching run size
		if isEOFReached {
			moreInput = false
		}
		if len(arr) == 0 {
			break
		}

		copyArray := make([]int, len(arr))
		copy(copyArray, arr)

		go func(runFile *os.File, data []int) {
			wg.Add(1)
			defer wg.Done()
			//quick sort
			sort.Ints(data)
			//put the quicksorted array in the run file
			for _, v := range data {
				_, err := fmt.Fprintln(runFile, v)
				if err != nil {
					err = errors.Wrap(err, "print to file")
					fmt.Println(err)
					return
				}
			}
		}(runFiles[curRunFileIndex], copyArray)

		curRunFileIndex++
	}
	wg.Wait()
	return nil
}

//read from input file till runsize and put in array
//assumes every input is an integer
func getInputFileBatch(scanner *bufio.Scanner, runSize int) ([]int, bool, error) {
	arr := make([]int, 0)
	isEOFReached := false
	for i := 0; i < runSize; i++ {
		scanned := scanner.Scan()
		if !scanned {
			if scanner.Err() != nil {
				return nil, isEOFReached, errors.Wrap(scanner.Err(), "scan file")
			}
			//scanner.Err() returns nil error for EOF
			isEOFReached = true
			//return array read till now
			return arr, isEOFReached, nil
		}
		num, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return nil, isEOFReached, errors.Wrap(err, "convert string to int")
		}
		arr = append(arr, num)
	}
	return arr, isEOFReached, nil
}

func deleteRunFiles(runFiles []*os.File) {
	for _, file := range runFiles {
		if file != nil {
			os.Remove(file.Name())
		}
	}
}

func closeRunFiles(runFiles []*os.File) {
	for _, file := range runFiles {
		if file != nil {
			file.Close()
		}
	}
}
