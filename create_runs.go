package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

//return sorted run files
func createRuns(inputFile string, runSize int, numRuns int) ([]*os.File, error) {
	f, err := os.Open(inputFile)
	if err != nil {
		return nil, errors.Wrap(err, "open input file")
	}
	defer f.Close()

	runFiles, err := getRunFilesArray(numRuns)
	if err != nil {
		return nil, errors.Wrap(err, "get run files array")
	}

	err = populateRunFiles(f, runFiles, runSize)
	if err != nil {
		deleteRunFiles(runFiles)
		return nil, errors.Wrap(err, "populate run files")
	}

	return runFiles, nil
}

func getRunFilesArray(numRuns int) ([]*os.File, error) {
	runFiles := make([]*os.File, numRuns)
	for i := 0; i < numRuns; i++ {
		runFileName := "temp" + strconv.Itoa(i) + ".txt"
		runFile, err := os.Create(runFileName)
		if err != nil {
			//delete already created run files
			deleteRunFiles(runFiles)
			return nil, errors.Wrap(err, "create file")
		}
		runFiles[i] = runFile
	}
	return runFiles, nil
}

func populateRunFiles(inputFile *os.File, runFiles []*os.File, runSize int) error {
	scanner := bufio.NewScanner(inputFile)
	scanner.Split(bufio.ScanLines)
	moreInput := true
	curRunFileIndex := 0
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

		//quick sort
		sort.Ints(arr)

		//condense the array into a single string
		runFileContent := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(arr)), "\n"), "[]")
		_, err = fmt.Fprintln(runFiles[curRunFileIndex], runFileContent)
		if err != nil {
			return errors.Wrap(err, "print to file")
		}
		//return the file pointer to the top of the file
		runFiles[curRunFileIndex].Seek(0, 0)
		curRunFileIndex++
	}
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
