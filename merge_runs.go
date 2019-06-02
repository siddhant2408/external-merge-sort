package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"os"
	"strconv"

	"github.com/pkg/errors"
)

const intMax = 10000000

func mergeRuns(outputFile string, numRuns int) error {
	//scanner map contains scanner objects for each run file
	scannerMap, deleteRunFiles, err := getRunFileScanners(numRuns)
	if err != nil {
		return errors.Wrap(err, "get run file scanners")
	}
	defer deleteRunFiles()

	h, err := initiateHeap(scannerMap, numRuns)
	if err != nil {
		return errors.Wrap(err, "initiate heap")
	}

	err = processKWayMerge(outputFile, h, scannerMap, numRuns)
	if err != nil {
		return errors.Wrap(err, "write to output file")
	}
	return nil
}

func getRunFiles(numRuns int) ([]*os.File, error) {
	runFiles := make([]*os.File, 0)
	for i := 0; i < numRuns; i++ {
		file, err := os.Open("temp" + strconv.Itoa(i) + ".txt")
		if err != nil {
			return nil, errors.Wrap(err, "open run file")
		}
		runFiles = append(runFiles, file)
	}
	return runFiles, nil
}

func getRunFileScanners(numRuns int) (map[int]*bufio.Scanner, func(), error) {
	runFiles, err := getRunFiles(numRuns)
	if err != nil {
		return nil, nil, errors.Wrap(err, "get run files")
	}
	scannerMap := make(map[int]*bufio.Scanner)
	for i, file := range runFiles {
		scanner := bufio.NewScanner(file)
		scanner.Split(bufio.ScanLines)
		scannerMap[i] = scanner
	}
	return scannerMap, func() {
		deleteRunFiles(runFiles)
	}, nil
}

//create a heap with top(min) values from each run
func initiateHeap(scannerMap map[int]*bufio.Scanner, numRuns int) (*intHeap, error) {
	h := &intHeap{}
	heap.Init(h)
	for i := 0; i < numRuns; i++ {
		scanner := scannerMap[i]
		scanned := scanner.Scan()
		if !scanned {
			if scanner.Err() != nil {
				return nil, errors.Wrap(scanner.Err(), "scan file")
			}
			return nil, errors.New("empty file")
		}
		num, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return nil, errors.Wrap(err, "convert string to int")
		}
		heap.Push(h, fileHeap{
			ele:       num,
			fileIndex: i,
		})
	}
	return h, nil
}

func processKWayMerge(outputFile string, h *intHeap, scannerMap map[int]*bufio.Scanner, numRuns int) error {
	outFile, err := os.OpenFile(outputFile, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return errors.Wrap(err, "open output file")
	}
	defer outFile.Close()

	//start iterating on runs and write to output file
	for count := 0; count != numRuns; {
		poppedEle := heap.Pop(h).(fileHeap)
		_, err := fmt.Fprintln(outFile, poppedEle.ele)
		if err != nil {
			return errors.Wrap(err, "add number to out file")
		}
		//get the next element from the popped element file and add to heap
		scanner := scannerMap[poppedEle.fileIndex]
		scanned := scanner.Scan()
		if !scanned {
			if scanner.Err() != nil {
				return errors.Wrap(scanner.Err(), "scan file")
			}
			//EOF reached
			heap.Push(h, fileHeap{
				ele: intMax,
			})
			count++
			continue
		}
		num, _ := strconv.Atoi(scanner.Text())
		heap.Push(h, fileHeap{
			ele:       num,
			fileIndex: poppedEle.fileIndex,
		})
	}
	return nil
}
