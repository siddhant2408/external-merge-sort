package main

import (
	"bufio"
	"container/heap"
	"io"

	"github.com/pkg/errors"
)

const bufferSize = 1 << 16

func (e *ExtSort) mergeRuns(runs []io.ReadWriter, dst io.Writer) error {
	//ignore merge phase for only one run
	if len(runs) == 1 {
		_, err := io.Copy(dst, runs[0])
		if err != nil {
			return errors.Wrap(err, "write to dst")
		}
		return nil
	}
	scannerMap, err := e.getRunIterators(runs)
	if err != nil {
		return errors.Wrap(err, "get run iterators")
	}
	h, err := e.initiateHeap(scannerMap)
	if err != nil {
		return errors.Wrap(err, "initiate merge heap")
	}
	err = e.processKWayMerge(dst, h, scannerMap)
	if err != nil {
		return errors.Wrap(err, "k-way merge")
	}
	return nil
}

func (e *ExtSort) getRunIterators(runFiles []io.ReadWriter) (map[int]*bufio.Scanner, error) {
	scannerMap := make(map[int]*bufio.Scanner, len(runFiles))
	for i, file := range runFiles {
		scanner := bufio.NewScanner(file)
		scanner.Split(bufio.ScanLines)
		scannerMap[i] = scanner
	}
	return scannerMap, nil
}

//create a heap with top(min) values from each run
func (e *ExtSort) initiateHeap(scannerMap map[int]*bufio.Scanner) (heap.Interface, error) {
	h := &mergeHeap{
		heapData: make([]*heapData, 0),
		less:     e.inputHandler.Less,
	}
	heap.Init(h)
	for i := 0; i < len(scannerMap); i++ {
		scanner := scannerMap[i]
		scanned := scanner.Scan()
		if !scanned {
			if scanner.Err() != nil {
				return nil, errors.Wrap(scanner.Err(), "scan file")
			}
			return nil, errors.New("empty file")
		}
		input, err := e.inputHandler.ToStructured(scanner.Bytes())
		if err != nil {
			return nil, errors.Wrap(err, "convert string to int")
		}
		heap.Push(h, &heapData{
			data:  input,
			runID: i,
		})
	}
	return h, nil
}

func (e *ExtSort) processKWayMerge(dst io.Writer, h heap.Interface, scannerMap map[int]*bufio.Scanner) error {
	bufferedWriter := bufio.NewWriterSize(dst, bufferSize)
	numRuns := len(scannerMap)
	//start iterating on runs and write to output file
	for runsCompleted := 0; runsCompleted != numRuns; {
		poppedEle := heap.Pop(h).(*heapData)
		byteData, err := e.inputHandler.ToBytes(poppedEle.data)
		if err != nil {
			return errors.Wrap(err, "convert to bytes")
		}
		err = e.writeToBuffer(bufferedWriter, byteData)
		if err != nil {
			return errors.Wrap(err, "write to buffer")
		}
		heapEle, isEOFReached, err := e.getValueFromRun(scannerMap[poppedEle.runID], poppedEle.runID)
		if err != nil {
			return errors.Wrap(err, "get next heap val")
		}
		if isEOFReached {
			runsCompleted++
			heapEle = maxVal
		}
		heap.Push(h, heapEle)
	}
	err := e.flushRemainingBuffer(bufferedWriter)
	if err != nil {
		return errors.Wrap(err, "flush remaining buffer")
	}
	return nil
}

func (e *ExtSort) writeToBuffer(bufferedWriter *bufio.Writer, data []byte) error {
	if bufferedWriter.Available() < len(data) {
		//push the buffered data to file
		err := bufferedWriter.Flush()
		if err != nil {
			return errors.Wrap(err, "flush to output")
		}
	}
	_, err := bufferedWriter.Write(data)
	if err != nil {
		return errors.Wrap(err, "add number to out file")
	}
	_, err = bufferedWriter.WriteString("\n")
	if err != nil {
		return errors.Wrap(err, "add number to out file")
	}
	return nil
}

func (e *ExtSort) getValueFromRun(scanner *bufio.Scanner, runID int) (*heapData, bool, error) {
	scanned := scanner.Scan()
	if !scanned {
		err := scanner.Err()
		if err != nil {
			return nil, false, errors.Wrap(err, "scan file")
		}
		//EOF reached
		return nil, true, nil
	}
	runData, err := e.inputHandler.ToStructured(scanner.Bytes())
	if err != nil {
		return nil, false, errors.Wrap(err, "get run data")
	}
	return &heapData{
		data:  runData,
		runID: runID,
	}, false, nil
}

func (e *ExtSort) flushRemainingBuffer(bufferedWriter *bufio.Writer) error {
	if bufferedWriter.Buffered() > 0 {
		err := bufferedWriter.Flush()
		if err != nil {
			return errors.Wrap(err, "flush remaining buffer")
		}
	}
	return nil
}
