package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"
)

const bufferSize = 1 << 16

var maxVal = &heapData{
	data: nil,
}

type runMerger struct {
	inputHandler InputHandler
}

func newRunMerger(inputHandler InputHandler) *runMerger {
	return &runMerger{
		inputHandler: inputHandler,
	}
}

func (r *runMerger) mergeRuns(runs []io.ReadWriter, dst io.Writer) error {
	//ignore merge phase for only one run
	if len(runs) == 1 {
		_, err := io.Copy(dst, runs[0])
		if err != nil {
			return errors.Wrap(err, "write to dst")
		}
		return nil
	}
	scannerMap, err := r.getRunIterators(runs)
	if err != nil {
		return errors.Wrap(err, "get run iterators")
	}
	h, err := r.initiateHeap(scannerMap)
	if err != nil {
		return errors.Wrap(err, "initiate merge heap")
	}
	merge := time.Now()
	err = r.processKWayMerge(dst, h, scannerMap)
	if err != nil {
		return errors.Wrap(err, "k-way merge")
	}
	fmt.Println("kway merge time:", time.Since(merge))
	return nil
}

func (r *runMerger) getRunIterators(runFiles []io.ReadWriter) (map[int]*bufio.Scanner, error) {
	scannerMap := make(map[int]*bufio.Scanner, len(runFiles))
	for i, file := range runFiles {
		scanner := bufio.NewScanner(file)
		scanner.Split(bufio.ScanLines)
		scannerMap[i] = scanner
	}
	return scannerMap, nil
}

//create a heap with top(min) values from each run
func (r *runMerger) initiateHeap(scannerMap map[int]*bufio.Scanner) (heap.Interface, error) {
	h := &mergeHeap{
		heapData: make([]*heapData, 0),
		less:     r.inputHandler.Less,
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
		input, err := r.inputHandler.ToStructured(scanner.Bytes())
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

func (r *runMerger) processKWayMerge(dst io.Writer, h heap.Interface, scannerMap map[int]*bufio.Scanner) error {
	bufferedWriter := bufio.NewWriterSize(dst, bufferSize)
	numRuns := len(scannerMap)
	//start iterating on runs and write to output file
	for runsCompleted := 0; runsCompleted != numRuns; {
		poppedEle := heap.Pop(h).(*heapData)
		byteData, err := r.inputHandler.ToBytes(poppedEle.data)
		if err != nil {
			return errors.Wrap(err, "convert to bytes")
		}
		err = r.writeToBuffer(bufferedWriter, byteData)
		if err != nil {
			return errors.Wrap(err, "write to buffer")
		}
		heapEle, isEOFReached, err := r.getValueFromRun(scannerMap[poppedEle.runID], poppedEle.runID)
		if err != nil {
			return errors.Wrap(err, "get next heap val")
		}
		if isEOFReached {
			runsCompleted++
			heapEle = maxVal
		}
		heap.Push(h, heapEle)
	}
	err := r.flushRemainingBuffer(bufferedWriter)
	if err != nil {
		return errors.Wrap(err, "flush remaining buffer")
	}
	return nil
}

func (r *runMerger) writeToBuffer(bufferedWriter *bufio.Writer, data []byte) error {
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

func (r *runMerger) getValueFromRun(scanner *bufio.Scanner, runID int) (*heapData, bool, error) {
	scanned := scanner.Scan()
	if !scanned {
		err := scanner.Err()
		if err != nil {
			return nil, false, errors.Wrap(err, "scan file")
		}
		//EOF reached
		return nil, true, nil
	}
	runData, err := r.inputHandler.ToStructured(scanner.Bytes())
	if err != nil {
		return nil, false, errors.Wrap(err, "get run data")
	}
	return &heapData{
		data:  runData,
		runID: runID,
	}, false, nil
}

func (r *runMerger) flushRemainingBuffer(bufferedWriter *bufio.Writer) error {
	if bufferedWriter.Buffered() > 0 {
		err := bufferedWriter.Flush()
		if err != nil {
			return errors.Wrap(err, "flush remaining buffer")
		}
	}
	return nil
}
