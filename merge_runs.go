package main

import (
	"bufio"
	"container/heap"
	"encoding/csv"
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
	iteratorMap := e.getRunIterators(runs)
	h, err := e.initiateHeap(iteratorMap)
	if err != nil {
		return errors.Wrap(err, "initiate merge heap")
	}
	err = e.processKWayMerge(dst, h, iteratorMap)
	if err != nil {
		return errors.Wrap(err, "k-way merge")
	}
	return nil
}

func (e *ExtSort) getRunIterators(runFiles []io.ReadWriter) map[int]*csv.Reader {
	iteratorMap := make(map[int]*csv.Reader, len(runFiles))
	for i, file := range runFiles {
		iteratorMap[i] = csv.NewReader(file)
	}
	return iteratorMap
}

//create a heap with top(min) values from each run
func (e *ExtSort) initiateHeap(iteratorMap map[int]*csv.Reader) (heap.Interface, error) {
	h := &mergeHeap{
		heapData: make([]*heapData, 0),
		less:     compareEmail,
	}
	heap.Init(h)
	for i := 0; i < len(iteratorMap); i++ {
		reader := iteratorMap[i]
		line, err := reader.Read()
		if err != nil {
			if err != io.EOF {
				return nil, errors.Wrap(err, "scan file")
			}
			return nil, errors.New("empty file")
		}
		heap.Push(h, &heapData{
			data:  line,
			runID: i,
		})
	}
	return h, nil
}

func (e *ExtSort) processKWayMerge(dst io.Writer, h heap.Interface, iteratorMap map[int]*csv.Reader) error {
	bufferedWriter := bufio.NewWriterSize(dst, bufferSize)
	numRuns := len(iteratorMap)
	//start iterating on runs and write to output file
	for runsCompleted := 0; runsCompleted != numRuns; {
		poppedEle := heap.Pop(h).(*heapData)
		err := e.writeToBuffer(bufferedWriter, poppedEle)
		if err != nil {
			return errors.Wrap(err, "write to buffer")
		}
		heapEle, isEOFReached, err := e.getValueFromRun(iteratorMap[poppedEle.runID], poppedEle.runID)
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

func (e *ExtSort) writeToBuffer(bufferedWriter *bufio.Writer, heapData *heapData) error {
	if bufferedWriter.Available() < e.getLineMemSize(heapData.data) {
		//push the buffered data to file
		err := bufferedWriter.Flush()
		if err != nil {
			return errors.Wrap(err, "flush to output")
		}
	}
	//fix this
	writer := csv.NewWriter(bufferedWriter)
	err := writer.Write(heapData.data)
	if err != nil {
		return errors.Wrap(err, "write to out buffer")
	}
	return nil
}

func (e *ExtSort) getValueFromRun(reader *csv.Reader, runID int) (*heapData, bool, error) {
	line, err := reader.Read()
	if err != nil {
		if err != io.EOF {
			return nil, false, errors.Wrap(err, "scan file")
		}
		//EOF reached
		return nil, true, nil
	}
	return &heapData{
		data:  line,
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
