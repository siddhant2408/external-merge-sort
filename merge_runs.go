package extsort

import (
	"container/heap"
	"encoding/csv"
	"io"
	"strings"

	"github.com/pkg/errors"
)

func (e *extSort) mergeRuns(runs []io.ReadSeeker, dst io.Writer) error {
	iteratorMap := e.getRunIterators(runs)
	h, initHeapMap, err := e.initiateHeap(iteratorMap)
	if err != nil {
		return errors.Wrap(err, "initiate merge heap")
	}
	err = e.processKWayMerge(dst, h, iteratorMap, initHeapMap)
	if err != nil {
		return errors.Wrap(err, "k-way merge")
	}
	return nil
}

func (e *extSort) getRunIterators(runFiles []io.ReadSeeker) map[int]*csv.Reader {
	iteratorMap := make(map[int]*csv.Reader, len(runFiles))
	for i, file := range runFiles {
		iteratorMap[i] = csv.NewReader(file)
	}
	return iteratorMap
}

//create a heap with top(min) values from each run
func (e *extSort) initiateHeap(iteratorMap map[int]*csv.Reader) (*mergeHeap, map[string]bool, error) {
	//build map using merge strategy here too
	initHeapMap := make(map[string]bool)
	h := &mergeHeap{
		heapData:        make([]*heapData, 0),
		compareKeyIndex: e.sortIndex,
	}
	for i := 0; i < len(iteratorMap); {
		reader := iteratorMap[i]
		line, err := reader.Read()
		if err != nil {
			if err != io.EOF {
				return nil, nil, errors.Wrap(err, "scan file")
			}
			i++
			continue
		}
		initHeapMap[line[e.sortIndex]] = true
		heap.Push(h, &heapData{
			data:  line,
			runID: i,
		})
		i++
	}
	return h, initHeapMap, nil
}

func (e *extSort) processKWayMerge(dst io.Writer, h *mergeHeap, iteratorMap map[int]*csv.Reader, heapEleMap map[string]bool) error {
	bytesRead := 0
	csvWriter := csv.NewWriter(dst)

	numRuns := len(iteratorMap)
	//start iterating on runs and write to output file
	for runsCompleted := 0; runsCompleted != numRuns; {
		minEleRunID := e.getMinEleRunID(h)
		runEle, isEOFReached, err := e.getValueFromRun(iteratorMap[minEleRunID], minEleRunID)
		if err != nil {
			return errors.Wrap(err, "get next heap val")
		}
		if isEOFReached {
			runsCompleted++
			//pop min and push MAX_ELE to heap
			poppedEle := heap.Pop(h).(*heapData)
			//end process if min element = maxVal
			if poppedEle == maxVal {
				break
			}
			bytesRead += e.getLineMemSize(poppedEle.data)
			err := csvWriter.Write(poppedEle.data)
			if err != nil {
				return errors.Wrap(err, "write to csv buffer")
			}
			heap.Push(h, maxVal)
			continue
		}

		//pop min and print to file
		poppedEle := heap.Pop(h).(*heapData)
		bytesRead += e.getLineMemSize(poppedEle.data)
		err = csvWriter.Write(poppedEle.data)
		if err != nil {
			return errors.Wrap(err, "write to csv buffer")
		}
		//remove min from heapEleMap
		index := e.sortIndex
		delete(heapEleMap, poppedEle.data[index])
		//push heapEle to heap
		heap.Push(h, runEle)
		heapEleMap[runEle.data[index]] = true

		if bytesRead > e.memLimit {
			bytesRead = 0
			csvWriter.Flush()
			err := csvWriter.Error()
			if err != nil {
				return errors.Wrap(err, "flush csv buffer")
			}
		}
	}
	err := e.flushRemainingBuffer(csvWriter)
	if err != nil {
		return errors.Wrap(err, "flush remaining buffer")
	}
	return nil
}

func (e *extSort) getValueFromRun(reader *csv.Reader, runID int) (*heapData, bool, error) {
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

func (e *extSort) flushRemainingBuffer(writer *csv.Writer) error {
	writer.Flush()
	err := writer.Error()
	if err != nil {
		return errors.Wrap(err, "flush csv buffer")
	}
	return nil
}

func (e *extSort) getMinEleRunID(h *mergeHeap) int {
	heapData := h.heapData[0]
	return heapData.runID
}

func compare(a, b string) (bool, error) {
	res := strings.Compare(a, b)
	if res == -1 {
		return true, nil
	}
	return false, nil
}
