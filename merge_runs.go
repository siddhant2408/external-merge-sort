package extsort

import (
	"container/heap"
	"encoding/csv"
	"io"

	"github.com/pkg/errors"
)

func (e *ExtSort) mergeRuns(runs []io.ReadSeeker, dst io.Writer) error {
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

func (e *ExtSort) getRunIterators(runFiles []io.ReadSeeker) map[int]*csv.Reader {
	iteratorMap := make(map[int]*csv.Reader, len(runFiles))
	for i, file := range runFiles {
		iteratorMap[i] = csv.NewReader(file)
	}
	return iteratorMap
}

//create a heap with top(min) values from each run
func (e *ExtSort) initiateHeap(iteratorMap map[int]*csv.Reader) (*mergeHeap, map[string]bool, error) {
	//build map using merge strategy here too
	initHeapMap := make(map[string]bool)
	h := &mergeHeap{
		heapData:        make([]*heapData, 0),
		compareKeyIndex: e.headerMap[e.sortType],
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
		//no duplicate email/sms should be there in the initial heap
		if e.eleExists(line, initHeapMap) {
			e.mergeEle(h, &heapData{
				data: line,
			})
			continue
		}
		initHeapMap[line[e.headerMap[e.sortType]]] = true
		heap.Push(h, &heapData{
			data:  line,
			runID: i,
		})
		i++
	}
	return h, initHeapMap, nil
}

func (e *ExtSort) processKWayMerge(dst io.Writer, h *mergeHeap, iteratorMap map[int]*csv.Reader, heapEleMap map[string]bool) error {
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
		//if heapEle exists in the heap, merge
		if e.eleExists(runEle.data, heapEleMap) {
			e.mergeEle(h, runEle)
			continue
		} else {
			//pop min and print to file
			poppedEle := heap.Pop(h).(*heapData)
			bytesRead += e.getLineMemSize(poppedEle.data)
			err := csvWriter.Write(poppedEle.data)
			if err != nil {
				return errors.Wrap(err, "write to csv buffer")
			}
			//remove min from heapEleMap
			index := e.headerMap[e.sortType]
			delete(heapEleMap, poppedEle.data[index])
			//push heapEle to heap
			heap.Push(h, runEle)
			heapEleMap[runEle.data[index]] = true
		}
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

func (e *ExtSort) flushRemainingBuffer(writer *csv.Writer) error {
	writer.Flush()
	err := writer.Error()
	if err != nil {
		return errors.Wrap(err, "flush csv buffer")
	}
	return nil
}

func (e *ExtSort) getMinEleRunID(h *mergeHeap) int {
	heapData := h.heapData[0]
	return heapData.runID
}

func (e *ExtSort) eleExists(heapEle []string, heapEleMap map[string]bool) bool {
	csvKeyIndex := e.headerMap[e.sortType]
	_, ok := heapEleMap[heapEle[csvKeyIndex]]
	return ok
}

func (e *ExtSort) mergeEle(h *mergeHeap, heapEle *heapData) {
	for i, line := range h.heapData {
		if h.heapData[i] == maxVal {
			continue
		}
		comparisonValIndex := e.headerMap[e.sortType]
		if line.data[comparisonValIndex] == heapEle.data[comparisonValIndex] {
			h.heapData[i].data = e.getMergedValue(line.data, heapEle.data)
			break
		}
	}
}

//in case of allow empty import, always pick the new element
//in case of no empty import, always pick the element with no empty attributes, new over old
func (e *ExtSort) getMergedValue(newEle []string, heapEle []string) []string {
	mergedEle := make([]string, len(heapEle))
	copy(mergedEle, heapEle)
	for i, _ := range newEle {
		if newEle[i] != "" || e.importEmpty {
			mergedEle[i] = newEle[i]
		}
	}
	return mergedEle
}

func emptyAttributeExists(newEle []string) bool {
	for _, v := range newEle {
		if v == "" {
			return true
		}
	}
	return false
}
