package extsort

import (
	"bufio"
	"container/heap"
	"io"
	"strconv"

	"github.com/pkg/errors"
)

const intMax = 10000000

type runMerger struct{}

func newRunMerger() *runMerger {
	return &runMerger{}
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
	err = r.processKWayMerge(dst, h, scannerMap)
	if err != nil {
		return errors.Wrap(err, "k-way merge")
	}
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
	h := &intHeap{}
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

func (r *runMerger) processKWayMerge(dst io.Writer, h heap.Interface, scannerMap map[int]*bufio.Scanner) error {
	// Create a buffered writer (10 KB) for the file
	bufferedWriter := bufio.NewWriterSize(dst, 10240)

	//start iterating on runs and write to output file
	for count := 0; count != len(scannerMap); {
		poppedEle := heap.Pop(h).(fileHeap)
		if bufferedWriter.Available() < len([]byte(strconv.Itoa(poppedEle.ele))) {
			//push the buffered data to file
			bufferedWriter.Flush()
		}
		_, err := bufferedWriter.WriteString(strconv.Itoa(poppedEle.ele) + "\n")
		if err != nil {
			return errors.Wrap(err, "add number to out file")
		}
		//get the next element from the popped element file and add to heap
		scanner := scannerMap[poppedEle.fileIndex]
		scanned := scanner.Scan()
		if !scanned {
			err := scanner.Err()
			if err != nil {
				return errors.Wrap(err, "scan file")
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
