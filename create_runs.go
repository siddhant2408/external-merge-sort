package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

const tempFilePrefix = "exttemp-*"

type runCreator struct {
	memLimit   int
	readWriter interface {
		create() (reader io.ReadWriter, deleteFunc func() error, resetFunc func() error, err error)
	}
}

func newRunCreator(memLimit int) *runCreator {
	return &runCreator{
		memLimit:   memLimit,
		readWriter: newReadWriter(),
	}
}

func (r *runCreator) createRuns(reader io.Reader) ([]io.ReadWriter, []func() error, error) {
	runs := make([]io.ReadWriter, 0)
	deleteRuns := make([]func() error, 0)
	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanLines)
	//initiate heap
	h := &intHeap{}
	heap.Init(h)
	//create runs
	isEOF := false
	var err error
	for !isEOF {
		//populate heap
		populate := time.Now()
		isEOF, err = r.populateHeap(h, scanner)
		if err != nil {
			deleteCreatedRuns(deleteRuns)
			return nil, nil, errors.Wrap(err, "populate heap")
		}
		if h.Len() == 0 {
			break
		}
		fmt.Println(time.Since(populate))
		flush := time.Now()
		run, delete, reset, err := r.flushHeapToRun(h)
		if err != nil {
			return nil, nil, errors.Wrap(err, "flush heap")
		}
		fmt.Println(time.Since(flush))
		runs = append(runs, run)
		deleteRuns = append(deleteRuns, delete)
		err = reset()
		if err != nil {
			deleteCreatedRuns(deleteRuns)
			return nil, nil, errors.Wrap(err, "reset run")
		}
	}
	return runs, deleteRuns, nil
}

func (r *runCreator) populateHeap(h heap.Interface, scanner *bufio.Scanner) (bool, error) {
	heapMemSize := 0
	for {
		scanned := scanner.Scan()
		if !scanned {
			if scanner.Err() != nil {
				return false, errors.Wrap(scanner.Err(), "read from input")
			}
			return true, nil
		}
		data := scanner.Text()
		num, err := strconv.Atoi(data)
		if err != nil {
			return false, errors.Wrap(err, "convert string to int")
		}
		heap.Push(h, &runHeap{
			ele: num,
		})
		heapMemSize += len(data)
		if heapMemSize > r.memLimit {
			return false, nil
		}
	}
}

func (r *runCreator) flushHeapToRun(h heap.Interface) (reader io.ReadWriter, deleteFunc func() error, resetFunc func() error, err error) {
	//New allocation each time. Use buffer pool
	b := new(strings.Builder)
	for h.Len() != 0 {
		poppedEle, _ := heap.Pop(h).(*runHeap)
		b.WriteString(strconv.Itoa(poppedEle.ele))
		b.WriteString("\n")
	}
	run, delete, reset, err := r.readWriter.create()
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "create read writer")
	}
	_, err = fmt.Fprintln(run, b.String())
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "print to file")
	}
	return run, delete, reset, nil
}

func deleteCreatedRuns(deleteFuncs []func() error) {
	for _, deleteRun := range deleteFuncs {
		//even if error occurs, no problem as it will be in temp directory
		_ = deleteRun()
	}
}
