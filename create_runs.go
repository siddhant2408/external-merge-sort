package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/pkg/errors"
)

const tempFilePrefix = "exttemp-*"

type runCreator struct {
	memLimit   int
	less       LessFunc
	converter  InputConverter
	readWriter interface {
		create() (reader io.ReadWriter, deleteFunc func() error, resetFunc func() error, err error)
	}
}

func newRunCreator(memLimit int, less LessFunc, converter InputConverter) *runCreator {
	return &runCreator{
		memLimit:   memLimit,
		less:       less,
		converter:  converter,
		readWriter: newReadWriter(),
	}
}

func (r *runCreator) createRuns(reader io.Reader) ([]io.ReadWriter, []func() error, error) {
	runs := make([]io.ReadWriter, 0)
	deleteRuns := make([]func() error, 0)
	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanLines)
	isEOF := false
	var err error
	sorter := &runSorter{less: r.less}
	for !isEOF {
		populate := time.Now()
		sorter.data, isEOF, err = r.getChunk(scanner)
		if err != nil {
			deleteCreatedRuns(deleteRuns)
			return nil, nil, errors.Wrap(err, "populate heap")
		}
		if len(sorter.data) == 0 {
			break
		}
		sort.Sort(sorter)
		fmt.Println("populate time: ", time.Since(populate))
		run, delete, reset, err := r.flushToRun(sorter.data)
		if err != nil {
			return nil, nil, errors.Wrap(err, "flush heap")
		}
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

func (r *runCreator) getChunk(scanner *bufio.Scanner) ([]interface{}, bool, error) {
	heapMemSize := 0
	arr := make([]interface{}, 0)
	for {
		scanned := scanner.Scan()
		if !scanned {
			if scanner.Err() != nil {
				return nil, false, errors.Wrap(scanner.Err(), "read from input")
			}
			return arr, true, nil
		}
		line := scanner.Bytes()
		runData, err := r.converter.ToStructured(line)
		if err != nil {
			return nil, false, errors.Wrap(err, "convert string to int")
		}
		arr = append(arr, runData)
		heapMemSize += len(line)
		if heapMemSize > r.memLimit {
			return arr, false, nil
		}
	}
}

func (r *runCreator) flushToRun(chunk []interface{}) (reader io.ReadWriter, deleteFunc func() error, resetFunc func() error, err error) {
	//New allocation each time. Use buffer pool
	b := new(bytes.Buffer)
	for _, v := range chunk {
		byteData, err := r.converter.ToBytes(v)
		if err != nil {
			return nil, nil, nil, errors.Wrap(err, "convert to bytes")
		}
		_, err = b.Write(byteData)
		if err != nil {
			return nil, nil, nil, errors.Wrap(err, "write to buffer")
		}
		_, err = b.WriteString("\n")
		if err != nil {
			return nil, nil, nil, errors.Wrap(err, "write new line")
		}
	}
	run, delete, reset, err := r.readWriter.create()
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "create read writer")
	}
	_, err = b.WriteTo(run)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "write to run")
	}
	return run, delete, reset, nil
}

func deleteCreatedRuns(deleteFuncs []func() error) {
	for _, deleteRun := range deleteFuncs {
		//even if error occurs, no problem as it will be in temp directory
		_ = deleteRun()
	}
}
