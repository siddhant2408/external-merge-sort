package main

import (
	"bufio"
	"bytes"
	"io"
	"sort"

	"github.com/pkg/errors"
)

const tempFilePrefix = "exttemp-*"

func (e *ExtSort) createRuns(reader io.Reader) ([]io.ReadWriter, []func() error, error) {
	runs := make([]io.ReadWriter, 0)
	deleteRuns := make([]func() error, 0)
	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanLines)
	isEOF := false
	var err error
	sorter := &runSorter{less: e.inputHandler.Less}
	for !isEOF {
		sorter.data, isEOF, err = e.getChunk(scanner)
		if err != nil {
			e.deleteCreatedRuns(deleteRuns)
			return nil, nil, errors.Wrap(err, "populate heap")
		}
		if len(sorter.data) == 0 {
			break
		}
		sort.Sort(sorter)
		run, delete, reset, err := e.flushToRun(sorter.data)
		if err != nil {
			return nil, nil, errors.Wrap(err, "flush heap")
		}
		runs = append(runs, run)
		deleteRuns = append(deleteRuns, delete)
		err = reset()
		if err != nil {
			e.deleteCreatedRuns(deleteRuns)
			return nil, nil, errors.Wrap(err, "reset run")
		}
	}
	return runs, deleteRuns, nil
}

func (e *ExtSort) getChunk(scanner *bufio.Scanner) ([]interface{}, bool, error) {
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
		runData, err := e.inputHandler.ToStructured(line)
		if err != nil {
			return nil, false, errors.Wrap(err, "convert string to int")
		}
		arr = append(arr, runData)
		heapMemSize += len(line)
		if heapMemSize > e.memLimit {
			return arr, false, nil
		}
	}
}

func (e *ExtSort) flushToRun(chunk []interface{}) (reader io.ReadWriter, deleteFunc func() error, resetFunc func() error, err error) {
	//New allocation each time. Use buffer pool
	b := new(bytes.Buffer)
	for _, v := range chunk {
		byteData, err := e.inputHandler.ToBytes(v)
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
	run, delete, reset, err := e.readWriter.create()
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "create read writer")
	}
	_, err = b.WriteTo(run)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "write to run")
	}
	return run, delete, reset, nil
}

func (e *ExtSort) deleteCreatedRuns(deleteFuncs []func() error) {
	for _, deleteRun := range deleteFuncs {
		//even if error occurs, no problem as it will be in temp directory
		_ = deleteRun()
	}
}
