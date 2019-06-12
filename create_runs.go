package main

import (
	"encoding/csv"
	"io"
	"sort"

	"github.com/pkg/errors"
)

func (e *ExtSort) createRuns(reader io.Reader) ([]io.ReadWriter, []func() error, error) {
	runs := make([]io.ReadWriter, 0)
	deleteRuns := make([]func() error, 0)
	csvReader := csv.NewReader(reader)
	//read headers
	_, err := csvReader.Read()
	if err != nil {
		return nil, nil, errors.Wrap(err, "read csv headers")
	}
	isEOF := false
	sorter := &runSorter{
		less: e.less,
	}
	for !isEOF {
		sorter.data, isEOF, err = e.getChunk(csvReader)
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

func (e *ExtSort) getChunk(csvReader *csv.Reader) ([][]string, bool, error) {
	heapMemSize := 0
	arr := make([][]string, 0)
	for {
		line, err := csvReader.Read()
		if err != nil {
			if err != io.EOF {
				return nil, false, errors.Wrap(err, "read from input")
			}
			//EOF reached
			return arr, true, nil
		}
		//skip empty lines
		if len(line) == 0 {
			continue
		}
		arr = append(arr, line)
		heapMemSize += e.getLineMemSize(line)
		if heapMemSize > e.memLimit {
			return arr, false, nil
		}
	}
}

func (e *ExtSort) getLineMemSize(line []string) int {
	size := 0
	for _, val := range line {
		size += len([]byte(val))
	}
	return size
}

func (e *ExtSort) flushToRun(chunk [][]string) (reader io.ReadWriter, deleteFunc func() error, resetFunc func() error, err error) {
	run, delete, reset, err := e.runCreator.create()
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "create read writer")
	}
	writer := csv.NewWriter(run)
	err = writer.WriteAll(chunk)
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
