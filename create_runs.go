package extsort

import (
	"encoding/csv"
	"io"
	"sort"

	"github.com/pkg/errors"
)

func (e *ExtSort) createRuns(reader io.Reader) ([]io.ReadSeeker, []func() error, error) {
	runs := make([]io.ReadSeeker, 0)
	deleteRuns := make([]func() error, 0)
	csvReader := csv.NewReader(reader)
	err := e.readHeaders(csvReader)
	if err != nil {
		return nil, nil, errors.Wrap(err, "read csv headers")
	}
	isEOF := false
	sorter := &runSorter{
		compareKeyIndex: e.headerMap[e.SortType],
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
		run, delete, err := e.runCreator.create(sorter.data)
		if err != nil {
			return nil, nil, errors.Wrap(err, "flush heap")
		}
		runs = append(runs, run)
		deleteRuns = append(deleteRuns, delete)
		_, err = run.Seek(0, 0)
		if err != nil {
			e.deleteCreatedRuns(deleteRuns)
			return nil, nil, errors.Wrap(err, "reset run")
		}
	}
	return runs, deleteRuns, nil
}

func (e *ExtSort) readHeaders(csvReader *csv.Reader) error {
	headers, err := csvReader.Read()
	if err != nil {
		return errors.Wrap(err, "read csv headers")
	}
	if err != nil {
		return errors.Wrap(err, "write csv headers")
	}
	for i, v := range headers {
		e.headerMap[v] = i
	}
	return nil
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

func (e *ExtSort) deleteCreatedRuns(deleteFuncs []func() error) {
	for _, deleteRun := range deleteFuncs {
		//even if error occurs, no problem as it will be in temp directory
		_ = deleteRun()
	}
}
