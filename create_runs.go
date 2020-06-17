package extsort

import (
	"encoding/csv"
	"io"
	"sort"

	"github.com/pkg/errors"
)

//check for valid sortIndex
func (e *extSort) createRuns(reader io.Reader) (_ []io.ReadSeeker, _ []func() error, err error) {
	runs := make([]io.ReadSeeker, 0)
	deleteRuns := make([]func() error, 0)
	csvReader := csv.NewReader(reader)
	csvReader.ReuseRecord = true
	isEOF := false
	sorter := &runSorter{
		compareKeyIndex: e.sortIndex,
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

func (e *extSort) getChunk(csvReader *csv.Reader) ([][]string, bool, error) {
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

func (e *extSort) getLineMemSize(line []string) int {
	size := 0
	for _, val := range line {
		size += len([]byte(val))
	}
	return size
}

func (e *extSort) deleteCreatedRuns(deleteFuncs []func() error) {
	for _, deleteRun := range deleteFuncs {
		//even if error occurs, no problem as it will be in temp directory
		_ = deleteRun()
	}
}
