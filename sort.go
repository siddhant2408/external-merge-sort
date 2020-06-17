package extsort

import (
	"io"

	"github.com/pkg/errors"
)

const minMemLimit = 1 << 20

//ExtSort is the sorting service
type ExtSort interface {
	Sort(dst io.Writer, src io.Reader, sortIndex int) error
}

type extSort struct {
	memLimit   int
	runCreator interface {
		create(chunk [][]string) (reader io.ReadSeeker, deleteFunc func() error, err error)
	}
	sortIndex int
}

//New returns the interface for external sort
func New(memLimit int) ExtSort {
	if memLimit < minMemLimit {
		memLimit = minMemLimit
	}
	return &extSort{
		memLimit:   memLimit,
		runCreator: newRunCreator(),
	}
}

//Sort sorts the srcFile and writes the result in the dstFile
func (e *extSort) Sort(dst io.Writer, src io.Reader, sortIndex int) error {
	e.sortIndex = sortIndex
	runs, deleteRuns, err := e.createRuns(src)
	if err != nil {
		return errors.Wrap(err, "create runs")
	}
	defer e.deleteCreatedRuns(deleteRuns)
	err = e.mergeRuns(runs, dst)
	if err != nil {
		return errors.Wrap(err, "merge runs")
	}
	return nil
}
