package extsort

import (
	"io"
	"strings"

	"github.com/pkg/errors"
)

const minMemLimit = 1 << 20

//ExtSort is the sorting service
type ExtSort struct {
	memLimit   int
	runCreator interface {
		create(chunk [][]string) (reader io.ReadSeeker, deleteFunc func() error, err error)
	}
	//map to determine the position of each header
	headerMap map[string]int
	//email or sms
	SortType string
	//import empty fields
	ImportEmpty bool
}

//New returns the interface for external sort
func New(memLimit int) *ExtSort {
	if memLimit < minMemLimit {
		memLimit = minMemLimit
	}
	return &ExtSort{
		memLimit:   memLimit,
		runCreator: newRunCreator(),
		headerMap:  make(map[string]int),
	}
}

//Sort sorts the srcFile and writes the result in the dstFile
func (e *ExtSort) Sort(dst io.Writer, src io.Reader) error {
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

func compare(a, b string) (bool, error) {
	res := strings.Compare(a, b)
	if res == -1 {
		return true, nil
	} else if res == 1 {
		return false, nil
	}
	return false, nil
}
