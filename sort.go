package main

import (
	"io"
	"os"

	"github.com/pkg/errors"
)

const (
	minMemLimit   = 1 << 20
	sortTypeEmail = "email"
	sortTypeSMS   = "sms"
)

//Less compares two csv lines
type Less func(a, b []string) (bool, error)

//ExtSort is the sorting service
type ExtSort struct {
	memLimit   int
	less       Less
	runCreator interface {
		create() (reader io.ReadWriter, deleteFunc func() error, resetFunc func() error, err error)
	}
	//email or sms
	sortType string
	//map to determine the position of each header
	headerMap map[string]int
}

//New returns the interface for external sort
func New(memLimit int, less Less, sortType string) *ExtSort {
	if memLimit < minMemLimit {
		memLimit = minMemLimit
	}
	return &ExtSort{
		memLimit:   memLimit,
		less:       less,
		runCreator: newRunCreator(),
		sortType:   sortType,
		headerMap:  make(map[string]int),
	}
}

//Sort sorts the srcFile and writes the result in the dstFile
func (e *ExtSort) Sort(srcFile string, dstFile string) error {
	src, err := os.Open(srcFile)
	if err != nil {
		return errors.Wrap(err, "open source file")
	}
	defer src.Close()

	dst, err := os.Create(dstFile)
	if err != nil {
		return errors.Wrap(err, "create dst file")
	}
	defer dst.Close()

	err = e.sort(src, dst)
	if err != nil {
		return errors.Wrap(err, "sort")
	}
	return nil
}

func (e *ExtSort) sort(src io.Reader, dst io.Writer) error {
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
