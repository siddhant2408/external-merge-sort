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

//ExtSort is the sorting service
type ExtSort struct {
	memLimit   int
	runCreator interface {
		create(chunk [][]string) (reader io.ReadSeeker, deleteFunc func() error, err error)
	}
	//email or sms
	sortType string
	//map to determine the position of each header
	headerMap map[string]int
	//import empty fields
	importEmpty bool
}

//New returns the interface for external sort
func New(memLimit int, sortType string, importEmpty bool) *ExtSort {
	if memLimit < minMemLimit {
		memLimit = minMemLimit
	}
	return &ExtSort{
		memLimit:    memLimit,
		runCreator:  newRunCreator(),
		sortType:    sortType,
		headerMap:   make(map[string]int),
		importEmpty: importEmpty,
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
