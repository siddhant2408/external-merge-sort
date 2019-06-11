package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
)

const minMemLimit = 1 << 20

//ExtSort is the sorting service
type ExtSort struct {
	memLimit   int
	runCreator interface {
		create() (reader io.ReadWriter, deleteFunc func() error, resetFunc func() error, err error)
	}
}

//New returns the interface for external sort
func New(memLimit int) *ExtSort {
	if memLimit < minMemLimit {
		memLimit = minMemLimit
	}
	return &ExtSort{
		memLimit:   memLimit,
		runCreator: newRunCreator(),
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
	now := time.Now()
	runs, deleteRuns, err := e.createRuns(src)
	if err != nil {
		return errors.Wrap(err, "create runs")
	}
	defer e.deleteCreatedRuns(deleteRuns)
	fmt.Println("create runs in:", time.Since(now))
	merge := time.Now()
	err = e.mergeRuns(runs, dst)
	if err != nil {
		return errors.Wrap(err, "merge runs")
	}
	fmt.Println("merge runs in:", time.Since(merge))
	return nil
}

func compareEmail(a, b []string) (bool, error) {
	res := strings.Compare(a[1], b[1])
	if res == -1 {
		return true, nil
	} else if res == 1 {
		return false, nil
	}
	return false, nil
}
