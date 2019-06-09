package main

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/pkg/errors"
)

const minMemLimit = 1 << 16

//InputHandler provides methods for manipulating input
type InputHandler interface {
	ToStructured(a []byte) (interface{}, error)
	ToBytes(a interface{}) ([]byte, error)
	Less(a interface{}, b interface{}) (bool, error)
}

//ExtSort is the sorting service
type ExtSort struct {
	memLimit     int
	inputHandler InputHandler
	readWriter   interface {
		create() (reader io.ReadWriter, deleteFunc func() error, resetFunc func() error, err error)
	}
}

//New returns the interface for external sort
func New(memLimit int, inputHandler InputHandler) *ExtSort {
	if memLimit < minMemLimit {
		memLimit = minMemLimit
	}
	return &ExtSort{
		memLimit:     memLimit,
		inputHandler: inputHandler,
		readWriter:   newReadWriter(),
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

	merge := time.Now()
	err = e.mergeRuns(runs, dst)
	if err != nil {
		return errors.Wrap(err, "merge runs")
	}
	fmt.Println("merge time: ", time.Since(merge))
	return nil
}
