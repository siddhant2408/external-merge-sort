package main

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/pkg/errors"
)

//Sorter interface provides external sorting mechanism
type Sorter interface {
	Sort(srcFile string, dstFile string) error
}

type extSort struct {
	runCreator interface {
		createRuns(reader io.Reader) ([]io.ReadWriter, []func() error, error)
	}
	runMerger interface {
		mergeRuns(runs []io.ReadWriter, dst io.Writer) error
	}
}

//New returns the interface for external sort
func New(memLimit int) Sorter {
	if memLimit == 0 {
		memLimit = 1 << 16
	}
	return &extSort{
		runCreator: newRunCreator(memLimit),
		runMerger:  newRunMerger(),
	}
}

func (e *extSort) Sort(srcFile string, dstFile string) error {
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

func (e *extSort) sort(src io.Reader, dst io.Writer) error {
	create := time.Now()
	runs, deleteRuns, err := e.runCreator.createRuns(src)
	if err != nil {
		return errors.Wrap(err, "create runs")
	}
	defer deleteCreatedRuns(deleteRuns)
	fmt.Println(time.Since(create))

	merge := time.Now()
	err = e.runMerger.mergeRuns(runs, dst)
	if err != nil {
		return errors.Wrap(err, "merge runs")
	}
	fmt.Println(time.Since(merge))
	return nil
}
