//Package extsort provides interface for sorting extremely large files that cannot be loaded in memory all at once.
package extsort

import (
	"container/heap"
	"io"
	"os"

	"github.com/pkg/errors"
)

//Sorter interface provides external sorting mechanism
type Sorter interface {
	Sort(srcFile string, dstFile string) error
}

type extSort struct {
	memLimit int
	heap     heap.Interface
	runFunc  CreateReadWriterFunc
}

//New returns the interface for external sort
func New(memLimit int, heap heap.Interface, runFunc CreateReadWriterFunc) (Sorter, error) {
	if memLimit == 0 {
		memLimit = 1 << 32
	}
	if runFunc == nil {
		runFunc = newReadWriter()
	}
	if heap == nil {
		return nil, errors.New("invalid heap option")
	}
	return &extSort{
		memLimit: memLimit,
		heap:     heap,
		runFunc:  runFunc,
	}, nil
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
	runs, deleteRuns, err := e.createRuns(src)
	if err != nil {
		return errors.Wrap(err, "create runs")
	}
	defer deleteRuns()
	err = e.mergeRuns(runs, dst)
	if err != nil {
		return errors.Wrap(err, "merge runs")
	}
	return nil
}
