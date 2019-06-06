//Package extsort provides interface for sorting extremely large files that cannot be loaded in memory all at once.
package extsort

import (
	"io"
	"os"

	"github.com/pkg/errors"
)

//Sorter interface provides external sorting mechanism
type Sorter interface {
	Sort(srcFile string, dstFile string) error
}

//Less determines the lesser of the two byte arrays
type Less func(a, b []byte) (bool, error)

type extSort struct {
	runCreator interface {
		createRuns(reader io.Reader) ([]io.ReadWriter, func() error, error)
	}
	runMerger interface {
		mergeRuns(runs []io.ReadWriter, dst io.Writer) error
	}
}

//New returns the interface for external sort
func New(memLimit int, less Less) (Sorter, error) {
	if memLimit == 0 {
		return nil, errors.New("invalid mem limit")
	}
	if less == nil {
		return nil, errors.New("nil less func")
	}
	return &extSort{
		runCreator: newRunCreator(memLimit, less),
		runMerger:  newRunMerger(less),
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
	runs, deleteRuns, err := e.runCreator.createRuns(src)
	if err != nil {
		return errors.Wrap(err, "create runs")
	}
	defer deleteRuns()
	err = e.runMerger.mergeRuns(runs, dst)
	if err != nil {
		return errors.Wrap(err, "merge runs")
	}
	return nil
}
