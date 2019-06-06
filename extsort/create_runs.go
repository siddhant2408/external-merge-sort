package extsort

import (
	"bufio"
	"io"
	"io/ioutil"
	"os"

	"github.com/pkg/errors"
)

const tempFilePrefix = "exttemp-*"

//CreateReadWriterFunc creates a run file which implements io.ReadWriter
type CreateReadWriterFunc func() (io.ReadWriter, func() error, error)

func newReadWriter() CreateReadWriterFunc {
	return func() (io.ReadWriter, func() error, error) {
		file, err := ioutil.TempFile(os.TempDir(), tempFilePrefix)
		if err != nil {
			return nil, nil, errors.Wrap(err, "create temp file")
		}
		return file, file.Close, nil
	}
}

func (e *extSort) createRuns(reader io.Reader) ([]io.ReadWriter, func() error, error) {
	runFiles := make([]io.ReadWriter, 0)
	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanLines)
	//scan each value and add it to heap till memLimit reached.
	isEOF := false
	heapSize := 0
	for !isEOF {
		scanned := scanner.Scan()
		if !scanned {
			isEOF = true
			//flush heap
			continue
		}
		data := scanner.Bytes()
		e.heap.Push(data)
		heapSize += len(data)
		if heapSize > e.memLimit {
			heapSize = 0
			//TODO take care of delete files
			run, _, err := e.runFunc()
			if err != nil {
				return nil, nil, errors.Wrap(err, "create read writer")
			}
			runFiles = append(runFiles, run)
			//create new io.ReadWriter and flush to it
		}
	}
	return nil, nil, nil
}
