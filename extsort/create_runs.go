package extsort

import (
	"bufio"
	"io"

	"github.com/pkg/errors"
)

const tempFilePrefix = "exttemp-*"

type runCreator struct {
	memLimit   int
	less       Less
	readWriter interface {
		create() (rw io.ReadWriter, delete func() error, err error)
	}
}

func newRunCreator(memLimit int, less Less) *runCreator {
	return &runCreator{
		memLimit:   memLimit,
		less:       less,
		readWriter: newReadWriter(),
	}
}

func (r *runCreator) createRuns(reader io.Reader) ([]io.ReadWriter, func() error, error) {
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


		heapEle, err := convert(data)
		if err != nil {
			
		}
		h.push()
		
		
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
