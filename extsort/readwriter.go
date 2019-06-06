package extsort

import (
	"io"
	"io/ioutil"
	"os"

	"github.com/pkg/errors"
)

type readWriter struct{}

func newReadWriter() *readWriter {
	return &readWriter{}
}

func (rw *readWriter) create() (io.ReadWriter, func() error, error) {
	file, err := ioutil.TempFile(os.TempDir(), tempFilePrefix)
	if err != nil {
		return nil, nil, errors.Wrap(err, "create temp file")
	}
	return file, file.Close, nil
}
