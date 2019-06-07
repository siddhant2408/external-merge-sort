package main

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

func (rw *readWriter) create() (reader io.ReadWriter, deleteFunc func() error, resetFunc func() error, err error) {
	file, err := ioutil.TempFile("./", tempFilePrefix)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "create temp file")
	}
	resetFunc = func() error {
		_, err := file.Seek(0, 0)
		return err
	}
	deleteFunc = func() error {
		return os.Remove(file.Name())
	}
	return file, deleteFunc, resetFunc, nil
}
