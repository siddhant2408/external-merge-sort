package main

import (
	"io"
	"io/ioutil"
	"os"

	"github.com/pkg/errors"
)

const tempFilePrefix = "exttemp-*"

type runCreator struct{}

func newRunCreator() *runCreator {
	return &runCreator{}
}

func (rw *runCreator) create() (reader io.ReadWriter, deleteFunc func() error, resetFunc func() error, err error) {
	file, err := ioutil.TempFile(os.TempDir(), tempFilePrefix)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "create temp file")
	}
	//resetFunc resets the file pointer to the top of the file
	resetFunc = func() error {
		_, err := file.Seek(0, 0)
		return err
	}
	deleteFunc = func() error {
		return os.Remove(file.Name())
	}
	return file, deleteFunc, resetFunc, nil
}
