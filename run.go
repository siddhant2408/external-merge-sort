package main

import (
	"encoding/csv"
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

func (rw *runCreator) create(chunk [][]string) (reader io.ReadSeeker, deleteFunc func() error, err error) {
	file, err := ioutil.TempFile(os.TempDir(), tempFilePrefix)
	if err != nil {
		return nil, nil, errors.Wrap(err, "create temp file")
	}
	deleteFunc = func() error {
		return os.Remove(file.Name())
	}
	writer := csv.NewWriter(file)
	err = writer.WriteAll(chunk)
	if err != nil {
		return nil, nil, errors.Wrap(err, "write to file")
	}
	return file, deleteFunc, nil
}
