package main

import (
	"bytes"
	"io"
	"sort"
	"strings"
	"testing"

	"github.com/pkg/errors"
)

func TestExtSort(t *testing.T) {
	e := &ExtSort{
		memLimit:   minMemLimit,
		less:       compareEmail,
		runCreator: &testRunCreator{},
		sortType:   sortTypeEmail,
		headerMap:  make(map[string]int),
	}
	input := new(bytes.Buffer)
	err := populateInput(input, 10)
	if err != nil {
		t.Fatal(err.Error())
	}
	output := new(bytes.Buffer)
	err = e.sort(input, output)
	if err != nil {
		t.Fatal(err.Error())
	}
	isSorted, err := isSorted(output, e.less)
	if err != nil {
		t.Fatal(err.Error())
	}
	if !isSorted {
		t.Fatal("output not sorted")
	}
}

func isSorted(b *bytes.Buffer, less Less) (bool, error) {
	var sortedData [][]string
	for {
		line, err := b.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return false, errors.Wrap(err, "read string")
		}
		sortedData = append(sortedData, strings.Split(line, ","))
	}
	return sort.IsSorted(&runSorter{
		data: sortedData,
		less: less,
	}), nil
}

type testRunCreator struct{}

func (tr *testRunCreator) create(chunk [][]string) (reader io.ReadSeeker, deleteFunc func() error, err error) {
	return bytes.NewReader(convertToByte(chunk)), func() error { return nil }, nil
}

func convertToByte(chunk [][]string) []byte {
	b := new(strings.Builder)
	for _, v := range chunk {
		b.WriteString(strings.Join(v, ","))
		b.WriteString("\n")
	}
	return []byte(b.String())
}
