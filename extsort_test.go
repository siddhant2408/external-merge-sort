package main

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestExtSort(t *testing.T) {
	r := &testRunCreator{}
	e := &ExtSort{
		less:       compareEmail,
		runCreator: r,
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
	if output.Len() != 10 {
		t.Log(output)
		t.Fatal("error")
	}
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
