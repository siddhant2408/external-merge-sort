package main

import (
	"bytes"
	"testing"
)

func TestCreateRuns(t *testing.T) {
	e := &ExtSort{
		memLimit:   minMemLimit,
		runCreator: &testRunCreator{},
		sortType:   sortTypeEmail,
		headerMap:  make(map[string]int),
	}
	input := new(bytes.Buffer)
	err := populateInput(input, 10)
	if err != nil {
		t.Fatal(err.Error())
	}
	runs, deleteFunc, err := e.createRuns(input)
	if err != nil {
		t.Fatal(err.Error())
	}
	if deleteFunc == nil {
		t.Fatal("nil delete functions")
	}
	if len(runs) != 1 {
		t.Fatal("incorrect number of runs")
	}
}
