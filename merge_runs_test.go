package main

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

func TestMergeRunsDiffEmail(t *testing.T) {
	var runs = make([]io.ReadSeeker, 3)
	for i := 0; i < 3; i++ {
		runs[i] = bytes.NewReader([]byte(fmt.Sprintf("%d,test+%d@sendinblue.com\n", i, i)))
	}
	e := &ExtSort{
		memLimit: minMemLimit,
		sortType: sortTypeEmail,
		headerMap: map[string]int{
			"email": 1,
		},
	}
	out := new(bytes.Buffer)
	err := e.mergeRuns(runs, out)
	if err != nil {
		t.Fatal(err.Error())
	}
	isSorted, err := isSorted(out, e.headerMap[e.sortType])
	if err != nil {
		t.Fatal(err.Error())
	}
	if !isSorted {
		t.Fatal("output not sorted")
	}
}

func TestMergeRunsSameEmail(t *testing.T) {
	var runs = make([]io.ReadSeeker, 3)
	for i := 0; i < 3; i++ {
		runs[i] = bytes.NewReader([]byte(fmt.Sprintf("%d,test@sendinblue.com\n", i)))
	}
	e := &ExtSort{
		memLimit: minMemLimit,
		sortType: sortTypeEmail,
		headerMap: map[string]int{
			"email": 1,
		},
	}
	out := new(bytes.Buffer)
	err := e.mergeRuns(runs, out)
	if err != nil {
		t.Fatal(err.Error())
	}
	isSorted, err := isSorted(out, e.headerMap[e.sortType])
	if err != nil {
		t.Fatal(err.Error())
	}
	if !isSorted {
		t.Fatal("output not sorted")
	}
}

func TestMergeDuplicates(t *testing.T) {
	runData := []string{
		"1,test@sendinblue.com,,20\n",
		"2,test@sendinblue.com,test,20\n",
	}
	var runs = make([]io.ReadSeeker, 2)
	for i := 0; i < 2; i++ {
		runs[i] = bytes.NewReader([]byte(runData[i]))
	}
	e := &ExtSort{
		memLimit: minMemLimit,
		sortType: sortTypeEmail,
		headerMap: map[string]int{
			"email": 1,
		},
		importEmpty: true,
	}
	out := new(bytes.Buffer)
	err := e.mergeRuns(runs, out)
	if err != nil {
		t.Fatal(err.Error())
	}
	line, err := out.ReadString('\n')
	if err != nil {
		t.Fatal(err.Error())
	}
	if line != runData[0] {
		t.Fatal("incorrect duplicate merge")
	}
}
