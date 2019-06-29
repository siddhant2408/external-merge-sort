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
		less:     compareEmail,
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
}

func TestMergeRunsSameEmail(t *testing.T) {
	var runs = make([]io.ReadSeeker, 3)
	for i := 0; i < 3; i++ {
		runs[i] = bytes.NewReader([]byte(fmt.Sprintf("%d,test@sendinblue.com\n", i)))
	}
	e := &ExtSort{
		memLimit: minMemLimit,
		less:     compareEmail,
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
}

func TestMergeDuplicates(t *testing.T) {
	runData := []string{
		fmt.Sprintf("1,test@sendinblue.com,sid,20\n"),
		fmt.Sprintf("2,test@sendinblue.com,,20\n"),
	}
	var runs = make([]io.ReadSeeker, 2)
	for i := 0; i < 1; i++ {
		runs[i] = bytes.NewReader([]byte(runData[i]))
	}
	e := &ExtSort{
		memLimit: minMemLimit,
		less:     compareEmail,
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
}
