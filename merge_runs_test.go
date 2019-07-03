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
		runs[i] = bytes.NewReader([]byte(fmt.Sprintf("%d,test@sendinblue.com", i)))
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
	expected := "0,test@sendinblue.com\n"
	actual, err := out.ReadString('\n')
	if err != nil {
		t.Fatal(err.Error())
	}
	if expected != actual {
		t.Fatalf("unexpected output, expected %s, got %s", expected, actual)
	}
}

func TestMergeDuplicatesAllowEmptyImport(t *testing.T) {
	e := &ExtSort{
		memLimit: minMemLimit,
		sortType: sortTypeEmail,
		headerMap: map[string]int{
			"email": 1,
		},
		importEmpty: true,
	}
	newEle := []string{"1", "test@sendinblue.com", "", "30"}
	heapEle := []string{"2", "test@sendinblue.com", "test", "20"}

	expected := []string{"1", "test@sendinblue.com", "", "30"}
	actual := e.getMergedValue(newEle, heapEle)

	for i, v := range actual {
		if expected[i] != v {
			t.Fatal("incorrect merge")
		}
	}
}

func TestMergeDuplicatesNoEmptyImport(t *testing.T) {
	e := &ExtSort{
		memLimit: minMemLimit,
		sortType: sortTypeEmail,
		headerMap: map[string]int{
			"email": 1,
		},
	}
	newEle := []string{"1", "test@sendinblue.com", "", "20"}
	heapEle := []string{"2", "test@sendinblue.com", "test", "20"}

	expected := []string{"1", "test@sendinblue.com", "test", "20"}
	actual := e.getMergedValue(newEle, heapEle)

	for i, v := range actual {
		if expected[i] != v {
			t.Fatal("incorrect merge")
		}
	}
}
