package extsort

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"strconv"
	"testing"
)

func TestCreateSingleRun(t *testing.T) {
	e := &extSort{
		memLimit:   minMemLimit,
		runCreator: &testRunCreator{},
		sortIndex:  1,
	}

	//prepare input
	var data [][]string
	data = append(data, []string{"id", "email", "name", "gender"})
	for i := 0; i < 5; i++ {
		data = append(data, []string{
			strconv.Itoa(i),
			fmt.Sprintf("test+%d@sendinblue.com", i),
			"test",
			"male"})
	}
	input := new(bytes.Buffer)
	err := csv.NewWriter(input).WriteAll(data)
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
	if runs == nil {
		t.Fatal("no run created")
	}
}

func TestCreateMultipleRuns(t *testing.T) {
	e := &extSort{
		memLimit:   minMemLimit,
		runCreator: &testRunCreator{},
		sortIndex:  1,
	}

	//prepare input
	var data [][]string
	data = append(data, []string{"id", "email", "name", "gender"})
	for i := 0; i < 50000; i++ {
		data = append(data, []string{
			strconv.Itoa(i),
			fmt.Sprintf("test+%d@sendinblue.com", i),
			"test",
			"male"})
	}
	input := new(bytes.Buffer)
	err := csv.NewWriter(input).WriteAll(data)
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
	if runs == nil {
		t.Fatal("no run created")
	}
	if len(runs) <= 1 {
		t.Fatal("incorrect runs created")
	}
}

func TestCreateRunsWithDuplicateEmails(t *testing.T) {
	e := &extSort{
		memLimit:   minMemLimit,
		runCreator: &testRunCreator{},
		sortIndex:  1,
	}

	//prepare input
	var data = [][]string{
		{"id", "email", "name", "gender"},
		{"0", "test@sendinblue.com", "test", "male"},
		{"1", "test@sendinblue.com", "test", "male"},
		{"0", "test+1@sendinblue.com", "test1", "male"},
	}
	input := new(bytes.Buffer)
	err := csv.NewWriter(input).WriteAll(data)
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
	if runs == nil {
		t.Fatal("no run created")
	}
}
