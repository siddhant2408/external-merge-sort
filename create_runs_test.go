package extsort

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"testing"
)

func TestCreateSingleRun(t *testing.T) {
	e := &ExtSort{
		memLimit:   minMemLimit,
		runCreator: &testRunCreator{},
		SortType:   sortTypeEmail,
		headerMap:  make(map[string]int),
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
	e := &ExtSort{
		memLimit:   minMemLimit,
		runCreator: &testRunCreator{},
		SortType:   sortTypeEmail,
		headerMap:  make(map[string]int),
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
	e := &ExtSort{
		memLimit:   minMemLimit,
		runCreator: &testRunCreator{},
		SortType:   sortTypeEmail,
		headerMap:  make(map[string]int),
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

func BenchmarkCreateMultipleRuns(b *testing.B) {
	e := &ExtSort{
		memLimit:   minMemLimit,
		runCreator: &runCreator{},
		SortType:   sortTypeEmail,
		headerMap:  make(map[string]int),
	}

	//prepare input
	var data [][]string
	data = append(data, []string{"id", "email", "name", "gender"})
	for i := 0; i < 100000; i++ {
		data = append(data, []string{
			strconv.Itoa(i),
			fmt.Sprintf("test+%d@sendinblue.com", i),
			"test",
			"male"})
	}
	f, err := ioutil.TempFile(os.TempDir(), "")
	if err != nil {
		b.Fatal(err.Error())
	}
	defer os.Remove(f.Name())
	err = csv.NewWriter(f).WriteAll(data)
	if err != nil {
		b.Fatal(err.Error())
	}
	f.Seek(0, 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runs, deleteFunc, err := e.createRuns(f)
		if err != nil {
			b.Fatal(err.Error())
		}
		if deleteFunc == nil {
			b.Fatal("nil delete functions")
		}
		if runs == nil {
			b.Fatal("no run created")
		}
		e.deleteCreatedRuns(deleteFunc)
		f.Seek(0, 0)
	}
}
