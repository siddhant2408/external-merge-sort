package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/pkg/errors"
)

func TestExtSortDiffEmails(t *testing.T) {
	e := &ExtSort{
		memLimit:   minMemLimit,
		runCreator: &testRunCreator{},
		sortType:   sortTypeEmail,
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

	output := new(bytes.Buffer)
	err = e.sort(input, output)
	if err != nil {
		t.Fatal(err.Error())
	}
	isSorted, err := isSorted(output, e.headerMap[e.sortType])
	if err != nil {
		t.Fatal(err.Error())
	}
	if !isSorted {
		t.Fatal("output not sorted")
	}
}

func TestExtSortSameEmails(t *testing.T) {
	e := &ExtSort{
		memLimit:   minMemLimit,
		runCreator: &testRunCreator{},
		sortType:   sortTypeEmail,
		headerMap:  make(map[string]int),
	}

	//prepare input
	var data [][]string
	data = append(data, []string{"id", "email", "name", "gender"})
	for i := 0; i < 5; i++ {
		data = append(data, []string{
			strconv.Itoa(i),
			"test@sendinblue.com",
			"test",
			"male",
		})
	}
	input := new(bytes.Buffer)
	err := csv.NewWriter(input).WriteAll(data)
	if err != nil {
		t.Fatal(err.Error())
	}

	out := new(bytes.Buffer)
	err = e.sort(input, out)
	if err != nil {
		t.Fatal(err.Error())
	}
	expected := "0,test@sendinblue.com,test,male\n"
	actual, err := out.ReadString('\n')
	if err != nil {
		t.Fatal(err.Error())
	}
	if expected != actual {
		t.Fatalf("unexpected output, expected %s, got %s", expected, actual)
	}
}

func TestExtSortDuplicateEmails(t *testing.T) {
	e := &ExtSort{
		memLimit:   minMemLimit,
		runCreator: &testRunCreator{},
		sortType:   sortTypeEmail,
		headerMap:  make(map[string]int),
	}

	//prepare input
	var data = [][]string{
		{"id", "email", "name", "gender"},
		{"1", "test+1@sendinblue.com", "test", "male"},
		{"2", "test+1@sendinblue.com", "test", "male"},
		{"3", "test+2@sedinblue.com", "test", "male"},
		{"4", "test+3@sendinblue.com", "test", "male"},
	}
	input := new(bytes.Buffer)
	err := csv.NewWriter(input).WriteAll(data)
	if err != nil {
		t.Fatal(err.Error())
	}

	out := new(bytes.Buffer)
	err = e.sort(input, out)
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

func TestExtSortSingleEmail(t *testing.T) {
	e := &ExtSort{
		memLimit:   minMemLimit,
		runCreator: &testRunCreator{},
		sortType:   sortTypeEmail,
		headerMap:  make(map[string]int),
	}

	//prepare input
	var data = [][]string{
		{"id", "email", "name", "gender"},
		{"1", "test+1@sendinblue.com", "test", "male"},
	}
	input := new(bytes.Buffer)
	err := csv.NewWriter(input).WriteAll(data)
	if err != nil {
		t.Fatal(err.Error())
	}

	out := new(bytes.Buffer)
	err = e.sort(input, out)
	if err != nil {
		t.Fatal(err.Error())
	}
	expected := "1,test+1@sendinblue.com,test,male\n"
	actual, err := out.ReadString('\n')
	if err != nil {
		t.Fatal(err.Error())
	}
	if expected != actual {
		t.Fatalf("unexpected output, expected %s, got %s", expected, actual)
	}
}

func TestExtSortEmptyCSV(t *testing.T) {
	e := &ExtSort{
		memLimit:   minMemLimit,
		runCreator: &testRunCreator{},
		sortType:   sortTypeEmail,
		headerMap:  make(map[string]int),
	}

	input := new(bytes.Buffer)
	out := new(bytes.Buffer)
	err := e.sort(input, out)
	if err == nil {
		t.Fatal("no error")
	}
}

func TestExtSortSingleAttribute(t *testing.T) {
	e := &ExtSort{
		memLimit:   minMemLimit,
		runCreator: &testRunCreator{},
		sortType:   sortTypeEmail,
		headerMap:  make(map[string]int),
	}

	//prepare input
	var data = [][]string{
		{"email"},
		{"test+1@sendinblue.com"},
		{"test+1@sendinblue.com"},
		{"test+2@sedinblue.com"},
		{"test+3@sendinblue.com"},
	}
	input := new(bytes.Buffer)
	err := csv.NewWriter(input).WriteAll(data)
	if err != nil {
		t.Fatal(err.Error())
	}

	out := new(bytes.Buffer)
	err = e.sort(input, out)
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

//check if sorted and duplicates merged
func isSorted(b *bytes.Buffer, compareKeyIndex int) (bool, error) {
	var prevVal string
	for {
		line, err := b.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return false, errors.Wrap(err, "read string")
		}
		curVal := strings.Split(line, ",")[compareKeyIndex]
		if prevVal == "" {
			prevVal = curVal
			continue
		}
		if curVal <= prevVal {
			return false, nil
		}
	}
	return true, nil
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
