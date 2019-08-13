package extsort

import (
	"bytes"
	"encoding/csv"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/pkg/errors"
)

type testExtSort struct {
	name         string
	data         [][]string
	expectedData []string
}

func TestExternalSortSuccess(t *testing.T) {
	for _, tc := range []testExtSort{
		{
			name: "Different Emails",
			data: [][]string{
				{"id", "email", "name", "gender"},
				{"1", "test+1@sendinblue.com", "test", "male"},
				{"2", "test+2@sendinblue.com", "test2", "male"},
				{"3", "test+3@sendinblue.com", "test3", "male"},
			},
			expectedData: []string{
				"id,email,name,gender",
				"1,test+1@sendinblue.com,test,male",
				"2,test+2@sendinblue.com,test2,male",
				"3,test+3@sendinblue.com,test3,male",
			},
		},
		{
			name: "Same Emails",
			data: [][]string{
				{"id", "email", "name", "gender"},
				{"1", "test@sendinblue.com", "test", "male"},
				{"2", "test@sendinblue.com", "test2", "male"},
				{"3", "test@sendinblue.com", "test3", "male"},
			},
			expectedData: []string{
				"id,email,name,gender",
				"1,test@sendinblue.com,test,male",
			},
		},
		{
			name: "Some Duplicate Emails",
			data: [][]string{
				{"id", "email", "name", "gender"},
				{"1", "test+1@sendinblue.com", "test", "male"},
				{"2", "test+1@sendinblue.com", "test", "male"},
				{"3", "test+2@sedinblue.com", "test", "male"},
				{"4", "test+3@sendinblue.com", "test", "male"},
			},
			expectedData: []string{
				"id,email,name,gender",
				"1,test+1@sendinblue.com,test,male",
				"3,test+2@sedinblue.com,test,male",
				"4,test+3@sendinblue.com,test,male",
			},
		},
		{
			name: "Single Email",
			data: [][]string{
				{"id", "email", "name", "gender"},
				{"1", "test+1@sendinblue.com", "test", "male"},
			},
			expectedData: []string{
				"id,email,name,gender",
				"1,test+1@sendinblue.com,test,male",
			},
		},
		{
			name: "Single Attribute",
			data: [][]string{
				{"email"},
				{"test+1@sendinblue.com"},
				{"test+1@sendinblue.com"},
				{"test+2@sedinblue.com"},
				{"test+3@sendinblue.com"},
			},
			expectedData: []string{
				"email",
				"test+1@sendinblue.com",
				"test+2@sedinblue.com",
				"test+3@sendinblue.com",
			},
		},
		{
			name: "Special Characters",
			data: [][]string{
				{"id", "email", "name", "gender"},
				{"1", "test+&^$(''@sendinblue.com", "test", "male"},
			},
			expectedData: []string{
				"id,email,name,gender",
				"1,test+&^$(''@sendinblue.com,test,male",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &ExtSort{
				memLimit:   minMemLimit,
				runCreator: &testRunCreator{},
				SortType:   sortTypeEmail,
				headerMap:  make(map[string]int),
			}
			input := new(bytes.Buffer)
			err := csv.NewWriter(input).WriteAll(tc.data)
			if err != nil {
				t.Fatal(err.Error())
			}

			output := new(bytes.Buffer)
			err = e.sort(input, output)
			if err != nil {
				t.Fatal(err.Error())
			}

			data := bytesToLine(output)

			if !reflect.DeepEqual(data, tc.expectedData) {
				t.Fatalf("expected %q, got %q", tc.expectedData, data)
			}

			isSorted, err := isSorted(output, e.headerMap[e.SortType])
			if err != nil {
				t.Fatal(err.Error())
			}
			if !isSorted {
				t.Fatal("output not sorted")
			}
		})
	}
}

func bytesToLine(output *bytes.Buffer) []string {
	data := make([]string, 0)
	for {
		line, err := output.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
		}
		data = append(data, line[:len(line)-1])
	}
	return data
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
