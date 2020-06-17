package extsort

import (
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/pkg/errors"
)

var (
	sorter ExtSort
)

func init() {
	sorter = New(0)
}

func BenchmarkSort(b *testing.B) {
	for _, csvSize := range []int{10000} {
		b.Run(fmt.Sprintf("csvSize_%d", csvSize), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				benchmarkSort(b, csvSize)
			}
		})
	}
}

func benchmarkSort(b *testing.B, csvSize int) {
	b.StopTimer()
	f := createInputFile(b, csvSize)
	defer os.Remove(f.Name())
	out, err := ioutil.TempFile("", "")
	if err != nil {
		b.Fatal(err)
	}
	b.StartTimer()
	err = sorter.Sort(out, f, 1)
	defer os.Remove("output.csv")
	if err != nil {
		b.Fatal(err.Error())
	}
}

func createInputFile(t testing.TB, size int) (f *os.File) {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatal(err)
	}
	err = populateInput(f, size)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Seek(0, 0)
	return f
}

func populateInput(w io.WriteSeeker, size int) error {
	writer := csv.NewWriter(w)
	defer writer.Flush()
	err := writer.WriteAll(getTestData(size))
	if err != nil {
		return errors.Wrap(err, "write to csv")
	}
	return nil
}

func getTestData(size int) [][]string {
	var data [][]string
	data = append(data, []string{"id", "email", "name", "age", "gender"})
	for i := 0; i < int(size); i++ {
		data = append(data, []string{strconv.Itoa(rand.Intn(size)), "test@xyz.com", "test", "123", "Male"})
	}
	return data
}
