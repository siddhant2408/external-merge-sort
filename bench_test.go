package extsort

import (
	"encoding/csv"
	"io"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/Pallinder/go-randomdata"
	"github.com/pkg/errors"
)

var (
	sorter    *ExtSort
	inputFile = "input.csv"
)

func init() {
	sorter = New(0, "email", true)
}

func BenchmarkSort_10K(b *testing.B) {
	b.StopTimer()
	createInputFile(inputFile, 10000)
	b.StartTimer()
	defer os.Remove(inputFile)
	var err error
	for i := 0; i < b.N; i++ {
		err = sorter.Sort(inputFile, "output.csv")
	}
	defer os.Remove("output.csv")
	if err != nil {
		b.Fatal(err.Error())
	}
}

func BenchmarkSort_100K(b *testing.B) {
	b.StopTimer()
	createInputFile(inputFile, 100000)
	b.StartTimer()
	defer os.Remove(inputFile)
	var err error
	for i := 0; i < b.N; i++ {
		err = sorter.Sort(inputFile, "output.csv")
	}
	defer os.Remove("output.csv")
	if err != nil {
		b.Fatal(err.Error())
	}
}

func BenchmarkSort_1M(b *testing.B) {
	b.StopTimer()
	createInputFile(inputFile, 1000000)
	b.StartTimer()
	defer os.Remove(inputFile)
	var err error
	for i := 0; i < b.N; i++ {
		err = sorter.Sort(inputFile, "output.csv")
	}
	defer os.Remove("output.csv")
	if err != nil {
		b.Fatal(err.Error())
	}
}

func createInputFile(name string, size int) {
	f, err := os.Create(name)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	err = populateInput(f, size)
	if err != nil {
		panic(err)
	}
}

func populateInput(w io.Writer, size int) error {
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
		data = append(data, []string{strconv.Itoa(rand.Intn(size)), randomdata.Email(), "sid", strconv.Itoa(rand.Intn(100)), "Male"})
	}
	return data
}
