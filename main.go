package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"

	"github.com/Pallinder/go-randomdata"
	"github.com/pkg/errors"
)

func main() {
	inputFile := "input.csv"
	outputFile := "output.csv"

	createInputFile(inputFile, 1000)
	err := New(0, sortTypeEmail, false).Sort(inputFile, outputFile)
	if err != nil {
		fmt.Println(err)
	}
}

func compare(a, b string) (bool, error) {
	res := strings.Compare(a, b)
	if res == -1 {
		return true, nil
	} else if res == 1 {
		return false, nil
	}
	return false, nil
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
