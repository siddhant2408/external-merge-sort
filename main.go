package main

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"

	randomdata "github.com/Pallinder/go-randomdata"
)

func main() {
	inputFile := "input.csv"
	outputFile := "output.csv"

	createInputFile(inputFile, 100000)
	err := New(0, compareEmail).Sort(inputFile, outputFile)
	if err != nil {
		fmt.Println(err)
	}
}

func compareEmail(a, b []string) (bool, error) {
	res := strings.Compare(a[1], b[1])
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

	writer := csv.NewWriter(f)
	defer writer.Flush()

	var data [][]string

	err = writer.Write([]string{"id", "email", "name", "age", "gender"})
	if err != nil {
		panic(err)
	}
	for i := 0; i < int(size); i++ {
		data = append(data, []string{strconv.Itoa(rand.Intn(size)), randomdata.Email(), "sid", strconv.Itoa(rand.Intn(100)), "Male"})
	}
	err = writer.WriteAll(data)
	if err != nil {
		panic(err)
	}
}
