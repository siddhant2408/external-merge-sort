package main

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"strconv"

	randomdata "github.com/Pallinder/go-randomdata"
)

func main() {
	inputFile := "input.csv"
	outputFile := "output.csv"
	//populate input file
	f, err := os.Create(inputFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	writer := csv.NewWriter(f)
	defer writer.Flush()

	var data [][]string
	inputSize := 100000

	err = writer.Write([]string{"id", "email", "name", "age", "gender"})
	if err != nil {
		panic(err)
	}
	for i := 0; i < int(inputSize); i++ {
		data = append(data, []string{strconv.Itoa(rand.Intn(inputSize)), randomdata.Email(), "sid", strconv.Itoa(rand.Intn(100)), "Male"})
	}
	err = writer.WriteAll(data)
	if err != nil {
		panic(err)
	}
	err = New(0).Sort(inputFile, outputFile)
	if err != nil {
		fmt.Println(err)
	}
}
