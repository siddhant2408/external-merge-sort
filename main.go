package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"

	"github.com/pkg/errors"
)

func main() {
	inputFile := "input.txt"
	outputFile := "output.txt"
	//populate input file
	f, err := os.Create(inputFile)
	if err != nil {
		panic(err)
	}
	inputSize := 100000
	for i := 0; i < int(inputSize); i++ {
		fmt.Fprintln(f, rand.Intn(inputSize))
	}
	f.Close()

	err = New(0, &input{}).Sort(inputFile, outputFile)
	if err != nil {
		fmt.Println(err)
	}
}

type input struct{}

func (i *input) ToStructured(a []byte) (interface{}, error) {
	val, err := strconv.Atoi(string(a))
	if err != nil {
		return nil, errors.Wrap(err, "string convert")
	}
	return val, nil
}

func (i *input) ToBytes(a interface{}) ([]byte, error) {
	return []byte(strconv.Itoa(a.(int))), nil
}

func (i *input) Less(a, b interface{}) (bool, error) {
	if a.(int) < b.(int) {
		return true, nil
	}
	return false, nil
}
