package main

import (
	"fmt"
	"math/rand"
	"os"
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
	for i := 0; i < inputSize; i++ {
		fmt.Fprintln(f, rand.Intn(inputSize))
	}
	f.Close()

	err = New(0).Sort(inputFile, outputFile)
	if err != nil {
		fmt.Println(err)
	}
}
