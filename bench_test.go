package main

import (
	"os"
	"testing"
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
