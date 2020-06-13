package extsort

// import (
// 	"encoding/csv"
// 	"fmt"
// 	"io"
// 	"math/rand"
// 	"os"
// 	"strconv"
// 	"testing"

// 	"github.com/Pallinder/go-randomdata"
// 	"github.com/pkg/errors"
// )

// var (
// 	sorter *ExtSort
// )

// func init() {
// 	sorter = New(0)
// }

// func BenchmarkSort(b *testing.B) {
// 	for _, csvSize := range []int{10000, 100000, 1000000} {
// 		b.Run(fmt.Sprintf("csvSize_%d", csvSize), func(b *testing.B) {
// 			for i := 0; i < b.N; i++ {
// 				benchmarkSort(b, csvSize)
// 			}
// 		})
// 	}
// }

// func benchmarkSort(b *testing.B, csvSize int) {
// 	b.StopTimer()
// 	createInputFile(inputFile, csvSize)
// 	b.StartTimer()
// 	defer os.Remove(inputFile)
// 	err := sorter.Sort(inputFile, "output.csv")
// 	defer os.Remove("output.csv")
// 	if err != nil {
// 		b.Fatal(err.Error())
// 	}
// }

// func createInputFile(name string, size int) {
// 	f, err := os.Create(name)
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer f.Close()
// 	err = populateInput(f, size)
// 	if err != nil {
// 		panic(err)
// 	}
// }

// func populateInput(w io.Writer, size int) error {
// 	writer := csv.NewWriter(w)
// 	defer writer.Flush()
// 	err := writer.WriteAll(getTestData(size))
// 	if err != nil {
// 		return errors.Wrap(err, "write to csv")
// 	}
// 	return nil
// }

// func getTestData(size int) [][]string {
// 	var data [][]string
// 	data = append(data, []string{"id", "email", "name", "age", "gender"})
// 	for i := 0; i < int(size); i++ {
// 		data = append(data, []string{strconv.Itoa(rand.Intn(size)), randomdata.Email(), "sid", strconv.Itoa(rand.Intn(100)), "Male"})
// 	}
// 	return data
// }
