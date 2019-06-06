package extsort

import (
	"container/heap"
)

//HeapSorter sorts the input using min heap
type HeapSorter interface {
	heap.Interface
	convert(input []byte) (interface{}, error)
	toString() (string, error)
}
