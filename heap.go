package main

type heapData struct {
	runID int
	data  interface{}
}

type mergeHeap struct {
	heapData []*heapData
	less     LessFunc
}

func (h *mergeHeap) Len() int { return len(h.heapData) }

func (h *mergeHeap) Less(i, j int) bool {
	if h.heapData[i].data == nil {
		return false
	} else if h.heapData[j].data == nil {
		return true
	}
	isLess, err := h.less(h.heapData[i].data, h.heapData[j].data)
	if err != nil {
		panic(err)
	}
	return isLess
}

func (h *mergeHeap) Swap(i, j int) { h.heapData[i], h.heapData[j] = h.heapData[j], h.heapData[i] }

func (h *mergeHeap) Push(x interface{}) {
	h.heapData = append(h.heapData, x.(*heapData))
}

func (h *mergeHeap) Pop() interface{} {
	old := h.heapData
	n := len(old)
	x := old[n-1]
	h.heapData = old[0 : n-1]
	return x
}
