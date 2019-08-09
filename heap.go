package extsort

var maxVal = &heapData{
	data: nil,
}

type heapData struct {
	runID int
	data  []string
}

type mergeHeap struct {
	heapData        []*heapData
	compareKeyIndex int
}

func (h *mergeHeap) Len() int { return len(h.heapData) }

func (h *mergeHeap) Less(i, j int) bool {
	if h.heapData[i] == maxVal {
		return false
	} else if h.heapData[j] == maxVal {
		return true
	}
	isLess, err := compare(h.heapData[i].data[h.compareKeyIndex], h.heapData[j].data[h.compareKeyIndex])
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
