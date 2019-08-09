package extsort

type runSorter struct {
	data            [][]string
	compareKeyIndex int
}

func (r *runSorter) Len() int {
	return len(r.data)
}

func (r *runSorter) Less(i, j int) bool {
	isLess, err := compare(r.data[i][r.compareKeyIndex], r.data[j][r.compareKeyIndex])
	if err != nil {
		panic(err)
	}
	return isLess
}

func (r *runSorter) Swap(i, j int) {
	r.data[i], r.data[j] = r.data[j], r.data[i]
}
