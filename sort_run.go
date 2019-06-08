package main

type runSorter struct {
	data []interface{}
	less LessFunc
}

func (r *runSorter) Len() int {
	return len(r.data)
}

func (r *runSorter) Less(i, j int) bool {
	isLess, err := r.less(r.data[i], r.data[j])
	if err != nil {
		panic(err)
	}
	return isLess
}

func (r *runSorter) Swap(i, j int) {
	r.data[i], r.data[j] = r.data[j], r.data[i]
}
