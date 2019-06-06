package extsort

import "io"

type runMerger struct {
	less Less
}

func newRunMerger(less Less) *runMerger {
	return &runMerger{
		less: less,
	}
}

func (r *runMerger) mergeRuns(runs []io.ReadWriter, dst io.Writer) error {
	return nil
}
