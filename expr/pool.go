package expr

import (
	"sync"

	"github.com/grafana/metrictank/schema"
)

var pointSlicePool *sync.Pool

// Pool tells the expr library which pool to use for temporary []schema.Point
// this lets the expr package effectively create and drop point slices as needed
// it is recommended you use the same pool in your application, e.g. to get slices
// when loading the initial data, and to return the buffers back to the pool once
// the output from this package's processing is no longer needed.
func Pool(p *sync.Pool) {
	pointSlicePool = p
}

// GetPooledSliceAtLeastSize returns a 0-len slice that can hold at least minSize Points
func GetPooledSliceAtLeastSize(minSize int) []schema.Point {
	out := pointSlicePool.Get().([]schema.Point)
	if cap(out) < minSize {
		// TODO - return `out` to pool? Could just fetch it again next iteration
		out = make([]schema.Point, 0, minSize)
	}
	return out
}
