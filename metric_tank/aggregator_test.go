package main

import (
	"math"
	"testing"
)

type testcase struct {
	ts       uint32
	span     uint32
	boundary uint32
}

func TestAggBoundary(t *testing.T) {
	cases := []testcase{
		{1, 120, 120},
		{50, 120, 120},
		{118, 120, 120},
		{119, 120, 120},
		{120, 120, 120},
		{121, 120, 240},
		{122, 120, 240},
		{150, 120, 240},
		{237, 120, 240},
		{238, 120, 240},
		{239, 120, 240},
		{240, 120, 240},
		{241, 120, 360},
	}
	for _, c := range cases {
		if ret := aggBoundary(c.ts, c.span); ret != c.boundary {
			t.Fatalf("aggBoundary for ts %d with span %d should be %d, not %d", c.ts, c.span, c.boundary, ret)
		}
	}
}

// note that values don't get "committed" to the metric until the aggregation interval is complete
func TestAggregator(t *testing.T) {
	clusterStatus = NewClusterStatus("default", false)
	compare := func(key string, metric Metric, expected []Point) {
		clusterStatus.Set(true)
		_, iters := metric.Get(0, 1000)
		got := make([]Point, 0, len(expected))
		for _, iter := range iters {
			for iter.Next() {
				ts, val := iter.Values()
				got = append(got, Point{val, ts})
			}
		}
		if len(got) != len(expected) {
			t.Fatalf("output for testcase %s mismatch: expected: %v points, got: %v", key, len(expected), len(got))

		} else {
			for i, g := range got {
				exp := expected[i]
				if exp.Val != g.Val || exp.Ts != g.Ts {
					t.Fatalf("output for testcase %s mismatch at point %d: expected: %v, got: %v", key, i, exp, g)
				}
			}
		}
		clusterStatus.Set(false)
	}
	agg := NewAggregator("test", 60, 120, 10, 86400)
	agg.Add(100, 123.4)
	agg.Add(110, 5)
	expected := []Point{}
	compare("simple-min-unfinished", agg.minMetric, expected)

	agg = NewAggregator("test", 60, 120, 10, 86400)
	agg.Add(100, 123.4)
	agg.Add(110, 5)
	agg.Add(130, 130)
	expected = []Point{
		{5, 120},
	}
	compare("simple-min-one-block", agg.minMetric, expected)

	agg = NewAggregator("test", 60, 120, 10, 86400)
	agg.Add(100, 123.4)
	agg.Add(110, 5)
	agg.Add(120, 4)
	expected = []Point{
		{4, 120},
	}
	compare("simple-min-one-block-done-cause-last-point-just-right", agg.minMetric, expected)

	agg = NewAggregator("test", 60, 120, 10, 86400)
	agg.Add(100, 123.4)
	agg.Add(110, 5)
	agg.Add(150, 1.123)
	agg.Add(180, 1)
	expected = []Point{
		{5, 120},
		{1, 180},
	}
	compare("simple-min-two-blocks-done-cause-last-point-just-right", agg.minMetric, expected)

	agg = NewAggregator("test", 60, 120, 10, 86400)
	agg.Add(100, 123.4)
	agg.Add(110, 5)
	agg.Add(190, 2451.123)
	agg.Add(200, 1451.123)
	agg.Add(220, 978894.445)
	agg.Add(250, 1)
	compare("simple-min-skip-a-block", agg.minMetric, []Point{
		{5, 120},
		{1451.123, 240},
	})
	compare("simple-max-skip-a-block", agg.maxMetric, []Point{
		{123.4, 120},
		{978894.445, 240},
	})
	compare("simple-cnt-skip-a-block", agg.cntMetric, []Point{
		{2, 120},
		{3, 240},
	})
	compare("simple-lst-skip-a-block", agg.lstMetric, []Point{
		{5, 120},
		{978894.445, 240},
	})
	compare("simple-sos-skip-a-block", agg.sosMetric, []Point{
		{math.Pow(float64(123.4), 2) + math.Pow(float64(5), 2), 120},
		{math.Pow(float64(2451.123), 2) + math.Pow(float64(1451.123), 2) + math.Pow(float64(978894.445), 2), 240},
	})
	compare("simple-sum-skip-a-block", agg.sumMetric, []Point{
		{128.4, 120},
		{2451.123 + 1451.123 + 978894.445, 240},
	})

}
