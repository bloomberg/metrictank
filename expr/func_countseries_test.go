package expr

import (
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/test"
	"github.com/raintank/schema"
)

func TestCountSeriesFive(t *testing.T) {
	out := []schema.Point{
		{Val: 5, Ts: 10},
		{Val: 5, Ts: 20},
		{Val: 5, Ts: 30},
		{Val: 5, Ts: 40},
		{Val: 5, Ts: 50},
		{Val: 5, Ts: 60},
	}
	testCountSeries(
		"five",
		[][]models.Series{{
			{
				Interval:   10,
				QueryPatt:  "abc",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  "abc",
				Datapoints: getCopy(b),
			},
			{
				Interval:   10,
				QueryPatt:  "abc",
				Datapoints: getCopy(c),
			},
		},
			{
				{
					Interval:   10,
					QueryPatt:  "ad",
					Datapoints: getCopy(d),
				},
				{
					Interval:   10,
					QueryPatt:  "ad",
					Datapoints: getCopy(a),
				},
			}},

		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "countSeries(abc,ad)",
				Datapoints: out,
			},
		},
		t,
	)
}
func TestCountSeriesNone(t *testing.T) {
	testCountSeries(
		"none",
		[][]models.Series{},

		[]models.Series{},
		t,
	)
}

func testCountSeries(name string, in [][]models.Series, out []models.Series, t *testing.T) {
	f := NewCountSeries()
	for _, i := range in {
		f.(*FuncCountSeries).in = append(f.(*FuncCountSeries).in, NewMock(i))
	}
	gots, err := f.Exec(make(map[Req][]models.Series))
	if err != nil {
		t.Fatalf("case %q: err should be nil. got %q", name, err)
	}
	if len(gots) != len(out) {
		t.Fatalf("case %q: isNonNull len output expected %d, got %d", name, len(out), len(gots))
	}
	for i, g := range gots {
		exp := out[i]
		if g.QueryPatt != exp.QueryPatt {
			t.Fatalf("case %q: expected target %q, got %q", name, exp.QueryPatt, g.QueryPatt)
		}
		if len(g.Datapoints) != len(exp.Datapoints) {
			t.Fatalf("case %q: len output expected %d, got %d", name, len(exp.Datapoints), len(g.Datapoints))
		}
		for j, p := range g.Datapoints {
			if (p.Val == exp.Datapoints[j].Val) && p.Ts == exp.Datapoints[j].Ts {
				continue
			}
			t.Fatalf("case %q: output point %d - expected %v got %v", name, j, exp.Datapoints[j], p)
		}
	}
}

func BenchmarkCountSeries10k_1NoNulls(b *testing.B) {
	benchmarkCountSeries(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkCountSeries10k_10NoNulls(b *testing.B) {
	benchmarkCountSeries(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkCountSeries10k_100NoNulls(b *testing.B) {
	benchmarkCountSeries(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkCountSeries10k_1000NoNulls(b *testing.B) {
	benchmarkCountSeries(b, 1000, test.RandFloats10k, test.RandFloats10k)
}

func BenchmarkCountSeries10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkCountSeries(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkCountSeries10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkCountSeries(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkCountSeries10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkCountSeries(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkCountSeries10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkCountSeries(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkCountSeries10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkCountSeries(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkCountSeries10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkCountSeries(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkCountSeries10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkCountSeries(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkCountSeries10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkCountSeries(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkCountSeries(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
	var input []models.Series
	for i := 0; i < numSeries; i++ {
		series := models.Series{
			QueryPatt: strconv.Itoa(i),
		}
		if i%2 == 0 {
			series.Datapoints = fn0()
		} else {
			series.Datapoints = fn1()
		}
		input = append(input, series)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := NewCountSeries()
		f.(*FuncCountSeries).in = append(f.(*FuncCountSeries).in, NewMock(input))
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
