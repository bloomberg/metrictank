package expr

import (
	"math"
	"math/rand"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/test"
	"github.com/raintank/schema"
)

func TestRemoveAboveValueSingleAllNonNull(t *testing.T) {
	testRemoveAboveBelowValue(
		"removeAboveValue",
		true,
		199,
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "abcd",
				Target:     "a",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  "abcd",
				Target:     "b",
				Datapoints: getCopy(b),
			},
			{
				Interval:   10,
				QueryPatt:  "abcd",
				Target:     "c",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				QueryPatt:  "abcd",
				Target:     "d",
				Datapoints: getCopy(d),
			},
		},
		[]models.Series{
			{
				Interval:  10,
				QueryPatt: "removeAboveValue(a, 199)",
				Datapoints: []schema.Point{
					{Val: 0, Ts: 10},
					{Val: 0, Ts: 20},
					{Val: 5.5, Ts: 30},
					{Val: math.NaN(), Ts: 40},
					{Val: math.NaN(), Ts: 50},
					{Val: math.NaN(), Ts: 60},
				},
			},
			{
				Interval:  10,
				QueryPatt: "removeAboveValue(b, 199)",
				Datapoints: []schema.Point{
					{Val: 0, Ts: 10},
					{Val: math.NaN(), Ts: 20},
					{Val: math.NaN(), Ts: 30},
					{Val: math.NaN(), Ts: 40},
					{Val: math.NaN(), Ts: 50},
					{Val: math.NaN(), Ts: 60},
				},
			},
			{
				Interval:  10,
				QueryPatt: "removeAboveValue(c, 199)",
				Datapoints: []schema.Point{
					{Val: 0, Ts: 10},
					{Val: 0, Ts: 20},
					{Val: 1, Ts: 30},
					{Val: 2, Ts: 40},
					{Val: 3, Ts: 50},
					{Val: 4, Ts: 60},
				},
			},
			{
				Interval:  10,
				QueryPatt: "removeAboveValue(d, 199)",
				Datapoints: []schema.Point{
					{Val: 0, Ts: 10},
					{Val: 33, Ts: 20},
					{Val: 199, Ts: 30},
					{Val: 29, Ts: 40},
					{Val: 80, Ts: 50},
					{Val: math.NaN(), Ts: 60},
				},
			},
		},
		t,
	)
}

func TestRemoveBelowValueSingleAllNonNull(t *testing.T) {
	testRemoveAboveBelowValue(
		"removeBelowValue",
		false,
		199,
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "abcd",
				Target:     "a",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  "abcd",
				Target:     "b",
				Datapoints: getCopy(b),
			},
			{
				Interval:   10,
				QueryPatt:  "abcd",
				Target:     "c",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				QueryPatt:  "abcd",
				Target:     "d",
				Datapoints: getCopy(d),
			},
		},
		[]models.Series{
			{
				Interval:  10,
				QueryPatt: "removeBelowValue(a, 199)",
				Datapoints: []schema.Point{
					{Val: math.NaN(), Ts: 10},
					{Val: math.NaN(), Ts: 20},
					{Val: math.NaN(), Ts: 30},
					{Val: math.NaN(), Ts: 40},
					{Val: math.NaN(), Ts: 50},
					{Val: 1234567890, Ts: 60},
				},
			},
			{
				Interval:  10,
				QueryPatt: "removeBelowValue(b, 199)",
				Datapoints: []schema.Point{
					{Val: math.NaN(), Ts: 10},
					{Val: math.MaxFloat64, Ts: 20},
					{Val: math.MaxFloat64 - 20, Ts: 30},
					{Val: math.NaN(), Ts: 40},
					{Val: 1234567890, Ts: 50},
					{Val: math.NaN(), Ts: 60},
				},
			},
			{
				Interval:  10,
				QueryPatt: "removeBelowValue(c, 199)",
				Datapoints: []schema.Point{
					{Val: math.NaN(), Ts: 10},
					{Val: math.NaN(), Ts: 20},
					{Val: math.NaN(), Ts: 30},
					{Val: math.NaN(), Ts: 40},
					{Val: math.NaN(), Ts: 50},
					{Val: math.NaN(), Ts: 60},
				},
			},
			{
				Interval:  10,
				QueryPatt: "removeBelowValue(d, 199)",
				Datapoints: []schema.Point{
					{Val: math.NaN(), Ts: 10},
					{Val: math.NaN(), Ts: 20},
					{Val: 199, Ts: 30},
					{Val: math.NaN(), Ts: 40},
					{Val: math.NaN(), Ts: 50},
					{Val: 250, Ts: 60},
				},
			},
		},
		t,
	)
}

func testRemoveAboveBelowValue(name string, above bool, n float64, in []models.Series, out []models.Series, t *testing.T) {
	f := NewRemoveAboveBelowValueConstructor(above)()
	f.(*FuncRemoveAboveBelowValue).in = NewMock(in)
	f.(*FuncRemoveAboveBelowValue).n = n
	gots, err := f.Exec(make(map[Req][]models.Series))
	if err != nil {
		t.Fatalf("case %q (%f): err should be nil. got %q", name, n, err)
	}
	if len(gots) != len(out) {
		t.Fatalf("case %q (%f): isNonNull len output expected %d, got %d", name, n, len(out), len(gots))
	}
	for i, g := range gots {
		exp := out[i]
		if g.QueryPatt != exp.QueryPatt {
			t.Fatalf("case %q (%f): expected target %q, got %q", name, n, exp.QueryPatt, g.QueryPatt)
		}
		if len(g.Datapoints) != len(exp.Datapoints) {
			t.Fatalf("case %q (%f) len output expected %d, got %d", name, n, len(exp.Datapoints), len(g.Datapoints))
		}
		for j, p := range g.Datapoints {
			bothNaN := math.IsNaN(p.Val) && math.IsNaN(exp.Datapoints[j].Val)
			if (bothNaN || p.Val == exp.Datapoints[j].Val) && p.Ts == exp.Datapoints[j].Ts {
				continue
			}
			t.Fatalf("case %q (%f): output point %d - expected %v got %v", name, n, j, exp.Datapoints[j], p)
		}
	}
}
func BenchmarkRemoveAboveBelowValue10k_1NoNulls(b *testing.B) {
	benchmarkRemoveAboveBelowValue(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkRemoveAboveBelowValue10k_10NoNulls(b *testing.B) {
	benchmarkRemoveAboveBelowValue(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkRemoveAboveBelowValue10k_100NoNulls(b *testing.B) {
	benchmarkRemoveAboveBelowValue(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkRemoveAboveBelowValue10k_1000NoNulls(b *testing.B) {
	benchmarkRemoveAboveBelowValue(b, 1000, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkRemoveAboveBelowValue10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkRemoveAboveBelowValue(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRemoveAboveBelowValue10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkRemoveAboveBelowValue(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRemoveAboveBelowValue10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkRemoveAboveBelowValue(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRemoveAboveBelowValue10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkRemoveAboveBelowValue(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRemoveAboveBelowValue10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkRemoveAboveBelowValue(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRemoveAboveBelowValue10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkRemoveAboveBelowValue(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRemoveAboveBelowValue10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkRemoveAboveBelowValue(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkRemoveAboveBelowValue10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkRemoveAboveBelowValue(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func benchmarkRemoveAboveBelowValue(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
		f := NewRemoveAboveBelowValueConstructor(rand.Int()%2 == 0)()
		f.(*FuncRemoveAboveBelowValue).in = NewMock(input)
		f.(*FuncRemoveAboveBelowValue).n = rand.Float64()
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
