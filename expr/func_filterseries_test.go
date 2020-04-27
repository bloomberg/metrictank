package expr

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

func TestFilterSeriesEqual(t *testing.T) {

	testFilterSeries(
		"abcd_equal",
		"max",
		"=",
		1234567890,
		[]models.Series{
			getQuerySeries("a", a),
			getQuerySeries("b", b),
			getQuerySeries("c", c),
			getQuerySeries("d", d),
		},
		[]models.Series{
			getQuerySeries("a", a),
		},
		t,
	)
}

func TestFilterSeriesNotEqual(t *testing.T) {

	testFilterSeries(
		"abcd_notequal",
		"max",
		"!=",
		1234567890,
		[]models.Series{
			getQuerySeries("a", a),
			getQuerySeries("b", b),
			getQuerySeries("c", c),
			getQuerySeries("d", d),
		},
		[]models.Series{
			getQuerySeries("b", b),
			getQuerySeries("c", c),
			getQuerySeries("d", d),
		},
		t,
	)
}

func TestFilterSeriesLessThan(t *testing.T) {

	testFilterSeries(
		"abcd_lessthan",
		"max",
		"<",
		1234567890,
		[]models.Series{
			getQuerySeries("a", a),
			getQuerySeries("b", b),
			getQuerySeries("c", c),
			getQuerySeries("d", d),
		},
		[]models.Series{
			getQuerySeries("c", c),
			getQuerySeries("d", d),
		},
		t,
	)
}

func TestFilterSeriesLessThanOrEqualTo(t *testing.T) {

	testFilterSeries(
		"abcd_lessorequal",
		"max",
		"<=",
		250,
		[]models.Series{
			getQuerySeries("a", a),
			getQuerySeries("b", b),
			getQuerySeries("c", c),
			getQuerySeries("d", d),
		},
		[]models.Series{
			getQuerySeries("c", c),
			getQuerySeries("d", d),
		},
		t,
	)
}

func TestFilterSeriesMoreThan(t *testing.T) {

	testFilterSeries(
		"abcd_more",
		"max",
		">",
		250,
		[]models.Series{
			getQuerySeries("a", a),
			getQuerySeries("b", b),
			getQuerySeries("c", c),
			getQuerySeries("d", d),
		},
		[]models.Series{
			getQuerySeries("a", a),
			getQuerySeries("b", b),
		},
		t,
	)
}

func TestFilterSeriesMoreThanOrEqual(t *testing.T) {

	testFilterSeries(
		"abcd_moreorequal",
		"max",
		">=",
		250,
		[]models.Series{
			getQuerySeries("a", a),
			getQuerySeries("b", b),
			getQuerySeries("c", c),
			getQuerySeries("d", d),
		},
		[]models.Series{
			getQuerySeries("a", a),
			getQuerySeries("b", b),
			getQuerySeries("d", d),
		},
		t,
	)
}

func testFilterSeries(name string, fn string, operator string, threshold float64, in []models.Series, out []models.Series, t *testing.T) {
	f := NewFilterSeries()
	f.(*FuncFilterSeries).in = NewMock(in)
	f.(*FuncFilterSeries).fn = fn
	f.(*FuncFilterSeries).operator = operator
	f.(*FuncFilterSeries).threshold = threshold

	// Copy input to check that it is unchanged later
	inputCopy := make([]models.Series, len(in))
	copy(inputCopy, in)

	dataMap := DataMap(make(map[Req][]models.Series))

	got, err := f.Exec(dataMap)
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatalf("Case %s: %s", name, err)
	}

	t.Run("DidNotModifyInput", func(t *testing.T) {
		if err := equalOutput(inputCopy, in, nil, nil); err != nil {
			t.Fatalf("Case %s: Input was modified, err = %s", name, err)
		}
	})

	t.Run("DoesNotDoubleReturnPoints", func(t *testing.T) {
		if err := dataMap.CheckForOverlappingPoints(); err != nil {
			t.Fatalf("Case %s: Point slices in datamap overlap, err = %s", name, err)
		}
	})
}

func BenchmarkFilterSeries10k_1NoNulls(b *testing.B) {
	benchmarkFilterSeries(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkFilterSeries10k_10NoNulls(b *testing.B) {
	benchmarkFilterSeries(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkFilterSeries10k_100NoNulls(b *testing.B) {
	benchmarkFilterSeries(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkFilterSeries10k_1000NoNulls(b *testing.B) {
	benchmarkFilterSeries(b, 1000, test.RandFloats10k, test.RandFloats10k)
}

func BenchmarkFilterSeries10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkFilterSeries(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkFilterSeries10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkFilterSeries(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkFilterSeries10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkFilterSeries(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkFilterSeries10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkFilterSeries(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkFilterSeries10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkFilterSeries(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkFilterSeries10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkFilterSeries(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkFilterSeries10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkFilterSeries(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkFilterSeries10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkFilterSeries(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkFilterSeries(b *testing.B, numSeries int, fn0, fn1 func() []schema.Point) {
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
		f := NewFilterSeries()
		f.(*FuncFilterSeries).in = NewMock(input)
		f.(*FuncFilterSeries).fn = "sum"
		f.(*FuncFilterSeries).operator = ">"
		f.(*FuncFilterSeries).threshold = rand.Float64()
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
