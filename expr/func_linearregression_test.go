package expr

import (
	"math"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
)

// todo these tests are black boxd now, unless this is changed to panic
/*func TestLinearRegressionInvalidStartSourceAt(t *testing.T) {
	funcLinearRegression := FuncLinearRegression{
		in: NewMock([]models.Series{}),
		startSourceAt: "test",
	}

	_, err := funcLinearRegression.Exec(initDataMap([]models.Series{}))
	if err == nil {
		t.Fatal("invalid 'startSourceAt' should result in error")
	}
}

func TestLinearRegressionInvalidEndSourceAt(t *testing.T) {
	funcLinearRegression := FuncLinearRegression{
		endSourceAt: "test",
	}

	_, err := funcLinearRegression.Exec(initDataMap([]models.Series{}))
	if err == nil {
		t.Fatal("invalid 'endSourceAt' should result in error")
	}
}*/

func TestLinearRegression(t *testing.T) {
	in := []models.Series{{
		Target: "test.value",
		Tags: map[string]string{
			"test": "value",
		},
		QueryPatt: "test.value",
		Interval:  60,
		QueryFrom: 120,
		QueryTo:   540,
		Datapoints: []schema.Point{
			{
				Val: 3,
				Ts:  180,
			},
			{
				Val: math.NaN(),
				Ts:  240,
			},
			{
				Val: 5,
				Ts:  300,
			},
			{
				Val: 6,
				Ts:  360,
			},
			{
				Val: math.NaN(),
				Ts:  420,
			},
			{
				Val: 8,
				Ts:  480,
			},
		},
	}}

	expected := []models.Series{{
		Target: "linearRegression(test.value, 180, 480)",
		Tags: map[string]string{
			"test":              "value",
			"linearRegressions": "180, 480",
		},
		QueryPatt: "linearRegression(test.value, 180, 480)",
		Interval:  60,
		QueryFrom: 1200,
		QueryTo:   1500,
		Datapoints: []schema.Point{
			{
				Val: 20,
				Ts:  1200,
			},
			{
				Val: 21,
				Ts:  1260,
			},
			{
				Val: 22,
				Ts:  1320,
			},
			{
				Val: 23,
				Ts:  1380,
			},
			{
				Val: 24,
				Ts:  1440,
			},
			{
				Val: 25,
				Ts:  1500,
			},
		},
	}}

	testLinearRegression(t, "00:03 19700101", "00:08 19700101", 1200, 1500, in, expected)
}

func testLinearRegression(t *testing.T, startSourceAt string, endSourceAt string, startTargetAt uint32, endTargetAt uint32, input []models.Series, expected []models.Series) {
	inputCopy := models.SeriesCopy(input) // to later verify that it is unchanged

	funcLinearRegression := FuncLinearRegression{
		in:            NewMock(input),
		startSourceAt: startSourceAt,
		endSourceAt:   endSourceAt,
	}

	context := Context{
		from: startTargetAt,
		to:   endTargetAt,
	}
	newContext := funcLinearRegression.Context(context)
	t.Run("ModifiedContext", func(t *testing.T) {
		if newContext.from == context.from || newContext.to == context.to {
			t.Fatal("context was not modified by linear regression function")
		}
	})

	actual, err := funcLinearRegression.Exec(initDataMap(input))
	if err != nil {
		t.Fatal(err)
	}
	if err := equalOutput(expected, actual, nil, err); err != nil {
		t.Fatal(err)
	}

	t.Run("DidNotModifyInput", func(t *testing.T) {
		if err := equalOutput(inputCopy, input, nil, nil); err != nil {
			t.Fatal("Input was modified: ", err)
		}
	})
}
