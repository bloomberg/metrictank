package expr

import (
	"math"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
)

/*func TestLinearRegressionInvalidStartSourceAt(t *testing.T) {
	funcLinearRegression := FuncLinearRegression{
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

	funcLinearRegression := FuncLinearRegression{
		in:            NewMock(in),
		startSourceAt: "00:03 19700101",
		endSourceAt:   "00:08 19700101",
	}
	funcLinearRegression.Context(Context{
		from: 1200,
		to:   1500,
	})

	actual, err := funcLinearRegression.Exec(initDataMap(in))
	if err != nil {
		t.Fatal(err)
	}
	if err := equalOutput(expected, actual, nil, err); err != nil {
		t.Fatal(err)
	}
}
