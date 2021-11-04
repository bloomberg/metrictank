package expr

import (
	"testing"
	"fmt"
	
"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
)

func TestLinearRegression(t *testing.T) {
	in := []models.Series{
		models.Series{
		Interval: 60,
		QueryFrom: 180,
		QueryTo: 480,
		Datapoints: []schema.Point{
			{
				Val: 3,
				Ts: 180,
			},
			{
				Val: 5,
				Ts: 300,
			},
			{
				Val: 6,
				Ts: 360,
			},
			{
				Val: 8,
				Ts: 480,
			},
		},
	},
}
	
	expected := models.Series{
		Target: "todo",
		QueryPatt: "todo",
		Interval: 60,
		QueryFrom: 180,//QueryFrom: 1200,
		QueryTo: 480,//QueryTo: 1500,
		Datapoints: []schema.Point{
			{
				Val: 20,
				Ts: 1200,
			},
			{
				Val: 21,
				Ts: 1260,
			},
			{
				Val: 22,
				Ts: 1320,
			},
			{
				Val: 23,
				Ts: 1380,
			},
			{
				Val: 24,
				Ts: 1440,
			},
			{
				Val: 25,
				Ts: 1500,
			},
		},
	}

	funcLinearRegression := FuncLinearRegression{
		in: NewMock(in),
		startSourceAt: "00:03 19700101",
		endSourceAt: "00:08 19700101",
	}
	dataMap := initDataMap(in) 
	actual, err := funcLinearRegression.Exec(dataMap)

	fmt.Println("expected", expected)
	fmt.Println("actual", actual)
	if err := equalOutput([]models.Series{expected}, actual, nil, err); err != nil {
		t.Fatal(err)
	}
}
