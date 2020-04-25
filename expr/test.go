package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/api/models"
)

var results []models.Series

func equalTags(exp, got []models.Series) error {
	for i, g := range got {
		o := exp[i]
		if len(g.Tags) != len(o.Tags) {
			return fmt.Errorf("series %d: len tags expected %d, got %d", i, len(o.Tags), len(g.Tags))
		}
		for k, v := range g.Tags {
			expectedVal, ok := o.Tags[k]
			if !ok {
				return fmt.Errorf("series %d: got unknown tag key '%s'", i, k)
			}

			if v != expectedVal {
				return fmt.Errorf("series %d: key '%s' had wrong value: expected '%s', got '%s'", i, k, expectedVal, v)
			}
		}
	}
	return nil
}

func equalOutput(exp, got []models.Series, expErr, gotErr error) error {
	if expErr == nil && gotErr != nil {
		return fmt.Errorf("err should be nil. got %q", gotErr)
	}
	if expErr != nil && gotErr == nil {
		return fmt.Errorf("err should be error %v. got %q", expErr, gotErr)
	}
	if len(got) != len(exp) {
		return fmt.Errorf("len output expected %d, got %d", len(exp), len(got))
	}
	for i := range got {
		if err := equalSeries(exp[i], got[i]); err != nil {
			return fmt.Errorf("series %d: %s", i, err)
		}
	}
	return nil
}

// cannot just use reflect.DeepEqual because NaN != NaN, whereas we want NaN == NaN
// https://github.com/golang/go/issues/12025
func equalSeries(exp, got models.Series) error {
	if got.Target != exp.Target {
		return fmt.Errorf("Target %q, got %q", exp.Target, got.Target)
	}
	if got.Interval != exp.Interval {
		return fmt.Errorf("Interval %d, got %d", exp.Interval, got.Interval)
	}
	if got.QueryPatt != exp.QueryPatt {
		return fmt.Errorf("QueryPatt %q, got %q", exp.QueryPatt, got.QueryPatt)
	}
	if got.QueryFrom != exp.QueryFrom {
		return fmt.Errorf("QueryFrom %d, got %d", exp.QueryFrom, got.QueryFrom)
	}
	if got.QueryTo != exp.QueryTo {
		return fmt.Errorf("QueryTo %d, got %d", exp.QueryTo, got.QueryTo)
	}
	if got.QueryCons != exp.QueryCons {
		return fmt.Errorf("QueryCons %v, got %v", exp.QueryCons, got.QueryCons)
	}
	if got.Consolidator != exp.Consolidator {
		return fmt.Errorf("Consolidator %v, got %v", exp.Consolidator, got.Consolidator)
	}
	if len(got.Datapoints) != len(exp.Datapoints) {
		return fmt.Errorf("output expected %d, got %d", len(exp.Datapoints), len(got.Datapoints))
	}
	for j, p := range got.Datapoints {
		if (doubleFuzzyEqual(p.Val, exp.Datapoints[j].Val)) && p.Ts == exp.Datapoints[j].Ts {
			continue
		}
		return fmt.Errorf("point %d - expected %v got %v", j, exp.Datapoints[j], p)
	}
	// TODO - compare Tags?
	return nil
}

func doubleFuzzyEqual(a, b float64) bool {
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}
	var epsilon = 1e-10
	return a == b || math.Abs(a-b) < epsilon
}
