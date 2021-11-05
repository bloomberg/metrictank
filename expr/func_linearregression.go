package expr

import (
	"fmt"
	"time"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/raintank/dur"
)

type FuncLinearRegression struct {
	in            GraphiteFunc
	startSourceAt string
	endSourceAt   string
	startTargetAt uint32
	endTargetAt   uint32
}

func NewLinearRegression() GraphiteFunc {
	return &FuncLinearRegression{}
}

func (s *FuncLinearRegression) Signature() ([]Arg, []Arg) {
	return []Arg{
			ArgSeriesList{
				val: &s.in,
			},
			ArgString{
				key:       "startSourceAt",
				opt:       true,
				validator: []Validator{IsATTime},
				val:       &s.startSourceAt,
			},
			ArgString{
				key:       "endSourceAt",
				opt:       true,
				validator: []Validator{IsATTime},
				val:       &s.endSourceAt,
			},
		},
		[]Arg{ArgSeriesList{}}
}

func (s *FuncLinearRegression) Context(context Context) Context {
	// im assuming from/to correspond to the python startTime/endTime here
	s.startTargetAt = context.from
	s.endTargetAt = context.to

	return context
}

func linearRegressionAnalysis(series models.Series, startSourceAt uint32, endSourceAt uint32) (float64, float64, bool) {
	n := float64(len(series.Datapoints))

	var sumI float64
	var sumII float64
	var sumV float64
	var sumIV float64
	for _, v := range series.Datapoints {
		if v.Ts < startSourceAt { // todo can optimize assuming this is sorted
			continue
		}
		if v.Ts > endSourceAt {
			break
		}

		// we can't use the index from the datapoints because missing datapoints
		// do not exist
		// todo make this sound more better
		i := (v.Ts - series.QueryFrom) / series.Interval
		sumI += float64(i)
		sumII += float64(i) * float64(i)
		sumV += v.Val
		sumIV += float64(i) * v.Val
	}
	fmt.Println("sumI", sumI, "sumII", sumII, "sumV", sumV, "sumIV", sumIV)

	denominator := n*sumII - sumI*sumI
	if denominator == 0 {
		return 0, 0, false
	}
	fmt.Println("denominator", denominator)

	factor := (n*sumIV - sumI*sumV) / denominator / float64(series.Interval)         // todo double check interval is correct to use here
	offset := (sumII*sumV-sumIV*sumI)/denominator - factor*float64(series.QueryFrom) // todo double check queryfrom is correct to use here
	return factor, offset, true
}

func (s *FuncLinearRegression) Exec(dataMap DataMap) ([]models.Series, error) {
	loc, err := time.LoadLocation("") // todo, no idea what timezone to use here, utc is a reasonable default
	if err != nil {
		return nil, err // todo wrap
	}

	now := time.Now()

	// todo this should account for empty params assuming thats how unspecified optional string args manifest
	startSourceAt, err := dur.ParseDateTime(s.startSourceAt, loc, now, uint32(now.Add(-24*time.Hour).Unix()))
	if err != nil {
		return nil, err // todo wrap
	}

	endSourceAt, err := dur.ParseDateTime(s.endSourceAt, loc, now, uint32(now.Unix()))
	if err != nil {
		return nil, err // todo wrap
	}

	// todo update context.from and .to if needed?

	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	// these values come from the request context but we don't have access to that context in Exec..
	results := []models.Series{}
	for _, serie := range series {
		factor, offset, forecast := linearRegressionAnalysis(serie, startSourceAt, endSourceAt)
		fmt.Println("factor", factor, "offset", offset, "forecast", forecast)
		if !forecast {
			continue
		}

		fmt.Println("size", (s.endTargetAt-s.startTargetAt)/serie.Interval)
		datapoints := []schema.Point{} //make([]schema.Point, (s.endTargetAt - s.startTargetAt) / serie.Interval)
		var i uint32
		for i = 0; i <= (s.endTargetAt-s.startTargetAt)/serie.Interval; i++ {
			datapoints = append(datapoints, schema.Point{
				Val: offset + (float64(s.startTargetAt)+float64(i)*float64(serie.Interval))*factor,
				Ts:  s.startTargetAt + i*serie.Interval,
			})
		}

		name := fmt.Sprintf("linearRegression(%s, %d, %d)", serie.Target, startSourceAt, endSourceAt)

		newSeries := serie.Copy([]schema.Point{})
		newSeries.Target = name
		newSeries.Datapoints = datapoints
		newSeries.Tags["linearRegressions"] = fmt.Sprintf("%d, %d", startSourceAt, endSourceAt)
		newSeries.QueryPatt = name
		newSeries.QueryFrom = s.startTargetAt
		newSeries.QueryTo = s.endTargetAt

		results = append(results, newSeries)
	}

	return results, nil
}
