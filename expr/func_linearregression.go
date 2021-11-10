package expr

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/raintank/dur"
)

type FuncLinearRegression struct {
	in GraphiteFunc

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
	s.startTargetAt = context.from
	s.endTargetAt = context.to

	return context
}

func linearRegressionAnalysis(series models.Series, startSourceAt uint32, endSourceAt uint32) (float64, float64, bool) {
	var n float64
	var sumI float64
	var sumII float64
	var sumV float64
	var sumIV float64

	for i := sort.Search(len(series.Datapoints), func(i int) bool { return series.Datapoints[i].Ts >= startSourceAt }); i < len(series.Datapoints) && series.Datapoints[i].Ts <= endSourceAt; i++ {
		if math.IsNaN(series.Datapoints[i].Val) {
			continue
		}

		n++
		sumI += float64(i)
		sumII += float64(i) * float64(i)
		sumV += series.Datapoints[i].Val
		sumIV += float64(i) * series.Datapoints[i].Val
	}

	denominator := n*sumII - sumI*sumI
	if denominator == 0 {
		return 0, 0, false
	}

	factor := (n*sumIV - sumI*sumV) / denominator / float64(series.Interval)
	offset := (sumII*sumV-sumIV*sumI)/denominator - factor*float64(startSourceAt)
	return factor, offset, true
}

func (s *FuncLinearRegression) Exec(dataMap DataMap) ([]models.Series, error) {
	loc, err := time.LoadLocation("") // todo, no idea what timezone to use here, utc is a reasonable default
	if err != nil {
		return nil, fmt.Errorf("failed to load location: %w", err)
	}

	now := time.Now()

	defaultStartSourceAt := uint32(now.Add(-24 * time.Hour).Unix())
	startSourceAt, err := dur.ParseDateTime(s.startSourceAt, loc, now, defaultStartSourceAt)
	if err != nil {
		return nil, fmt.Errorf("failed to parse 'startSourceAt' argument %q: %w", s.startSourceAt, err)
	}

	defaultEndSourceAt := uint32(now.Unix())
	endSourceAt, err := dur.ParseDateTime(s.endSourceAt, loc, now, defaultEndSourceAt)
	if err != nil {
		return nil, fmt.Errorf("failed to parse 'endSourceAt' argument %q: %w", s.endSourceAt, err)
	}

	// todo update context.from and .to if needed?

	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	results := []models.Series{}
	for _, serie := range series {
		factor, offset, isValid := linearRegressionAnalysis(serie, startSourceAt, endSourceAt)
		if !isValid {
			continue
		}

		datapoints := []schema.Point{}
		{
			var i uint32
			for i = 0; i <= (s.endTargetAt-s.startTargetAt)/serie.Interval; i++ {
				datapoints = append(datapoints, schema.Point{
					Val: offset + (float64(s.startTargetAt)+float64(i)*float64(serie.Interval))*factor,
					Ts:  s.startTargetAt + i*serie.Interval,
				})
			}
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
