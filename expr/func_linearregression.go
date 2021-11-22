package expr

import (
	"fmt"
	"math"
	"time"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/raintank/dur"
)

type FuncLinearRegression struct {
	in GraphiteFunc

	startSourceAt       string
	endSourceAt         string
	parsedStartSourceAt uint32
	parsedEndSourceAt   uint32

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

	if err := s.parseSourceAt(); err != nil {
		return context // todo panic?
	}
	context.from = s.parsedStartSourceAt
	context.to = s.parsedEndSourceAt

	return context
}

func (s *FuncLinearRegression) parseSourceAt() error {
	loc, err := time.LoadLocation("") // todo, no idea what timezone to use here, utc is a reasonable default
	if err != nil {
		return fmt.Errorf("failed to load location: %w", err)
	}
	now := time.Now()

	defaultStartSourceAt := uint32(now.Add(-24 * time.Hour).Unix())
	s.parsedStartSourceAt, err = dur.ParseDateTime(s.startSourceAt, loc, now, defaultStartSourceAt)
	if err != nil {
		return fmt.Errorf("failed to parse 'startSourceAt' argument %q: %w", s.startSourceAt, err)
	}

	defaultEndSourceAt := uint32(now.Unix())
	s.parsedEndSourceAt, err = dur.ParseDateTime(s.endSourceAt, loc, now, defaultEndSourceAt)
	if err != nil {
		return fmt.Errorf("failed to parse 'endSourceAt' argument %q: %w", s.endSourceAt, err)
	}

	return nil
}

func (s *FuncLinearRegression) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	results := []models.Series{}
	for _, serie := range series {
		factor, offset, isValid := linearRegressionAnalysis(serie)
		if !isValid {
			continue
		}

		startTargetAt := normalize(s.startTargetAt, serie.Interval)
		endTargetAt := normalize(s.endTargetAt, serie.Interval)
		size := int((endTargetAt - startTargetAt) / serie.Interval)

		datapoints := pointSlicePool.GetMin(size)
		for i := 0; i < size; i++ {
			datapoint := schema.Point{
				Val: offset + (float64(startTargetAt)+float64(i)*float64(serie.Interval))*factor,
				Ts:  startTargetAt + uint32(i)*serie.Interval,
			}
			datapoints = append(datapoints, datapoint)
		}

		newSeries := serie.Copy([]schema.Point{})
		newSeries.Target = fmt.Sprintf("linearRegression(%s, %d, %d)", serie.Target, s.parsedStartSourceAt, s.parsedEndSourceAt)
		newSeries.Datapoints = datapoints
		newSeries.Tags["linearRegressions"] = fmt.Sprintf("%d, %d", s.parsedStartSourceAt, s.parsedEndSourceAt)
		newSeries.QueryPatt = newSeries.Target
		newSeries.QueryFrom = s.startTargetAt
		newSeries.QueryTo = s.endTargetAt

		results = append(results, newSeries)
	}
	return results, nil
}

func linearRegressionAnalysis(series models.Series) (float64, float64, bool) {
	startSourceAt := series.QueryFrom // todo check if this is still needed
	// i think this was because the sumseries didnt normalize this properly
	if len(series.Datapoints) > 0 {
		startSourceAt = series.Datapoints[0].Ts
	}

	var n float64
	var sumI float64
	var sumII float64
	var sumV float64
	var sumIV float64
	for i, point := range series.Datapoints {
		if math.IsNaN(point.Val) {
			continue
		}

		n++
		sumI += float64(i)
		sumII += float64(i) * float64(i)
		sumV += point.Val
		sumIV += float64(i) * point.Val
	}

	denominator := n*sumII - sumI*sumI
	if denominator == 0 {
		return 0, 0, false
	}
	factor := (n*sumIV - sumI*sumV) / denominator / float64(series.Interval)
	offset := (sumII*sumV-sumIV*sumI)/denominator - factor*float64(startSourceAt)

	return factor, offset, true
}

func normalize(timestamp, interval uint32) uint32 {
	normalized := timestamp / interval * interval
	if normalized < timestamp {
		normalized += interval
	}

	return normalized
}
