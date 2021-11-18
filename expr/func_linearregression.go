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

	startSourceAt string
	endSourceAt   string
	parsedStartSourceAt uint32
	parsedEndSourceAt uint32

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
		fmt.Println("DOM DEBUG PARSE ERR:", err)
	}// todo handle error
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

func normalize(timestamp, interval uint32) uint32 {
	fmt.Println("DOM DEBUG timestamp:", timestamp, "interval:", interval)
	fmt.Println("timestamp / interval:", timestamp / interval)
	fmt.Println("timestamp / interval * interval:", timestamp / interval * interval)
	normalized := timestamp / interval * interval
	if normalized < timestamp {
		normalized += interval
	}

	return normalized
}

func (s *FuncLinearRegression) Exec(dataMap DataMap) ([]models.Series, error) {
	fmt.Println("DOM DEBUG dataMap:", dataMap)

	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	fmt.Println("DOM DEBUG s.parsedStartSourceAt:", s.parsedStartSourceAt, "s.parsedEndSourceAt:", s.parsedEndSourceAt, "startTargetAt:", s.startTargetAt, "endTargetAt:", s.endTargetAt)
	results := []models.Series{}
	for _, serie := range series {
		fmt.Println("DOM DEBUG series:", serie)
		deducedInterval := deduceInterval(serie)
		fmt.Println("DOM DEBUG acutal interval:", serie.Interval, "deduced:", deducedInterval)
		startTargetAt := normalize(s.startTargetAt, deducedInterval)
		fmt.Println("DOM DEBUG base start target at:", s.startTargetAt, "normalized", startTargetAt)

		factor, offset, isValid := linearRegressionAnalysis(serie, serie.QueryFrom)
		if !isValid {
			continue
		}
		fmt.Println("DOM DEBUG factor:", factor, "offset:", offset)

		datapoints := pointSlicePool.GetMin(int((s.endTargetAt - startTargetAt) / deducedInterval))
                //for i := 0; i < int((s.endTargetAt - startTargetAt) / deducedInterval); i++ {
		for i := 0; i < len(serie.Datapoints); i++ {
			fmt.Println("startTargetAt:", startTargetAt, "i:", i)
			datapoint := schema.Point{
				Val: offset + (float64(startTargetAt)+float64(i)*float64(deducedInterval))*factor,
				Ts:  startTargetAt + uint32(i)*deducedInterval,
			}
			fmt.Println("DOM DEBUG:", offset, "+ (", startTargetAt, "+", i, "*", deducedInterval, ") *", factor, "=", offset + (float64(startTargetAt)+float64(i)*float64(deducedInterval))*factor, "=", datapoint.Val, "@", datapoint.Ts)
			datapoints = append(datapoints, datapoint)
		}

		name := fmt.Sprintf("linearRegression(%s, %d, %d)", serie.Target, s.parsedStartSourceAt, s.parsedEndSourceAt)

		newSeries := serie.Copy([]schema.Point{})
		newSeries.Target = name
		newSeries.Datapoints = datapoints
		newSeries.Tags["linearRegressions"] = fmt.Sprintf("%d, %d", s.parsedStartSourceAt, s.parsedEndSourceAt)
		newSeries.QueryPatt = name
		newSeries.QueryFrom = s.startTargetAt
		newSeries.QueryTo = s.endTargetAt

		fmt.Println("newSeries:", newSeries)
		results = append(results, newSeries)
	}
	return results, nil
}

func deduceInterval(series models.Series) uint32 {
	if len(series.Datapoints) < 2 {
		return 0
	}

	return series.Datapoints[1].Ts - series.Datapoints[0].Ts
}

func linearRegressionAnalysis(series models.Series, startSourceAt uint32) (float64, float64, bool) {
	if len(series.Datapoints) > 0 {
		startSourceAt = series.Datapoints[0].Ts
	}

	var n float64
	var sumI float64
	var sumII float64
	var sumV float64
	var sumIV float64

	fmt.Println("DOM DEBUG series.Interval:", series.Interval, "startSourceAt:", startSourceAt)
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
	fmt.Println("DOM DEBUG n:", n, "sumI:", sumI, "sumV:", sumV, "sumII:", sumII, "sumIV:", sumIV)

	denominator := n*sumII - sumI*sumI
	fmt.Println("DOM DEBUG denominator:", denominator)
	if denominator == 0 {
		return 0, 0, false
	}

	factor := (n*sumIV - sumI*sumV) / denominator / float64(series.Interval)
	offset := (sumII*sumV-sumIV*sumI)/denominator - factor*float64(startSourceAt)
	return factor, offset, true
}
