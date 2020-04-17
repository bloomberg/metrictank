package expr

import (
	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/errors"
	"github.com/raintank/dur"
)

var ErrIntPositive = errors.NewBadRequest("integer must be positive")
var ErrInvalidAggFunc = errors.NewBadRequest("Invalid aggregation func")
var ErrNonNegativePercent = errors.NewBadRequest("The requested percent is required to be greater than 0")

// Validator is a function to validate an input
type Validator func(e *expr) error

// IntPositive validates whether an int is positive (greater than zero)
func IntPositive(e *expr) error {
	if e.int < 1 {
		return ErrIntPositive
	}
	return nil
}

func IsAggFunc(e *expr) error {
	if getCrossSeriesAggFunc(e.str) == nil {
		return ErrInvalidAggFunc
	}
	return nil
}

func IsConsolFunc(e *expr) error {
	return consolidation.Validate(e.str)
}

func IsIntervalString(e *expr) error {
	_, err := dur.ParseDuration(e.str)
	return err
}

func IsSignedIntervalString(e *expr) error {
	durStr := e.str
	if durStr[0] == '-' {
		durStr = durStr[1:]
	} else if durStr[0] == '+' {
		durStr = durStr[1:]
	}
	_, err := dur.ParseDuration(durStr)
	return err
}

func IsOperator(e *expr) error {
	switch e.str {
	case "=", "!=", ">", ">=", "<", "<=":
		return nil
	}
	return errors.NewBadRequest("Unsupported operator: " + e.str)
}

func NonNegativePercent(e *expr) error {
	if e.float < 0 || e.int < 0 {
		return ErrNonNegativePercent
	}
	return nil
}
