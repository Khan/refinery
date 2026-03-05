package config

import "strings"

// SpanData is the interface required for matching span fields in a SpanCounterConfig.
// It is satisfied by *types.Payload.
type SpanData interface {
	Get(key string) any
	Exists(key string) bool
}

// SpanCounterConfig defines a custom span count to be computed and added to
// the root span under Key. Spans are counted if they satisfy all Conditions.
type SpanCounterConfig struct {
	Key        string                        `yaml:"Key"`
	Conditions []*RulesBasedSamplerCondition `yaml:"Conditions,omitempty"`
}

// Init initializes all conditions. Must be called before MatchesSpan.
func (c *SpanCounterConfig) Init() error {
	for _, cond := range c.Conditions {
		if err := cond.Init(); err != nil {
			return err
		}
	}
	return nil
}

// MatchesSpan returns true if the span satisfies all conditions.
// span is the span being tested; root is the root span's data (may be nil).
func (c *SpanCounterConfig) MatchesSpan(span SpanData, root SpanData) bool {
	for _, cond := range c.Conditions {
		var value any
		var exists bool
		for _, field := range cond.Fields {
			if strings.HasPrefix(field, RootPrefix) {
				if root != nil {
					f := field[len(RootPrefix):]
					if root.Exists(f) {
						value = root.Get(f)
						exists = true
						break
					}
				}
			} else {
				if span.Exists(field) {
					value = span.Get(field)
					exists = true
					break
				}
			}
		}

		if cond.Matches != nil {
			if !cond.Matches(value, exists) {
				return false
			}
		} else {
			if !ConditionMatchesValue(cond, value, exists) {
				return false
			}
		}
	}
	return true
}

// ConditionMatchesValue evaluates a condition against a value when the
// condition's Matches function has not been set (i.e. Datatype is unspecified).
// This is exported so that sample/rules.go can share the implementation.
func ConditionMatchesValue(condition *RulesBasedSamplerCondition, value interface{}, exists bool) bool {
	var match bool
	switch exists {
	case true:
		switch condition.Operator {
		case Exists:
			match = exists
		case NEQ:
			if comparison, ok := compareValues(value, condition.Value); ok {
				match = comparison != equal
			}
		case EQ:
			if comparison, ok := compareValues(value, condition.Value); ok {
				match = comparison == equal
			}
		case GT:
			if comparison, ok := compareValues(value, condition.Value); ok {
				match = comparison == more
			}
		case GTE:
			if comparison, ok := compareValues(value, condition.Value); ok {
				match = comparison == more || comparison == equal
			}
		case LT:
			if comparison, ok := compareValues(value, condition.Value); ok {
				match = comparison == less
			}
		case LTE:
			if comparison, ok := compareValues(value, condition.Value); ok {
				match = comparison == less || comparison == equal
			}
		}
	case false:
		switch condition.Operator {
		case NotExists:
			match = !exists
		}
	}
	return match
}

const (
	less  = -1
	equal = 0
	more  = 1
)

// compareValues compares two values of potentially mixed numeric types.
// a is the span field value (float64, int64, bool, or string).
// b is the condition value (float64, int64, int, bool, or string).
func compareValues(a, b interface{}) (int, bool) {
	if a == nil {
		if b == nil {
			return equal, true
		}
		return less, true
	}

	if b == nil {
		return more, true
	}

	switch at := a.(type) {
	case int64:
		switch bt := b.(type) {
		case int:
			i := int(at)
			switch {
			case i < bt:
				return less, true
			case i > bt:
				return more, true
			default:
				return equal, true
			}
		case int64:
			switch {
			case at < bt:
				return less, true
			case at > bt:
				return more, true
			default:
				return equal, true
			}
		case float64:
			f := float64(at)
			switch {
			case f < bt:
				return less, true
			case f > bt:
				return more, true
			default:
				return equal, true
			}
		}
	case float64:
		switch bt := b.(type) {
		case int:
			f := float64(bt)
			switch {
			case at < f:
				return less, true
			case at > f:
				return more, true
			default:
				return equal, true
			}
		case int64:
			f := float64(bt)
			switch {
			case at < f:
				return less, true
			case at > f:
				return more, true
			default:
				return equal, true
			}
		case float64:
			switch {
			case at < bt:
				return less, true
			case at > bt:
				return more, true
			default:
				return equal, true
			}
		}
	case bool:
		switch bt := b.(type) {
		case bool:
			switch {
			case !at && bt:
				return less, true
			case at && !bt:
				return more, true
			default:
				return equal, true
			}
		}
	case string:
		switch bt := b.(type) {
		case string:
			return strings.Compare(at, bt), true
		}
	}

	return equal, false
}
