package config

import "strings"

// SpanData is the interface required for matching span fields in a SpanCounter.
// It is satisfied by *types.Payload.
type SpanData interface {
	Get(key string) any
	Exists(key string) bool
}

// SpanCounter defines a custom span count to be computed and emitted.
//
// By default (no ScopeConditions), Spans are counted if they satisfy all
// Conditions, and the trace-wide total is written to the root span under Key.
//
// When ScopeConditions is set, the counter is computed per-anchor: every span
// matching ScopeConditions receives the count of matching descendant spans in
// its own subtree (including itself if it matches Conditions). EmitTotalOnRoot
// controls whether the trace-wide total is additionally written to the root.
type SpanCounter struct {
	Key             string                        `yaml:"Key"`
	RootKey         string                        `yaml:"RootKey,omitempty"`
	Conditions      []*RulesBasedSamplerCondition `yaml:"Conditions,omitempty"`
	ScopeConditions []*RulesBasedSamplerCondition `yaml:"ScopeConditions,omitempty"`
	EmitTotalOnRoot *bool                         `yaml:"EmitTotalOnRoot,omitempty"`
}

// Init initializes all conditions. Must be called before MatchesSpan.
func (c *SpanCounter) Init() error {
	for _, cond := range c.Conditions {
		if err := cond.Init(); err != nil {
			return err
		}
	}
	for _, cond := range c.ScopeConditions {
		if err := cond.Init(); err != nil {
			return err
		}
	}
	return nil
}

// MatchesSpan returns true if the span satisfies all Conditions.
// span is the span being tested; root is the root span's data (may be nil).
func (c *SpanCounter) MatchesSpan(span SpanData, root SpanData) bool {
	return evaluateConditions(c.Conditions, span, root)
}

// MatchesScope returns true if the span satisfies all ScopeConditions.
// Returns false if ScopeConditions is empty (an unscoped counter has no
// per-anchor anchors). span is the span being tested; root is the root
// span's data (may be nil).
func (c *SpanCounter) MatchesScope(span SpanData, root SpanData) bool {
	if len(c.ScopeConditions) == 0 {
		return false
	}
	return evaluateConditions(c.ScopeConditions, span, root)
}

// ShouldEmitTotalOnRoot reports whether the trace-wide total should be
// written to the root span. Defaults to true when ScopeConditions is empty
// (today's behavior) and false when ScopeConditions is set, unless an
// explicit EmitTotalOnRoot value overrides.
func (c *SpanCounter) ShouldEmitTotalOnRoot() bool {
	if c.EmitTotalOnRoot != nil {
		return *c.EmitTotalOnRoot
	}
	return len(c.ScopeConditions) == 0
}

// EffectiveRootKey returns the field name to use when writing the trace-wide
// total to the root span. When ScopeConditions is set and RootKey is
// non-empty, RootKey is used so the per-anchor and per-trace counts land on
// separate field names. Otherwise (unscoped, or scoped with no RootKey
// override) the root write uses Key, preserving today's behavior.
func (c *SpanCounter) EffectiveRootKey() string {
	if len(c.ScopeConditions) > 0 && c.RootKey != "" {
		return c.RootKey
	}
	return c.Key
}

func evaluateConditions(conditions []*RulesBasedSamplerCondition, span SpanData, root SpanData) bool {
	for _, cond := range conditions {
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
