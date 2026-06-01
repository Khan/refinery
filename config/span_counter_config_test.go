package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// spanData is a simple map-backed implementation of SpanData for tests.
type spanData map[string]any

func (s spanData) Get(key string) any   { return s[key] }
func (s spanData) Exists(key string) bool { _, ok := s[key]; return ok }

// cond builds an initialized RulesBasedSamplerCondition from a field name,
// operator, and optional value. It calls Init() so that the Matches function
// is set when Datatype is empty (the ConditionMatchesValue path).
func cond(field, operator string, value any) *RulesBasedSamplerCondition {
	c := &RulesBasedSamplerCondition{
		Field:    field,
		Operator: operator,
		Value:    value,
	}
	if err := c.Init(); err != nil {
		panic("cond Init: " + err.Error())
	}
	return c
}

// condTyped builds an initialized condition with an explicit Datatype, which
// causes Init to set a type-coercing Matches function instead of falling
// through to ConditionMatchesValue.
func condTyped(field, operator string, value any, datatype string) *RulesBasedSamplerCondition {
	c := &RulesBasedSamplerCondition{
		Field:    field,
		Operator: operator,
		Value:    value,
		Datatype: datatype,
	}
	if err := c.Init(); err != nil {
		panic("condTyped Init: " + err.Error())
	}
	return c
}

// ----------------------------------------------------------------------------
// compareValues
// ----------------------------------------------------------------------------

func TestCompareValues(t *testing.T) {
	tests := []struct {
		name    string
		a, b    any
		want    int
		wantOK  bool
	}{
		// nil handling
		{"nil==nil", nil, nil, equal, true},
		{"nil<nonnil", nil, int64(1), less, true},
		{"nonnil>nil", int64(1), nil, more, true},

		// int64 vs int64
		{"i64 less", int64(1), int64(2), less, true},
		{"i64 equal", int64(3), int64(3), equal, true},
		{"i64 more", int64(5), int64(4), more, true},

		// int64 vs int
		{"i64 vs int less", int64(1), int(2), less, true},
		{"i64 vs int equal", int64(3), int(3), equal, true},
		{"i64 vs int more", int64(5), int(4), more, true},

		// int64 vs float64
		{"i64 vs f64 less", int64(1), float64(1.5), less, true},
		{"i64 vs f64 equal", int64(2), float64(2.0), equal, true},
		{"i64 vs f64 more", int64(3), float64(2.9), more, true},

		// float64 vs float64
		{"f64 less", float64(1.1), float64(1.2), less, true},
		{"f64 equal", float64(2.5), float64(2.5), equal, true},
		{"f64 more", float64(3.0), float64(2.0), more, true},

		// float64 vs int
		{"f64 vs int less", float64(0.5), int(1), less, true},
		{"f64 vs int equal", float64(2.0), int(2), equal, true},
		{"f64 vs int more", float64(2.1), int(2), more, true},

		// float64 vs int64
		{"f64 vs i64 less", float64(0.5), int64(1), less, true},
		{"f64 vs i64 equal", float64(2.0), int64(2), equal, true},
		{"f64 vs i64 more", float64(3.0), int64(2), more, true},

		// bool
		{"bool false<true", false, true, less, true},
		{"bool true>false", true, false, more, true},
		{"bool equal", true, true, equal, true},

		// string
		{"str less", "apple", "banana", less, true},
		{"str equal", "foo", "foo", equal, true},
		{"str more", "zoo", "ant", more, true},

		// type mismatch → ok=false
		{"mismatch int64 str", int64(1), "1", equal, false},
		{"mismatch f64 str", float64(1.0), "1.0", equal, false},
		{"mismatch bool str", true, "true", equal, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := compareValues(tc.a, tc.b)
			assert.Equal(t, tc.wantOK, ok, "ok")
			if tc.wantOK {
				assert.Equal(t, tc.want, got, "comparison result")
			}
		})
	}
}

// ----------------------------------------------------------------------------
// ConditionMatchesValue
// ----------------------------------------------------------------------------

func TestConditionMatchesValue(t *testing.T) {
	tests := []struct {
		name     string
		operator string
		condVal  any
		spanVal  any
		exists   bool
		want     bool
	}{
		// Exists / NotExists
		{"exists true", Exists, nil, "anything", true, true},
		{"exists false", Exists, nil, nil, false, false},
		{"not-exists true", NotExists, nil, nil, false, true},
		{"not-exists false", NotExists, nil, "x", true, false},

		// EQ
		{"eq string match", EQ, "foo", "foo", true, true},
		{"eq string no-match", EQ, "foo", "bar", true, false},
		{"eq int64 match", EQ, int64(42), int64(42), true, true},
		{"eq int64 no-match", EQ, int64(42), int64(0), true, false},
		{"eq type mismatch", EQ, "1", int64(1), true, false}, // compareValues returns ok=false → no match

		// NEQ
		{"neq match", NEQ, "foo", "bar", true, true},
		{"neq no-match", NEQ, "foo", "foo", true, false},

		// GT / GTE / LT / LTE
		{"gt true", GT, int64(1), int64(2), true, true},
		{"gt false eq", GT, int64(1), int64(1), true, false},
		{"gte equal", GTE, int64(1), int64(1), true, true},
		{"gte more", GTE, int64(1), int64(2), true, true},
		{"gte less", GTE, int64(2), int64(1), true, false},
		{"lt true", LT, int64(2), int64(1), true, true},
		{"lt false", LT, int64(1), int64(2), true, false},
		{"lte equal", LTE, int64(2), int64(2), true, true},
		{"lte less", LTE, int64(3), int64(2), true, true},
		{"lte more", LTE, int64(1), int64(2), true, false},

		// field does not exist with non-NotExists operator → no match
		{"eq field missing", EQ, "foo", nil, false, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := &RulesBasedSamplerCondition{
				Operator: tc.operator,
				Value:    tc.condVal,
			}
			got := ConditionMatchesValue(c, tc.spanVal, tc.exists)
			assert.Equal(t, tc.want, got)
		})
	}
}

// ----------------------------------------------------------------------------
// SpanCounter.MatchesSpan
// ----------------------------------------------------------------------------

func TestMatchesSpan_NoConditions(t *testing.T) {
	// A counter with no conditions matches every span.
	counter := SpanCounter{Key: "all"}
	assert.True(t, counter.MatchesSpan(spanData{"foo": "bar"}, nil))
	assert.True(t, counter.MatchesSpan(spanData{}, nil))
}

func TestMatchesSpan_SingleCondition(t *testing.T) {
	counter := SpanCounter{
		Key:        "errors",
		Conditions: []*RulesBasedSamplerCondition{cond("error", EQ, true)},
	}

	assert.True(t, counter.MatchesSpan(spanData{"error": true}, nil))
	assert.False(t, counter.MatchesSpan(spanData{"error": false}, nil))
	assert.False(t, counter.MatchesSpan(spanData{}, nil))
}

func TestMatchesSpan_MultipleConditionsAllMustMatch(t *testing.T) {
	counter := SpanCounter{
		Key: "slow-errors",
		Conditions: []*RulesBasedSamplerCondition{
			cond("error", EQ, true),
			cond("duration_ms", GT, int64(500)),
		},
	}

	assert.True(t, counter.MatchesSpan(spanData{"error": true, "duration_ms": int64(1000)}, nil))
	assert.False(t, counter.MatchesSpan(spanData{"error": true, "duration_ms": int64(100)}, nil))
	assert.False(t, counter.MatchesSpan(spanData{"error": false, "duration_ms": int64(1000)}, nil))
	assert.False(t, counter.MatchesSpan(spanData{}, nil))
}

func TestMatchesSpan_RootPrefixedField(t *testing.T) {
	// "root.service.name" reads from the root span data, not the span itself.
	counter := SpanCounter{
		Key:        "svc-db",
		Conditions: []*RulesBasedSamplerCondition{cond("root.service.name", EQ, "database")},
	}

	root := spanData{"service.name": "database"}
	span := spanData{"duration_ms": int64(5)}

	assert.True(t, counter.MatchesSpan(span, root))
	assert.False(t, counter.MatchesSpan(span, spanData{"service.name": "api"}))
}

func TestMatchesSpan_RootPrefixedField_NilRoot(t *testing.T) {
	// When root is nil a root-prefixed field is never found → field is absent.
	counter := SpanCounter{
		Key:        "svc",
		Conditions: []*RulesBasedSamplerCondition{cond("root.service.name", EQ, "database")},
	}
	assert.False(t, counter.MatchesSpan(spanData{}, nil))
}

func TestMatchesSpan_MultiFieldFallback(t *testing.T) {
	// When multiple fields are listed, the first one found is used.
	c := &RulesBasedSamplerCondition{
		Fields:   []string{"trace.trace_id", "traceId"},
		Operator: Exists,
	}
	if err := c.Init(); err != nil {
		t.Fatal(err)
	}
	counter := SpanCounter{Key: "has-trace", Conditions: []*RulesBasedSamplerCondition{c}}

	assert.True(t, counter.MatchesSpan(spanData{"trace.trace_id": "abc"}, nil))
	assert.True(t, counter.MatchesSpan(spanData{"traceId": "abc"}, nil))
	assert.False(t, counter.MatchesSpan(spanData{}, nil))
}

func TestMatchesSpan_MultiFieldFallback_FirstWins(t *testing.T) {
	// If the first field exists but evaluates to a non-match, the second field
	// is not consulted — only the first found field is used.
	c := &RulesBasedSamplerCondition{
		Fields:   []string{"a", "b"},
		Operator: EQ,
		Value:    "yes",
	}
	if err := c.Init(); err != nil {
		t.Fatal(err)
	}
	counter := SpanCounter{Key: "k", Conditions: []*RulesBasedSamplerCondition{c}}

	// "a" is found with wrong value; "b" has the right value but is not checked.
	assert.False(t, counter.MatchesSpan(spanData{"a": "no", "b": "yes"}, nil))
	// Only "b" exists → fallback to "b" → match.
	assert.True(t, counter.MatchesSpan(spanData{"b": "yes"}, nil))
}

func TestMatchesSpan_TypedCondition(t *testing.T) {
	// When Datatype is set, Init wires up a type-coercing Matches function.
	// Verify that MatchesSpan delegates to it correctly.
	counter := SpanCounter{
		Key:        "count-int",
		Conditions: []*RulesBasedSamplerCondition{condTyped("code", EQ, 200, "int")},
	}

	// span value arrives as string "200"; the typed matcher coerces it.
	assert.True(t, counter.MatchesSpan(spanData{"code": "200"}, nil))
	assert.False(t, counter.MatchesSpan(spanData{"code": "404"}, nil))
}

func TestMatchesSpan_ExistsAndNotExists(t *testing.T) {
	exists := SpanCounter{
		Key:        "has-field",
		Conditions: []*RulesBasedSamplerCondition{cond("db.query", Exists, nil)},
	}
	notExists := SpanCounter{
		Key:        "no-field",
		Conditions: []*RulesBasedSamplerCondition{cond("db.query", NotExists, nil)},
	}

	withField := spanData{"db.query": "SELECT 1"}
	without := spanData{}

	assert.True(t, exists.MatchesSpan(withField, nil))
	assert.False(t, exists.MatchesSpan(without, nil))
	assert.False(t, notExists.MatchesSpan(withField, nil))
	assert.True(t, notExists.MatchesSpan(without, nil))
}

// ----------------------------------------------------------------------------
// SpanCounter.MatchesScope / ShouldEmitTotalOnRoot
// ----------------------------------------------------------------------------

func TestMatchesScope_EmptyScopeNeverMatches(t *testing.T) {
	counter := SpanCounter{Key: "k"}
	assert.False(t, counter.MatchesScope(spanData{"foo": "bar"}, nil))
	assert.False(t, counter.MatchesScope(spanData{}, nil))
}

func TestMatchesScope_AllConditionsMustMatch(t *testing.T) {
	counter := SpanCounter{
		Key: "k",
		ScopeConditions: []*RulesBasedSamplerCondition{
			cond("graphql.operation.name", Exists, nil),
			cond("kind", EQ, "server"),
		},
	}
	assert.True(t, counter.MatchesScope(spanData{"graphql.operation.name": "Q", "kind": "server"}, nil))
	assert.False(t, counter.MatchesScope(spanData{"graphql.operation.name": "Q"}, nil))
	assert.False(t, counter.MatchesScope(spanData{"kind": "server"}, nil))
}

func TestMatchesScope_RootPrefixSupported(t *testing.T) {
	counter := SpanCounter{
		Key: "k",
		ScopeConditions: []*RulesBasedSamplerCondition{
			cond("root.service.name", EQ, "api"),
		},
	}
	assert.True(t, counter.MatchesScope(spanData{}, spanData{"service.name": "api"}))
	assert.False(t, counter.MatchesScope(spanData{}, spanData{"service.name": "worker"}))
	assert.False(t, counter.MatchesScope(spanData{}, nil))
}

func TestShouldEmitTotalOnRoot(t *testing.T) {
	// Unscoped → always emit on root (today's behavior).
	assert.True(t, (&SpanCounter{Key: "k"}).ShouldEmitTotalOnRoot())

	// Scoped, no RootKey → per-anchor only, no root write.
	scoped := SpanCounter{
		Key:             "k",
		ScopeConditions: []*RulesBasedSamplerCondition{cond("anchor", Exists, nil)},
	}
	assert.False(t, scoped.ShouldEmitTotalOnRoot())

	// Scoped + RootKey set → emit total on root.
	scoped.RootKey = "rk"
	assert.True(t, scoped.ShouldEmitTotalOnRoot())
}

func TestEffectiveRootKey(t *testing.T) {
	// Unscoped + no RootKey → Key (today's behavior).
	c := SpanCounter{Key: "k"}
	assert.Equal(t, "k", c.EffectiveRootKey())

	// Unscoped + RootKey set → still Key; RootKey is ignored when unscoped.
	c = SpanCounter{Key: "k", RootKey: "rk"}
	assert.Equal(t, "k", c.EffectiveRootKey())

	// Scoped + no RootKey → empty string; ShouldEmitTotalOnRoot is false so
	// nothing is written to the root and this value isn't consulted.
	c = SpanCounter{
		Key:             "k",
		ScopeConditions: []*RulesBasedSamplerCondition{cond("anchor", Exists, nil)},
	}
	assert.Equal(t, "", c.EffectiveRootKey())

	// Scoped + RootKey set → RootKey overrides the root write.
	c = SpanCounter{
		Key:             "anchor_count",
		RootKey:         "trace_count",
		ScopeConditions: []*RulesBasedSamplerCondition{cond("anchor", Exists, nil)},
	}
	assert.Equal(t, "trace_count", c.EffectiveRootKey())
}

// ----------------------------------------------------------------------------
// validateSpanCounterEntry (custom rules)
// ----------------------------------------------------------------------------

func TestValidateSpanCounterEntry_DuplicateKey(t *testing.T) {
	seen := map[string]int{}
	results := validateSpanCounterEntry(0, map[string]any{"Key": "k"}, seen)
	assert.Empty(t, results)
	results = validateSpanCounterEntry(1, map[string]any{"Key": "k"}, seen)
	require.Len(t, results, 1)
	assert.Equal(t, Error, results[0].Severity)
	assert.Contains(t, results[0].Message, "collides")
}

func TestValidateSpanCounterEntry_RootKeyCollidesWithKey(t *testing.T) {
	seen := map[string]int{}
	// Counter 0 declares Key="shared".
	results := validateSpanCounterEntry(0, map[string]any{"Key": "shared"}, seen)
	assert.Empty(t, results)
	// Counter 1 declares RootKey="shared" → collision with counter 0's Key.
	results = validateSpanCounterEntry(1, map[string]any{
		"Key":     "other",
		"RootKey": "shared",
		"ScopeConditions": []any{
			map[string]any{"Field": "x", "Operator": "exists"},
		},
	}, seen)
	require.NotEmpty(t, results)
	var sawErr bool
	for _, r := range results {
		if r.Severity == Error && strings.Contains(r.Message, "RootKey") && strings.Contains(r.Message, "collides") {
			sawErr = true
		}
	}
	assert.True(t, sawErr, "RootKey colliding with another counter's Key must error")
}

func TestValidateSpanCounterEntry_RootKeyWithoutScopeWarns(t *testing.T) {
	seen := map[string]int{}
	results := validateSpanCounterEntry(0, map[string]any{
		"Key":     "k",
		"RootKey": "rk",
	}, seen)
	require.NotEmpty(t, results)
	var sawWarn bool
	for _, r := range results {
		if r.Severity == Warning && strings.Contains(r.Message, "RootKey") && strings.Contains(r.Message, "ignored") {
			sawWarn = true
		}
	}
	assert.True(t, sawWarn, "RootKey without ScopeConditions must warn")
}

func TestValidateSpanCounterEntry_RootKeyEqualToKeyIsNoop(t *testing.T) {
	seen := map[string]int{}
	results := validateSpanCounterEntry(0, map[string]any{
		"Key":     "k",
		"RootKey": "k",
		"ScopeConditions": []any{
			map[string]any{"Field": "x", "Operator": "exists"},
		},
	}, seen)
	assert.Empty(t, results, "RootKey == Key is harmless and emits no diagnostics")
}

func TestValidateSpanCounterEntry_ReservedNamespace(t *testing.T) {
	seen := map[string]int{}
	results := validateSpanCounterEntry(0, map[string]any{"Key": "meta.refinery.reserved"}, seen)
	require.Len(t, results, 1)
	assert.Equal(t, Error, results[0].Severity)
	assert.Contains(t, results[0].Message, "reserved")
}

func TestValidateSpanCounterEntry_MetaNamespaceWarning(t *testing.T) {
	seen := map[string]int{}
	results := validateSpanCounterEntry(0, map[string]any{"Key": "meta.custom"}, seen)
	require.Len(t, results, 1)
	assert.Equal(t, Warning, results[0].Severity)
	assert.Contains(t, results[0].Message, "meta.")
}

// TestValidateRules_ScopeConditionsThroughMetadata exercises the full
// ValidateRules path with a realistic rules document that uses
// ScopeConditions. This is a regression test for a bug where the
// metadata-driven walker tried to resolve ScopeConditions against a
// non-existent "ScopeConditions" group and produced "unknown group" /
// "unknown field" errors for every condition entry.
func TestValidateRules_ScopeConditionsThroughMetadata(t *testing.T) {
	m, err := LoadRulesMetadata()
	require.NoError(t, err)

	rules := map[string]any{
		"RulesVersion": 2,
		"Samplers": map[string]any{
			"__default__": map[string]any{
				"DeterministicSampler": map[string]any{
					"SampleRate": 1,
				},
			},
		},
		"SpanCounters": []any{
			map[string]any{
				"Key":     "graphql.db_call_count",
				"RootKey": "trace.db_call_total",
				"ScopeConditions": []any{
					map[string]any{"Field": "graphql.field", "Operator": "exists"},
				},
				"Conditions": []any{
					map[string]any{"Field": "db.system", "Operator": "=", "Value": "postgresql", "Datatype": "string"},
				},
			},
		},
	}

	results := m.ValidateRules(rules)
	for _, r := range results {
		assert.NotContains(t, r.Message, "unknown group ScopeConditions",
			"metadata walker should not look up ScopeConditions as a group")
		assert.NotContains(t, r.Message, "unknown field ScopeConditions.",
			"metadata walker should not try to validate ScopeConditions.* directly")
	}

	// Same shape but with a bogus operator inside ScopeConditions — the
	// metadata-driven "choice" validation on Operator should still catch
	// this even though we routed ScopeConditions entries through the
	// "Conditions" group manually.
	rules["SpanCounters"].([]any)[0].(map[string]any)["ScopeConditions"] = []any{
		map[string]any{"Field": "graphql.field", "Operator": "nonsense-op"},
	}
	results = m.ValidateRules(rules)
	var sawBadOp bool
	for _, r := range results {
		if r.Severity == Error && strings.Contains(r.Message, "nonsense-op") {
			sawBadOp = true
		}
	}
	assert.True(t, sawBadOp, "bogus Operator inside ScopeConditions must still fail metadata validation")
}

func TestValidateSpanCounterEntry_HasRootSpanInScope(t *testing.T) {
	seen := map[string]int{}
	results := validateSpanCounterEntry(0, map[string]any{
		"Key": "k",
		"ScopeConditions": []any{
			map[string]any{"Operator": HasRootSpan},
		},
	}, seen)
	require.NotEmpty(t, results)
	var sawErr bool
	for _, r := range results {
		if r.Severity == Error && strings.Contains(r.Message, HasRootSpan) {
			sawErr = true
		}
	}
	assert.True(t, sawErr, "must reject HasRootSpan in ScopeConditions")
}
