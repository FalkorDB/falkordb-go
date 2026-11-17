package falkordb

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// namedKey exercises map keys whose underlying kind is a string but whose type
// is not string itself.
type namedKey string

// Every one of these types used to panic ToString with
// "Unrecognized type to convert to string".
func TestToStringSupportedTypes(t *testing.T) {
	sample := time.Date(2026, 8, 18, 20, 18, 4, 0, time.UTC)

	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{"nil", nil, "null"},
		{"string", "hello", `"hello"`},
		{"string with quotes", `say "hi"`, `"say \"hi\""`},
		{"string with backslash", `a\\b`, `"a\\\\b"`},
		{"newline", "a\nb", `"a\nb"`},
		{"non-ascii passes through", "café", `"café"`},
		{"nbsp passes through", "a\u00a0b", "\"a\u00a0b\""},
		{"bool", true, "true"},
		{"int", 42, "42"},
		{"int8", int8(8), "8"},
		{"int16", int16(16), "16"},
		{"int32", int32(32), "32"},
		{"int64", int64(64), "64"},
		{"negative int", -7, "-7"},
		{"uint", uint(1), "1"},
		{"uint8", uint8(2), "2"},
		{"uint16", uint16(3), "3"},
		{"uint32", uint32(4), "4"},
		{"uint64", uint64(5), "5"},
		{"float32", float32(1.5), "1.5"},
		{"float64", 2.25, "2.25"},
		{"bytes", []byte("raw"), `"raw"`},
		{"time", sample, `"2026-08-18T20:18:04Z"`},
		{"duration", 90 * time.Second, "90"},
		{"[]interface{}", []interface{}{1, "a"}, `[1,"a"]`},
		{"[]string", []string{"a", "b"}, `["a","b"]`},
		{"[]int", []int{1, 2, 3}, "[1,2,3]"},
		{"[]float64", []float64{1.5, 2.5}, "[1.5,2.5]"},
		{"[]bool", []bool{true, false}, "[true,false]"},
		{"array", [2]int{7, 8}, "[7,8]"},
		{"nested slice", []interface{}{[]int{1}}, "[[1]]"},
		{"nil slice", []string(nil), "null"},
		{"empty slice", []int{}, "[]"},
		{"map", map[string]interface{}{"a": 1}, "{`a`: 1}"},
		{"map[string]int", map[string]int{"a": 1, "b": 2}, "{`a`: 1,`b`: 2}"},
		{"map with named key type", map[namedKey]int{"b": 2, "a": 1}, "{`a`: 1,`b`: 2}"},
		{"nil map", map[string]interface{}(nil), "null"},
		{"pointer", func() *int { v := 5; return &v }(), "5"},
		{"nil pointer", (*int)(nil), "null"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := toString(tt.value)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, got)
			assert.Equal(t, tt.expected, ToString(tt.value))
		})
	}
}

func TestToStringUnsupportedTypeReturnsError(t *testing.T) {
	_, err := toString(make(chan int))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsupportedType)

	_, err = toString(map[int]string{1: "a"})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsupportedType)

	// An unsupported value nested inside a container is reported too.
	_, err = toString([]interface{}{1, make(chan int)})
	assert.ErrorIs(t, err, ErrUnsupportedType)
}

// ToString keeps its signature and must never panic, even for values it cannot
// represent, because Node.Encode and Edge.Encode render properties with it.
func TestToStringNeverPanics(t *testing.T) {
	assert.NotPanics(t, func() { ToString(make(chan int)) })
	assert.NotPanics(t, func() { ToString(struct{ A int }{1}) })
}

func TestBuildParamsHeaderIsDeterministic(t *testing.T) {
	params := map[string]interface{}{"b": 2, "a": 1, "c": "x"}

	header, err := buildParamsHeader(params)
	require.NoError(t, err)
	assert.Equal(t, `CYPHER a=1 b=2 c="x" `, header)

	// Sorted keys mean repeated calls produce identical query text, which keeps
	// FalkorDB's query cache effective.
	for i := 0; i < 20; i++ {
		repeated, err := buildParamsHeader(params)
		require.NoError(t, err)
		assert.Equal(t, header, repeated)
	}
}

func TestBuildParamsHeaderReportsBadParam(t *testing.T) {
	_, err := buildParamsHeader(map[string]interface{}{"bad": make(chan int)})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsupportedType)
	assert.Contains(t, err.Error(), `parameter "bad"`)
}

func TestBuildParamsHeaderEmpty(t *testing.T) {
	header, err := buildParamsHeader(map[string]interface{}{})
	require.NoError(t, err)
	assert.Equal(t, "CYPHER ", header)
}

// A map key is interpolated into the query text, so it must be back-quoted or a
// crafted key can close the map literal and append arbitrary clauses. The
// payload below created a `:Pwned` node and commented out the caller's query.
func TestMapKeyCannotInjectCypher(t *testing.T) {
	payload := "x: 1} CREATE (:Pwned) //"

	got, err := toString(map[string]interface{}{payload: 1})
	require.NoError(t, err)
	assert.Equal(t, "{`x: 1} CREATE (:Pwned) //`: 1}", got)

	// A key that cannot be back-quoted safely is rejected outright.
	_, err = toString(map[string]interface{}{"has`backquote": 1})
	assert.ErrorIs(t, err, ErrUnsupportedType)
}

func TestParamNameMustBeIdentifier(t *testing.T) {
	for _, name := range []string{
		"x=1 CREATE (:Pwned) //", "", "1abc", "a-b", "a b", "a`b", `a"b`,
	} {
		_, err := buildParamsHeader(map[string]interface{}{name: 1})
		assert.ErrorIsf(t, err, ErrUnsupportedType, "name %q must be rejected", name)
	}

	for _, name := range []string{"a", "_a", "A1", "snake_case_1"} {
		_, err := buildParamsHeader(map[string]interface{}{name: 1})
		assert.NoErrorf(t, err, "name %q must be accepted", name)
	}
}

// strconv.Quote would render these as \xNN or \uXXXX, which FalkorDB does not
// decode, silently corrupting the value.
func TestStringEscapingMatchesServer(t *testing.T) {
	_, err := toString("a\x00b")
	assert.ErrorIs(t, err, ErrUnsupportedType, "NUL cannot be carried and must be rejected")

	_, err = toString([]byte{0xff, 0xfe})
	assert.ErrorIs(t, err, ErrUnsupportedType, "invalid UTF-8 must be rejected")

	got, err := toString("a\u00a0b")
	require.NoError(t, err)
	assert.Equal(t, "\"a\u00a0b\"", got, "NBSP must pass through, not become \\u00a0")
}

func TestNumericLimits(t *testing.T) {
	_, err := toString(uint64(math.MaxUint64))
	assert.ErrorIs(t, err, ErrUnsupportedType, "uint64 above MaxInt64 is silently clamped by the server")

	got, err := toString(uint64(math.MaxInt64))
	require.NoError(t, err)
	assert.Equal(t, "9223372036854775807", got)

	for _, f := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		_, err := toString(f)
		assert.ErrorIs(t, err, ErrUnsupportedType)
	}
}

// A stack overflow is a fatal error that recover cannot catch, so cyclic values
// have to be rejected rather than followed.
func TestCyclicValuesAreRejected(t *testing.T) {
	m := map[string]interface{}{}
	m["self"] = m
	_, err := toString(m)
	assert.ErrorIs(t, err, ErrUnsupportedType)

	s := make([]interface{}, 1)
	s[0] = s
	_, err = toString(s)
	assert.ErrorIs(t, err, ErrUnsupportedType)

	assert.NotPanics(t, func() { ToString(m) })
}

// FalkorDB strings are UTF-8 text, so a Go string holding invalid UTF-8 must be
// rejected on the same terms as a []byte rather than stored incorrectly.
func TestInvalidUTF8Rejected(t *testing.T) {
	for name, v := range map[string]interface{}{
		"string": "a\xffb",
		"bytes":  []byte{0xff, 0xfe},
	} {
		_, err := toString(v)
		assert.ErrorIsf(t, err, ErrUnsupportedType, "%s must be rejected", name)
	}

	// The fallback in ToString must cope with it too.
	assert.NotPanics(t, func() { ToString("a\xffb") })
}
