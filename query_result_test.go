package falkordb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Passing a parameter whose type ToString did not handle used to panic the
// caller's program from inside Query.
func TestQueryParamTypesDoNotPanic(t *testing.T) {
	params := []interface{}{
		int32(33), int16(33), int8(33), uint(33), uint64(33),
		float32(33.5), []int{1, 2, 3}, []byte("John Doe"),
		90 * time.Second, map[string]int{"a": 1},
	}

	for _, p := range params {
		require.NotPanics(t, func() {
			_, err := graph.Query("RETURN $p", map[string]interface{}{"p": p}, nil)
			assert.NoError(t, err)
		}, "param %T", p)
	}
}

func TestQueryParamRoundTrip(t *testing.T) {
	res, err := graph.Query(
		"RETURN $a AS a, $b AS b, $c AS c",
		map[string]interface{}{"a": int32(7), "b": float32(1.5), "c": []int{1, 2}},
		nil,
	)
	require.NoError(t, err)
	require.True(t, res.Next())

	a, err := res.Record().GetByIndex(0)
	require.NoError(t, err)
	assert.Equal(t, int64(7), a)

	b, err := res.Record().GetByIndex(1)
	require.NoError(t, err)
	assert.InDelta(t, 1.5, b, 0.0001)

	c, err := res.Record().GetByIndex(2)
	require.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1), int64(2)}, c)
}

func TestQueryRejectsUnsupportedParam(t *testing.T) {
	_, err := graph.Query("RETURN $p", map[string]interface{}{"p": make(chan int)}, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsupportedType)
}

// CallProcedure used to iterate the argument indices rather than the arguments,
// emitting `CALL algo.pageRank(0,1)`. That form is accepted by the server but
// matches nothing, so the bug silently returned empty results.
func TestCallProcedurePassesArguments(t *testing.T) {
	res, err := graph.CallProcedure("algo.pageRank", []string{"node", "score"}, "Person", "Visited")
	require.NoError(t, err)
	require.True(t, res.Next(), "procedure should receive the argument values, not their indices")

	node, err := res.Record().GetByIndex(0)
	require.NoError(t, err)
	assert.Equal(t, "John Doe", node.(*Node).GetProperty("name"))
}

func TestCallProcedureRejectsUnsupportedArgument(t *testing.T) {
	_, err := graph.CallProcedure("db.labels", nil, make(chan int))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsupportedType)
}

// FalkorDB encodes DATETIME, DATE and TIME as seconds since the Unix epoch and
// DURATION as a number of seconds. Before these types were decoded the records
// were silently dropped and Query returned no error at all.
func TestTemporalTypes(t *testing.T) {
	res, err := graph.Query("RETURN date(), localtime(), localdatetime(), duration({days: 2})", nil, nil)
	require.NoError(t, err)
	require.True(t, res.Next())

	values := res.Record().Values()
	require.Len(t, values, 4)

	for i, name := range []string{"date", "localtime", "localdatetime"} {
		ts, ok := values[i].(time.Time)
		require.True(t, ok, "%s should decode to time.Time, got %T", name, values[i])
		assert.False(t, ts.IsZero(), "%s should not be the zero time", name)
		assert.WithinDuration(t, time.Now().UTC(), ts, 48*time.Hour, "%s should be close to now", name)
	}

	assert.Equal(t, 48*time.Hour, values[3])
}

func TestParseScalarTemporalValues(t *testing.T) {
	qr := &QueryResult{graph: graph}

	// 1787011200 is 2026-08-18T00:00:00Z.
	ts, err := qr.parseScalar([]interface{}{int64(VALUE_DATE), int64(1787011200)})
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC), ts)

	d, err := qr.parseScalar([]interface{}{int64(VALUE_DURATION), int64(172800)})
	require.NoError(t, err)
	assert.Equal(t, 48*time.Hour, d)
}

// The compact protocol type IDs are positional, so a value appended in the
// wrong place would silently mis-decode every temporal value.
func TestScalarTypeIDsMatchProtocol(t *testing.T) {
	assert.Equal(t, ResultSetScalarTypes(11), VALUE_POINT)
	assert.Equal(t, ResultSetScalarTypes(12), VALUE_VECTORF32)
	assert.Equal(t, ResultSetScalarTypes(13), VALUE_DATETIME)
	assert.Equal(t, ResultSetScalarTypes(14), VALUE_DATE)
	assert.Equal(t, ResultSetScalarTypes(15), VALUE_TIME)
	assert.Equal(t, ResultSetScalarTypes(16), VALUE_DURATION)
}

// parseResults used to discard the error from parseRecords, so QueryResultNew
// reported success while handing back a slice of nil Records.
func TestQueryResultNewPropagatesParseError(t *testing.T) {
	response := []interface{}{
		[]interface{}{[]interface{}{int64(COLUMN_SCALAR), "n"}},
		[]interface{}{[]interface{}{[]interface{}{int64(99), "x"}}},
		[]interface{}{"Query internal execution time: 0.1 milliseconds"},
	}

	qr, err := QueryResultNew(graph, response)
	require.Error(t, err)
	assert.Nil(t, qr)
	assert.Contains(t, err.Error(), "unknown scalar type")
}

func TestQueryResultNewRejectsMalformedResponse(t *testing.T) {
	_, err := QueryResultNew(graph, "not a response")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected response type")

	_, err = QueryResultNew(graph, []interface{}{"header", "records"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "malformed response")
}

// End-to-end proof that a hostile map key cannot escape its literal. The
// payload below used to create a `:Pwned` node and comment out the query.
func TestQueryParamCannotInjectCypher(t *testing.T) {
	db, err := FromURL("falkor://0.0.0.0:6379")
	require.NoError(t, err)
	victim := db.SelectGraph("injection_regression")
	defer victim.Delete()

	_, err = victim.Query("CREATE (:Marker)", nil, nil)
	require.NoError(t, err)

	params := map[string]interface{}{
		"p": map[string]interface{}{"x: 1} CREATE (:Pwned) //": 1},
	}
	res, err := victim.Query("RETURN $p", params, nil)
	require.NoError(t, err, "the payload should be inert, not a syntax error")
	require.True(t, res.Next())

	value, err := res.Record().GetByIndex(0)
	require.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"x: 1} CREATE (:Pwned) //": int64(1)}, value,
		"the key must survive as data")

	count, err := victim.Query("MATCH (n:Pwned) RETURN count(n)", nil, nil)
	require.NoError(t, err)
	require.True(t, count.Next())
	created, err := count.Record().GetByIndex(0)
	require.NoError(t, err)
	assert.Equal(t, int64(0), created, "no node may be created by a parameter value")
}

// A malformed response must produce an error rather than panic the caller.
func TestMalformedResponsesReportErrors(t *testing.T) {
	stats := []interface{}{"Cached execution: 0"}
	header := []interface{}{[]interface{}{int64(COLUMN_SCALAR), "n"}}

	tests := map[string]interface{}{
		"statistics not a list":   []interface{}{"nope"},
		"statistic not a string":  []interface{}{[]interface{}{int64(1)}},
		"statistic without colon": []interface{}{[]interface{}{"no separator"}},
		"header not a list":       []interface{}{"nope", []interface{}{}, stats},
		"header column not list":  []interface{}{[]interface{}{"nope"}, []interface{}{}, stats},
		"header column too short": []interface{}{[]interface{}{[]interface{}{int64(1)}}, []interface{}{}, stats},
		"header type not int":     []interface{}{[]interface{}{[]interface{}{"x", "n"}}, []interface{}{}, stats},
		"header name not string":  []interface{}{[]interface{}{[]interface{}{int64(1), int64(2)}}, []interface{}{}, stats},
		"trailing stats bad":      []interface{}{header, []interface{}{}, "nope"},
	}

	for name, response := range tests {
		t.Run(name, func(t *testing.T) {
			require.NotPanics(t, func() {
				_, err := QueryResultNew(graph, response)
				assert.Error(t, err)
			})
		})
	}
}
