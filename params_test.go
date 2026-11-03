package falkordb

import (
	"context"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pocLeakKey and pocDeleteKey would close the map literal and append clauses
// to the query if they were written into the CYPHER header unquoted.
const (
	pocLeakKey   = "a: 1} MATCH (s:Secret) RETURN s.value AS value //"
	pocDeleteKey = "a: 1} MATCH (s:Secret) DETACH DELETE s RETURN 1 AS x //"
)

// countingHook counts the commands a client tries to send.
type countingHook struct{ commands int }

func (h *countingHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *countingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.commands++
		return next(ctx, cmd)
	}
}

func (h *countingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.commands += len(cmds)
		return next(ctx, cmds)
	}
}

func TestParamsHeaderKeepsIdentifiersUnquoted(t *testing.T) {
	assert.Equal(t, "CYPHER x=1 ", BuildParamsHeader(map[string]interface{}{"x": 1}))
	assert.Equal(t, "CYPHER _Param_1=1 ", BuildParamsHeader(map[string]interface{}{"_Param_1": 1}))
	assert.Equal(t, "CYPHER p={foo_1: 1} ", BuildParamsHeader(map[string]interface{}{"p": map[string]interface{}{"foo_1": 1}}))
	assert.Equal(t, "{_k: [{v2: 1}]}", ToString(map[string]interface{}{"_k": []interface{}{map[string]interface{}{"v2": 1}}}))
}

func TestParamsHeaderQuotesNonIdentifiers(t *testing.T) {
	for _, name := range []string{"first-name", "a b", "ü", "1abc", pocLeakKey} {
		quoted := "`" + name + "`"
		assert.Equal(t, "CYPHER "+quoted+"=1 ", BuildParamsHeader(map[string]interface{}{name: 1}), "parameter name %q", name)
		assert.Equal(t, "{"+quoted+": 1}", ToString(map[string]interface{}{name: 1}), "map key %q", name)
	}
}

func TestParamsHeaderQuotesNestedMapKeys(t *testing.T) {
	value := map[string]interface{}{
		"outer": []interface{}{
			map[string]interface{}{"inner": map[string]interface{}{"a b": 1}},
		},
	}
	assert.Equal(t, "{outer: [{inner: {`a b`: 1}}]}", ToString(value))
	assert.Equal(t, "CYPHER p={outer: [{inner: {`a b`: 1}}]} ", BuildParamsHeader(map[string]interface{}{"p": value}))
}

func TestUnrepresentableNamesAreRejected(t *testing.T) {
	for _, name := range []string{"", "a`b", "a\x00b"} {
		cases := map[string]map[string]interface{}{
			"parameter name": {name: 1},
			"map key":        {"p": map[string]interface{}{name: 1}},
			"nested map key": {"p": map[string]interface{}{"l": []interface{}{map[string]interface{}{name: 1}}}},
		}
		for where, params := range cases {
			hook := &countingHook{}
			conn := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
			conn.AddHook(hook)
			g := graphNew("unsent", conn)

			_, err := g.Query("RETURN 1", params, nil)
			assert.ErrorIs(t, err, ErrInvalidName, "%s %q", where, name)
			assert.Equal(t, 0, hook.commands, "%s %q: nothing should be sent", where, name)
			conn.Close()

			assertPanicsWithInvalidName(t, func() { BuildParamsHeader(params) }, "%s %q", where, name)
		}
		assertPanicsWithInvalidName(t, func() { ToString(map[string]interface{}{name: 1}) }, "map key %q", name)
	}
}

func assertPanicsWithInvalidName(t *testing.T, f func(), msgAndArgs ...interface{}) {
	t.Helper()
	defer func() {
		err, _ := recover().(error)
		assert.ErrorIs(t, err, ErrInvalidName, msgAndArgs...)
	}()
	f()
}

func TestInvalidNameErrorIsBounded(t *testing.T) {
	name := strings.Repeat("x", 1000) + "`"
	_, err := graph.Query("RETURN 1", map[string]interface{}{name: 1}, nil)
	require.ErrorIs(t, err, ErrInvalidName)
	assert.Less(t, len(err.Error()), 200)
}

func paramInjectionGraph(t *testing.T) *Graph {
	g := graphNew("param_injection", graph.Conn)
	g.Delete()
	t.Cleanup(func() { g.Delete() })
	_, err := g.Query("CREATE (:Secret {value: 'hidden'}), (:Public {value: 'ok'})", nil, nil)
	require.NoError(t, err)
	return g
}

func TestMapKeyCannotRewriteQuery(t *testing.T) {
	g := paramInjectionGraph(t)

	res, err := g.Query("MATCH (p:Public) WHERE p.value = $filter.a RETURN p.value AS value",
		map[string]interface{}{"filter": map[string]interface{}{pocLeakKey: 1}}, nil)
	require.NoError(t, err)
	for res.Next() {
		v, _ := res.Record().Get("value")
		assert.NotEqual(t, "hidden", v, "the map key rewrote the query")
	}
	assert.Empty(t, res.results)
}

func TestMapKeyCannotDeleteData(t *testing.T) {
	g := paramInjectionGraph(t)

	res, err := g.Query("RETURN $x AS x", map[string]interface{}{"x": map[string]interface{}{pocDeleteKey: 1}}, nil)
	require.NoError(t, err)
	require.True(t, res.Next())
	x, _ := res.Record().Get("x")
	assert.Equal(t, map[string]interface{}{pocDeleteKey: int64(1)}, x)

	res, err = g.Query("MATCH (s:Secret) RETURN count(s)", nil, nil)
	require.NoError(t, err)
	require.True(t, res.Next())
	count, _ := res.Record().GetByIndex(0)
	assert.Equal(t, int64(1), count, "the map key deleted data")
}

func TestNonIdentifierMapKeysRoundTrip(t *testing.T) {
	value := map[string]interface{}{"first-name": "Ann", "a b": int64(1), "ü": true}
	res, err := graph.Query("RETURN $m", map[string]interface{}{"m": value}, nil)
	require.NoError(t, err)
	require.True(t, res.Next())
	m, _ := res.Record().GetByIndex(0)
	assert.Equal(t, value, m)
}

func TestNonIdentifierParamName(t *testing.T) {
	res, err := graph.Query("RETURN $`my param`", map[string]interface{}{"my param": int64(7)}, nil)
	require.NoError(t, err)
	require.True(t, res.Next())
	v, _ := res.Record().GetByIndex(0)
	assert.Equal(t, int64(7), v)
}
