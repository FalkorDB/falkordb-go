package falkordb

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

type namedString string

func TestParamTypes(t *testing.T) {
	createGraph()
	cases := []struct {
		param    interface{}
		expected interface{}
	}{
		{float32(1.1), 1.1},
		{int32(7), int64(7)},
		{int8(-3), int64(-3)},
		{uint(5), int64(5)},
		{[]int{1, 2}, []interface{}{int64(1), int64(2)}},
		{[][]string{{"a"}}, []interface{}{[]interface{}{"a"}}},
		{namedString("bob"), "bob"},
		{map[string]string{"k": "v"}, map[string]interface{}{"k": "v"}},
		{[]interface{}{int32(1)}, []interface{}{int64(1)}},
	}
	for _, c := range cases {
		res, err := graph.Query("RETURN $param", map[string]interface{}{"param": c.param}, nil)
		if !assert.NoError(t, err, "param %T", c.param) {
			continue
		}
		res.Next()
		rIface, err := res.Record().GetByIndex(0)
		assert.NoError(t, err)
		assert.Equal(t, c.expected, rIface, "param %T", c.param)
	}
}

func TestUnsupportedParamTypeReturnsError(t *testing.T) {
	createGraph()
	for _, param := range []interface{}{struct{}{}, uint64(math.MaxUint64), []interface{}{struct{}{}}, math.NaN()} {
		assert.NotPanics(t, func() {
			_, err := graph.Query("RETURN $param", map[string]interface{}{"param": param}, nil)
			assert.ErrorContains(t, err, "param", "param %T", param)
		})
	}
}

func TestToStringNormalizesTypes(t *testing.T) {
	assert.Equal(t, "7", ToString(int32(7)))
	assert.Equal(t, "[1,2]", ToString([]int{1, 2}))
	assert.Panics(t, func() { ToString(struct{}{}) })
}
