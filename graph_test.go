package falkordb

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// GRAPH.EXPLAIN replies with an array of plan lines. Decoding it as a single
// string made ExecutionPlan always fail with
// "redis: unexpected type=[]interface {} for String".
func TestExecutionPlanReturnsPlanText(t *testing.T) {
	plan, err := graph.ExecutionPlan("MATCH (s:Person)-[e:Visited]->(d:Country) RETURN s")
	assert.Nil(t, err)
	assert.NotEmpty(t, plan)

	lines := strings.Split(plan, "\n")
	assert.Greater(t, len(lines), 1, "plan should keep one line per operation, got %q", plan)
	assert.Equal(t, "Results", strings.TrimSpace(lines[0]))
	assert.Contains(t, plan, "Node By Label Scan")
}

func TestExecutionPlanReportsQueryErrors(t *testing.T) {
	_, err := graph.ExecutionPlan("MATCH (s RETURN s")
	assert.Error(t, err)
}
