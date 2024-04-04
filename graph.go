package falkordb

import (
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

// QueryOptions are a set of additional arguments to be emitted with a query.
type QueryOptions struct {
	timeout int
}

// Graph represents a graph, which is a collection of nodes and edges.
type Graph struct {
	Id     string
	Conn   *redis.Client
	schema GraphSchema
}

// New creates a new graph.
func graphNew(Id string, conn *redis.Client) *Graph {
	g := new(Graph)
	g.Id = Id
	g.Conn = conn
	g.schema = GraphSchemaNew(g)
	return g
}

// ExecutionPlan gets the execution plan for given query.
func (g *Graph) ExecutionPlan(q string) (string, error) {
	return g.Conn.Do(ctx, "GRAPH.EXPLAIN", g.Id, q).Text()
}

// Delete removes the graph.
func (g *Graph) Delete() error {
	err := g.Conn.Do(ctx, "GRAPH.DELETE", g.Id).Err()

	// clear internal mappings
	g.schema.clear()

	return err
}

// NewQueryOptions instantiates a new QueryOptions struct.
func NewQueryOptions() *QueryOptions {
	return &QueryOptions{
		timeout: -1,
	}
}

// SetTimeout sets the timeout member of the QueryOptions struct
func (options *QueryOptions) SetTimeout(timeout int) *QueryOptions {
	options.timeout = timeout
	return options
}

// GetTimeout retrieves the timeout of the QueryOptions struct
func (options *QueryOptions) GetTimeout() int {
	return options.timeout
}

// Query executes a query against the graph.
func (g *Graph) Query(q string, params map[string]interface{}, options *QueryOptions) (*QueryResult, error) {
	if params != nil {
		q = BuildParamsHeader(params) + q
	}
	var r interface{}
	var err error
	if options != nil && options.timeout >= 0 {
		r, err = g.Conn.Do(ctx, "GRAPH.QUERY", g.Id, q, "--compact", "timeout", options.timeout).Result()
	} else {
		r, err = g.Conn.Do(ctx, "GRAPH.QUERY", g.Id, q, "--compact").Result()
	}
	if err != nil {
		return nil, err
	}

	return QueryResultNew(g, r)
}

// ROQuery executes a read only query against the graph.
func (g *Graph) ROQuery(q string, params map[string]interface{}, options *QueryOptions) (*QueryResult, error) {
	if params != nil {
		q = BuildParamsHeader(params) + q
	}
	var r interface{}
	var err error
	if options != nil && options.timeout >= 0 {
		r, err = g.Conn.Do(ctx, "GRAPH.RO_QUERY", g.Id, q, "--compact", "timeout", options.timeout).Result()
	} else {
		r, err = g.Conn.Do(ctx, "GRAPH.RO_QUERY", g.Id, q, "--compact").Result()
	}
	if err != nil {
		return nil, err
	}

	return QueryResultNew(g, r)
}

// Procedures

// CallProcedure invokes procedure.
func (g *Graph) CallProcedure(procedure string, yield []string, args ...interface{}) (*QueryResult, error) {
	q := fmt.Sprintf("CALL %s(", procedure)

	tmp := make([]string, 0, len(args))
	for arg := range args {
		tmp = append(tmp, ToString(arg))
	}
	q += fmt.Sprintf("%s)", strings.Join(tmp, ","))

	if len(yield) > 0 {
		q += fmt.Sprintf(" YIELD %s", strings.Join(yield, ","))
	}

	return g.Query(q, nil, nil)
}
