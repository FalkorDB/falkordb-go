package falkordb

import (
	"context"
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
	Conn   redis.UniversalClient
	schema GraphSchema
}

// New creates a new graph.
func graphNew(Id string, conn redis.UniversalClient) *Graph {
	g := new(Graph)
	g.Id = Id
	g.Conn = conn
	g.schema = GraphSchemaNew(g)
	return g
}

// ExecutionPlan gets the execution plan for given query.
func (g *Graph) ExecutionPlan(query string) (string, error) {
	return g.ExecutionPlanContext(context.Background(), query)
}

// ExecutionPlanContext is like ExecutionPlan but honors the cancellation and
// deadline of ctx.
func (g *Graph) ExecutionPlanContext(ctx context.Context, query string) (string, error) {
	// GRAPH.EXPLAIN replies with one array element per plan line.
	lines, err := g.Conn.Do(ctx, "GRAPH.EXPLAIN", g.Id, query).StringSlice()
	if err != nil {
		return "", err
	}
	return strings.Join(lines, "\n"), nil
}

// Delete removes the graph.
func (g *Graph) Delete() error {
	return g.DeleteContext(context.Background())
}

// DeleteContext is like Delete but honors the cancellation and deadline of ctx.
func (g *Graph) DeleteContext(ctx context.Context) error {
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

func (g *Graph) query(ctx context.Context, command string, query string, params map[string]interface{}, options *QueryOptions) (*QueryResult, error) {
	if params != nil {
		header, err := buildParamsHeader(params)
		if err != nil {
			return nil, err
		}
		query = header + query
	}
	var r interface{}
	var err error
	if options != nil && options.timeout >= 0 {
		r, err = g.Conn.Do(ctx, command, g.Id, query, "--compact", "timeout", options.timeout).Result()
	} else {
		r, err = g.Conn.Do(ctx, command, g.Id, query, "--compact").Result()
	}
	if err != nil {
		return nil, err
	}

	return queryResultNew(ctx, g, r)
}

// Query executes a query against the graph.
func (g *Graph) Query(query string, params map[string]interface{}, options *QueryOptions) (*QueryResult, error) {
	return g.query(context.Background(), "GRAPH.QUERY", query, params, options)
}

// QueryContext is like Query but honors the cancellation and deadline of ctx.
// Cancelling ctx aborts the client side wait; use QueryOptions.SetTimeout to
// also bound execution on the server.
func (g *Graph) QueryContext(ctx context.Context, query string, params map[string]interface{}, options *QueryOptions) (*QueryResult, error) {
	return g.query(ctx, "GRAPH.QUERY", query, params, options)
}

// ROQuery executes a read only query against the graph.
func (g *Graph) ROQuery(query string, params map[string]interface{}, options *QueryOptions) (*QueryResult, error) {
	return g.query(context.Background(), "GRAPH.RO_QUERY", query, params, options)
}

// ROQueryContext is like ROQuery but honors the cancellation and deadline of ctx.
func (g *Graph) ROQueryContext(ctx context.Context, query string, params map[string]interface{}, options *QueryOptions) (*QueryResult, error) {
	return g.query(ctx, "GRAPH.RO_QUERY", query, params, options)
}

// Procedures

// CallProcedure invokes procedure.
func (g *Graph) CallProcedure(procedure string, yield []string, args ...interface{}) (*QueryResult, error) {
	return g.CallProcedureContext(context.Background(), procedure, yield, args...)
}

// CallProcedureContext is like CallProcedure but honors the cancellation and
// deadline of ctx.
func (g *Graph) CallProcedureContext(ctx context.Context, procedure string, yield []string, args ...interface{}) (*QueryResult, error) {
	query := fmt.Sprintf("CALL %s(", procedure)

	tmp := make([]string, 0, len(args))
	for _, arg := range args {
		s, err := toString(arg)
		if err != nil {
			return nil, err
		}
		tmp = append(tmp, s)
	}
	query += fmt.Sprintf("%s)", strings.Join(tmp, ","))

	if len(yield) > 0 {
		query += fmt.Sprintf(" YIELD %s", strings.Join(yield, ","))
	}

	return g.QueryContext(ctx, query, nil, nil)
}
