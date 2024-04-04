package falkordb

import (
	"fmt"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
)

// QueryOptions are a set of additional arguments to be emitted with a query.
type QueryOptions struct {
	timeout int
}

// Graph represents a graph, which is a collection of nodes and edges.
type Graph struct {
	Id                string
	Conn              *redis.Client
	labels            []string   // List of node labels.
	relationshipTypes []string   // List of relation types.
	properties        []string   // List of properties.
	mutex             sync.Mutex // Lock, used for updating internal state.
}

// New creates a new graph.
func graphNew(Id string, conn *redis.Client) Graph {
	return Graph{
		Id:                Id,
		Conn:              conn,
		labels:            make([]string, 0),
		relationshipTypes: make([]string, 0),
		properties:        make([]string, 0),
	}
}

// ExecutionPlan gets the execution plan for given query.
func (g *Graph) ExecutionPlan(q string) (string, error) {
	return g.Conn.Do(ctx, "GRAPH.EXPLAIN", g.Id, q).Text()
}

// Delete removes the graph.
func (g *Graph) Delete() error {
	err := g.Conn.Do(ctx, "GRAPH.DELETE", g.Id).Err()

	// clear internal mappings
	g.labels = g.labels[:0]
	g.properties = g.properties[:0]
	g.relationshipTypes = g.relationshipTypes[:0]

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

func (g *Graph) getLabel(lblIdx int) string {
	if lblIdx >= len(g.labels) {
		// Missing label, refresh label mapping table.
		g.mutex.Lock()

		// Recheck now that we've got the lock.
		if lblIdx >= len(g.labels) {
			g.labels = g.Labels()
			// Retry.
			if lblIdx >= len(g.labels) {
				// Error!
				panic("Unknown label index.")
			}
		}
		g.mutex.Unlock()
	}

	return g.labels[lblIdx]
}

func (g *Graph) getRelation(relIdx int) string {
	if relIdx >= len(g.relationshipTypes) {
		// Missing relation type, refresh relation type mapping table.
		g.mutex.Lock()

		// Recheck now that we've got the lock.
		if relIdx >= len(g.relationshipTypes) {
			g.relationshipTypes = g.RelationshipTypes()
			// Retry.
			if relIdx >= len(g.relationshipTypes) {
				// Error!
				panic("Unknown relation type index.")
			}
		}
		g.mutex.Unlock()
	}

	return g.relationshipTypes[relIdx]
}

func (g *Graph) getProperty(propIdx int) string {
	if propIdx >= len(g.properties) {
		// Missing property, refresh property mapping table.
		g.mutex.Lock()

		// Recheck now that we've got the lock.
		if propIdx >= len(g.properties) {
			g.properties = g.PropertyKeys()

			// Retry.
			if propIdx >= len(g.properties) {
				// Error!
				panic("Unknown property index.")
			}
		}
		g.mutex.Unlock()
	}

	return g.properties[propIdx]
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

// Labels, retrieves all node labels.
func (g *Graph) Labels() []string {
	qr, _ := g.CallProcedure("db.labels", nil)

	l := make([]string, len(qr.results))

	for idx, r := range qr.results {
		l[idx] = r.GetByIndex(0).(string)
	}
	return l
}

// RelationshipTypes, retrieves all edge relationship types.
func (g *Graph) RelationshipTypes() []string {
	qr, _ := g.CallProcedure("db.relationshipTypes", nil)

	rt := make([]string, len(qr.results))

	for idx, r := range qr.results {
		rt[idx] = r.GetByIndex(0).(string)
	}
	return rt
}

// PropertyKeys, retrieves all properties names.
func (g *Graph) PropertyKeys() []string {
	qr, _ := g.CallProcedure("db.propertyKeys", nil)

	p := make([]string, len(qr.results))

	for idx, r := range qr.results {
		p[idx] = r.GetByIndex(0).(string)
	}
	return p
}
