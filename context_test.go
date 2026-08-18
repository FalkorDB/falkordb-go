package falkordb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// A cancelled context must abort before anything reaches the server. The
// mutation is deliberately observable so the test fails if the query ran.
func TestQueryContextCancelledDoesNotReachServer(t *testing.T) {
	db, err := FromURL("falkor://0.0.0.0:6379")
	assert.Nil(t, err)
	g := db.SelectGraph("ctx_cancel")
	g.Delete() // start from a clean slate even if a previous run leaked
	// Deferred calls run LIFO, so Delete must be registered last to run
	// before the connection is closed.
	defer g.Conn.Close()
	defer g.Delete()

	_, err = g.Query("CREATE (:Marker {id: 0})", nil, nil)
	assert.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = g.QueryContext(ctx, "CREATE (:Marker {id: 1})", nil, nil)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled), "want context.Canceled, got %v", err)

	res, err := g.Query("MATCH (m:Marker) RETURN count(m)", nil, nil)
	assert.Nil(t, err)
	assert.True(t, res.Next())
	count, err := res.Record().GetByIndex(0)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), count, "cancelled query must not have been executed")
}

func TestQueryContextDeadlineExceeded(t *testing.T) {
	db, err := FromURL("falkor://0.0.0.0:6379")
	assert.Nil(t, err)
	g := db.SelectGraph("ctx_deadline")
	g.Delete() // start from a clean slate even if a previous run leaked
	// Deferred calls run LIFO, so Delete must be registered last to run
	// before the connection is closed.
	defer g.Conn.Close()
	defer g.Delete()

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()

	_, err = g.QueryContext(ctx, "RETURN 1", nil, nil)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded), "want context.DeadlineExceeded, got %v", err)
}

func TestROQueryContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := graph.ROQueryContext(ctx, "MATCH (n) RETURN n", nil, nil)
	assert.True(t, errors.Is(err, context.Canceled), "want context.Canceled, got %v", err)
}

// Decoding a node forces a schema refresh round trip. That refresh must run
// under the caller's context rather than a detached background one.
func TestSchemaRefreshHonorsQueryContext(t *testing.T) {
	db, err := FromURL("falkor://0.0.0.0:6379")
	assert.Nil(t, err)
	g := db.SelectGraph("ctx_schema")
	g.Delete() // start from a clean slate even if a previous run leaked
	// Deferred calls run LIFO, so Delete must be registered last to run
	// before the connection is closed.
	defer g.Conn.Close()
	defer g.Delete()

	_, err = g.Query("CREATE (:Fresh {v: 1})", nil, nil)
	assert.Nil(t, err)

	// A brand new Graph has an empty schema cache, so returning the node
	// requires resolving its label through db.labels.
	g2 := db.SelectGraph("ctx_schema")
	assert.Empty(t, g2.schema.labels)

	response, err := g2.Conn.Do(context.Background(), "GRAPH.QUERY", g2.Id,
		"MATCH (n:Fresh) RETURN n", "--compact").Result()
	assert.Nil(t, err)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = queryResultNew(cancelled, g2, response)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled),
		"schema refresh ignored the caller context, got %v", err)

	// The same response decodes cleanly under a live context.
	qr, err := queryResultNew(context.Background(), g2, response)
	assert.Nil(t, err)
	assert.True(t, qr.Next())
	node, err := qr.Record().GetByIndex(0)
	assert.Nil(t, err)
	assert.Equal(t, []string{"Fresh"}, node.(*Node).Labels)
}

func TestContextVariantsHappyPath(t *testing.T) {
	ctx := context.Background()

	res, err := graph.QueryContext(ctx, "RETURN 1", nil, nil)
	assert.Nil(t, err)
	assert.True(t, res.Next())

	res, err = graph.ROQueryContext(ctx, "MATCH (n:Person) RETURN n.name", nil, nil)
	assert.Nil(t, err)
	assert.True(t, res.Next())

	plan, err := graph.ExecutionPlanContext(ctx, "MATCH (n) RETURN n")
	assert.Nil(t, err)
	assert.Contains(t, plan, "Results")

	res, err = graph.CallProcedureContext(ctx, "db.labels", nil)
	assert.Nil(t, err)
	assert.False(t, res.Empty())
}

func TestContextVariantsPassParameters(t *testing.T) {
	res, err := graph.QueryContext(context.Background(), "RETURN $n",
		map[string]interface{}{"n": int32(7)}, nil)
	assert.Nil(t, err)
	assert.True(t, res.Next())
	v, err := res.Record().GetByIndex(0)
	assert.Nil(t, err)
	assert.Equal(t, int64(7), v)
}

func TestDBContextVariants(t *testing.T) {
	db, err := FalkorDBNewContext(context.Background(), &ConnectionOption{Addr: "0.0.0.0:6379"})
	assert.Nil(t, err)
	defer db.Conn.Close()

	names, err := db.ListGraphsContext(context.Background())
	assert.Nil(t, err)
	assert.Contains(t, names, "social")

	_, err = db.ConfigGetContext(context.Background(), "QUERY_MEM_CAPACITY")
	assert.Nil(t, err)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = db.ListGraphsContext(cancelled)
	assert.True(t, errors.Is(err, context.Canceled), "want context.Canceled, got %v", err)
	assert.True(t, errors.Is(db.ConfigSetContext(cancelled, "TIMEOUT_DEFAULT", 0), context.Canceled))
	assert.True(t, errors.Is(db.UDFFlushContext(cancelled), context.Canceled))
}

// A cancelled context must fail the constructor rather than silently skipping
// the sentinel probe and returning a client wired to the wrong endpoint.
func TestConstructorsReportContextCancellation(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	db, err := FromURLContext(cancelled, "falkor://0.0.0.0:6379")
	assert.Nil(t, db)
	assert.True(t, errors.Is(err, context.Canceled), "want context.Canceled, got %v", err)

	db, err = FalkorDBNewContext(cancelled, &ConnectionOption{Addr: "0.0.0.0:6379"})
	assert.Nil(t, db)
	assert.True(t, errors.Is(err, context.Canceled), "want context.Canceled, got %v", err)
}

// The schema cache is shared by every query on a *Graph. Decoding nodes and
// edges concurrently used to race on it; run under -race to catch regressions.
func TestConcurrentQueriesShareSchemaSafely(t *testing.T) {
	db, err := FromURL("falkor://0.0.0.0:6379")
	assert.Nil(t, err)
	defer db.Conn.Close()

	// A fresh *Graph starts with an empty cache, so every goroutine races to
	// populate it.
	g := db.SelectGraph("social")

	const goroutines = 8
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := g.QueryContext(context.Background(),
				"MATCH (s)-[e]->(d) RETURN s,e,d", nil, nil)
			if err != nil {
				errs <- err
				return
			}
			for res.Next() {
				node, err := res.Record().GetByIndex(0)
				if err != nil {
					errs <- err
					return
				}
				if labels := node.(*Node).Labels; len(labels) == 0 || labels[0] != "Person" {
					errs <- fmt.Errorf("torn schema read: got labels %v", labels)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// The pre-existing non-context API must keep working unchanged.
func TestNonContextAPIStillWorks(t *testing.T) {
	res, err := graph.Query("MATCH (s:Person) RETURN s.name", nil, nil)
	assert.Nil(t, err)
	assert.True(t, res.Next())
	name, err := res.Record().GetByIndex(0)
	assert.Nil(t, err)
	assert.Equal(t, "John Doe", name)
}
