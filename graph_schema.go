package falkordb

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// GraphSchema caches the label, relationship-type and property-key tables that
// the compact protocol refers to by index. A *Graph may be shared by concurrent
// goroutines, so the cache is guarded by a mutex.
type GraphSchema struct {
	// mu is a pointer so GraphSchema stays copyable, which GraphSchemaNew
	// relies on when returning by value.
	mu            *sync.RWMutex
	graph         *Graph
	version       int
	labels        []string
	relationships []string
	properties    []string
}

func GraphSchemaNew(graph *Graph) GraphSchema {
	return GraphSchema{
		mu:            &sync.RWMutex{},
		graph:         graph,
		version:       0,
		labels:        []string{},
		relationships: []string{},
		properties:    []string{},
	}
}

func (gs *GraphSchema) clear() {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	gs.labels = []string{}
	gs.relationships = []string{}
	gs.properties = []string{}
}

// fetch runs procedure and returns its single string column. The server round
// trip deliberately happens with no lock held, so a slow refresh never blocks
// readers and the cache cannot deadlock on itself.
func (gs *GraphSchema) fetch(ctx context.Context, procedure string) ([]string, error) {
	qr, err := gs.graph.CallProcedureContext(ctx, procedure, nil)
	if err != nil {
		return nil, err
	}

	values := make([]string, len(qr.results))
	for idx, r := range qr.results {
		value, err := r.GetByIndex(0)
		if err != nil {
			return nil, err
		}
		s, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("%s returned %T, expected a string", procedure, value)
		}
		values[idx] = s
	}
	return values, nil
}

func (gs *GraphSchema) refresh_labels(ctx context.Context) error {
	values, err := gs.fetch(ctx, "db.labels")
	if err != nil {
		return err
	}
	gs.mu.Lock()
	gs.labels = values
	gs.mu.Unlock()
	return nil
}

func (gs *GraphSchema) refresh_relationships(ctx context.Context) error {
	values, err := gs.fetch(ctx, "db.relationshipTypes")
	if err != nil {
		return err
	}
	gs.mu.Lock()
	gs.relationships = values
	gs.mu.Unlock()
	return nil
}

func (gs *GraphSchema) refresh_properties(ctx context.Context) error {
	values, err := gs.fetch(ctx, "db.propertyKeys")
	if err != nil {
		return err
	}
	gs.mu.Lock()
	gs.properties = values
	gs.mu.Unlock()
	return nil
}

func (gs *GraphSchema) cached(table *[]string, idx int) (string, bool) {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	if idx < 0 || idx >= len(*table) {
		return "", false
	}
	return (*table)[idx], true
}

func (gs *GraphSchema) getLabel(ctx context.Context, lblIdx int) (string, error) {
	if label, ok := gs.cached(&gs.labels, lblIdx); ok {
		return label, nil
	}
	if err := gs.refresh_labels(ctx); err != nil {
		return "", err
	}
	if label, ok := gs.cached(&gs.labels, lblIdx); ok {
		return label, nil
	}
	return "", errors.New("Unknown label index.")
}

func (gs *GraphSchema) getRelation(ctx context.Context, relIdx int) (string, error) {
	if relation, ok := gs.cached(&gs.relationships, relIdx); ok {
		return relation, nil
	}
	if err := gs.refresh_relationships(ctx); err != nil {
		return "", err
	}
	if relation, ok := gs.cached(&gs.relationships, relIdx); ok {
		return relation, nil
	}
	return "", errors.New("Unknown relationship index.")
}

func (gs *GraphSchema) getProperty(ctx context.Context, propIdx int) (string, error) {
	if property, ok := gs.cached(&gs.properties, propIdx); ok {
		return property, nil
	}
	if err := gs.refresh_properties(ctx); err != nil {
		return "", err
	}
	if property, ok := gs.cached(&gs.properties, propIdx); ok {
		return property, nil
	}
	return "", errors.New("Unknown property index.")
}
