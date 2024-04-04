package falkordb

type GraphSchema struct {
	graph         *Graph
	version       int
	labels        []string
	relationships []string
	properties    []string
}

func GraphSchemaNew(graph *Graph) GraphSchema {
	return GraphSchema{
		graph:         graph,
		version:       0,
		labels:        []string{},
		relationships: []string{},
		properties:    []string{},
	}
}

func (gs *GraphSchema) clear() {
	gs.labels = []string{}
	gs.relationships = []string{}
	gs.properties = []string{}
}

func (gs *GraphSchema) refresh_labels() {
	qr, _ := gs.graph.CallProcedure("db.labels", nil)

	gs.labels = make([]string, len(qr.results))

	for idx, r := range qr.results {
		gs.labels[idx] = r.GetByIndex(0).(string)
	}
}

func (gs *GraphSchema) refresh_relationships() {
	qr, _ := gs.graph.CallProcedure("db.relationshipTypes", nil)

	gs.relationships = make([]string, len(qr.results))

	for idx, r := range qr.results {
		gs.relationships[idx] = r.GetByIndex(0).(string)
	}
}

func (gs *GraphSchema) refresh_properties() {
	qr, _ := gs.graph.CallProcedure("db.propertyKeys", nil)

	gs.properties = make([]string, len(qr.results))

	for idx, r := range qr.results {
		gs.properties[idx] = r.GetByIndex(0).(string)
	}
}

func (gs *GraphSchema) getLabel(lblIdx int) string {
	if lblIdx >= len(gs.labels) {
		gs.refresh_labels()
		// Retry.
		if lblIdx >= len(gs.labels) {
			// Error!
			panic("Unknown label index.")
		}
	}

	return gs.labels[lblIdx]
}

func (gs *GraphSchema) getRelation(relIdx int) string {
	if relIdx >= len(gs.relationships) {
		gs.refresh_relationships()
		// Retry.
		if relIdx >= len(gs.relationships) {
			// Error!
			panic("Unknown relation type index.")
		}
	}

	return gs.relationships[relIdx]
}

func (gs *GraphSchema) getProperty(propIdx int) string {
	if propIdx >= len(gs.properties) {
		gs.refresh_properties()

		// Retry.
		if propIdx >= len(gs.properties) {
			// Error!
			panic("Unknown property index.")
		}
	}

	return gs.properties[propIdx]
}
