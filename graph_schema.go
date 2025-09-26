package falkordb

import "errors"

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

func (gs *GraphSchema) refresh_labels() error {
	qr, err := gs.graph.CallProcedure("db.labels", nil)
	if err != nil {
		return err
	}

	gs.labels = make([]string, len(qr.results))

	for idx, r := range qr.results {
		label, err := r.GetByIndex(0)
		if err != nil {
			return err
		}

		gs.labels[idx] = label.(string)
	}
	return nil
}

func (gs *GraphSchema) refresh_relationships() error {
	qr, err := gs.graph.CallProcedure("db.relationshipTypes", nil)
	if err != nil {
		return err
	}

	gs.relationships = make([]string, len(qr.results))

	for idx, r := range qr.results {
		relationship, err := r.GetByIndex(0)
		if err != nil {
			return err
		}
		gs.relationships[idx] = relationship.(string)
	}
	return nil
}

func (gs *GraphSchema) refresh_properties() error {
	qr, err := gs.graph.CallProcedure("db.propertyKeys", nil)
	if err != nil {
		return err
	}

	gs.properties = make([]string, len(qr.results))

	for idx, r := range qr.results {
		property, err := r.GetByIndex(0)
		if err != nil {
			return err
		}
		gs.properties[idx] = property.(string)
	}
	return nil
}

func (gs *GraphSchema) getLabel(lblIdx int) (string, error) {
	if lblIdx >= len(gs.labels) {
		err := gs.refresh_labels()
		if err != nil {
			return "", err
		}
		if lblIdx >= len(gs.labels) {
			return "", errors.New("Unknown label index.")

		}
	}

	return gs.labels[lblIdx], nil
}

func (gs *GraphSchema) getRelation(relIdx int) (string, error) {
	if relIdx >= len(gs.relationships) {
		err := gs.refresh_relationships()
		if err != nil {
			return "", err
		}
		if relIdx >= len(gs.relationships) {
			return "", errors.New("Unknown label index.")
		}
	}

	return gs.relationships[relIdx], nil
}

func (gs *GraphSchema) getProperty(propIdx int) (string, error) {
	if propIdx >= len(gs.properties) {
		err := gs.refresh_properties()
		if err != nil {
			return "", err
		}
		if propIdx >= len(gs.properties) {
			return "", errors.New("Unknown property index.")
		}
	}

	return gs.properties[propIdx], nil
}
