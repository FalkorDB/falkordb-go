package redisgraph

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/gomodule/redigo/redis"
	"github.com/olekukonko/tablewriter"
)

const (
	LABELS_ADDED            string = "Labels added"
	NODES_CREATED           string = "Nodes created"
	NODES_DELETED           string = "Nodes deleted"
	RELATIONSHIPS_DELETED   string = "Relationships deleted"
	PROPERTIES_SET          string = "Properties set"
	RELATIONSHIPS_CREATED   string = "Relationships created"
	INTERNAL_EXECUTION_TIME string = "internal execution time"
)

type ResultSetColumnTypes int

const (
	COLUMN_UNKNOWN ResultSetColumnTypes = iota
	COLUMN_SCALAR
	COLUMN_NODE
	COLUMN_RELATION
)

type ResultSetScalarTypes int

const (
	PROPERTY_UNKNOWN ResultSetScalarTypes = iota
	PROPERTY_NULL
	PROPERTY_STRING
	PROPERTY_INTEGER
	PROPERTY_BOOLEAN
	PROPERTY_DOUBLE
	PROPERTY_ERROR
)

type QueryResultHeader struct {
	column_names []string
	column_types []ResultSetColumnTypes
}

// QueryResult represents the results of a query.
type QueryResult struct {
	results    [][]interface{}
	statistics map[string]float64
	header     QueryResultHeader
	graph      *Graph
}

func QueryResultNew(g *Graph, response interface{}) (*QueryResult, error) {
	var err error
	qr := &QueryResult{
		results:    nil,
		statistics: nil,
		header: QueryResultHeader{
			column_names: make([]string, 0),
			column_types: make([]ResultSetColumnTypes, 0),
		},
		graph: g,
	}

	r, _ := redis.Values(response, nil)
	if len(r) == 1 {
		qr.parseStatistics(r[0])
	} else {
		err = qr.parseResults(r)
		qr.parseStatistics(r[2])
	}

	return qr, err
}

func (qr *QueryResult) Empty() bool {
	return len(qr.results) == 0
}

func (qr *QueryResult) parseResults(raw_result_set []interface{}) error {
	header := raw_result_set[0]
	qr.parseHeader(header)
	err := qr.parseRecords(raw_result_set)
	return err
}

func (qr *QueryResult) parseStatistics(raw_statistics interface{}) {
	statistics, _ := redis.Strings(raw_statistics, nil)
	qr.statistics = make(map[string]float64)

	for _, rs := range statistics {
		v := strings.Split(rs, ": ")
		f, _ := strconv.ParseFloat(strings.Split(v[1], " ")[0], 64)
		qr.statistics[v[0]] = f
	}
}

func (qr *QueryResult) parseHeader(raw_header interface{}) {
	header, _ := redis.Values(raw_header, nil)

	for _, col := range header {
		c, _ := redis.Values(col, nil)
		ct, _ := redis.Int(c[0], nil)
		cn, _ := redis.String(c[1], nil)

		qr.header.column_types = append(qr.header.column_types, ResultSetColumnTypes(ct))
		qr.header.column_names = append(qr.header.column_names, cn)
	}
}

func (qr *QueryResult) parseRecords(raw_result_set []interface{}) error {
	var err error
	records, _ := redis.Values(raw_result_set[1], nil)
	qr.results = make([][]interface{}, len(records))

	for i, r := range records {
		cells, _ := redis.Values(r, nil)
		record := make([]interface{}, len(cells))

		for idx, c := range cells {
			t := qr.header.column_types[idx]
			switch t {
			case COLUMN_SCALAR:
				s, _ := redis.Values(c, nil)
				record[idx], err = qr.parseScalar(s)
				break
			case COLUMN_NODE:
				record[idx], err = qr.parseNode(c)
				break
			case COLUMN_RELATION:
				record[idx], err = qr.parseEdge(c)
				break
			default:
				panic("Unknown column type.")
			}
			if err != nil {
				return err
			}
		}
		qr.results[i] = record
	}
	return nil
}

func (qr *QueryResult) parseProperties(props []interface{}) (map[string]interface{}, error) {
	// [[name, value type, value] X N]
	properties := make(map[string]interface{})
	for _, prop := range props {
		p, _ := redis.Values(prop, nil)
		idx, _ := redis.Int(p[0], nil)
		prop_name := qr.graph.getProperty(idx)
		prop_value, err := qr.parseScalar(p[1:])
		if err != nil {
			return nil, err
		}
		properties[prop_name] = prop_value
	}

	return properties, nil
}

func (qr *QueryResult) parseNode(cell interface{}) (*Node, error) {
	// Node ID (integer),
	// [label string offset (integer)],
	// [[name, value type, value] X N]

	var label string
	c, _ := redis.Values(cell, nil)
	id, _ := redis.Uint64(c[0], nil)
	labels, _ := redis.Ints(c[1], nil)
	if len(labels) > 0 {
		label = qr.graph.getLabel(labels[0])
	}

	rawProps, _ := redis.Values(c[2], nil)
	properties, err := qr.parseProperties(rawProps)
	if err != nil {
		return nil, err
	}

	n := NodeNew(label, "", properties)
	n.ID = id
	return n, nil
}

func (qr *QueryResult) parseEdge(cell interface{}) (*Edge, error) {
	// Edge ID (integer),
	// reltype string offset (integer),
	// src node ID offset (integer),
	// dest node ID offset (integer),
	// [[name, value, value type] X N]

	c, _ := redis.Values(cell, nil)
	id, _ := redis.Uint64(c[0], nil)
	r, _ := redis.Int(c[1], nil)
	relation := qr.graph.getRelation(r)

	src_node_id, _ := redis.Uint64(c[2], nil)
	dest_node_id, _ := redis.Uint64(c[3], nil)
	rawProps, _ := redis.Values(c[4], nil)
	properties, err := qr.parseProperties(rawProps)
	if err != nil {
		return nil, err
	}
	e := EdgeNew(relation, nil, nil, properties)

	e.ID = id
	e.srcNodeID = src_node_id
	e.destNodeID = dest_node_id
	return e, nil
}

func (qr *QueryResult) parseScalar(cell []interface{}) (interface{}, error) {
	var e error
	v := cell[1]
	t, _ := redis.Int(cell[0], nil)

	var s interface{}
	switch ResultSetScalarTypes(t) {
	case PROPERTY_NULL:
		s = nil

	case PROPERTY_STRING:
		s, _ = redis.String(v, nil)

	case PROPERTY_INTEGER:
		s, _ = redis.Int(v, nil)

	case PROPERTY_BOOLEAN:
		s, _ = redis.Bool(v, nil)

	case PROPERTY_DOUBLE:
		s, _ = redis.Float64(v, nil)

	case PROPERTY_ERROR:
		s = nil
		e = v.(error)

	case PROPERTY_UNKNOWN:
		panic("Unknown scalar type\n")
	}

	return s, e
}

// PrettyPrint prints the QueryResult to stdout, pretty-like.
func (qr *QueryResult) PrettyPrint() {
	if qr.Empty() {
		return
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetAutoFormatHeaders(false)
	table.SetHeader(qr.header.column_names)

	if len(qr.results) > 0 {
		// Convert to [][]string.
		results := make([][]string, len(qr.results))
		for i, record := range qr.results {
			results[i] = make([]string, len(record))
			for j, elem := range record {
				results[i][j] = fmt.Sprint(elem)
			}
		}
		table.AppendBulk(results)
	} else {
		table.Append([]string{"No data returned."})
	}
	table.Render()

	for k, v := range qr.statistics {
		fmt.Fprintf(os.Stdout, "\n%s %f", k, v)
	}

	fmt.Fprintf(os.Stdout, "\n")
}

func (qr *QueryResult) getStat(stat string) int {
	if val, ok := qr.statistics[stat]; ok {
		return int(val)
	} else {
		return 0
	}
}

func (qr *QueryResult) LabelsAdded() int {
	return qr.getStat(LABELS_ADDED)
}

func (qr *QueryResult) NodesCreated() int {
	return qr.getStat(NODES_CREATED)
}

func (qr *QueryResult) NodesDeleted() int {
	return qr.getStat(NODES_DELETED)
}

func (qr *QueryResult) PropertiesSet() int {
	return qr.getStat(PROPERTIES_SET)
}

func (qr *QueryResult) RelationshipsCreated() int {
	return qr.getStat(RELATIONSHIPS_CREATED)
}

func (qr *QueryResult) RelationshipsDeleted() int {
	return qr.getStat(RELATIONSHIPS_DELETED)
}

func (qr *QueryResult) RunTime() int {
	return qr.getStat(INTERNAL_EXECUTION_TIME)
}
