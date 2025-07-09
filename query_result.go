package falkordb

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
)

const (
	LABELS_ADDED            string = "Labels added"
	NODES_CREATED           string = "Nodes created"
	NODES_DELETED           string = "Nodes deleted"
	RELATIONSHIPS_DELETED   string = "Relationships deleted"
	PROPERTIES_SET          string = "Properties set"
	RELATIONSHIPS_CREATED   string = "Relationships created"
	INDICES_CREATED         string = "Indices created"
	INDICES_DELETED         string = "Indices deleted"
	INTERNAL_EXECUTION_TIME string = "Query internal execution time"
	CACHED_EXECUTION        string = "Cached execution"
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
	VALUE_UNKNOWN ResultSetScalarTypes = iota
	VALUE_NULL
	VALUE_STRING
	VALUE_INTEGER
	VALUE_BOOLEAN
	VALUE_DOUBLE
	VALUE_ARRAY
	VALUE_EDGE
	VALUE_NODE
	VALUE_PATH
	VALUE_MAP
	VALUE_POINT
	VALUE_VECTORF32
	VALUE_DATETIME // Deprecated, use VALUE_POINT instead
	VALUE_DATE
	VALUE_TIME
)

type QueryResultHeader struct {
	column_names []string
	column_types []ResultSetColumnTypes
}

// QueryResult represents the results of a query.
type QueryResult struct {
	graph            *Graph
	header           QueryResultHeader
	results          []*Record
	statistics       map[string]float64
	currentRecordIdx int
}

func QueryResultNew(g *Graph, response interface{}) (*QueryResult, error) {
	qr := &QueryResult{
		results:    nil,
		statistics: nil,
		header: QueryResultHeader{
			column_names: make([]string, 0),
			column_types: make([]ResultSetColumnTypes, 0),
		},
		graph:            g,
		currentRecordIdx: -1,
	}

	r := response.([]interface{})

	if len(r) == 1 {
		qr.parseStatistics(r[0])
	} else {
		qr.parseResults(r)
		qr.parseStatistics(r[2])
	}

	return qr, nil
}

func (qr *QueryResult) Empty() bool {
	return len(qr.results) == 0
}

func (qr *QueryResult) parseResults(raw_result_set []interface{}) {
	header := raw_result_set[0]
	qr.parseHeader(header)
	qr.parseRecords(raw_result_set)
}

func (qr *QueryResult) parseStatistics(raw_statistics interface{}) {
	statistics := raw_statistics.([]interface{})
	qr.statistics = make(map[string]float64)

	for _, rs := range statistics {
		v := strings.Split(rs.(string), ": ")
		f, _ := strconv.ParseFloat(strings.Split(v[1], " ")[0], 64)
		qr.statistics[v[0]] = f
	}
}

func (qr *QueryResult) parseHeader(raw_header interface{}) {
	header := raw_header.([]interface{})

	for _, col := range header {
		c := col.([]interface{})
		ct := c[0].(int64)
		cn := c[1].(string)

		qr.header.column_types = append(qr.header.column_types, ResultSetColumnTypes(ct))
		qr.header.column_names = append(qr.header.column_names, cn)
	}
}

func (qr *QueryResult) parseRecords(raw_result_set []interface{}) error {
	records := raw_result_set[1].([]interface{})
	qr.results = make([]*Record, len(records))

	for i, r := range records {
		cells := r.([]interface{})
		values := make([]interface{}, len(cells))

		for idx, c := range cells {
			t := qr.header.column_types[idx]
			switch t {
			case COLUMN_SCALAR:
				s, err := qr.parseScalar(c.([]interface{}))
				if err != nil {
					return err
				}
				values[idx] = s
			case COLUMN_NODE:
				v, err := qr.parseNode(c)
				if err != nil {
					return err
				}
				values[idx] = v
			case COLUMN_RELATION:
				v, err := qr.parseEdge(c)
				if err != nil {
					return err
				}
				values[idx] = v
			default:
				return errors.New("unknown column type")
			}
		}
		qr.results[i] = recordNew(values, qr.header.column_names)
	}
	return nil
}

func (qr *QueryResult) parseProperties(props []interface{}) (map[string]interface{}, error) {
	// [[name, value type, value] X N]
	properties := make(map[string]interface{})
	for _, prop := range props {
		p := prop.([]interface{})
		idx := p[0].(int64)
		prop_name, err := qr.graph.schema.getProperty(int(idx))
		if err != nil {
			return nil, err
		}
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

	c := cell.([]interface{})
	id := c[0].(int64)
	labelIds := c[1].([]interface{})
	labels := make([]string, len(labelIds))
	for i := 0; i < len(labelIds); i++ {
		label, err := qr.graph.schema.getLabel(int(labelIds[i].(int64)))
		if err != nil {
			return nil, err
		}
		labels[i] = label
	}

	rawProps := c[2].([]interface{})
	properties, err := qr.parseProperties(rawProps)
	if err != nil {
		return nil, err
	}

	n := NodeNew(labels, "", properties)
	n.ID = uint64(id)
	return n, nil
}

func (qr *QueryResult) parseEdge(cell interface{}) (*Edge, error) {
	// Edge ID (integer),
	// reltype string offset (integer),
	// src node ID offset (integer),
	// dest node ID offset (integer),
	// [[name, value, value type] X N]

	c := cell.([]interface{})
	id := c[0].(int64)
	r := c[1].(int64)
	relation, err := qr.graph.schema.getRelation(int(r))
	if err != nil {
		return nil, err
	}

	src_node_id := c[2].(int64)
	dest_node_id := c[3].(int64)
	rawProps := c[4].([]interface{})
	properties, err := qr.parseProperties(rawProps)
	if err != nil {
		return nil, err
	}
	e := EdgeNew(relation, nil, nil, properties)

	e.ID = uint64(id)
	e.srcNodeID = uint64(src_node_id)
	e.destNodeID = uint64(dest_node_id)
	return e, nil
}

func (qr *QueryResult) parseArray(cell interface{}) ([]interface{}, error) {
	var array = cell.([]interface{})
	var arrayLength = len(array)
	for i := 0; i < arrayLength; i++ {
		s, err := qr.parseScalar(array[i].([]interface{}))
		if err != nil {
			return nil, err
		}
		array[i] = s
	}
	return array, nil
}

func (qr *QueryResult) parsePath(cell interface{}) (Path, error) {
	arrays := cell.([]interface{})
	nodes, err := qr.parseScalar(arrays[0].([]interface{}))
	if err != nil {
		return Path{}, err
	}
	edges, err := qr.parseScalar(arrays[1].([]interface{}))
	if err != nil {
		return Path{}, err
	}
	return PathNew(nodes.([]interface{}), edges.([]interface{})), nil
}

func (qr *QueryResult) parseMap(cell interface{}) (map[string]interface{}, error) {
	var raw_map = cell.([]interface{})
	var mapLength = len(raw_map)
	var parsed_map = make(map[string]interface{})

	for i := 0; i < mapLength; i += 2 {
		key := raw_map[i].(string)
		s, err := qr.parseScalar(raw_map[i+1].([]interface{}))
		if err != nil {
			return nil, err
		}
		parsed_map[key] = s
	}

	return parsed_map, nil
}

func (qr *QueryResult) parsePoint(cell interface{}) (map[string]interface{}, error) {
	var parsed_point = make(map[string]interface{})
	var array = cell.([]interface{})
	lat, _ := strconv.ParseFloat(array[0].(string), 64)
	parsed_point["latitude"] = lat
	lon, _ := strconv.ParseFloat(array[1].(string), 64)
	parsed_point["longitude"] = lon
	return parsed_point, nil
}

func (qr *QueryResult) parseVectorF32(cell interface{}) ([]float32, error) {
	var array = cell.([]interface{})
	var arrayLength = len(array)
	var res = make([]float32, arrayLength)
	for i := 0; i < arrayLength; i++ {
		res[i] = float32(array[i].(float64))
	}
	return res, nil
}

func (qr *QueryResult) parseScalar(cell []interface{}) (interface{}, error) {
	t := cell[0].(int64)
	v := cell[1]
	switch ResultSetScalarTypes(t) {
	case VALUE_NULL:
		return nil, nil

	case VALUE_STRING:
		return v.(string), nil

	case VALUE_INTEGER:
		return v.(int64), nil

	case VALUE_BOOLEAN:
		return v.(string) == "true", nil

	case VALUE_DOUBLE:
		return strconv.ParseFloat(v.(string), 64)

	case VALUE_ARRAY:
		return qr.parseArray(v)

	case VALUE_EDGE:
		return qr.parseEdge(v)

	case VALUE_NODE:
		return qr.parseNode(v)

	case VALUE_PATH:
		return qr.parsePath(v)

	case VALUE_MAP:
		return qr.parseMap(v)

	case VALUE_POINT:
		return qr.parsePoint(v)

	case VALUE_VECTORF32:
		return qr.parseVectorF32(v)

	case VALUE_DATETIME:
		return time.Unix(v.(int64), 0), nil

	case VALUE_DATE:
		return time.Unix(v.(int64), 0), nil

	case VALUE_TIME:
		return time.UnixMilli(v.(int64)), nil

	case VALUE_UNKNOWN:
		return nil, errors.New("unknown scalar type")
	}

	return nil, errors.New("unknown scalar type")
}

func (qr *QueryResult) getStat(stat string) float64 {
	if val, ok := qr.statistics[stat]; ok {
		return val
	} else {
		return 0.0
	}
}

// Next returns true only if there is a record to be processed.
func (qr *QueryResult) Next() bool {
	if qr.Empty() {
		return false
	}
	if qr.currentRecordIdx < len(qr.results)-1 {
		qr.currentRecordIdx++
		return true
	} else {
		return false
	}
}

// Record returns the current record.
func (qr *QueryResult) Record() *Record {
	if qr.currentRecordIdx >= 0 && qr.currentRecordIdx < len(qr.results) {
		return qr.results[qr.currentRecordIdx]
	} else {
		return nil
	}
}

// PrettyPrint prints the QueryResult to stdout, pretty-like.
func (qr *QueryResult) PrettyPrint() {
	if qr.Empty() {
		return
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetAutoFormatHeaders(false)
	table.SetHeader(qr.header.column_names)
	row_count := len(qr.results)
	col_count := len(qr.header.column_names)
	if len(qr.results) > 0 {
		// Convert to [][]string.
		results := make([][]string, row_count)
		for i, record := range qr.results {
			results[i] = make([]string, col_count)
			for j, elem := range record.Values() {
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

func (qr *QueryResult) LabelsAdded() int {
	return int(qr.getStat(LABELS_ADDED))
}

func (qr *QueryResult) NodesCreated() int {
	return int(qr.getStat(NODES_CREATED))
}

func (qr *QueryResult) NodesDeleted() int {
	return int(qr.getStat(NODES_DELETED))
}

func (qr *QueryResult) PropertiesSet() int {
	return int(qr.getStat(PROPERTIES_SET))
}

func (qr *QueryResult) RelationshipsCreated() int {
	return int(qr.getStat(RELATIONSHIPS_CREATED))
}

func (qr *QueryResult) RelationshipsDeleted() int {
	return int(qr.getStat(RELATIONSHIPS_DELETED))
}

func (qr *QueryResult) IndicesCreated() int {
	return int(qr.getStat(INDICES_CREATED))
}

func (qr *QueryResult) IndicesDeleted() int {
	return int(qr.getStat(INDICES_DELETED))
}

// Returns the query internal execution time in milliseconds
func (qr *QueryResult) InternalExecutionTime() float64 {
	return qr.getStat(INTERNAL_EXECUTION_TIME)
}

func (qr *QueryResult) CachedExecution() int {
	return int(qr.getStat(CACHED_EXECUTION))
}
