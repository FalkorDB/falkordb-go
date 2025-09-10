package falkordb

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var graph *Graph

func createGraph() {
	db, _ := FromURL("falkor://0.0.0.0:6379")
	graph = db.SelectGraph("social")
	graph.Delete()

	_, err := graph.Query("CREATE (:Person {name: 'John Doe', age: 33, gender: 'male', status: 'single'})-[:Visited {year: 2017}]->(c:Country {name: 'Japan', population: 126800000})", nil, nil)

	if err != nil {
		panic(err)
	}
}

func setup() {
	createGraph()
}

func shutdown() {
	graph.Conn.Close()
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	shutdown()
	os.Exit(code)
}

func TestMatchQuery(t *testing.T) {
	q := "MATCH (s)-[e]->(d) RETURN s,e,d"
	res, err := graph.Query(q, nil, nil)
	if err != nil {
		t.Error(err)
	}

	checkQueryResults(t, res)
}

func TestMatchROQuery(t *testing.T) {
	q := "MATCH (s)-[e]->(d) RETURN s,e,d"
	res, err := graph.ROQuery(q, nil, nil)
	if err != nil {
		t.Error(err)
	}

	checkQueryResults(t, res)
}

func checkQueryResults(t *testing.T, res *QueryResult) {
	assert.Equal(t, len(res.results), 1, "expecting 1 result record")

	res.Next()
	r := res.Record()

	s, ok := r.GetByIndex(0).(*Node)
	assert.True(t, ok, "First column should contain nodes.")
	e, ok := r.GetByIndex(1).(*Edge)
	assert.True(t, ok, "Second column should contain edges.")
	d, ok := r.GetByIndex(2).(*Node)
	assert.True(t, ok, "Third column should contain nodes.")

	assert.Equal(t, s.Labels[0], "Person", "Node should be of type 'Person'")
	assert.Equal(t, e.Relation, "Visited", "Edge should be of relation type 'Visited'")
	assert.Equal(t, d.Labels[0], "Country", "Node should be of type 'Country'")

	assert.Equal(t, len(s.Properties), 4, "Person node should have 4 properties")

	assert.Equal(t, s.GetProperty("name"), "John Doe", "Unexpected property value.")
	assert.Equal(t, s.GetProperty("age"), int64(33), "Unexpected property value.")
	assert.Equal(t, s.GetProperty("gender"), "male", "Unexpected property value.")
	assert.Equal(t, s.GetProperty("status"), "single", "Unexpected property value.")

	assert.Equal(t, e.GetProperty("year"), int64(2017), "Unexpected property value.")

	assert.Equal(t, d.GetProperty("name"), "Japan", "Unexpected property value.")
	assert.Equal(t, d.GetProperty("population"), int64(126800000), "Unexpected property value.")
}

func TestCreateQuery(t *testing.T) {
	q := "CREATE (w:WorkPlace {name:'FalkorDB'})"
	res, err := graph.Query(q, nil, nil)
	if err != nil {
		t.Error(err)
	}

	assert.True(t, res.Empty(), "Expecting empty result-set")

	// Validate statistics.
	assert.Equal(t, res.NodesCreated(), 1, "Expecting a single node to be created.")
	assert.Equal(t, res.PropertiesSet(), 1, "Expecting a songle property to be added.")

	q = "MATCH (w:WorkPlace) RETURN w"
	res, err = graph.Query(q, nil, nil)
	if err != nil {
		t.Error(err)
	}

	assert.False(t, res.Empty(), "Expecting resultset to include a single node.")
	res.Next()
	r := res.Record()
	w := r.GetByIndex(0).(*Node)
	assert.Equal(t, w.Labels[0], "WorkPlace", "Unexpected node label.")
}

func TestCreateROQueryFailure(t *testing.T) {
	q := "CREATE (w:WorkPlace {name:'FalkorDB'})"
	_, err := graph.ROQuery(q, nil, nil)
	assert.NotNil(t, err, "error should not be nil")
}

func TestErrorReporting(t *testing.T) {
	q := "RETURN toupper(5)"
	res, err := graph.Query(q, nil, nil)
	assert.Nil(t, res)
	assert.NotNil(t, err)

	q = "MATCH (p:Person) RETURN toupper(p.age)"
	res, err = graph.Query(q, nil, nil)
	assert.Nil(t, res)
	assert.NotNil(t, err)
}

func TestArray(t *testing.T) {
	graph.Query("MATCH (n) DELETE n", nil, nil)

	q := "CREATE (:person{name:'a',age:32,array:[0,1,2]})"
	res, err := graph.Query(q, nil, nil)
	if err != nil {
		t.Error(err)
	}

	q = "CREATE (:person{name:'b',age:30,array:[3,4,5]})"
	res, err = graph.Query(q, nil, nil)
	if err != nil {
		t.Error(err)
	}

	q = "WITH [0,1,2] as x return x"
	res, err = graph.Query(q, nil, nil)
	if err != nil {
		t.Error(err)
	}

	res.Next()
	r := res.Record()
	assert.Equal(t, len(res.results), 1, "expecting 1 result record")
	assert.Equal(t, []interface{}{int64(0), int64(1), int64(2)}, r.GetByIndex(0))

	q = "unwind([0,1,2]) as x return x"
	res, err = graph.Query(q, nil, nil)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, len(res.results), 3, "expecting 3 result record")

	i := 0
	for res.Next() {
		r = res.Record()
		assert.Equal(t, int64(i), r.GetByIndex(0))
		i++
	}

	q = "MATCH(n) return collect(n) as x"
	res, err = graph.Query(q, nil, nil)
	if err != nil {
		t.Error(err)
	}

	a := NodeNew([]string{"person"}, "", nil)
	b := NodeNew([]string{"person"}, "", nil)

	a.SetProperty("name", "a")
	a.SetProperty("age", int64(32))
	a.SetProperty("array", []interface{}{int64(0), int64(1), int64(2)})

	b.SetProperty("name", "b")
	b.SetProperty("age", int64(30))
	b.SetProperty("array", []interface{}{int64(3), int64(4), int64(5)})

	assert.Equal(t, 1, len(res.results), "expecting 1 results record")

	res.Next()
	r = res.Record()
	arr := r.GetByIndex(0).([]interface{})

	assert.Equal(t, 2, len(arr))

	resA := arr[0].(*Node)
	resB := arr[1].(*Node)
	// the order of values in the array returned by collect operation is not defined
	// check for the node that contains the name "a" and set it to be resA
	if resA.GetProperty("name") != "a" {
		resA = arr[1].(*Node)
		resB = arr[0].(*Node)
	}

	assert.Equal(t, a.GetProperty("name"), resA.GetProperty("name"), "Unexpected property value.")
	assert.Equal(t, a.GetProperty("age"), resA.GetProperty("age"), "Unexpected property value.")
	assert.Equal(t, a.GetProperty("array"), resA.GetProperty("array"), "Unexpected property value.")

	assert.Equal(t, b.GetProperty("name"), resB.GetProperty("name"), "Unexpected property value.")
	assert.Equal(t, b.GetProperty("age"), resB.GetProperty("age"), "Unexpected property value.")
	assert.Equal(t, b.GetProperty("array"), resB.GetProperty("array"), "Unexpected property value.")
}

func TestMap(t *testing.T) {
	createGraph()

	q := "RETURN {val_1: 5, val_2: 'str', inner: {x: [1]}}"
	res, err := graph.Query(q, nil, nil)
	if err != nil {
		t.Error(err)
	}
	res.Next()
	r := res.Record()
	mapval := r.GetByIndex(0).(map[string]interface{})

	inner_map := map[string]interface{}{"x": []interface{}{int64(1)}}
	expected := map[string]interface{}{"val_1": int64(5), "val_2": "str", "inner": inner_map}
	assert.Equal(t, mapval, expected, "expecting a map literal")

	q = "MATCH (a:Country) RETURN a { .name }"
	res, err = graph.Query(q, nil, nil)
	if err != nil {
		t.Error(err)
	}
	res.Next()
	r = res.Record()
	mapval = r.GetByIndex(0).(map[string]interface{})

	expected = map[string]interface{}{"name": "Japan"}
	assert.Equal(t, mapval, expected, "expecting a map projection")
}

func TestPath(t *testing.T) {
	createGraph()
	q := "MATCH p = (:Person)-[:Visited]->(:Country) RETURN p"
	res, err := graph.Query(q, nil, nil)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, len(res.results), 1, "expecting 1 result record")

	res.Next()
	r := res.Record()

	p, ok := r.GetByIndex(0).(Path)
	assert.True(t, ok, "First column should contain path.")

	assert.Equal(t, 2, p.NodesCount(), "Path should contain two nodes")
	assert.Equal(t, 1, p.EdgeCount(), "Path should contain one edge")

	s := p.FirstNode()
	e := p.GetEdge(0)
	d := p.LastNode()

	assert.Equal(t, s.Labels[0], "Person", "Node should be of type 'Person'")
	assert.Equal(t, e.Relation, "Visited", "Edge should be of relation type 'Visited'")
	assert.Equal(t, d.Labels[0], "Country", "Node should be of type 'Country'")

	assert.Equal(t, len(s.Properties), 4, "Person node should have 4 properties")

	assert.Equal(t, s.GetProperty("name"), "John Doe", "Unexpected property value.")
	assert.Equal(t, s.GetProperty("age"), int64(33), "Unexpected property value.")
	assert.Equal(t, s.GetProperty("gender"), "male", "Unexpected property value.")
	assert.Equal(t, s.GetProperty("status"), "single", "Unexpected property value.")

	assert.Equal(t, e.GetProperty("year"), int64(2017), "Unexpected property value.")

	assert.Equal(t, d.GetProperty("name"), "Japan", "Unexpected property value.")
	assert.Equal(t, d.GetProperty("population"), int64(126800000), "Unexpected property value.")

}

func TestPoint(t *testing.T) {
	q := "RETURN point({latitude: 37.0, longitude: -122.0})"
	res, err := graph.Query(q, nil, nil)
	if err != nil {
		t.Error(err)
	}
	res.Next()
	r := res.Record()
	point := r.GetByIndex(0).(map[string]interface{})
	assert.Equal(t, point["latitude"], 37.0, "Unexpected latitude value")
	assert.Equal(t, point["longitude"], -122.0, "Unexpected longitude value")
}

func TestVectorF32(t *testing.T) {
	q := "RETURN vecf32([1.0, 2.0, 3.0])"
	res, err := graph.Query(q, nil, nil)
	if err != nil {
		t.Error(err)
	}
	res.Next()
	r := res.Record()
	vec := r.GetByIndex(0).([]float32)
	assert.Equal(t, vec, []float32{1.0, 2.0, 3.0}, "Unexpected vector value")
}
func TestGetTime(t *testing.T) {
	q := "RETURN localtime({hour: 12}) AS time"
	res, err := graph.Query(q, nil, nil)
	if err != nil {
		t.Error(err)
	}
	res.Next()
	r := res.Record()
	timeValue := r.GetByIndex(0).(time.Time)
	assert.Equal(t, timeValue.Hour(), 12, "Unexpected Time value")
}

func TestGetDate(t *testing.T) {
	q := "RETURN date({year: 1984, month: 1, day: 1}) as date"
	res, err := graph.Query(q, nil, nil)
	if err != nil {
		t.Error(err)
	}
	res.Next()
	r := res.Record()
	dateValue := r.GetByIndex(0).(time.Time)
	assert.Equal(t, dateValue.Year(), 1984, "Unexpected Date value")
}

func TestGetDateTime(t *testing.T) {
	q := "RETURN localdatetime({year : 1984}) as date"
	res, err := graph.Query(q, nil, nil)
	if err != nil {
		t.Error(err)
	}
	res.Next()
	r := res.Record()
	dateTimeValue := r.GetByIndex(0).(time.Time)
	assert.Equal(t, dateTimeValue.Year(), 1984, "Unexpected DateTime value")
}

func TestGetDuration(t *testing.T) {
	q := "RETURN duration({hours: 2, minutes: 30}) AS duration"
	res, err := graph.Query(q, nil, nil)
	if err != nil {
		t.Error(err)
	}
	res.Next()
	r := res.Record()
	durationValue := r.GetByIndex(0).(time.Duration)
	expectedDuration := 2*time.Hour + 30*time.Minute
	assert.Equal(t, durationValue, expectedDuration, "Unexpected Duration value")
}

func TestParameterizedQuery(t *testing.T) {
	createGraph()
	params := []interface{}{int64(1), 2.3, "str", true, false, nil, []interface{}{int64(0), int64(1), int64(2)}, []interface{}{"0", "1", "2"}}
	q := "RETURN $param"
	params_map := make(map[string]interface{})
	for index, param := range params {
		params_map["param"] = param
		res, err := graph.Query(q, params_map, nil)
		if err != nil {
			t.Error(err)
		}
		res.Next()
		assert.Equal(t, res.Record().GetByIndex(0), params[index], "Unexpected parameter value")
	}
}

func TestCreateIndex(t *testing.T) {
	res, err := graph.Query("CREATE INDEX FOR (u:user) ON (u.name)", nil, nil)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, 1, res.IndicesCreated(), "Expecting 1 index created")

	_, err = graph.Query("CREATE INDEX FOR (u:user) ON (u.name)", nil, nil)
	if err == nil {
		t.Error("expecting error")
	}

	res, err = graph.Query("DROP INDEX FOR (u:user) ON (u.name)", nil, nil)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, 1, res.IndicesDeleted(), "Expecting 1 index deleted")

	_, err = graph.Query("DROP INDEX FOR (u:user) ON (u.name)", nil, nil)
	assert.Equal(t, err.Error(), "ERR Unable to drop index on :user(name): no such index.")
}

func TestQueryStatistics(t *testing.T) {
	err := graph.Delete()
	assert.Nil(t, err)

	q := "CREATE (:Person{name:'a',age:32,array:[0,1,2]})"
	res, err := graph.Query(q, nil, nil)
	assert.Nil(t, err)

	assert.Equal(t, 1, res.NodesCreated(), "Expecting 1 node created")
	assert.Equal(t, 0, res.NodesDeleted(), "Expecting 0 nodes deleted")
	assert.Greater(t, res.InternalExecutionTime(), 0.0, "Expecting internal execution time not to be 0.0")
	assert.Equal(t, true, res.Empty(), "Expecting empty resultset")

	res, err = graph.Query("MATCH (n) DELETE n", nil, nil)
	assert.Nil(t, err)
	assert.Equal(t, 1, res.NodesDeleted(), "Expecting 1 nodes deleted")

	res, err = graph.Query("CREATE (:Person {name: 'John Doe', age: 33, gender: 'male', status: 'single'})-[:Visited {year: 2017}]->(c:Country {name: 'Japan', population: 126800000})", nil, nil)

	assert.Nil(t, err)
	assert.Equal(t, 2, res.NodesCreated(), "Expecting 2 node created")
	assert.Equal(t, 0, res.NodesDeleted(), "Expecting 0 nodes deleted")
	assert.Equal(t, 7, res.PropertiesSet(), "Expecting 7 properties set")
	assert.Equal(t, 1, res.RelationshipsCreated(), "Expecting 1 relationships created")
	assert.Equal(t, 0, res.RelationshipsDeleted(), "Expecting 0 relationships deleted")
	assert.Greater(t, res.InternalExecutionTime(), 0.0, "Expecting internal execution time not to be 0.0")
	assert.Equal(t, true, res.Empty(), "Expecting empty resultset")
	q = "MATCH p = (:Person)-[:Visited]->(:Country) RETURN p"
	res, err = graph.Query(q, nil, nil)
	assert.Nil(t, err)
	assert.Equal(t, len(res.results), 1, "expecting 1 result record")
	assert.Equal(t, false, res.Empty(), "Expecting resultset to have records")
	res, err = graph.Query("MATCH ()-[r]-() DELETE r", nil, nil)
	assert.Nil(t, err)
	assert.Equal(t, 1, res.RelationshipsDeleted(), "Expecting 1 relationships deleted")
}

func TestUtils(t *testing.T) {
	res := RandomString(10)
	assert.Equal(t, len(res), 10)

	res = ToString("test_string")
	assert.Equal(t, res, "\"test_string\"")

	res = ToString(10)
	assert.Equal(t, res, "10")

	res = ToString(1.2)
	assert.Equal(t, res, "1.2")

	res = ToString(true)
	assert.Equal(t, res, "true")

	var arr = []interface{}{1, 2, 3, "boom"}
	res = ToString(arr)
	assert.Equal(t, res, "[1,2,3,\"boom\"]")

	jsonMap := make(map[string]interface{})
	jsonMap["object"] = map[string]interface{}{"foo": 1}
	res = ToString(jsonMap)
	assert.Equal(t, res, "{object: {foo: 1}}")
}

func TestMultiLabelNode(t *testing.T) {
	// clear database
	err := graph.Delete()
	assert.Nil(t, err)

	// create a multi label node
	_, err = graph.Query("CREATE (:A:B)", nil, nil)
	assert.Nil(t, err)

	// fetch node
	res, err := graph.Query("MATCH (n) RETURN n", nil, nil)
	assert.Nil(t, err)

	res.Next()
	r := res.Record()
	n := r.GetByIndex(0).(*Node)

	// expecting 2 labels
	assert.Equal(t, len(n.Labels), 2, "expecting 2 labels")
	assert.Equal(t, n.Labels[0], "A")
	assert.Equal(t, n.Labels[1], "B")
}

func TestNodeMapDatatype(t *testing.T) {
	err := graph.Delete()
	assert.Nil(t, err)

	// Create 2 nodes connect via a single edge.
	res, err := graph.Query("CREATE (:Person {name: 'John Doe', age: 33, gender: 'male', status: 'single'})-[:Visited {year: 2017}]->(c:Country {name: 'Japan', population: 126800000, states: ['Kanto', 'Chugoku']})", nil, nil)

	assert.Nil(t, err)
	assert.Equal(t, 2, res.NodesCreated(), "Expecting 2 node created")
	assert.Equal(t, 0, res.NodesDeleted(), "Expecting 0 nodes deleted")
	assert.Equal(t, 8, res.PropertiesSet(), "Expecting 8 properties set")
	assert.Equal(t, 1, res.RelationshipsCreated(), "Expecting 1 relationships created")
	assert.Equal(t, 0, res.RelationshipsDeleted(), "Expecting 0 relationships deleted")
	assert.Greater(t, res.InternalExecutionTime(), 0.0, "Expecting internal execution time not to be 0.0")
	assert.Equal(t, true, res.Empty(), "Expecting empty resultset")
	res, err = graph.Query("MATCH p = (:Person)-[:Visited]->(:Country) RETURN p", nil, nil)
	assert.Nil(t, err)
	assert.Equal(t, len(res.results), 1, "expecting 1 result record")
	assert.Equal(t, false, res.Empty(), "Expecting resultset to have records")
	res, err = graph.Query("MATCH ()-[r]-() DELETE r", nil, nil)
	assert.Nil(t, err)
	assert.Equal(t, 1, res.RelationshipsDeleted(), "Expecting 1 relationships deleted")
}

func TestTimeout(t *testing.T) {
	// Instantiate a new QueryOptions struct with a 1-second timeout
	options := NewQueryOptions().SetTimeout(1)

	// Verify that the timeout was set properly
	assert.Equal(t, 1, options.GetTimeout())

	// Issue a long-running query with a 1-millisecond timeout.
	res, err := graph.Query("UNWIND range(0, 1000000) AS v WITH v WHERE v % 2 = 1 RETURN COUNT(v)", nil, options)
	assert.Nil(t, res)
	assert.NotNil(t, err)

	params := make(map[string]interface{})
	params["ub"] = 1000000
	res, err = graph.Query("UNWIND range(0, $ub) AS v RETURN v", params, options)
	assert.Nil(t, res)
	assert.NotNil(t, err)
}
