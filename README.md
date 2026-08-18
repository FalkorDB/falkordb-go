[![license](https://img.shields.io/github/license/FalkorDB/falkordb-go.svg)](https://github.com/FalkorDB/falkordb-go)
[![GitHub issues](https://img.shields.io/github/release/FalkorDB/falkordb-go.svg)](https://github.com/FalkorDB/falkordb-go/releases/latest)
[![Codecov](https://codecov.io/gh/FalkorDB/falkordb-go/branch/master/graph/badge.svg)](https://codecov.io/gh/FalkorDB/falkordb-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/FalkorDB/falkordb-go)](https://goreportcard.com/report/github.com/FalkorDB/falkordb-go)
[![GoDoc](https://godoc.org/github.com/FalkorDB/falkordb-go?status.svg)](https://godoc.org/github.com/FalkorDB/falkordb-go/v2)

# falkordb-go
[![Discord](https://img.shields.io/discord/1146782921294884966?style=flat-square)](https://discord.gg/6M4QwDXn2w)

`falkordb-go` is a Golang client for the [FalkorDB](https://falkordb.com) database.

## Installation


Make sure to initialize a Go module:

```
go mod init github.com/my/repo
```

Simply do:

```sh
$ go get github.com/FalkorDB/falkordb-go/v2
```

## Usage

The complete `falkordb-go` API is documented on [GoDoc](https://godoc.org/github.com/falkordb/falkordb-go).

```go
package main

import (
	"fmt"
	"log"

	"github.com/FalkorDB/falkordb-go/v2"
)

func main() {
	db, _ := falkordb.FalkorDBNew(&falkordb.ConnectionOption{Addr: "0.0.0.0:6379"})
	// db, _ := falkordb.FalkorDBNewCluster(&falkordb.ConnectionClusterOption{Addrs: []string{"0.0.0.0:6379"}})

	graph := db.SelectGraph("social")

	query := "CREATE (:Person {name: 'John Doe', age: 33, gender: 'male', status: 'single'})-[:VISITED]->(:Country {name: 'Japan'})"
	_, err := graph.Query(query, nil, nil)
	if err != nil {
		log.Fatal(err)
	}

	query = "MATCH (p:Person)-[v:VISITED]->(c:Country) RETURN p.name, p.age, c.name"
	// result is a QueryResult struct containing the query's generated records and statistics.
	result, err := graph.Query(query, nil, nil)
	if err != nil {
		log.Fatal(err)
	}

	// Pretty-print the full result set as a table.
	result.PrettyPrint()

	// Iterate over each individual Record in the result.
	fmt.Println("Visited countries by person:")
	for result.Next() { // Next returns true until the iterator is depleted.
		// Get the current Record.
		r := result.Record()

		// Entries in the Record can be accessed by index or key.
		pName, err := r.GetByIndex(0)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("\nName: %s\n", pName)
		pAge, _ := r.Get("p.age")
		fmt.Printf("\nAge: %d\n", pAge)
	}

	// Path matching example.
	query = "MATCH p = (:Person)-[:VISITED]->(:Country) RETURN p"
	result, err = graph.Query(query, nil, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Paths of persons visiting countries:")
	for result.Next() {
		r := result.Record()
		v, err := r.GetByIndex(0)
		if err != nil {
			log.Fatal(err)
		}
		p, ok := v.(falkordb.Path)
		fmt.Printf("%s %v\n", p, ok)
	}
}
```

Running the above produces the output:

```sh
+----------+-------+--------+
|  p.name  | p.age | c.name |
+----------+-------+--------+
| John Doe |    33 | Japan  |
+----------+-------+--------+

Query internal execution time 1.623063

Name: John Doe

Age: 33
```

## Query parameters

Pass parameters as a map rather than interpolating values into the query string:

```go
params := map[string]interface{}{"name": "John Doe", "minAge": 18}
res, err := graph.Query("MATCH (p:Person {name: $name}) WHERE p.age > $minAge RETURN p", params, nil)
```

Supported parameter types are `nil`, strings, `[]byte`, booleans, every signed and
unsigned integer type, `float32`, `float64`, `time.Time`, `time.Duration`, slices
and arrays of any supported type, and maps with string keys. Values that cannot be
represented are reported as an error wrapping `falkordb.ErrUnsupportedType`.

A `time.Time` is sent as an RFC 3339 string and a `time.Duration` as a whole number
of seconds, matching FalkorDB's second-granularity `DURATION` type.

Parameter names must be plain Cypher identifiers, and map keys are back-quoted, so
neither can inject Cypher into the query. Because FalkorDB strings are UTF-8 text,
a `[]byte` must hold valid UTF-8 and no value may contain a NUL byte; anything else
is rejected rather than silently corrupted. Integers above `math.MaxInt64` are
rejected too, since the server would otherwise saturate them without an error.

## Temporal values

FalkorDB's `date()`, `localtime()`, `localdatetime()` and `duration()` values are
decoded into native Go types — `time.Time` for the first three and `time.Duration`
for the last:

```go
res, _ := graph.Query("RETURN date(), duration({days: 2})", nil, nil)
for res.Next() {
	values := res.Record().Values()
	day := values[0].(time.Time)
	span := values[1].(time.Duration)
	fmt.Println(day, span)
}
```

## Running queries with timeouts

Queries can be run with a millisecond-level timeout as described in [the documentation](https://docs.falkordb.com/configuration.html#timeout). To take advantage of this feature, the `QueryOptions` struct should be used:

```go
options := NewQueryOptions().SetTimeout(10) // 10-millisecond timeout
res, err := graph.Query("MATCH (src {name: 'John Doe'})-[*]->(dest) RETURN dest", nil, options)
```

## User Defined Functions (UDFs)

FalkorDB supports User Defined Functions written in JavaScript. The `falkordb-go` client provides methods to manage UDF libraries:

```go
db, _ := falkordb.FalkorDBNew(&falkordb.ConnectionOption{Addr: "0.0.0.0:6379"})

// Define a UDF library
library := "StringUtils"
source := `
	function UpperCaseOdd(s) {
		return s.split('').map((char, i) => (i % 2 !== 0 ? char.toUpperCase() : char)).join('');
	}
	falkor.register('UpperCaseOdd', UpperCaseOdd);
`

// Load the UDF library
err := db.UDFLoad(library, source)

// List all loaded UDF libraries
udfs, err := db.UDFList()

// Use the UDF in a query
graph := db.SelectGraph("demo")
result, _ := graph.Query("RETURN StringUtils.UpperCaseOdd('hello')", nil, nil)

// Delete a specific UDF library
err = db.UDFDelete(library)

// Or flush all UDF libraries
err = db.UDFFlush()
```

For more information on UDFs, see the [FalkorDB UDF documentation](https://docs.falkordb.com/udfs/).

## Running tests

A simple test suite is provided, and can be run with:

```sh
$ go test
```

The tests expect a FalkorDB server to be available at localhost:6379

## License

falkordb-go is distributed under the BSD3 license - see [LICENSE](LICENSE)