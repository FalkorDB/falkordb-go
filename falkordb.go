package falkordb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

type FalkorDB struct {
	Conn redis.UniversalClient
}

type ConnectionOption = redis.Options

type ConnectionClusterOption = redis.ClusterOptions

func isSentinel(conn redis.UniversalClient) bool {
	c, ok := conn.(*redis.Client)
	if !ok {
		return false
	}
	info, err := c.InfoMap(ctx, "server").Result()
	if err != nil {
		return false
	}
	svr, ok := info["Server"]
	if !ok {
		return false
	}
	return svr["redis_mode"] == "sentinel"
}

// querySentinelMasterName asks a Sentinel for the name of the master it
// monitors.
func querySentinelMasterName(sentinel *redis.Client) (string, error) {
	masters, err := sentinel.Do(ctx, "SENTINEL", "MASTERS").Result()
	if err != nil {
		return "", err
	}
	return sentinelMasterName(masters)
}

// sentinelMasterName returns the name of the single master in a SENTINEL
// MASTERS reply. Each master is a map under RESP3, which go-redis decodes as
// map[interface{}]interface{}, and a flat list of field/value pairs under
// RESP2.
func sentinelMasterName(reply interface{}) (string, error) {
	masters, ok := reply.([]interface{})
	if !ok {
		return "", fmt.Errorf("unexpected SENTINEL MASTERS reply %T", reply)
	}
	if len(masters) != 1 {
		return "", errors.New("multiple masters, require service name")
	}
	var name interface{}
	switch master := masters[0].(type) {
	case map[interface{}]interface{}:
		name = master["name"]
	case map[string]interface{}:
		name = master["name"]
	case []interface{}:
		for i := 0; i+1 < len(master); i += 2 {
			if master[i] == "name" {
				name = master[i+1]
				break
			}
		}
	default:
		return "", fmt.Errorf("unexpected SENTINEL MASTERS entry %T", masters[0])
	}
	if s, ok := name.(string); ok && s != "" {
		return s, nil
	}
	return "", errors.New("SENTINEL MASTERS entry has no master name")
}

// FalkorDB Class for interacting with a FalkorDB server.
func FalkorDBNew(options *ConnectionOption) (*FalkorDB, error) {
	db := redis.NewClient(options)

	if isSentinel(db) {
		masterName, err := querySentinelMasterName(db)
		// The connection to the Sentinel was only needed to find the master.
		db.Close()
		if err != nil {
			return nil, err
		}
		db = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:       masterName,
			SentinelAddrs:    []string{options.Addr},
			ClientName:       options.ClientName,
			Username:         options.Username,
			Password:         options.Password,
			SentinelUsername: options.Username,
			SentinelPassword: options.Password,
			MaxRetries:       options.MaxRetries,
			MinRetryBackoff:  options.MinRetryBackoff,
			MaxRetryBackoff:  options.MaxRetryBackoff,
			TLSConfig:        options.TLSConfig,
			PoolFIFO:         options.PoolFIFO,
			PoolSize:         options.PoolSize,
			PoolTimeout:      options.PoolTimeout,
		})
	}
	return &FalkorDB{Conn: db}, nil
}

// FalkorDBNewCluster creates a new FalkorDB cluster instance.
func FalkorDBNewCluster(options *ConnectionClusterOption) (*FalkorDB, error) {
	db := redis.NewClusterClient(options)
	return &FalkorDB{Conn: db}, nil
}

// Creates a new FalkorDB instance from a URL.
func FromURL(url string) (*FalkorDB, error) {
	if strings.HasPrefix(url, "falkor://") {
		url = "redis://" + url[len("falkor://"):]
	} else if strings.HasPrefix(url, "falkors://") {
		url = "rediss://" + url[len("falkors://"):]
	}

	options, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	db := redis.NewClient(options)
	if isSentinel(db) {
		masterName, err := querySentinelMasterName(db)
		// The connection to the Sentinel was only needed to find the master.
		db.Close()
		if err != nil {
			return nil, err
		}
		db = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    masterName,
			SentinelAddrs: []string{options.Addr},
		})
	}
	return &FalkorDB{Conn: db}, nil
}

// Selects a graph by creating a new Graph instance.
func (db *FalkorDB) SelectGraph(graphName string) *Graph {
	return graphNew(graphName, db.Conn)
}

// List all graph names.
// See: https://docs.falkordb.com/commands/graph.list.html
func (db *FalkorDB) ListGraphs() ([]string, error) {
	return db.Conn.Do(ctx, "GRAPH.LIST").StringSlice()
}

// Retrieve a DB level configuration.
// For a list of available configurations see: https://docs.falkordb.com/configuration.html#falkordb-configuration-parameters
func (db *FalkorDB) ConfigGet(key string) (interface{}, error) {
	return db.Conn.Do(ctx, "GRAPH.CONFIG", "GET", key).Result()
}

// Update a DB level configuration.
// For a list of available configurations see: https://docs.falkordb.com/configuration.html#falkordb-configuration-parameters
func (db *FalkorDB) ConfigSet(key string, value interface{}) error {
	return db.Conn.Do(ctx, "GRAPH.CONFIG", "SET", key, value).Err()
}

// Load a UDF library into the database.
// See: https://docs.falkordb.com/udfs/
func (db *FalkorDB) UDFLoad(library string, source string) error {
	return db.Conn.Do(ctx, "GRAPH.UDF", "LOAD", library, source).Err()
}

// List all loaded UDF libraries.
// Returns a nested list where each element contains a library name followed by a list of function names.
// Example return format: [[library1, [func1, func2]], [library2, [func3, func4]]]
// See: https://docs.falkordb.com/udfs/
func (db *FalkorDB) UDFList() (interface{}, error) {
	return db.Conn.Do(ctx, "GRAPH.UDF", "LIST").Result()
}

// Delete a specific UDF library by name.
// See: https://docs.falkordb.com/udfs/
func (db *FalkorDB) UDFDelete(library string) error {
	return db.Conn.Do(ctx, "GRAPH.UDF", "DELETE", library).Err()
}

// Flush all loaded UDF libraries.
// See: https://docs.falkordb.com/udfs/
func (db *FalkorDB) UDFFlush() error {
	return db.Conn.Do(ctx, "GRAPH.UDF", "FLUSH").Err()
}
