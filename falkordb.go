package falkordb

import (
	"context"
	"errors"
	"strings"

	"github.com/redis/go-redis/v9"
)

type FalkorDB struct {
	Conn redis.UniversalClient
}

type ConnectionOption = redis.Options

type ConnectionClusterOption = redis.ClusterOptions

// isSentinel probes whether conn is a Sentinel. A failed probe is reported so a
// cancelled context cannot be mistaken for "this is not a sentinel", which would
// silently hand back a client wired to the wrong endpoint.
func isSentinel(ctx context.Context, conn redis.UniversalClient) (bool, error) {
	c, ok := conn.(*redis.Client)
	if !ok {
		return false, nil
	}
	info, err := c.InfoMap(ctx, "server").Result()
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		// The server is unreachable or does not support INFO; fall back to
		// treating it as a regular client, as before.
		return false, nil
	}
	svr, ok := info["Server"]
	if !ok {
		return false, nil
	}
	return svr["redis_mode"] == "sentinel", nil
}

// FalkorDB Class for interacting with a FalkorDB server.
func FalkorDBNew(options *ConnectionOption) (*FalkorDB, error) {
	return FalkorDBNewContext(context.Background(), options)
}

// FalkorDBNewContext is like FalkorDBNew but the connection handshake honors the
// cancellation and deadline of ctx. The context governs setup only; it is not
// retained for subsequent commands.
func FalkorDBNewContext(ctx context.Context, options *ConnectionOption) (*FalkorDB, error) {
	db := redis.NewClient(options)

	sentinel, err := isSentinel(ctx, db)
	if err != nil {
		db.Close()
		return nil, err
	}
	if sentinel {
		masters, err := db.Do(ctx, "SENTINEL", "MASTERS").Result()
		if err != nil {
			return nil, err
		}
		if len(masters.([]interface{})) != 1 {
			return nil, errors.New("multiple masters, require service name")
		}
		str := "name"
		var strInterface interface{} = str
		masterName := masters.([]interface{})[0].(map[interface{}]interface{})[strInterface].(string)
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
	return FromURLContext(context.Background(), url)
}

// FromURLContext is like FromURL but the connection handshake honors the
// cancellation and deadline of ctx. The context governs setup only; it is not
// retained for subsequent commands.
func FromURLContext(ctx context.Context, url string) (*FalkorDB, error) {
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
	sentinel, err := isSentinel(ctx, db)
	if err != nil {
		db.Close()
		return nil, err
	}
	if sentinel {
		masters, err := db.Do(ctx, "SENTINEL", "MASTERS").Result()
		if err != nil {
			return nil, err
		}
		if len(masters.([]interface{})) != 1 {
			return nil, errors.New("multiple masters, require service name")
		}
		masterName := masters.([]interface{})[0].(map[string]interface{})["name"].(string)
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
	return db.ListGraphsContext(context.Background())
}

// ListGraphsContext is like ListGraphs but honors the cancellation and deadline of ctx.
func (db *FalkorDB) ListGraphsContext(ctx context.Context) ([]string, error) {
	return db.Conn.Do(ctx, "GRAPH.LIST").StringSlice()
}

// Retrieve a DB level configuration.
// For a list of available configurations see: https://docs.falkordb.com/configuration.html#falkordb-configuration-parameters
func (db *FalkorDB) ConfigGet(key string) (interface{}, error) {
	return db.ConfigGetContext(context.Background(), key)
}

// ConfigGetContext is like ConfigGet but honors the cancellation and deadline of ctx.
func (db *FalkorDB) ConfigGetContext(ctx context.Context, key string) (interface{}, error) {
	return db.Conn.Do(ctx, "GRAPH.CONFIG", "GET", key).Result()
}

// Update a DB level configuration.
// For a list of available configurations see: https://docs.falkordb.com/configuration.html#falkordb-configuration-parameters
func (db *FalkorDB) ConfigSet(key string, value interface{}) error {
	return db.ConfigSetContext(context.Background(), key, value)
}

// ConfigSetContext is like ConfigSet but honors the cancellation and deadline of ctx.
func (db *FalkorDB) ConfigSetContext(ctx context.Context, key string, value interface{}) error {
	return db.Conn.Do(ctx, "GRAPH.CONFIG", "SET", key, value).Err()
}

// Load a UDF library into the database.
// See: https://docs.falkordb.com/udfs/
func (db *FalkorDB) UDFLoad(library string, source string) error {
	return db.UDFLoadContext(context.Background(), library, source)
}

// UDFLoadContext is like UDFLoad but honors the cancellation and deadline of ctx.
func (db *FalkorDB) UDFLoadContext(ctx context.Context, library string, source string) error {
	return db.Conn.Do(ctx, "GRAPH.UDF", "LOAD", library, source).Err()
}

// List all loaded UDF libraries.
// Returns a nested list where each element contains a library name followed by a list of function names.
// Example return format: [[library1, [func1, func2]], [library2, [func3, func4]]]
// See: https://docs.falkordb.com/udfs/
func (db *FalkorDB) UDFList() (interface{}, error) {
	return db.UDFListContext(context.Background())
}

// UDFListContext is like UDFList but honors the cancellation and deadline of ctx.
func (db *FalkorDB) UDFListContext(ctx context.Context) (interface{}, error) {
	return db.Conn.Do(ctx, "GRAPH.UDF", "LIST").Result()
}

// Delete a specific UDF library by name.
// See: https://docs.falkordb.com/udfs/
func (db *FalkorDB) UDFDelete(library string) error {
	return db.UDFDeleteContext(context.Background(), library)
}

// UDFDeleteContext is like UDFDelete but honors the cancellation and deadline of ctx.
func (db *FalkorDB) UDFDeleteContext(ctx context.Context, library string) error {
	return db.Conn.Do(ctx, "GRAPH.UDF", "DELETE", library).Err()
}

// Flush all loaded UDF libraries.
// See: https://docs.falkordb.com/udfs/
func (db *FalkorDB) UDFFlush() error {
	return db.UDFFlushContext(context.Background())
}

// UDFFlushContext is like UDFFlush but honors the cancellation and deadline of ctx.
func (db *FalkorDB) UDFFlushContext(ctx context.Context) error {
	return db.Conn.Do(ctx, "GRAPH.UDF", "FLUSH").Err()
}
