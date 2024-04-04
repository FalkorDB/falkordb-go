package falkordb

import (
	"context"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

type FalkorDB struct {
	Conn *redis.Client
}

type ConnectionOption = redis.Options

func isSentinel(conn *redis.Client) bool {
	info, _ := conn.InfoMap(ctx, "server").Result()
	return info["server"]["redis_mode"] == "sentinel"
}

func FalkorDBNew(address string, options *ConnectionOption) (*FalkorDB, error) {
	rdb := redis.NewClient(options)

	if isSentinel(rdb) {
		rdb = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    "master-name",
			SentinelAddrs: []string{":9126", ":9127", ":9128"},
		})
	}
	return &FalkorDB{Conn: rdb}, nil
}

func FromURL(url string) (*FalkorDB, error) {
	options, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	rdb := redis.NewClient(options)
	if isSentinel(rdb) {
		rdb = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    "master-name",
			SentinelAddrs: []string{":9126", ":9127", ":9128"},
		})
	}
	return &FalkorDB{Conn: rdb}, nil
}

func (db *FalkorDB) SelectGraph(graphName string) Graph {
	return graphNew(graphName, db.Conn)
}

func (db *FalkorDB) ListGraphs() ([]string, error) {
	return db.Conn.Do(ctx, "GRAPH.LIST").StringSlice()
}

func (db *FalkorDB) ConfigGet(key string) string {
	return db.Conn.Do(ctx, "GRAPH.CONFIG", "GET", key).String()
}

func (db *FalkorDB) ConfigSet(key, value string) error {
	return db.Conn.Do(ctx, "GRAPH.CONFIG", "SET", key).Err()
}
