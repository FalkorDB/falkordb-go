package falkordb

import (
	"context"
	"errors"

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
	db := redis.NewClient(options)

	if isSentinel(db) {
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
			SentinelAddrs: []string{address},
		})
	}
	return &FalkorDB{Conn: db}, nil
}

func FromURL(url string) (*FalkorDB, error) {
	options, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	db := redis.NewClient(options)
	if isSentinel(db) {
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

func (db *FalkorDB) SelectGraph(graphName string) *Graph {
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
