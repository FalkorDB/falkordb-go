package falkordb

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

type FalkorDB struct {
	Conn *redis.Client
}

// ConnectionOption specifies an option for dialing a Redis server.
type ConnectionOption struct {
	f func(*connectionOptions)
}

type connectionOptions struct {
	readTimeout         time.Duration
	writeTimeout        time.Duration
	tlsHandshakeTimeout time.Duration
	dialer              *net.Dialer
	dialContext         func(ctx context.Context, network, addr string) (net.Conn, error)
	db                  int
	username            string
	password            string
	clientName          string
	useTLS              bool
	skipVerify          bool
	tlsConfig           *tls.Config
}

// DialTLSHandshakeTimeout specifies the maximum amount of time waiting to
// wait for a TLS handshake. Zero means no timeout.
// If no DialTLSHandshakeTimeout option is specified then the default is 30 seconds.
func DialTLSHandshakeTimeout(d time.Duration) ConnectionOption {
	return ConnectionOption{func(do *connectionOptions) {
		do.tlsHandshakeTimeout = d
	}}
}

// DialReadTimeout specifies the timeout for reading a single command reply.
func DialReadTimeout(d time.Duration) ConnectionOption {
	return ConnectionOption{func(do *connectionOptions) {
		do.readTimeout = d
	}}
}

// DialWriteTimeout specifies the timeout for writing a single command.
func DialWriteTimeout(d time.Duration) ConnectionOption {
	return ConnectionOption{func(do *connectionOptions) {
		do.writeTimeout = d
	}}
}

// DialConnectTimeout specifies the timeout for connecting to the Redis server when
// no DialNetDial option is specified.
// If no DialConnectTimeout option is specified then the default is 30 seconds.
func DialConnectTimeout(d time.Duration) ConnectionOption {
	return ConnectionOption{func(do *connectionOptions) {
		do.dialer.Timeout = d
	}}
}

// DialKeepAlive specifies the keep-alive period for TCP connections to the Redis server
// when no DialNetDial option is specified.
// If zero, keep-alives are not enabled. If no DialKeepAlive option is specified then
// the default of 5 minutes is used to ensure that half-closed TCP sessions are detected.
func DialKeepAlive(d time.Duration) ConnectionOption {
	return ConnectionOption{func(do *connectionOptions) {
		do.dialer.KeepAlive = d
	}}
}

// DialNetDial specifies a custom dial function for creating TCP
// connections, otherwise a net.Dialer customized via the other options is used.
// DialNetDial overrides DialConnectTimeout and DialKeepAlive.
func DialNetDial(dial func(network, addr string) (net.Conn, error)) ConnectionOption {
	return ConnectionOption{func(do *connectionOptions) {
		do.dialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dial(network, addr)
		}
	}}
}

// DialContextFunc specifies a custom dial function with context for creating TCP
// connections, otherwise a net.Dialer customized via the other options is used.
// DialContextFunc overrides DialConnectTimeout and DialKeepAlive.
func DialContextFunc(f func(ctx context.Context, network, addr string) (net.Conn, error)) ConnectionOption {
	return ConnectionOption{func(do *connectionOptions) {
		do.dialContext = f
	}}
}

// DialDatabase specifies the database to select when dialing a connection.
func DialDatabase(db int) ConnectionOption {
	return ConnectionOption{func(do *connectionOptions) {
		do.db = db
	}}
}

// DialPassword specifies the password to use when connecting to
// the Redis server.
func DialPassword(password string) ConnectionOption {
	return ConnectionOption{func(do *connectionOptions) {
		do.password = password
	}}
}

// DialUsername specifies the username to use when connecting to
// the Redis server when Redis ACLs are used.
// A DialPassword must also be passed otherwise this option will have no effect.
func DialUsername(username string) ConnectionOption {
	return ConnectionOption{func(do *connectionOptions) {
		do.username = username
	}}
}

// DialClientName specifies a client name to be used
// by the Redis server connection.
func DialClientName(name string) ConnectionOption {
	return ConnectionOption{func(do *connectionOptions) {
		do.clientName = name
	}}
}

// DialTLSConfig specifies the config to use when a TLS connection is dialed.
// Has no effect when not dialing a TLS connection.
func DialTLSConfig(c *tls.Config) ConnectionOption {
	return ConnectionOption{func(do *connectionOptions) {
		do.tlsConfig = c
	}}
}

// DialTLSSkipVerify disables server name verification when connecting over
// TLS. Has no effect when not dialing a TLS connection.
func DialTLSSkipVerify(skip bool) ConnectionOption {
	return ConnectionOption{func(do *connectionOptions) {
		do.skipVerify = skip
	}}
}

// DialUseTLS specifies whether TLS should be used when connecting to the
// server. This option is ignore by DialURL.
func DialUseTLS(useTLS bool) ConnectionOption {
	return ConnectionOption{func(do *connectionOptions) {
		do.useTLS = useTLS
	}}
}

func isSentinel(conn *redis.Client) bool {
	info, _ := conn.InfoMap(ctx, "server").Result()
	return info["server"]["redis_mode"] == "sentinel"
}

// , options ...ConnectionOption
func FalkorDBNew(address string) (*FalkorDB, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

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
