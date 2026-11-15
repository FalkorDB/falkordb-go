package falkordb

import (
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerMode(t *testing.T) {
	assert.Equal(t, "sentinel", serverMode(map[string]string{"redis_mode": "sentinel"}))
	assert.Equal(t, "sentinel", serverMode(map[string]string{"server_mode": "sentinel"}))
	assert.Equal(t, "cluster", serverMode(map[string]string{"server_mode": "cluster"}))
	assert.Equal(t, "", serverMode(map[string]string{"redis_version": "7.2.4"}))
}

func TestIsSentinel(t *testing.T) {
	cases := []struct {
		name string
		info string
		want bool
	}{
		{"redis sentinel", redisSentinelInfo, true},
		{"redis standalone", redisStandaloneInfo, false},
		{"valkey sentinel", valkeySentinelInfo, true},
		{"valkey standalone", valkeyStandaloneInfo, false},
		{"valkey cluster node", valkeyClusterInfo, false},
		{"valkey sentinel in redis compatibility mode", valkeyCompatSentinelInfo, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newFakeInfoServer(t, tc.info)
			conn := redis.NewClient(&redis.Options{Addr: s.addr()})
			defer conn.Close()
			assert.Equal(t, tc.want, isSentinel(conn))
		})
	}
}

// A Valkey Sentinel address passed to either constructor must be resolved
// through SENTINEL MASTERS, not used as a data node: graph commands sent to
// the Sentinel itself fail with "unknown command 'GRAPH.QUERY'".
func TestConstructorsDetectValkeySentinel(t *testing.T) {
	constructors := map[string]func(addr string) (*FalkorDB, error){
		"FalkorDBNew": func(addr string) (*FalkorDB, error) {
			return FalkorDBNew(&ConnectionOption{Addr: addr})
		},
		"FromURL": func(addr string) (*FalkorDB, error) {
			return FromURL("falkor://" + addr)
		},
	}
	for name, connect := range constructors {
		t.Run(name+"/sentinel", func(t *testing.T) {
			s := newFakeInfoServer(t, valkeySentinelInfo)
			_, err := connect(s.addr())
			require.Error(t, err)
			assert.Contains(t, err.Error(), "fake sentinel reached")
			assert.Contains(t, s.received(), "SENTINEL MASTERS")
		})
		t.Run(name+"/standalone", func(t *testing.T) {
			s := newFakeInfoServer(t, valkeyStandaloneInfo)
			db, err := connect(s.addr())
			require.NoError(t, err)
			defer db.Conn.Close()
			assert.NotContains(t, s.received(), "SENTINEL MASTERS")
		})
	}
}
