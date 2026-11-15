package falkordb

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newFakeSentinel stands in for a Sentinel that monitors the given masters.
// It answers SENTINEL MASTERS in the shape of the negotiated protocol and
// fails SENTINEL GET-MASTER-ADDR-BY-NAME, so a test can see which master name
// the failover client asks for without a real master behind it.
func newFakeSentinel(t *testing.T, masters ...string) *fakeServer {
	return newFakeServer(t, func(args []string, resp3 bool) (string, bool) {
		switch cmd := strings.ToUpper(strings.Join(args, " ")); {
		case cmd == "INFO SERVER":
			return respBulk(redisSentinelInfo), true
		case cmd == "SENTINEL MASTERS":
			reply := fmt.Sprintf("*%d\r\n", len(masters))
			for _, name := range masters {
				reply += respMap(resp3, "name", name, "ip", "127.0.0.1", "port", "6379", "flags", "master")
			}
			return reply, true
		case strings.HasPrefix(cmd, "SENTINEL GET-MASTER-ADDR-BY-NAME"):
			return "-ERR fake sentinel has no master\r\n", true
		}
		return "", false
	})
}

// FromURL and FalkorDBNew read the master name out of SENTINEL MASTERS. The
// reply is a map per master under RESP3 (go-redis's default) and a flat
// field/value list under RESP2; each constructor used to assert one Go type
// and panicked on any other. FromURL asserted map[string]interface{}, which
// go-redis never returns, so it panicked on every Sentinel.
func TestConstructorsReadSentinelMasterName(t *testing.T) {
	constructors := map[string]func(addr string, protocol int) (*FalkorDB, error){
		"FalkorDBNew": func(addr string, protocol int) (*FalkorDB, error) {
			return FalkorDBNew(&ConnectionOption{Addr: addr, Protocol: protocol})
		},
		"FromURL": func(addr string, protocol int) (*FalkorDB, error) {
			return FromURL(fmt.Sprintf("falkor://%s?protocol=%d", addr, protocol))
		},
	}
	for name, connect := range constructors {
		for _, protocol := range []int{2, 3} {
			t.Run(fmt.Sprintf("%s/RESP%d", name, protocol), func(t *testing.T) {
				s := newFakeSentinel(t, "mymaster")
				var db *FalkorDB
				var err error
				require.NotPanics(t, func() { db, err = connect(s.addr(), protocol) })
				require.NoError(t, err)
				defer db.Conn.Close()
				// The client used to discover the master is closed.
				assert.Eventually(t, func() bool { return s.openConns() == 0 }, time.Second, 10*time.Millisecond)

				// The failover client resolves the master by the name it was
				// given; the fake Sentinel records the lookup.
				assert.Error(t, db.Conn.Ping(context.Background()).Err())
				assert.Contains(t, s.received(), "SENTINEL GET-MASTER-ADDR-BY-NAME MYMASTER")
			})
		}
	}
}

func TestSentinelMasterCount(t *testing.T) {
	for _, masters := range [][]string{nil, {"a", "b"}} {
		s := newFakeSentinel(t, masters...)
		var err error
		require.NotPanics(t, func() { _, err = FromURL("falkor://" + s.addr()) })
		assert.EqualError(t, err, "multiple masters, require service name")
		assert.Eventually(t, func() bool { return s.openConns() == 0 }, time.Second, 10*time.Millisecond)
	}
}

func TestSentinelMasterName(t *testing.T) {
	// The values go-redis returns for SENTINEL MASTERS from a real Sentinel.
	resp3 := []interface{}{map[interface{}]interface{}{"name": "mymaster", "ip": "127.0.0.1", "flags": "master"}}
	resp2 := []interface{}{[]interface{}{"name", "mymaster", "ip", "127.0.0.1", "flags", "master"}}
	for _, reply := range []interface{}{resp3, resp2} {
		name, err := sentinelMasterName(reply)
		require.NoError(t, err)
		assert.Equal(t, "mymaster", name)
	}

	for _, reply := range []interface{}{
		"not a list",
		[]interface{}{"not a master"},
		[]interface{}{map[interface{}]interface{}{"ip": "127.0.0.1"}},
		[]interface{}{[]interface{}{"ip", "127.0.0.1", "name"}},
		[]interface{}{map[interface{}]interface{}{"name": int64(1)}},
	} {
		var err error
		require.NotPanics(t, func() { _, err = sentinelMasterName(reply) })
		assert.Error(t, err, "%#v", reply)
	}
}
