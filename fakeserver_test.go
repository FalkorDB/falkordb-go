package falkordb

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// INFO server headers as the servers send them, trimmed after the mode line.
const (
	// falkordb/falkordb:latest (Redis 8) running as a Sentinel.
	redisSentinelInfo = "# Server\r\nredis_version:8.10.2\r\nredis_git_sha1:498ecd0d\r\nredis_git_dirty:1\r\nredis_build_id:34b3b3397048fc3f\r\nredis_mode:sentinel\r\narch_bits:64\r\n"
	// falkordb/falkordb:latest running standalone.
	redisStandaloneInfo = "# Server\r\nredis_version:8.10.2\r\nredis_git_sha1:498ecd0d\r\nredis_git_dirty:1\r\nredis_build_id:34b3b3397048fc3f\r\nredis_mode:standalone\r\narch_bits:64\r\n"
	// falkordb/falkordb:edge (Valkey 9.1) running as a Sentinel. With
	// extended-redis-compatibility off, the default, Valkey names the field
	// server_mode instead of redis_mode.
	valkeySentinelInfo = "# Server\r\nredis_version:7.2.4\r\nserver_name:valkey\r\nvalkey_version:9.1.2\r\nvalkey_release_stage:ga\r\nredis_git_sha1:7f1dffed\r\nredis_git_dirty:1\r\nredis_build_id:22ba26550f3540a2\r\nserver_mode:sentinel\r\narch_bits:64\r\n"
	// falkordb/falkordb:edge running standalone.
	valkeyStandaloneInfo = "# Server\r\nredis_version:7.2.4\r\nserver_name:valkey\r\nvalkey_version:9.1.2\r\nvalkey_release_stage:ga\r\nredis_git_sha1:7f1dffed\r\nredis_git_dirty:1\r\nredis_build_id:22ba26550f3540a2\r\nserver_mode:standalone\r\narch_bits:64\r\n"
	// falkordb/falkordb:edge running as a cluster node.
	valkeyClusterInfo = "# Server\r\nredis_version:7.2.4\r\nserver_name:valkey\r\nvalkey_version:9.1.2\r\nvalkey_release_stage:ga\r\nredis_git_sha1:7f1dffed\r\nredis_git_dirty:1\r\nredis_build_id:22ba26550f3540a2\r\nserver_mode:cluster\r\narch_bits:64\r\n"
	// Valkey with extended-redis-compatibility on reports redis_mode.
	valkeyCompatSentinelInfo = "# Server\r\nredis_version:7.2.4\r\nserver_name:valkey\r\nvalkey_version:9.1.2\r\nvalkey_release_stage:ga\r\nredis_git_sha1:7f1dffed\r\nredis_git_dirty:1\r\nredis_build_id:22ba26550f3540a2\r\nredis_mode:sentinel\r\narch_bits:64\r\n"
)

// fakeHandler returns the raw RESP reply to a command, or ok=false for an
// unknown command. resp3 says whether the connection negotiated RESP3.
type fakeHandler func(args []string, resp3 bool) (reply string, ok bool)

// fakeServer speaks just enough RESP to stand in for a server or Sentinel in
// tests that must not need one. It answers the connection handshake itself,
// passes every other command to its handler and records all of them,
// upper-cased and space-joined.
type fakeServer struct {
	ln      net.Listener
	handler fakeHandler
	wg      sync.WaitGroup

	mu       sync.Mutex
	commands []string
	conns    map[net.Conn]struct{}
	closed   bool
}

func newFakeServer(t *testing.T, handler fakeHandler) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := &fakeServer{ln: ln, handler: handler, conns: map[net.Conn]struct{}{}}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			if s.closed {
				s.mu.Unlock()
				conn.Close()
				return
			}
			s.conns[conn] = struct{}{}
			s.wg.Add(1)
			s.mu.Unlock()
			go s.serve(conn)
		}
	}()
	t.Cleanup(s.close)
	return s
}

// close stops the server: it closes the listener and every connection still
// open, and waits for their goroutines to finish.
func (s *fakeServer) close() {
	s.mu.Lock()
	s.closed = true
	s.ln.Close()
	for conn := range s.conns {
		conn.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

// newFakeInfoServer answers INFO with info and SENTINEL MASTERS with an
// error, which is enough to see which way the mode detection went.
func newFakeInfoServer(t *testing.T, info string) *fakeServer {
	return newFakeServer(t, func(args []string, _ bool) (string, bool) {
		switch strings.ToUpper(strings.Join(args, " ")) {
		case "INFO SERVER":
			return respBulk(info), true
		case "SENTINEL MASTERS":
			return "-ERR fake sentinel reached\r\n", true
		}
		return "", false
	})
}

func (s *fakeServer) addr() string { return s.ln.Addr().String() }

func (s *fakeServer) received() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.commands...)
}

// openConns returns the number of client connections still open.
func (s *fakeServer) openConns() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

func (s *fakeServer) serve(conn net.Conn) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		conn.Close()
	}()
	r := bufio.NewReader(conn)
	resp3 := false
	for {
		args, err := readCommand(r)
		if err != nil {
			return
		}
		s.mu.Lock()
		s.commands = append(s.commands, strings.ToUpper(strings.Join(args, " ")))
		s.mu.Unlock()

		var reply string
		switch strings.ToUpper(args[0]) {
		case "HELLO":
			resp3 = len(args) > 1 && args[1] == "3"
			proto := "2"
			if resp3 {
				proto = "3"
			}
			reply = respMap(resp3, "server", "valkey", "version", "9.1.2", "proto", proto)
		case "CLIENT":
			reply = "+OK\r\n"
		default:
			var ok bool
			if reply, ok = s.handler(args, resp3); !ok {
				reply = "-ERR unknown command '" + args[0] + "'\r\n"
			}
		}
		if _, err := io.WriteString(conn, reply); err != nil {
			return
		}
	}
}

// readCommand reads one RESP array of bulk strings.
func readCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("unexpected %q", line)
	}
	n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil || n < 1 {
		return nil, fmt.Errorf("unexpected %q", line)
	}
	args := make([]string, n)
	for i := range args {
		hdr, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(strings.TrimSpace(hdr[1:]))
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args[i] = string(buf[:size])
	}
	return args, nil
}

func respBulk(s string) string { return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s) }

// respMap encodes field/value pairs as a RESP3 map, or as the flat array a
// RESP2 server sends in its place.
func respMap(resp3 bool, kv ...string) string {
	var b strings.Builder
	if resp3 {
		fmt.Fprintf(&b, "%%%d\r\n", len(kv)/2)
	} else {
		fmt.Fprintf(&b, "*%d\r\n", len(kv))
	}
	for _, s := range kv {
		b.WriteString(respBulk(s))
	}
	return b.String()
}
