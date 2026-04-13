// Copyright © 2026 OpenCHAMI a Series of LF Projects, LLC
//
// SPDX-License-Identifier: MIT

package ssh_test

// In-process SSH test server and shared test helpers used across scale,
// concurrent, and node-behaviour tests.

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Cray-HPE/hms-compcredentials"
	gossh "golang.org/x/crypto/ssh"

	"github.com/OpenCHAMI/remote-console/internal/nodes"
	"github.com/OpenCHAMI/remote-console/internal/ssh"
)

// ---------------------------------------------------------------------------
// Test credentials (shared across all tests)
// ---------------------------------------------------------------------------

const (
	testSSHUser = "testuser"
	testSSHPass = "testpass"
)

// ---------------------------------------------------------------------------
// sshServer — in-process SSH server
// ---------------------------------------------------------------------------

// sshServer is a minimal in-process SSH server. When a client establishes a
// shell or exec session, onShell is called in its own goroutine with the
// accepted channel. onShell is responsible for draining the channel and closing
// it when done. All connections share a single listener on a random loopback port.
type sshServer struct {
	listener net.Listener
	cfg      *gossh.ServerConfig
	onShell  func(ch gossh.Channel)
}

// newSSHServer starts an in-process SSH server that accepts password auth with
// testSSHUser/testSSHPass and calls onShell for every established shell session.
func newSSHServer(t *testing.T, onShell func(gossh.Channel)) *sshServer {
	t.Helper()

	_, hostKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate SSH host key: %v", err)
	}
	signer, err := gossh.NewSignerFromKey(hostKey)
	if err != nil {
		t.Fatalf("create SSH host signer: %v", err)
	}

	cfg := &gossh.ServerConfig{
		PasswordCallback: func(c gossh.ConnMetadata, pass []byte) (*gossh.Permissions, error) {
			if c.User() == testSSHUser && string(pass) == testSSHPass {
				return nil, nil
			}
			return nil, fmt.Errorf("access denied")
		},
	}
	cfg.AddHostKey(signer)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	srv := &sshServer{listener: ln, cfg: cfg, onShell: onShell}
	go srv.serve()
	return srv
}

func (s *sshServer) addr() string { return s.listener.Addr().String() }

func (s *sshServer) serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handleConn(conn)
	}
}

func (s *sshServer) handleConn(conn net.Conn) {
	sshConn, chans, reqs, err := gossh.NewServerConn(conn, s.cfg)
	if err != nil {
		conn.Close()
		return
	}
	defer sshConn.Close()
	go gossh.DiscardRequests(reqs)

	for newChan := range chans {
		if newChan.ChannelType() != "session" {
			_ = newChan.Reject(gossh.UnknownChannelType, "unsupported")
			continue
		}
		ch, reqs, err := newChan.Accept()
		if err != nil {
			return
		}
		go s.handleSession(ch, reqs)
	}
}

func (s *sshServer) handleSession(ch gossh.Channel, reqs <-chan *gossh.Request) {
	for req := range reqs {
		switch req.Type {
		case "pty-req":
			_ = req.Reply(true, nil)
		case "shell", "exec":
			_ = req.Reply(true, nil)
			s.onShell(ch)
			return
		default:
			if req.WantReply {
				_ = req.Reply(false, nil)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// sshSession — controllable session handed to tests
// ---------------------------------------------------------------------------

// sshSession gives a test direct control over one established shell session.
type sshSession struct {
	send chan<- []byte // write here to push bytes to the client
	recv <-chan []byte // read here to see bytes the client wrote
	drop func()       // close the server-side channel immediately
}

// runSession is an onShell implementation for tests that need per-session
// control. It pumps stdin/stdout and sends the session to out, then keeps the
// channel alive until drop is called.
func runSession(ch gossh.Channel, out chan<- *sshSession) {
	sendCh := make(chan []byte, 16)
	recvCh := make(chan []byte, 16)
	dropped := make(chan struct{})

	sess := &sshSession{
		send: sendCh,
		recv: recvCh,
		drop: sync.OnceFunc(func() {
			close(dropped)
			ch.Close()
		}),
	}

	// stdin → recvCh
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := ch.Read(buf)
			if n > 0 {
				cp := make([]byte, n)
				copy(cp, buf[:n])
				select {
				case recvCh <- cp:
				case <-dropped:
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()

	// sendCh → stdout
	go func() {
		for {
			select {
			case data := <-sendCh:
				if _, err := ch.Write(data); err != nil {
					return
				}
			case <-dropped:
				return
			}
		}
	}()

	select {
	case out <- sess:
	case <-dropped:
	}
	<-dropped
}

// ---------------------------------------------------------------------------
// Shared test helpers
// ---------------------------------------------------------------------------

// nodeAddr parses "host:port" and returns the components.
func nodeAddr(t *testing.T, addr string) (host string, port int) {
	t.Helper()
	colon := strings.LastIndex(addr, ":")
	if colon < 0 {
		t.Fatalf("bad addr %q", addr)
	}
	if _, err := fmt.Sscanf(addr[colon+1:], "%d", &port); err != nil {
		t.Fatalf("parse port from %q: %v", addr, err)
	}
	return addr[:colon], port
}

// singleNode builds a one-node map and matching credentials map for tests
// that only need a single SSH node.
func singleNode(t *testing.T, addr string) (id string, nodeMap map[string]*nodes.NodeConsoleInfo, passwords map[string]compcredentials.CompCredentials) {
	t.Helper()
	host, port := nodeAddr(t, addr)
	id = "test-node-0"
	nodeMap = map[string]*nodes.NodeConsoleInfo{
		id: {
			ID:             id,
			ConnectionType: nodes.SSH,
			ConnectionHost: host,
			ConnectionPort: port,
		},
	}
	passwords = map[string]compcredentials.CompCredentials{
		id: {Username: testSSHUser, Password: testSSHPass},
	}
	return
}

// newManager creates an SSHConsoleManager with fast reconnect intervals
// suitable for tests.
func newManager(t *testing.T, logsDir string) *ssh.SSHConsoleManager {
	t.Helper()
	cfg := ssh.DefaultSSHConfig()
	cfg.TCPKeepAlive = 0
	cfg.ReconnectMinInterval = 100 * time.Millisecond
	cfg.ReconnectMaxInterval = 1 * time.Second
	return ssh.NewSSHConsoleManager(cfg, "", logsDir)
}

// waitForMarker reads from ch until the accumulated output contains marker or
// the timeout expires. Returns the full output seen.
func waitForMarker(t *testing.T, ch <-chan []byte, marker string, timeout time.Duration) string {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	var buf strings.Builder
	for {
		select {
		case data, ok := <-ch:
			if !ok {
				t.Fatalf("client channel closed before marker %q; received: %q", marker, buf.String())
			}
			buf.Write(data)
			if strings.Contains(buf.String(), marker) {
				return buf.String()
			}
		case <-timer.C:
			t.Fatalf("timeout waiting for marker %q; received: %q", marker, buf.String())
			return ""
		}
	}
}
