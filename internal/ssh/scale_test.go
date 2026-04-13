// Copyright © 2026 OpenCHAMI a Series of LF Projects, LLC
//
// SPDX-License-Identifier: MIT

package ssh_test

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	compcredentials "github.com/Cray-HPE/hms-compcredentials"
	gossh "golang.org/x/crypto/ssh"

	"github.com/OpenCHAMI/remote-console/internal/nodes"
	"github.com/OpenCHAMI/remote-console/internal/ssh"
)

const (
	// scaleNodeCount is the number of simultaneous SSH console connections to test.
	// Increase this to stress the manager further.
	scaleNodeCount   = 10000
	scaleConnTimeout = 60 * time.Second
)

// TestSSHConsoleManagerScale verifies that SSHConsoleManager can sustain
// scaleNodeCount simultaneous SSH console connections without leaking goroutines.
//
// The test uses an in-process SSH server so there is no container or network
// overhead — all connections are localhost loopback. Run with:
//
//	go test -v -run TestSSHConsoleManagerScale ./internal/ssh/ -timeout 120s
func TestSSHConsoleManagerScale(t *testing.T) {
	var sessions atomic.Int64
	srv := newSSHServer(t, func(ch gossh.Channel) {
		sessions.Add(1)
		_, _ = io.Copy(io.Discard, ch)
		ch.Close()
	})

	host, port := nodeAddr(t, srv.addr())

	nodeMap := make(map[string]*nodes.NodeConsoleInfo, scaleNodeCount)
	passwords := make(map[string]compcredentials.CompCredentials, scaleNodeCount)
	for i := 0; i < scaleNodeCount; i++ {
		id := fmt.Sprintf("x0c0s%db0n0", i)
		nodeMap[id] = &nodes.NodeConsoleInfo{
			ID:             id,
			ConnectionType: nodes.SSH,
			ConnectionHost: host,
			ConnectionPort: port,
		}
		passwords[id] = compcredentials.CompCredentials{
			Username: testSSHUser,
			Password: testSSHPass,
		}
	}

	// Register nodes in the global node map so IsCurrentNode returns true
	// inside each node's Run goroutine.
	nodes.SetNodesForTest(nodeMap)
	defer nodes.SetNodesForTest(nil)

	cfg := ssh.DefaultSSHConfig()
	cfg.TCPKeepAlive = 0
	cfg.ReconnectMinInterval = 200 * time.Millisecond
	cfg.ReconnectMaxInterval = 2 * time.Second

	manager := ssh.NewSSHConsoleManager(cfg, "", t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	goroutinesBefore := runtime.NumGoroutine()

	if err := manager.UpdateNodes(ctx, nodeMap, passwords); err != nil {
		t.Fatalf("UpdateNodes: %v", err)
	}

	// Wait until all nodes have established a shell session on the server.
	t.Logf("Waiting for %d nodes to connect (timeout %s)…", scaleNodeCount, scaleConnTimeout)
	deadline := time.Now().Add(scaleConnTimeout)
	for time.Now().Before(deadline) {
		if sessions.Load() >= int64(scaleNodeCount) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	connected := sessions.Load()
	if connected < int64(scaleNodeCount) {
		t.Errorf("only %d/%d nodes connected within %s", connected, scaleNodeCount, scaleConnTimeout)
	} else {
		t.Logf("All %d nodes connected", connected)
	}

	// Goroutines per node include the SSH library internals (transport reader/
	// writer, channel mux, global-request handler) on both client and server
	// sides, plus explicit goroutines (Run, session.Wait, handleConn,
	// handleSession, DiscardRequests). Flag only if we see more than 25/node,
	// which would indicate something structurally wrong.
	goroutinesPeak := runtime.NumGoroutine()
	goroutinesAdded := goroutinesPeak - goroutinesBefore
	t.Logf("Goroutines added: %d (%.1f per node)", goroutinesAdded, float64(goroutinesAdded)/float64(scaleNodeCount))

	const maxGoroutinesPerNode = 25
	if goroutinesAdded > scaleNodeCount*maxGoroutinesPerNode {
		t.Errorf("goroutine count looks high: %d added for %d nodes (limit %d/node)",
			goroutinesAdded, scaleNodeCount, maxGoroutinesPerNode)
	}

	// Cancel the manager context and give goroutines time to drain.
	// Cancellation closes all SSH clients, which unblocks streamStdout's Read
	// and lets every Run goroutine exit cleanly.
	cancel()
	time.Sleep(5 * time.Second)

	goroutinesAfter := runtime.NumGoroutine()
	goroutinesLeaked := goroutinesAfter - goroutinesBefore
	t.Logf("Goroutines remaining after shutdown: %d leaked", goroutinesLeaked)

	const maxLeakedGoroutines = 20
	if goroutinesLeaked > maxLeakedGoroutines {
		t.Errorf("possible goroutine leak after shutdown: %d goroutines above baseline", goroutinesLeaked)
	}
}

