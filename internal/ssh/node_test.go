// Copyright © 2026 OpenCHAMI a Series of LF Projects, LLC
//
// SPDX-License-Identifier: MIT

package ssh_test

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	compcredentials "github.com/Cray-HPE/hms-compcredentials"
	gossh "golang.org/x/crypto/ssh"

	"github.com/OpenCHAMI/remote-console/internal/nodes"
)

// ---------------------------------------------------------------------------
// 1. Reconnect on disconnect
// ---------------------------------------------------------------------------

// TestSSHConsoleNodeReconnect verifies that when the server drops a connection
// the node broadcasts a disconnect marker, then reconnects automatically and
// broadcasts a connected marker.
func TestSSHConsoleNodeReconnect(t *testing.T) {
	sessionCh := make(chan *sshSession)
	srv := newSSHServer(t, func(ch gossh.Channel) { runSession(ch, sessionCh) })
	id, nodeMap, passwords := singleNode(t, srv.addr())

	nodes.SetNodesForTest(nodeMap)
	defer nodes.SetNodesForTest(nil)

	manager := newManager(t, t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := manager.UpdateNodes(ctx, nodeMap, passwords); err != nil {
		t.Fatal(err)
	}

	// Attach before the first connect so we capture the initial connected marker.
	clientCh, err := manager.Attach(id, "reconnect-client")
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Detach(id, "reconnect-client")

	sess := <-sessionCh
	waitForMarker(t, clientCh, fmt.Sprintf("[Console %s connected at", id), 15*time.Second)

	// Drop the connection server-side.
	sess.drop()

	// Node should broadcast a disconnect marker.
	waitForMarker(t, clientCh, fmt.Sprintf("[Console %s disconnected at", id), 15*time.Second)

	// Server accepts the reconnect; node should broadcast another connected marker.
	<-sessionCh
	waitForMarker(t, clientCh, fmt.Sprintf("[Console %s connected at", id), 15*time.Second)
}

// ---------------------------------------------------------------------------
// 2. Fan-out broadcast to multiple clients
// ---------------------------------------------------------------------------

// TestSSHConsoleNodeFanOut verifies that output from the SSH session is
// delivered to every attached client and that a slow client (full buffer)
// only loses its own data — it does not block other clients.
func TestSSHConsoleNodeFanOut(t *testing.T) {
	sessionCh := make(chan *sshSession)
	srv := newSSHServer(t, func(ch gossh.Channel) { runSession(ch, sessionCh) })
	id, nodeMap, passwords := singleNode(t, srv.addr())

	nodes.SetNodesForTest(nodeMap)
	defer nodes.SetNodesForTest(nil)

	manager := newManager(t, t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := manager.UpdateNodes(ctx, nodeMap, passwords); err != nil {
		t.Fatal(err)
	}

	const numClients = 5
	clients := make([]<-chan []byte, numClients)
	for i := 0; i < numClients; i++ {
		ch, err := manager.Attach(id, fmt.Sprintf("fan-client-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		i := i
		t.Cleanup(func() { manager.Detach(id, fmt.Sprintf("fan-client-%d", i)) })
		clients[i] = ch
	}

	// Wait for connection, then drain the connected markers from all clients.
	sess := <-sessionCh
	for _, ch := range clients {
		waitForMarker(t, ch, fmt.Sprintf("[Console %s connected at", id), 15*time.Second)
	}

	// Server sends a known payload; every client must receive it.
	sess.send <- []byte("broadcast-test-payload\r\n")
	for i, ch := range clients {
		waitForMarker(t, ch, "broadcast-test-payload", 5*time.Second)
		t.Logf("client %d received payload", i)
	}

	// Slow-client test: fill one client's buffer so it is full, then verify
	// the fast client still receives new data unimpeded.
	slowCh, err := manager.Attach(id, "slow-client")
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Detach(id, "slow-client")

	fastCh, err := manager.Attach(id, "fast-client")
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Detach(id, "fast-client")

	filler := []byte(strings.Repeat("x", 1024))
	for i := 0; i < 64; i++ {
		sess.send <- filler
	}
	time.Sleep(200 * time.Millisecond) // let broadcast goroutine saturate the slow client

	sess.send <- []byte("after-slow-client-fill\r\n")
	waitForMarker(t, fastCh, "after-slow-client-fill", 5*time.Second)
	_ = slowCh // slow client may have dropped data — expected behaviour
}

// ---------------------------------------------------------------------------
// 3. Data flow: output reaches clients, input reaches server
// ---------------------------------------------------------------------------

// TestSSHConsoleNodeDataFlow verifies that bytes written by the server arrive
// in the client channel (output path) and bytes written by the client arrive
// at the server's stdin (input path).
func TestSSHConsoleNodeDataFlow(t *testing.T) {
	sessionCh := make(chan *sshSession)
	srv := newSSHServer(t, func(ch gossh.Channel) { runSession(ch, sessionCh) })
	id, nodeMap, passwords := singleNode(t, srv.addr())

	nodes.SetNodesForTest(nodeMap)
	defer nodes.SetNodesForTest(nil)

	manager := newManager(t, t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := manager.UpdateNodes(ctx, nodeMap, passwords); err != nil {
		t.Fatal(err)
	}

	clientCh, err := manager.Attach(id, "data-client")
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Detach(id, "data-client")

	sess := <-sessionCh
	waitForMarker(t, clientCh, fmt.Sprintf("[Console %s connected at", id), 15*time.Second)

	// Output path: server → client.
	sess.send <- []byte("hello-from-server\r\n")
	waitForMarker(t, clientCh, "hello-from-server", 5*time.Second)

	// Input path: client → server.
	if _, err := manager.Write(id, []byte("hello-from-client\r\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	var recvBuf strings.Builder
	for {
		select {
		case data := <-sess.recv:
			recvBuf.Write(data)
			if strings.Contains(recvBuf.String(), "hello-from-client") {
				t.Logf("server received: %q", recvBuf.String())
				return
			}
		case <-timer.C:
			t.Fatalf("server did not receive client input; got: %q", recvBuf.String())
		}
	}
}

// ---------------------------------------------------------------------------
// 4. Node lifecycle via UpdateNodes
// ---------------------------------------------------------------------------

// TestSSHConsoleNodeLifecycle verifies that removing a node via UpdateNodes
// cancels its Run goroutine and that all associated goroutines exit cleanly.
func TestSSHConsoleNodeLifecycle(t *testing.T) {
	sessionCh := make(chan *sshSession)
	srv := newSSHServer(t, func(ch gossh.Channel) { runSession(ch, sessionCh) })
	id, nodeMap, passwords := singleNode(t, srv.addr())

	nodes.SetNodesForTest(nodeMap)
	// Do NOT defer SetNodesForTest(nil) — we remove the node explicitly below.

	manager := newManager(t, t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	goroutinesBefore := runtime.NumGoroutine()

	if err := manager.UpdateNodes(ctx, nodeMap, passwords); err != nil {
		t.Fatal(err)
	}

	// Wait for the node to connect.
	<-sessionCh
	t.Logf("goroutines with node: %d (added %d)", runtime.NumGoroutine(), runtime.NumGoroutine()-goroutinesBefore)

	// Remove the node: update global map first so IsCurrentNode returns false,
	// then tell the manager.
	nodes.SetNodesForTest(nil)
	if err := manager.UpdateNodes(ctx, map[string]*nodes.NodeConsoleInfo{}, map[string]compcredentials.CompCredentials{}); err != nil {
		t.Fatal(err)
	}

	// Attach should now fail.
	if _, err := manager.Attach(id, "post-remove"); err == nil {
		t.Error("Attach succeeded after node removal")
	}

	// Goroutines should drain within a few seconds.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= goroutinesBefore+5 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	leaked := runtime.NumGoroutine() - goroutinesBefore
	t.Logf("goroutines after removal: delta=%d", leaked)
	if leaked > 10 {
		t.Errorf("goroutine leak after node removal: %d above baseline", leaked)
	}
}
