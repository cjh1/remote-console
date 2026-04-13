// Copyright © 2026 OpenCHAMI a Series of LF Projects, LLC
//
// SPDX-License-Identifier: MIT

package ssh_test

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	compcredentials "github.com/Cray-HPE/hms-compcredentials"
	gossh "golang.org/x/crypto/ssh"

	"github.com/OpenCHAMI/remote-console/internal/nodes"
	"github.com/OpenCHAMI/remote-console/internal/ssh"
)

// TestSSHConsoleManagerConcurrent exercises concurrent access to SSHConsoleManager
// and SSHConsoleNode. Run with -race to catch synchronisation bugs:
//
//	go test -race -v -run TestSSHConsoleManagerConcurrent ./internal/ssh/ -timeout 60s
//
// Workers run simultaneously for concurrentDuration, each hammering a different
// surface area:
//
//	attachWorker  – Attach, drain a few bytes, Detach (stresses clientsMu)
//	writeWorker   – Write to random nodes while connections may be tearing down
//	                (stresses connMu)
//	credsWorker   – UpdateCredentials in a tight loop (stresses credsMu + connMu)
//	rotateWorker  – ReopenLogs repeatedly (stresses reopenLogCh vs broadcast)
//	updateWorker  – UpdateNodes with a shifting node set (stresses nodesMu vs
//	                everything else)
const (
	concurrentNodes    = 100
	concurrentWorkers  = 20
	concurrentDuration = 30 * time.Second
)

func TestSSHConsoleManagerConcurrent(t *testing.T) {
	srv := newSSHServer(t, func(ch gossh.Channel) {
		_, _ = io.Copy(io.Discard, ch)
		ch.Close()
	})
	sshHost, sshPort := nodeAddr(t, srv.addr())

	makeNodeMap := func(ids []string) map[string]*nodes.NodeConsoleInfo {
		m := make(map[string]*nodes.NodeConsoleInfo, len(ids))
		for _, id := range ids {
			id := id
			m[id] = &nodes.NodeConsoleInfo{
				ID:             id,
				ConnectionType: nodes.SSH,
				ConnectionHost: sshHost,
				ConnectionPort: sshPort,
			}
		}
		return m
	}
	makePasswords := func(ids []string) map[string]compcredentials.CompCredentials {
		m := make(map[string]compcredentials.CompCredentials, len(ids))
		for _, id := range ids {
			m[id] = compcredentials.CompCredentials{
				Username: testSSHUser,
				Password: testSSHPass,
			}
		}
		return m
	}

	allIDs := make([]string, concurrentNodes)
	for i := range allIDs {
		allIDs[i] = fmt.Sprintf("concurrent-node-%d", i)
	}
	nodeMap := makeNodeMap(allIDs)
	passwords := makePasswords(allIDs)

	nodes.SetNodesForTest(nodeMap)
	defer nodes.SetNodesForTest(nil)

	cfg := ssh.DefaultSSHConfig()
	cfg.TCPKeepAlive = 0
	cfg.ReconnectMinInterval = 100 * time.Millisecond
	cfg.ReconnectMaxInterval = 500 * time.Millisecond

	manager := ssh.NewSSHConsoleManager(cfg, "", t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := manager.UpdateNodes(ctx, nodeMap, passwords); err != nil {
		t.Fatalf("initial UpdateNodes: %v", err)
	}

	// Wait until all nodes are registered (Attach succeeds) before starting workers.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		allUp := true
		for _, id := range allIDs {
			ch, err := manager.Attach(id, "probe")
			if err != nil {
				allUp = false
				break
			}
			manager.Detach(id, "probe")
			_ = ch
		}
		if allUp {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	goroutinesBefore := runtime.NumGoroutine()
	end := time.Now().Add(concurrentDuration)
	var wg sync.WaitGroup

	// attachWorker: Attach → drain a few messages → Detach in a tight loop.
	// Stresses clientsMu against broadcast from the Run goroutine.
	for i := 0; i < concurrentWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(workerID)))
			clientID := fmt.Sprintf("attach-worker-%d", workerID)
			for time.Now().Before(end) {
				id := allIDs[rng.Intn(len(allIDs))]
				ch, err := manager.Attach(id, clientID)
				if err != nil {
					// Node may have been temporarily removed by updateWorker.
					time.Sleep(time.Millisecond)
					continue
				}
				// Drain up to 3 queued messages then detach.
				for i := 0; i < 3; i++ {
					select {
					case _, ok := <-ch:
						if !ok {
							i = 3 // channel closed, exit drain
						}
					case <-time.After(5 * time.Millisecond):
						i = 3
					}
				}
				manager.Detach(id, clientID)
			}
		}(i)
	}

	// writeWorker: Write to random nodes.
	// Stresses connMu against connect/disconnect cycling.
	for i := 0; i < concurrentWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(workerID + 1000)))
			payload := fmt.Appendf(nil, "worker-%d-ping\r\n", workerID)
			for time.Now().Before(end) {
				id := allIDs[rng.Intn(len(allIDs))]
				if _, err := manager.Write(id, payload); err != nil {
					// Node removed — not a bug.
					time.Sleep(time.Millisecond)
				}
			}
		}(i)
	}

	// credsWorker: UpdateCredentials in a tight loop.
	// Stresses credsMu and the connMu path inside UpdateCreds.
	for i := 0; i < concurrentWorkers/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			updated := makePasswords(allIDs)
			for time.Now().Before(end) {
				manager.UpdateCredentials(updated)
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}

	// rotateWorker: Signal log rotation.
	// Stresses the reopenLogCh non-blocking send against broadcast.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for time.Now().Before(end) {
			manager.ReopenLogs()
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// updateWorker: Shuffle the node set — remove a few nodes and restore them.
	// Stresses nodesMu against Attach/Detach/Write happening concurrently.
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(42))
		for time.Now().Before(end) {
			// Drop 1–3 nodes.
			drop := rng.Intn(3) + 1
			skipIdx := make(map[int]bool, drop)
			for len(skipIdx) < drop {
				skipIdx[rng.Intn(len(allIDs))] = true
			}
			subset := make([]string, 0, len(allIDs)-drop)
			for i, id := range allIDs {
				if !skipIdx[i] {
					subset = append(subset, id)
				}
			}
			subMap := makeNodeMap(subset)
			nodes.SetNodesForTest(subMap)
			if err := manager.UpdateNodes(ctx, subMap, makePasswords(subset)); err != nil {
				t.Errorf("UpdateNodes (subset): %v", err)
			}
			time.Sleep(20 * time.Millisecond)

			// Restore the full set.
			nodes.SetNodesForTest(nodeMap)
			if err := manager.UpdateNodes(ctx, nodeMap, passwords); err != nil {
				t.Errorf("UpdateNodes (full): %v", err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	wg.Wait()

	// Shut down and verify goroutines drain.
	cancel()
	time.Sleep(5 * time.Second)

	goroutinesAfter := runtime.NumGoroutine()
	leaked := goroutinesAfter - goroutinesBefore
	t.Logf("Goroutines before workers: %d, after shutdown: %d, delta: %d",
		goroutinesBefore, goroutinesAfter, leaked)
	if leaked > 20 {
		t.Errorf("possible goroutine leak after shutdown: %d goroutines above pre-worker baseline", leaked)
	}
}
