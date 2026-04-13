// Copyright © 2026 OpenCHAMI a Series of LF Projects, LLC
//
// SPDX-License-Identifier: MIT

package ssh

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"

	"github.com/Cray-HPE/hms-compcredentials"

	"github.com/OpenCHAMI/remote-console/internal/nodes"
)

// SSHConsoleManager manages the set of active SSHConsoleNodes.
type SSHConsoleManager struct {
	cfg      SSHConfig
	keyPath  string
	logsPath string // e.g. /var/log/conman/conman

	// nodesMu protects the nodes map.
	// Lock ordering: nodesMu → clientsMu, nodesMu → connMu
	nodesMu    sync.RWMutex
	nodes map[string]*SSHConsoleNode
	// cancels maps nodeID to the cancel func for that node's Run goroutine.
	cancels    map[string]context.CancelFunc
}

// NewSSHConsoleManager creates a new manager. keyPath is the SSH private key
// path (may be empty if all nodes use password auth). logsPath is the directory
// where per-node console log files are written.
func NewSSHConsoleManager(cfg SSHConfig, keyPath, logsPath string) *SSHConsoleManager {
	return &SSHConsoleManager{
		cfg:         cfg,
		keyPath:     keyPath,
		logsPath:    logsPath,
		nodes: make(map[string]*SSHConsoleNode),
		cancels:     make(map[string]context.CancelFunc),
	}
}

// logPath returns the log file path for a node.
func (m *SSHConsoleManager) logPath(nodeID string) string {
	return filepath.Join(m.logsPath, "console."+nodeID)
}

// nodeChanged reports whether a node's connection parameters have changed.
func nodeChanged(existing *nodes.NodeConsoleInfo, updated *nodes.NodeConsoleInfo) bool {
	return existing.ConnectionHost != updated.ConnectionHost ||
		existing.ConnectionPort != updated.ConnectionPort ||
		existing.ConsoleEntryCommand != updated.ConsoleEntryCommand
}

// credsChanged reports whether credentials have changed.
func credsChanged(a, b compcredentials.CompCredentials) bool {
	return a.Username != b.Username || a.Password != b.Password
}

// UpdateNodes diffs the provided SSH node map against the currently active set.
// New nodes are started, removed nodes are cancelled, changed nodes are restarted,
// and nodes with only credential changes get UpdateCreds called.
// ctx must be the long-lived service context — node goroutines run until it is
// cancelled or the node is explicitly stopped.
func (m *SSHConsoleManager) UpdateNodes(
	ctx context.Context,
	sshNodes map[string]*nodes.NodeConsoleInfo,
	passwords map[string]compcredentials.CompCredentials,
) error {
	m.nodesMu.Lock()
	defer m.nodesMu.Unlock()

	// Stop nodes that are no longer in the updated set.
	for nodeID, cancel := range m.cancels {
		if _, stillActive := sshNodes[nodeID]; !stillActive {
			slog.Info("Stopping SSH console node", "nodeID", nodeID)
			cancel()
			delete(m.nodes, nodeID)
			delete(m.cancels, nodeID)
		}
	}

	for nodeID, info := range sshNodes {
		creds := passwords[nodeID]

		existing, exists := m.nodes[nodeID]
		if !exists {
			// New node.
			m.startNode(ctx, nodeID, info, creds)
			continue
		}

		if nodeChanged(existing.info, info) {
			// Connection parameters changed — restart.
			slog.Info("SSH console node parameters changed, restarting", "nodeID", nodeID)
			m.cancels[nodeID]()
			m.startNode(ctx, nodeID, info, creds)
			continue
		}

		if credsChanged(existing.creds, creds) {
			// Credentials only — reconnect in place without full restart.
			slog.Info("SSH console node credentials changed, reconnecting", "nodeID", nodeID)
			existing.UpdateCreds(creds)
		}
	}

	return nil
}

// startNode creates and starts a new SSHConsoleNode. Must be called with nodesMu held.
func (m *SSHConsoleManager) startNode(ctx context.Context, nodeID string, info *nodes.NodeConsoleInfo, creds compcredentials.CompCredentials) {
	nodeCtx, nodeCancel := context.WithCancel(ctx)
	node := newSSHConsoleNode(nodeID, info, creds, m.keyPath, m.logPath(nodeID), m.cfg)
	node.cancel = nodeCancel
	m.nodes[nodeID] = node
	m.cancels[nodeID] = nodeCancel
	slog.Info("Starting SSH console node", "nodeID", nodeID, "host", info.ConnectionHost)
	go node.Run(nodeCtx)
}

// UpdateCredentials updates credentials for nodes whose passwords have changed.
// Nodes that changed connection info should use UpdateNodes instead.
func (m *SSHConsoleManager) UpdateCredentials(passwords map[string]compcredentials.CompCredentials) {
	m.nodesMu.RLock()
	type pending struct {
		node  *SSHConsoleNode
		creds compcredentials.CompCredentials
	}
	var updates []pending
	for nodeID, node := range m.nodes {
		if creds, ok := passwords[nodeID]; ok {
			if credsChanged(node.creds, creds) {
				updates = append(updates, pending{node, creds})
			}
		}
	}
	m.nodesMu.RUnlock()

	for _, u := range updates {
		u.node.UpdateCreds(u.creds)
	}
}

// ReopenLogs signals all active nodes to reopen their log files after rotation.
func (m *SSHConsoleManager) ReopenLogs() {
	m.nodesMu.RLock()
	defer m.nodesMu.RUnlock()
	for _, node := range m.nodes {
		node.ReopenLog()
	}
}

// Attach connects an interactive client to an SSH console node.
// Returns an error if the node is not managed by this manager.
func (m *SSHConsoleManager) Attach(nodeID, clientID string) (chan []byte, error) {
	m.nodesMu.RLock()
	node, ok := m.nodes[nodeID]
	m.nodesMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("SSH console node %q not found", nodeID)
	}
	return node.Attach(clientID), nil
}

// Detach disconnects a client from an SSH console node. No-op if not found.
func (m *SSHConsoleManager) Detach(nodeID, clientID string) {
	m.nodesMu.RLock()
	node, ok := m.nodes[nodeID]
	m.nodesMu.RUnlock()
	if ok {
		node.Detach(clientID)
	}
}

// Write sends data to the SSH session stdin for a node.
func (m *SSHConsoleManager) Write(nodeID string, p []byte) (int, error) {
	m.nodesMu.RLock()
	node, ok := m.nodes[nodeID]
	m.nodesMu.RUnlock()
	if !ok {
		return 0, fmt.Errorf("SSH console node %q not found", nodeID)
	}
	return node.Write(p)
}
