// Copyright © 2026 OpenCHAMI a Series of LF Projects, LLC
//
// SPDX-License-Identifier: MIT

package ssh

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	compcredentials "github.com/Cray-HPE/hms-compcredentials"
	gossh "golang.org/x/crypto/ssh"

	"github.com/OpenCHAMI/remote-console/internal/nodes"
)

// SSHConsoleNode manages a single persistent SSH console connection.
// Its Run goroutine is the sole writer for the log file and client channels —
// no lock is needed for those writes.
type SSHConsoleNode struct {
	nodeID  string
	info    *nodes.NodeConsoleInfo
	keyPath string
	cfg     SSHConfig

	credsMu sync.RWMutex
	creds   compcredentials.CompCredentials

	// clientsMu protects clients map.
	// Lock ordering: nodesMu (manager) → clientsMu → connMu
	clientsMu sync.RWMutex
	clients   map[string]chan []byte // closed and deleted on Detach or Run exit

	// connMu protects sshClient and stdin.
	connMu    sync.Mutex
	sshClient *gossh.Client
	stdin     io.WriteCloser

	// stdinMu serializes Write calls. golang.org/x/crypto/ssh reuses a per-channel
	// packet buffer (packetPool) across WriteExtended calls, so concurrent writes
	// to the same stdin pipe race. Multiple interactive clients may call Write
	// simultaneously; this mutex ensures they are serialized.
	stdinMu sync.Mutex

	// logFile is opened at the start of Run and written only from the Run goroutine.
	logFile     *os.File
	logPath     string    // logsPath + "/console." + nodeID
	reopenLogCh chan struct{} // buffered depth-1; signals broadcast to reopen after rotation

	// cancel is called by the manager to stop the Run goroutine.
	// ctx is NOT stored in the struct to avoid the go vet context-in-struct antipattern.
	cancel context.CancelFunc
}

// newSSHConsoleNode constructs an SSHConsoleNode. The caller (manager) must set
// node.cancel and call go node.Run(nodeCtx) after construction.
func newSSHConsoleNode(nodeID string, info *nodes.NodeConsoleInfo, creds compcredentials.CompCredentials, keyPath, logPath string, cfg SSHConfig) *SSHConsoleNode {
	return &SSHConsoleNode{
		nodeID:      nodeID,
		info:        info,
		keyPath:     keyPath,
		cfg:         cfg,
		creds:       creds,
		clients:     make(map[string]chan []byte),
		logPath:     logPath,
		reopenLogCh: make(chan struct{}, 1),
	}
}

// streamStdout reads from the SSH session stdout and broadcasts to all clients
// until the connection drops.
func (n *SSHConsoleNode) streamStdout(stdout io.Reader) {
	buf := make([]byte, 32*1024)
	for {
		nr, err := stdout.Read(buf)
		if nr > 0 {
			n.broadcast(buf[:nr])
		}
		if err != nil {
			break
		}
	}
}

// connectAndStream attempts one connection, streams output until disconnect,
// then cleans up. Returns true if the connection was established (used by Run
// to reset the backoff on successful connects).
func (n *SSHConsoleNode) connectAndStream(ctx context.Context) bool {
	stdout, err := n.connect(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return false
		}
		slog.Error("SSH console connect failed", "nodeID", n.nodeID, "error", err)
		n.broadcast([]byte(fmt.Sprintf("\n[Console %s connect failed: %v]\n", n.nodeID, err)))
		return false
	}

	// When ctx is cancelled, close the SSH client so streamStdout's Read
	// unblocks immediately instead of waiting for the remote side to hang up.
	ctxDone := make(chan struct{})
	defer close(ctxDone)
	go func() {
		select {
		case <-ctx.Done():
			n.connMu.Lock()
			if n.sshClient != nil {
				n.sshClient.Close()
			}
			n.connMu.Unlock()
		case <-ctxDone:
		}
	}()

	n.streamStdout(stdout)

	// Clean up the connection. Nil stdin under connMu so Write sees nil and
	// drops silently, then close it under stdinMu so an in-flight Write cannot
	// race with Close on the underlying SSH channel buffer.
	n.connMu.Lock()
	if n.sshClient != nil {
		n.sshClient.Close()
		n.sshClient = nil
	}
	stdin := n.stdin
	n.stdin = nil
	n.connMu.Unlock()
	if stdin != nil {
		n.stdinMu.Lock()
		stdin.Close()
		n.stdinMu.Unlock()
	}

	n.broadcast([]byte(fmt.Sprintf("\n[Console %s disconnected at %s]\n",
		n.nodeID, time.Now().Format(time.RFC3339))))
	return true
}

// Run is the main loop for the node. It connects, reads output, fans it out,
// and reconnects on disconnect. It exits when ctx is cancelled or the node
// is removed from inventory.
func (n *SSHConsoleNode) Run(ctx context.Context) {
	var err error
	n.logFile, err = os.OpenFile(n.logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		slog.Error("Failed to open SSH console log file", "nodeID", n.nodeID, "path", n.logPath, "error", err)
		// Continue with nil logFile; broadcast skips write when nil.
	}
	defer func() {
		if n.logFile != nil {
			n.logFile.Close()
		}
		// Close and delete all client channels so streamOutput goroutines
		// see ok=false and exit cleanly.
		n.clientsMu.Lock()
		for id, ch := range n.clients {
			close(ch)
			delete(n.clients, id)
		}
		n.clientsMu.Unlock()
	}()

	backoff := n.cfg.ReconnectMinInterval

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !nodes.IsCurrentNode(n.nodeID) {
			slog.Info("Node removed from inventory, stopping SSH console", "nodeID", n.nodeID)
			return
		}

		if connected := n.connectAndStream(ctx); connected {
			backoff = n.cfg.ReconnectMinInterval
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > n.cfg.ReconnectMaxInterval {
			backoff = n.cfg.ReconnectMaxInterval
		}
	}
}

// dialClient reads credentials, dials TCP, and completes the SSH handshake.
func (n *SSHConsoleNode) dialClient(ctx context.Context) (*gossh.Client, error) {
	n.credsMu.RLock()
	creds := n.creds
	n.credsMu.RUnlock()

	auth, err := n.buildAuth(creds)
	if err != nil {
		return nil, fmt.Errorf("build SSH auth: %w", err)
	}

	port := n.info.ConnectionPort
	if port == 0 {
		port = 22
	}
	addr := fmt.Sprintf("%s:%d", n.info.ConnectionHost, port)

	dialer := &net.Dialer{}
	if n.cfg.TCPKeepAlive > 0 {
		dialer.KeepAlive = n.cfg.TCPKeepAlive
	}
	netConn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	sshConn, chans, reqs, err := gossh.NewClientConn(netConn, addr, &gossh.ClientConfig{
		User:            creds.Username,
		Auth:            auth,
		HostKeyCallback: gossh.InsecureIgnoreHostKey(), //nolint:gosec // BMC management network; matches existing StrictHostKeyChecking=no
	})
	if err != nil {
		netConn.Close()
		return nil, fmt.Errorf("SSH handshake with %s: %w", addr, err)
	}

	return gossh.NewClient(sshConn, chans, reqs), nil
}

// startSession opens a PTY session on client, wires up stdout/stdin pipes, and
// starts the shell or entry command. Returns stdout and stdin on success.
func (n *SSHConsoleNode) startSession(client *gossh.Client) (stdout io.Reader, stdin io.WriteCloser, err error) {
	session, err := client.NewSession()
	if err != nil {
		return nil, nil, fmt.Errorf("new SSH session: %w", err)
	}

	if err := session.RequestPty(n.cfg.TerminalType, 24, 80, gossh.TerminalModes{
		gossh.ECHO:          1,
		gossh.TTY_OP_ISPEED: 115200, // matches conman seropts="115200,8n1"
		gossh.TTY_OP_OSPEED: 115200,
	}); err != nil {
		session.Close()
		return nil, nil, fmt.Errorf("request PTY: %w", err)
	}

	// StdoutPipe and StdinPipe must be called before Shell/Start.
	stdout, err = session.StdoutPipe()
	if err != nil {
		session.Close()
		return nil, nil, fmt.Errorf("stdout pipe: %w", err)
	}
	stdin, err = session.StdinPipe()
	if err != nil {
		session.Close()
		return nil, nil, fmt.Errorf("stdin pipe: %w", err)
	}

	if n.info.ConsoleEntryCommand == "" {
		err = session.Shell()
	} else {
		err = session.Start(n.info.ConsoleEntryCommand)
	}
	if err != nil {
		session.Close()
		return nil, nil, fmt.Errorf("start SSH session: %w", err)
	}

	// Reap the session when it ends to release server-side resources.
	go func() {
		if err := session.Wait(); err != nil {
			slog.Debug("SSH session ended", "nodeID", n.nodeID, "error", err)
		}
	}()

	return stdout, stdin, nil
}

// connect dials the remote host, opens a PTY session, and stores connection
// state. Returns the stdout reader on success; caller must drain it until
// error/EOF.
func (n *SSHConsoleNode) connect(ctx context.Context) (io.Reader, error) {
	client, err := n.dialClient(ctx)
	if err != nil {
		return nil, err
	}

	// Track whether connect succeeds so the defer can clean up on failure.
	success := false
	defer func() {
		if !success {
			n.connMu.Lock()
			n.sshClient = nil
			n.stdin = nil
			n.connMu.Unlock()
			client.Close()
		}
	}()

	n.connMu.Lock()
	n.sshClient = client
	n.connMu.Unlock()

	stdout, stdin, err := n.startSession(client)
	if err != nil {
		return nil, err
	}

	n.connMu.Lock()
	n.stdin = stdin
	n.connMu.Unlock()

	n.broadcast([]byte(fmt.Sprintf("\n[Console %s connected at %s]\n",
		n.nodeID, time.Now().Format(time.RFC3339))))

	success = true
	return stdout, nil
}

// buildAuth selects the SSH auth method based on credentials and keyPath.
// Priority matches the original ssh-pwd-console / ssh-key-console scripts:
//   - Password set → password auth (even when a key file is also present)
//   - No password   → key auth: cert+key if cert exists, key-only otherwise
func (n *SSHConsoleNode) buildAuth(creds compcredentials.CompCredentials) ([]gossh.AuthMethod, error) {
	// Password takes priority — matches ssh-pwd-console behaviour.
	if creds.Password != "" {
		return []gossh.AuthMethod{gossh.Password(creds.Password)}, nil
	}

	// No password: attempt key-based auth — matches ssh-key-console behaviour.
	if n.keyPath == "" {
		return nil, fmt.Errorf("no SSH auth available for %s: no password and no key path configured", n.nodeID)
	}

	keyData, err := os.ReadFile(n.keyPath)
	if err != nil {
		return nil, fmt.Errorf("read SSH key %s: %w", n.keyPath, err)
	}
	signer, err := gossh.ParsePrivateKey(keyData)
	if err != nil {
		return nil, fmt.Errorf("parse SSH private key: %w", err)
	}

	// Check for a certificate alongside the key.
	certPath := n.keyPath + "-cert.pub"
	certData, err := os.ReadFile(certPath)
	if err == nil {
		pubKey, _, _, _, err := gossh.ParseAuthorizedKey(certData)
		if err != nil {
			return nil, fmt.Errorf("parse SSH certificate: %w", err)
		}
		cert, ok := pubKey.(*gossh.Certificate)
		if !ok {
			return nil, fmt.Errorf("file %s is not an SSH certificate", certPath)
		}
		certSigner, err := gossh.NewCertSigner(cert, signer)
		if err != nil {
			return nil, fmt.Errorf("create cert signer: %w", err)
		}
		return []gossh.AuthMethod{gossh.PublicKeys(certSigner)}, nil
	}

	// Key only (no cert).
	return []gossh.AuthMethod{gossh.PublicKeys(signer)}, nil
}

// broadcast sends data to the log file and all attached client channels.
// Called only from the Run goroutine — no lock needed for logFile writes.
func (n *SSHConsoleNode) broadcast(data []byte) {
	// Handle log rotation signal (non-blocking, depth-1 channel debounces).
	select {
	case <-n.reopenLogCh:
		if n.logFile != nil {
			n.logFile.Close()
		}
		var err error
		n.logFile, err = os.OpenFile(n.logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			slog.Error("Failed to reopen SSH console log after rotation", "nodeID", n.nodeID, "error", err)
			n.logFile = nil
		}
	default:
	}

	if n.logFile != nil {
		if _, err := n.logFile.Write(data); err != nil {
			slog.Warn("Failed to write to SSH console log", "nodeID", n.nodeID, "error", err)
		}
	}

	n.clientsMu.RLock()
	defer n.clientsMu.RUnlock()
	for id, ch := range n.clients {
		select {
		case ch <- append([]byte{}, data...):
		default:
			slog.Warn("SSH console client channel full, dropping data", "nodeID", n.nodeID, "clientID", id)
		}
	}
}

// Attach registers a client receive channel. Returns a buffered channel that
// receives console output. The manager ensures the node exists before calling.
func (n *SSHConsoleNode) Attach(clientID string) chan []byte {
	ch := make(chan []byte, 64)
	n.clientsMu.Lock()
	n.clients[clientID] = ch
	n.clientsMu.Unlock()
	return ch
}

// Detach deregisters a client channel. Idempotent.
func (n *SSHConsoleNode) Detach(clientID string) {
	n.clientsMu.Lock()
	defer n.clientsMu.Unlock()
	if ch, ok := n.clients[clientID]; ok {
		close(ch)
		delete(n.clients, clientID)
	}
}

// Write sends data to the SSH session's stdin. Returns len(p), nil silently
// when disconnected so callers do not treat transient disconnects as fatal.
func (n *SSHConsoleNode) Write(p []byte) (int, error) {
	n.stdinMu.Lock()
	defer n.stdinMu.Unlock()

	n.connMu.Lock()
	stdin := n.stdin
	n.connMu.Unlock()

	if stdin == nil {
		return len(p), nil // silent drop during reconnect
	}
	return stdin.Write(p)
}

// UpdateCreds updates stored credentials and forces a reconnect so the new
// credentials take effect.
func (n *SSHConsoleNode) UpdateCreds(creds compcredentials.CompCredentials) {
	n.credsMu.Lock()
	n.creds = creds
	n.credsMu.Unlock()

	// Close the current connection to trigger reconnect with new creds.
	n.connMu.Lock()
	client := n.sshClient
	n.connMu.Unlock()
	if client != nil {
		client.Close()
	}
}

// ReopenLog signals the Run goroutine to reopen the log file (after rotation).
func (n *SSHConsoleNode) ReopenLog() {
	select {
	case n.reopenLogCh <- struct{}{}:
	default: // already signalled; no-op
	}
}
