// Copyright © 2026 OpenCHAMI a Series of LF Projects, LLC
//
// SPDX-License-Identifier: MIT

package console

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/OpenCHAMI/remote-console/internal/nodes"
	"github.com/OpenCHAMI/remote-console/internal/ssh"
	"github.com/gorilla/websocket"
	"github.com/nxadm/tail/ratelimiter"
)

// interactiveConsoleSession manages the lifecycle of an interactive console session.
type interactiveConsoleSession struct {
	io          consoleIO
	nodeID      string
	cancel      context.CancelFunc
	ws          *webSocketSession        // WebSocket session
	rateLimiter *ratelimiter.LeakyBucket // Rate limit console output
	wg          sync.WaitGroup           // Tracks all goroutines
}

// close shuts down the session. Safe to call multiple times.
func (s *interactiveConsoleSession) close() {
	s.closeWithReason(sessionCloseNormal, "")
}

// closeWithReason shuts down the session with a specific WebSocket close reason.
func (s *interactiveConsoleSession) closeWithReason(reason sessionCloseReason, message string) {
	slog.Info("Closing interactive console session", "nodeID", s.nodeID)

	// 1. Shut down the backend (SSH detach or PTY SIGTERM). Idempotent.
	if err := s.io.Close(); err != nil {
		slog.Debug("Error closing console IO", "nodeID", s.nodeID, "error", err)
	}

	// 2. Cancel the session context to unblock both goroutines.
	if s.cancel != nil {
		s.cancel()
	}

	// 3. Send a WebSocket close frame. This causes writePump to close the
	//    underlying TCP connection, which unblocks streamInput's ReadMessage.
	s.ws.close(reason, message)
}

// streamOutput reads console output from the backend and writes it to the WebSocket.
func (s *interactiveConsoleSession) streamOutput(ctx context.Context) {
	defer s.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.ws.Done():
			return
		case data, ok := <-s.io.Output():
			if !ok {
				// Backend shut down permanently.
				s.close()
				return
			}

			// Apply rate limiting.
			kb := uint16((len(data) + 1023) / 1024)
			for !s.rateLimiter.Pour(kb) {
				select {
				case <-ctx.Done():
					return
				case <-s.ws.Done():
					return
				default:
					time.Sleep(100 * time.Millisecond) // Wait for bucket to drain
				}
			}

			if err := s.ws.Write(websocket.BinaryMessage, data); err != nil {
				// WebSocket closed/cancelled, exit gracefully
				slog.Info("WebSocket write failed, closing session", "nodeID", s.nodeID, "error", err)
				return
			}
		}
	}
}

// streamInput reads from the WebSocket and writes user input to the console backend.
func (s *interactiveConsoleSession) streamInput(ctx context.Context) {
	defer s.wg.Done()

	for {
		select {
		// Check for session closure
		case <-ctx.Done():
			return
		// Check for WebSocket closure
		case <-s.ws.Done():
			return
		default:
		}

		messageType, message, err := s.ws.Read()
		if err != nil {
			// Check if it's an unexpected close (not normal, going away, or abnormal)
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseNormalClosure,
				websocket.CloseGoingAway,
				websocket.CloseAbnormalClosure,
			) {
				slog.Warn("WebSocket unexpected close", "nodeID", s.nodeID, "error", err)
				s.closeWithReason(sessionCloseError, "websocket read failed")
			} else {
				slog.Info("WebSocket closed normally", "nodeID", s.nodeID)
				s.close()
			}
			return
		}

		if messageType == websocket.TextMessage || messageType == websocket.BinaryMessage {
			// Both backends return len(p), nil when disconnected (silent drop),
			// so write errors here indicate a genuine problem worth logging.
			if _, err := s.io.Write(message); err != nil {
				slog.Error("Failed to write to console backend", "nodeID", s.nodeID, "error", err)
				s.closeWithReason(sessionCloseError, "failed to write to console")
				return
			}
		}
	}
}

// Start launches the session goroutines and blocks until both exit.
func (s *interactiveConsoleSession) Start() {
	sessionCtx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	// Start WebSocket session
	s.ws.Start()

	// Start I/O goroutines
	s.wg.Add(2)
	go s.streamInput(sessionCtx)
	go s.streamOutput(sessionCtx)

	// Wait for I/O goroutines to complete
	s.wg.Wait()
}

func newInteractiveConsoleSession(nodeID string, conn *websocket.Conn, cio consoleIO) *interactiveConsoleSession {
	session := &interactiveConsoleSession{
		io:          cio,
		nodeID:      nodeID,
		rateLimiter: ratelimiter.NewLeakyBucket(rateLimitBurstKB, rateLimitInterval),
	}

	session.ws = newWebSocketSession(conn, fmt.Sprintf("interactive session %s", nodeID))

	return session
}

func doInteractiveConsole(w http.ResponseWriter, r *http.Request, sshMgr *ssh.SSHConsoleManager) {
	// Make sure the request is cleaned up
	defer drainAndCloseRequestBody(r)

	nodeID, err := extractNodeId(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Make sure we are monitoring a valid node
	node := nodes.CurrentNodes()[nodeID]
	if node == nil {
		http.Error(w, "Node doesn't exist", http.StatusNotFound)
		return
	}
	useSSH := node.IsSSH()

	slog.Info("Starting interactive console session", "nodeID", nodeID, "backend", map[bool]string{true: "ssh", false: "conman"}[useSSH])

	// Upgrade HTTP connection to WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("Failed to upgrade WebSocket connection", "nodeID", nodeID, "error", err)
		// Can't send HTTP error after upgrade attempt
		return
	}

	// From here on, errors must be sent via WebSocket close frames
	var cio consoleIO
	if useSSH {
		cio, err = newSSHConsoleIO(nodeID, sshMgr)
	} else {
		cio, err = newConmanConsoleIO(nodeID)
	}
	if err != nil {
		slog.Error("Failed to create console IO backend", "nodeID", nodeID, "error", err)
		_ = conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "failed to connect to console"))
		conn.Close()
		return
	}

	session := newInteractiveConsoleSession(nodeID, conn, cio)
	defer session.close() // Ensure cleanup always happens

	// Start session (blocks until all goroutines complete)
	session.Start()

	slog.Info("Interactive console session ended", "nodeID", nodeID)
}
