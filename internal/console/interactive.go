package console

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/OpenCHAMI/remote-console/internal/nodes"
	"github.com/creack/pty"
	"github.com/gorilla/websocket"
	"github.com/nxadm/tail/ratelimiter"
)

// interactiveConsoleSession manages the lifecycle of an interactive console session
type interactiveConsoleSession struct {
	cmd         *exec.Cmd
	ptmx        *os.File
	ptmxMutex   sync.RWMutex // Protects ptmx during reconnection
	nodeID      string
	
	// Context for coordinating shutdown across all goroutines
	ctx         context.Context
	cancel      context.CancelFunc

	ws            *webSocketSession	       // WebSocket session
	rateLimiter   *ratelimiter.LeakyBucket // Rate limit console output
	wg            sync.WaitGroup           // Tracks all goroutines
	processExited chan struct{}            // Closed when current conman process exits
}

// Close performs graceful shutdown of the console session
// This method is idempotent and safe to call multiple times
func (s *interactiveConsoleSession) Close() {
	slog.Info("Starting close for console session", "nodeID", s.nodeID)

	// Cancel context to signal all goroutines to stop 
	s.cancel()

	// Try graceful disconnect via ConMan escape sequence
	s.ptmxMutex.RLock()
	ptmx := s.ptmx
	s.ptmxMutex.RUnlock()
	
	if ptmx != nil {
		slog.Info("Sending ConMan escape sequence (&.) to disconnect from console", "nodeID", s.nodeID)
		// Ignore write errors - PTY might already be closed
		ptmx.Write([]byte("&."))
		time.Sleep(100 * time.Millisecond) // Brief pause to let it process
	}

	// Signal process termination (idempotent - safe to signal multiple times)
	if s.cmd != nil && s.cmd.Process != nil {
		slog.Info("Sending SIGTERM to conman process for console", "nodeID", s.nodeID)
		// Ignore signal errors - process might already be dead
		s.cmd.Process.Signal(syscall.SIGTERM)
	}

	// Close PTY - this will cause streamOutput to exit
	s.ptmxMutex.Lock()
	if s.ptmx != nil {
		// Close returns error if already closed, but that's fine
		s.ptmx.Close()
		s.ptmx = nil
	}
	s.ptmxMutex.Unlock()

	// Close WebSocket 
	s.ws.Close()

	slog.Info("Close completed for console session", "nodeID", s.nodeID)
}

// monitorProcess watches for process exit and attempts reconnection if node still exists
// This runs in a loop, monitoring each new process after successful reconnection
func (s *interactiveConsoleSession) monitorProcess() {
	for {
		<-s.processExited
		slog.Info("Conman process exited for console", "nodeID", s.nodeID)
		
		// Wait before reconnecting to prevent tight loop
		select {
		case <-time.After(time.Second):
			// Continue to reconnection
		case <-s.ctx.Done():
			slog.Info("Session closing during reconnect delay, stopping monitor for console", "nodeID", s.nodeID)
			return
		}
		
		// Check if the node still exists (might have been updated/changed)
		if !nodes.IsCurrentNode(s.nodeID) {
			slog.Info("Node no longer exists, closing session", "nodeID", s.nodeID)
			s.Close()
			return
		}
		
		slog.Info("Node still exists, attempting to reconnect", "nodeID", s.nodeID)
		s.reconnect()
		
		// Check if session is closing after reconnect attempt
		select {
		case <-s.ctx.Done():
			slog.Info("Session closing after reconnect, stopping monitor for console", "nodeID", s.nodeID)
			return
		default:
		}
	}
}

// startConmanProcess starts a new conman process with PTY
func (s *interactiveConsoleSession) startConmanProcess() error {
	// Check if session is closing
	select {
	case <-s.ctx.Done():
		return fmt.Errorf("session closing, cannot start conman process: %w", s.ctx.Err())
	default:
	}
	
	s.cmd = exec.Command("conman", s.nodeID)

	ptmx, err := pty.Start(s.cmd)
	if err != nil {
		return fmt.Errorf("failed to start conman with PTY: %w", err)
	}

	s.ptmxMutex.Lock()
	s.ptmx = ptmx
	s.ptmxMutex.Unlock()

	// Immediately start waiting on the process to avoid zombies
	s.processExited = make(chan struct{})
	go func() {
		s.cmd.Wait()
		// Notify monitorProcess of exit
		close(s.processExited)
	}()

	return nil
}

// reconnect attempts to restart the conman process and reconnect streams
// Logs errors but does not fail - monitorProcess will retry on next process exit
func (s *interactiveConsoleSession) reconnect() {

	// Notify user via WebSocket
	reconnectMsg := fmt.Sprintf("\n[Reconnecting to %s...]\n", s.nodeID)
	err := s.ws.Write(websocket.TextMessage, []byte(reconnectMsg))
	if err != nil {
		slog.Warn("WebSocket write failed, closing session", "nodeID", s.nodeID, "error", err)
		s.Close()
		return
	}

	// Close old PTY if it exists
	s.ptmxMutex.Lock()
	if s.ptmx != nil {
		s.ptmx.Close()
	}
	s.ptmxMutex.Unlock()

	// Try to start conman again
	slog.Info("Attempting to reconnect conman", "nodeID", s.nodeID)
	
	if err := s.startConmanProcess(); err != nil {
		slog.Warn("Failed to start conman", "nodeID", s.nodeID, "error", err)
		// Don't close - let monitorProcess retry
		return
	}

	// Process started successfully
	slog.Info("Successfully started conman for console", "nodeID", s.nodeID)

	// Restart output streaming
	// The console output itself will indicate when we're truly connected
	// Track this new goroutine in the main WaitGroup
	s.wg.Add(1)
	go s.streamOutput()
	// Note: streamInput is already running and will continue to work with the new PTY
}

// isEIO checks if an error is an I/O error (EIO)
// This happens when reading from a PTY after the process has been killed
func isEIO(err error) bool {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno == syscall.EIO
	}

	return false
}

// streamOutput reads from PTY and writes to WebSocket
func (s *interactiveConsoleSession) streamOutput() {
	defer s.wg.Done()

	buf := make([]byte, 4096)
	for {
		s.ptmxMutex.RLock()
		if s.ptmx == nil {
			s.ptmxMutex.RUnlock()
			slog.Debug("PTY is nil, exiting streamOutput for console", "nodeID", s.nodeID)
			return
		}
		
		n, err := s.ptmx.Read(buf)
		s.ptmxMutex.RUnlock()
		if err != nil {
			// Don't log I/O errors - they're expected when the process is killed
			if err != io.EOF && !isEIO(err) {
				slog.Error("Error reading from PTY", "nodeID", s.nodeID, "error", err)
			}
			// PTY closed, exit gracefully without calling close() (close() already closed PTY)
			return
		}

		if n > 0 {
			slog.Debug("PTY read", "nodeID", s.nodeID, "bytes", n, "data", string(buf[:n]))
			
			// Apply rate limiting (convert bytes to KB, rounded up)
			kb := uint16((n + 1023) / 1024)
			for !s.rateLimiter.Pour(kb) {
				slog.Debug("Rate limit reached, waiting for capacity", "nodeID", s.nodeID)
				time.Sleep(100 * time.Millisecond) // Wait for bucket to drain
			}
			
			err := s.ws.Write(websocket.BinaryMessage, buf[:n])
			if err != nil {
				// WebSocket closed/cancelled, exit gracefully
				return
			}
		}
	}
}

// streamInput reads from WebSocket and writes to PTY
func (s *interactiveConsoleSession) streamInput() {
	defer s.wg.Done()

	s.ws.configureReadDeadlines()

	for {
		messageType, message, err := s.ws.Read()
		if err != nil {
			// Check if it's an unexpected close (not normal, going away, or abnormal)
			if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				slog.Warn("WebSocket unexpected close error", "nodeID", s.nodeID, "error", err)
			} else {
				slog.Info("WebSocket closed normally for console", "nodeID", s.nodeID)
			}
			s.Close()
			return
		}

		if messageType == websocket.TextMessage || messageType == websocket.BinaryMessage {
			// Write user input to PTY
			// Hold RLock during the entire write to prevent reconnect() from swapping PTY
			s.ptmxMutex.RLock()
			if s.ptmx == nil {
				s.ptmxMutex.RUnlock()
				slog.Debug("PTY is nil, skipping input for console", "nodeID", s.nodeID)
				// Check if closing to exit faster
				select {
				case <-s.ctx.Done():
					return
				default:
				}
				continue
			}
			
			_, err := s.ptmx.Write(message)
			s.ptmxMutex.RUnlock()
			
			if err != nil {
				slog.Error("Failed to write to PTY", "nodeID", s.nodeID, "error", err)
				s.Close()
				return
			}
		}
	}
}

// start begins the console session by launching all goroutines and waiting for completion
func (s *interactiveConsoleSession) Start() {
	// Start WebSocket session
	s.ws.Start()

	// Start initial conman process with PTY
	if err := s.startConmanProcess(); err != nil {
		slog.Error("Failed to start conman with PTY", "nodeID", s.nodeID, "error", err)
		err = s.ws.Write(websocket.TextMessage, []byte("Error: Failed to start conman with PTY"))
		if err != nil {
			slog.Warn("Failed to send error message via WebSocket", "nodeID", s.nodeID, "error", err)
		}
		s.Close()
		return
	}

	// Monitor process exit for reconnection attempts
	go s.monitorProcess()

	// Start I/O goroutines
	s.wg.Add(2)
	go s.streamInput()
	go s.streamOutput()

	// Wait for I/O goroutines to complete
	s.wg.Wait()
}


func NewInteractiveConsoleSession(nodeID string, conn *websocket.Conn) *interactiveConsoleSession {
	ctx, cancel := context.WithCancel(context.Background())
	
	session := &interactiveConsoleSession{
		nodeID:      nodeID,
		rateLimiter: ratelimiter.NewLeakyBucket(rateLimitBurstKB, rateLimitInterval),
		ctx:         ctx,
		cancel:  cancel,
	}

	session.ws = NewWebSocketSession(conn, fmt.Sprintf("interactive session %s", nodeID))

	return session
}

func doInteractiveConsole(w http.ResponseWriter, r *http.Request) {
	// Make sure the request is cleaned up
	defer drainAndCloseRequestBody(r)

	nodeID, err := extractNodeId(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Make sure we are monitoring a valid node
	if exists := nodes.IsCurrentNode(nodeID); !exists {
		http.Error(w, "Node doesn't exists", http.StatusNotFound)
		return
	}

	slog.Info("Starting interactive console session", "nodeID", nodeID)

	// Upgrade HTTP connection to WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("Failed to upgrade WebSocket connection", "nodeID", nodeID, "error", err)
		// Can't send HTTP error after upgrade attempt
		return
	}

	// From here on, errors must be sent via WebSocket close frames
	session := NewInteractiveConsoleSession(nodeID, conn)
	defer session.Close() // Ensure cleanup always happens

	// Start session (blocks until all goroutines complete)
	session.Start()

	slog.Info("Interactive console session ended", "nodeID", nodeID)
}
