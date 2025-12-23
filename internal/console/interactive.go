package console

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

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

	ws            *webSocketSession
	rateLimiter   *ratelimiter.LeakyBucket // Rate limit console output
	wg            sync.WaitGroup           // Tracks all I/O goroutines including reconnected ones
	processExited chan struct{}            // Closed when current conman process exits
}

// Close performs graceful shutdown of the console session
// This method is idempotent and safe to call multiple times
func (s *interactiveConsoleSession) Close() {
	log.Printf("Starting close for console session: %s", s.nodeID)

	// Cancel context to signal all goroutines to stop (idempotent)
	s.cancel()

	// Try graceful disconnect via ConMan escape sequence
	s.ptmxMutex.RLock()
	ptmx := s.ptmx
	s.ptmxMutex.RUnlock()
	
	if ptmx != nil {
		log.Printf("Sending ConMan escape sequence (&.) to disconnect from console: %s", s.nodeID)
		// Ignore write errors - PTY might already be closed
		ptmx.Write([]byte("&."))
		time.Sleep(100 * time.Millisecond) // Brief pause to let it process
	}

	// Signal process termination (idempotent - safe to signal multiple times)
	if s.cmd != nil && s.cmd.Process != nil {
		log.Printf("Sending SIGTERM to conman process for console: %s", s.nodeID)
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

	// Close WebSocket - already handles multiple closes internally
	s.ws.Close()

	log.Printf("Close completed for console session: %s", s.nodeID)
}

// monitorProcess watches for process exit and attempts reconnection if node still exists
// This runs in a loop, monitoring each new process after successful reconnection
func (s *interactiveConsoleSession) monitorProcess() {
	for {
		<-s.processExited
		log.Printf("Conman process exited for console: %s", s.nodeID)
		
		// Check if session is closing
		select {
		case <-s.ctx.Done():
			log.Printf("Session closing, stopping monitor for console: %s", s.nodeID)
			return
		default:
		}
			
		// Check if the node still exists (might have been updated/changed)
		if !validateNode(s.nodeID) {
			log.Printf("Node %s no longer exists, closing session", s.nodeID)
			s.Close()
			return
		}
		
		log.Printf("Node %s still exists, attempting to reconnect...", s.nodeID)
		if !s.reconnect() {
			// Reconnection failed or was cancelled
			return
		}
		// Successfully reconnected, loop back to monitor the new process
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
		close(s.processExited)
	}()

	return nil

	// // Wait briefly to see if process exits immediately (connection failure)
	// time.Sleep(500 * time.Millisecond)
	
	// select {
	// case <-s.processExit:
	// 	msg := "conman process exited immediately"
	// 	// Process exited - read any output to include in error
	// 	buf := make([]byte, 4096)
	// 	n, _ := ptmx.Read(buf)
	// 	if n > 0 {
	// 		output := strings.TrimSpace(string(buf[:n]))
	// 		msg = fmt.Sprintf("%s: %s", msg, output)
	// 	}
	// 	ptmx.Close()
	// 	return errors.New(msg)
	// default:
	// 	// Process is still running - now safe to expose PTY to other goroutines
	// 	s.ptmxMutex.Lock()
	// 	s.ptmx = ptmx
	// 	s.ptmxMutex.Unlock()
	// 	return nil
	// }
}

// reconnect attempts to restart the conman process and reconnect streams
// Returns true if reconnection succeeded, false if it failed or was cancelled
func (s *interactiveConsoleSession) reconnect() bool {
	// Check if context is already cancelled
	select {
	case <-s.ctx.Done():
		log.Printf("Context cancelled before reconnection for %s: %v", s.nodeID, s.ctx.Err())
		s.Close()
		return false
	default:
	}
	
	// Notify user via WebSocket
	reconnectMsg := fmt.Sprintf("\n[Reconnecting to %s...]\n", s.nodeID)
	err := s.ws.Write(websocket.TextMessage, []byte(reconnectMsg))
	if err != nil {
		log.Printf("Failed to send reconnect message: %v", err)
		s.Close()
		return false
	}

	// Close old PTY if it exists
	s.ptmxMutex.Lock()
	if s.ptmx != nil {
		s.ptmx.Close()
	}
	s.ptmxMutex.Unlock()

	// Try to start conman with retries over 30 seconds
	timeout := time.After(30 * time.Second)
	retryDelay := time.Second
	attempt := 0
	
	for {
		attempt++
		log.Printf("Attempting to reconnect conman for %s (attempt %d)", s.nodeID, attempt)
		
		if err := s.startConmanProcess(); err == nil {
			// Success!
			log.Printf("Successfully reconnected conman for console: %s", s.nodeID)
			
			// Check if we are closed (double-check right before starting goroutine)
			select {
			case <-s.ctx.Done():
				log.Printf("Context cancelled after reconnection for %s: %v", s.nodeID, s.ctx.Err())
				s.Close()
				return false
			default:
			}

			// Restart output streaming
			// The console output itself will indicate when we're truly connected
			// Track this new goroutine in the main WaitGroup
			s.wg.Add(1)
			go s.streamOutput()
			// Note: streamInput is already running and will continue to work with the new PTY
			
			// Return true - monitorProcess will continue monitoring this new process
			return true
		} else {
			log.Printf("Failed to start conman (attempt %d): %v", attempt, err)
		}
		
		// Wait before retry, checking for timeout
		select {
		case <-timeout:
			log.Printf("Reconnection timeout after %d attempts for %s", attempt, s.nodeID)
			errorMsg := fmt.Sprintf("\r\n[Reconnection failed after %d attempts]\r\n", attempt)
			err := s.ws.Write(websocket.TextMessage, []byte(errorMsg))
			if err != nil {
				log.Printf("Failed to send reconnection failure message: %v", err)
			}
			
			s.Close()
			return false
		case <-s.ctx.Done():
			log.Printf("Session closed during reconnection for %s: %v", s.nodeID, s.ctx.Err())
			s.Close()
			return false
		// Wait before next retry
		case <-time.After(retryDelay):
		}
	}
}

// isEIO checks if an error is an I/O error (EIO)
// This happens when reading from a PTY after the process has been killed
func isEIO(err error) bool {
	if err == nil {
		return false
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno == syscall.EIO
	}
	return false
}

// streamOutput reads from PTY and writes to WebSocket
func (s *interactiveConsoleSession) streamOutput() {
	defer s.wg.Done()

	// Check if session is closing before starting
	select {
	case <-s.ctx.Done():
		log.Printf("Session closing, streamOutput exiting for console: %s: %v", s.nodeID, s.ctx.Err())
		return
	default:
	}

	buf := make([]byte, 4096)
	for {
		s.ptmxMutex.RLock()
		ptmx := s.ptmx
		s.ptmxMutex.RUnlock()
		
		if ptmx == nil {
			log.Printf("PTY is nil, exiting streamOutput for console: %s", s.nodeID)
			return
		}
		
		n, err := ptmx.Read(buf)
		if err != nil {
			// Don't log I/O errors - they're expected when the process is killed
			if err != io.EOF && !isEIO(err) {
				log.Printf("Error reading from PTY: %v", err)
			}
			// PTY closed, exit gracefully without calling close() (close() already closed PTY)
			return
		}

		if n > 0 {
			log.Printf("console %s PTY read (%d bytes): %q", s.nodeID, n, string(buf[:n]))
			
			// Apply rate limiting (convert bytes to KB, rounded up)
			kb := uint16((n + 1023) / 1024)
			for !s.rateLimiter.Pour(kb) {
				log.Printf("Rate limit reached for console %s, waiting for capacity", s.nodeID)
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
				log.Printf("WebSocket unexpected close error: %v", err)
			} else {
				log.Printf("WebSocket closed normally for console: %s", s.nodeID)
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
				log.Printf("PTY is nil, skipping input for console: %s", s.nodeID)
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
				log.Printf("Failed to write to PTY: %v", err)
				s.Close()
				return
			}
		}
	}
}

// start begins the console session by launching all goroutines and waiting for completion
func (s *interactiveConsoleSession) Start() {
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

	session.ws = NewWebSocketSession(conn, fmt.Sprintf("interactive session %s", nodeID), session.Close)
	session.ws.Start()

	// Start conman process with PTY
	if err := session.startConmanProcess(); err != nil {
		log.Printf("Failed to start conman with PTY: %v", err)
		err = session.ws.Write(websocket.TextMessage, []byte("Error: Failed to start conman with PTY"))
		if err != nil {
			log.Printf("Failed to send error message via WebSocket: %v", err)
		}
		session.Close()
		return nil
	}

	return session
}

func doInteractiveConsole(w http.ResponseWriter, r *http.Request) {
	// Make sure the request is cleaned up
	defer drainAndCloseRequestBody(r)

	// Only allow 'GET' calls
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, fmt.Sprintf("(%s) Not Allowed", r.Method), http.StatusMethodNotAllowed)
		return
	}

	nodeID, err := extractNodeId(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Make sure we are monitoring a valid node
	if exists := validateNode(nodeID); !exists {
		http.Error(w, "Node doesn't exists", http.StatusNotFound)
		return
	}

	// Upgrade HTTP connection to WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade WebSocket connection: %v", err)
		// Can't send HTTP error after upgrade attempt
		return
	}

	// From here on, errors must be sent via WebSocket close frames
	session := NewInteractiveConsoleSession(nodeID, conn)
	if session == nil {
		conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "Error starting console session"))
		conn.Close()
		return
	}

	defer session.Close() // Ensure cleanup always happens

	log.Printf("Started conman process for console: %s", nodeID)

	// Start session (blocks until all goroutines complete)
	session.Start()

	log.Printf("Interactive console session ended for: %s", nodeID)
}
