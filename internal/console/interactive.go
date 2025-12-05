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
)

// interactiveConsoleSession manages the lifecycle of an interactive console session
type interactiveConsoleSession struct {
	cmd         *exec.Cmd
	ptmx        *os.File
	conn        *websocket.Conn
	nodeID      string
	doneOnce    sync.Once
	processExit chan struct{}
	ctx         context.Context
	cancel      context.CancelFunc
}

// close performs graceful shutdown of the console session
func (s *interactiveConsoleSession) close() {
	s.doneOnce.Do(func() {
		log.Printf("Starting close for console session: %s", s.nodeID)

		// Cancel context to signal all goroutines
		if s.cancel != nil {
			s.cancel()
		}

		// Try graceful disconnect via ConMan escape sequence
		if s.ptmx != nil {
			log.Printf("Sending ConMan escape sequence (&.) to disconnect from console: %s", s.nodeID)
			s.ptmx.Write([]byte("&."))
			time.Sleep(100 * time.Millisecond) // Brief pause to let it process
		}

		// Ensure process termination
		if s.cmd != nil && s.cmd.Process != nil {
			timer := time.NewTimer(2 * time.Second)
			select {
			case <-s.processExit:
				timer.Stop()
				log.Printf("Conman process exited gracefully for console: %s", s.nodeID)
			case <-timer.C:
				log.Printf("Force killing conman process for console: %s", s.nodeID)
				s.cmd.Process.Kill()
				// Wait for process to be reaped
				<-s.processExit
			}
		}

		// Close PTY
		if s.ptmx != nil {
			s.ptmx.Close()
		}

		// Close WebSocket connection gracefully
		if s.conn != nil {
			// Send close message first
			s.conn.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			// Then close the connection
			s.conn.Close()
		}

		log.Printf("Close completed for console session: %s", s.nodeID)
	})
}

// monitorProcess watches for process exit and triggers close
func (s *interactiveConsoleSession) monitorProcess() {
	s.cmd.Wait()
	log.Printf("Conman process exited for console: %s", s.nodeID)
	close(s.processExit)
	s.close()
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
func (s *interactiveConsoleSession) streamOutput(wg *sync.WaitGroup) {
	defer wg.Done()

	buf := make([]byte, 4096)
	for {
		select {
		case <-s.ctx.Done():
			s.close()
			return
		default:
			n, err := s.ptmx.Read(buf)
			if err != nil {
				// Don't log I/O errors - they're expected when the process is killed
				if err != io.EOF && !isEIO(err) {
					log.Printf("Error reading from PTY: %v", err)
				}
				s.close()
				return
			}

			if n > 0 {
				log.Printf("console %s PTY read (%d bytes): %q", s.nodeID, n, string(buf[:n]))
				if err := s.conn.WriteMessage(websocket.BinaryMessage, buf[:n]); err != nil {
					// Don't log if WebSocket is already closed (happens during normal shutdown)
					if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) &&
						err.Error() != "websocket: close sent" {
						log.Printf("Failed to write to WebSocket: %v", err)
					}
					s.close()
					return
				}
			}
		}
	}
}

// streamInput reads from WebSocket and writes to PTY
func (s *interactiveConsoleSession) streamInput(wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			s.close()
			return
		default:
			messageType, message, err := s.conn.ReadMessage()
			if err != nil {
				// Check if it's an unexpected close (not normal, going away, or abnormal)
				if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Printf("WebSocket unexpected close error: %v", err)
				} else {
					log.Printf("WebSocket closed normally for console: %s", s.nodeID)
				}
				s.close()
				return
			}

			if messageType == websocket.TextMessage || messageType == websocket.BinaryMessage {
				// Write user input to PTY
				if _, err := s.ptmx.Write(message); err != nil {
					log.Printf("Failed to write to PTY: %v", err)
					s.close()
					return
				}
			}
		}
	}
}

func (s *interactiveConsoleSession) keepAlive() {
	keepAlive(s.ctx, s.conn)
}

func newInteractiveConsoleSession(ctx context.Context, nodeID string, conn *websocket.Conn) *interactiveConsoleSession {
	sessionCtx, cancel := context.WithCancel(ctx)
	session := &interactiveConsoleSession{
		nodeID:      nodeID,
		conn:        conn,
		processExit: make(chan struct{}),
		ctx:         sessionCtx,
		cancel:      cancel,
	}

	// Start conman process with PTY
	session.cmd = exec.Command("conman", nodeID)

	// Create a PTY for the command
	var err error
	session.ptmx, err = pty.Start(session.cmd)
	if err != nil {
		log.Printf("Failed to start conman with PTY: %v", err)
		conn.WriteMessage(websocket.TextMessage, []byte("Error: Failed to start conman with PTY"))
		return nil
	}

	return session
}

func doInteractiveConsole(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

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
	session := newInteractiveConsoleSession(ctx, nodeID, conn)
	if session == nil {
		conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "Error starting console session"))
		conn.Close()
		return
	}

	defer session.close() // Ensure cleanup always happens

	log.Printf("Started conman process for console: %s", nodeID)

	// Monitor process exit
	go session.monitorProcess()

	// Start keep-alive ping/pong
	go session.keepAlive()

	// Start I/O goroutines
	var wg sync.WaitGroup
	wg.Add(2)
	go session.streamInput(&wg)
	go session.streamOutput(&wg)

	// Wait for I/O goroutines to complete
	wg.Wait()

	log.Printf("Interactive console session ended for: %s", nodeID)
}
