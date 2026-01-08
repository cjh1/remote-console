package console

import (
	"bufio"
	"container/ring"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
	
	"github.com/gorilla/websocket"
	"github.com/nxadm/tail"
	"github.com/nxadm/tail/ratelimiter"
)

type consoleTailSession struct {
	nodeID          string
	tail            *tail.Tail
	consoleLogsPath string
	closeOnce       sync.Once
	ws              *webSocketSession
	rateLimiter     *ratelimiter.LeakyBucket // Rate limit console output
}

func newConsoleTailSession(ctx context.Context, consoleLogsPath string, nodeID string, conn *websocket.Conn) *consoleTailSession {
	cts := &consoleTailSession{
		nodeID:          nodeID,
		consoleLogsPath: consoleLogsPath,
		rateLimiter:     ratelimiter.NewLeakyBucket(rateLimitBurstKB, rateLimitInterval),
	}

	cts.ws = NewWebSocketSession(conn, fmt.Sprintf("tail session %s", nodeID), cts.close)
	cts.ws.Start()

	return cts
}

func (cts *consoleTailSession) close() {
	cts.closeOnce.Do(func() {
		if cts.tail != nil {
			slog.Info("Stopping tail for console tail session", "nodeID", cts.nodeID)
			cts.tail.Config.Poll = false
			cts.tail.Cleanup()
			err := cts.tail.Stop()
			if err != nil {
				slog.Error("Error stopping tail", "error", err, "nodeID", cts.nodeID)
			}
		}

		slog.Info("Closing console tail session", "nodeID", cts.nodeID)

		cts.ws.Close()
	
		slog.Info("Close completed for console tail session", "nodeID", cts.nodeID)
	})
}

func (cts *consoleTailSession) waitForClientClose() {
	slog.Debug("Waiting for client close on tail session", "nodeID", cts.nodeID)
	for {
		cts.ws.configureReadDeadlines()
		_, _, err := cts.ws.Read()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				slog.Info("WebSocket closed normally for tail session", "nodeID", cts.nodeID)
			} else {
				slog.Warn("WebSocket closed for tail session", "nodeID", cts.nodeID, "error", err)
			}

			cts.close()
			break
		}
	}
}

func (cts *consoleTailSession) streamConsoleTail(follow bool) {
	// Read the lines of the tail output while looking for a cancel signal
	for line := range cts.tail.Lines {
		// Stream the line to the websocket
		if line == nil {
			slog.Info("Tailing console complete", "nodeID", cts.nodeID, "follow", follow)

			cts.tail.Config.Poll = false
			cts.tail.Cleanup()
			cts.tail.Stop()
			slog.Info("Tail loop exiting", "nodeID", cts.nodeID, "follow", follow)
			return
		}

		// Add newline back (tail library strips it)
		lineText := line.Text + "\n"
	
		// Apply rate limiting (convert bytes to KB, rounded up)
		kb := uint16((len(lineText) + 1023) / 1024)
		for !cts.rateLimiter.Pour(kb) {
			slog.Debug("Rate limit reached for tail, waiting for capacity", "nodeID", cts.nodeID)
			time.Sleep(100 * time.Millisecond) // Wait for bucket to drain
		}
		
		err := cts.ws.Write(websocket.TextMessage, []byte(lineText))
		if err != nil {
			slog.Error("Failed to write message to websocket", "error", err, "nodeID", cts.nodeID)
			cts.close()
			cts.tail.Config.Poll = false
			cts.tail.Cleanup()
			cts.tail.Stop()
			return
		}
	}
}

// readLastNLines reads the last numLines lines from the specified file and returns them along with the file position
func readLastNLines(filename string, numLines int) ([]string, int64, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	r := ring.New(numLines)
	count := 0

	scanner := bufio.NewScanner(file)
	// Set max line length to 1MB (well below the 10MB bucket capacity)
	const maxLineLength = 1024 * 1024
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, maxLineLength)
	
	for scanner.Scan() {
		r.Value = scanner.Text()
		r = r.Next()
		count++
	}

	if err := scanner.Err(); err != nil {
		return nil, 0, fmt.Errorf("error reading file: %w", err)
	}

	// Get current position in file (where we stopped reading)
	currentPos, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get file position: %w", err)
	}

	// Return lines in order
	var lines []string
	linesToReturn := numLines
	if count < numLines {
		linesToReturn = count
		// Move back to start of actual data
		r = r.Move(-count)
	}

	// Iterate the ring to get the lines
	for i := 0; i < linesToReturn; i++ {
		if r.Value != nil {
			lines = append(lines, r.Value.(string))
		}
		r = r.Next()
	}

	return lines, currentPos, nil
}

func (cts *consoleTailSession) tailConsole(follow bool, numLines int) {

	slog.Info("Tail session starting", "nodeID", cts.nodeID, "follow", follow, "numLines", numLines)

	filename := fmt.Sprintf("%s/console.%s", cts.consoleLogsPath, cts.nodeID)

	var seekOffset int64
	// If numLines is specified, send last N lines first
	if numLines > 0 {
		lines, currentPos, err := readLastNLines(filename, numLines)
		
		if err == nil {
			for _, line := range lines {
				lineText := line + "\n"
				
			// Apply rate limiting (convert bytes to KB, rounded up)
			kb := uint16((len(lineText) + 1023) / 1024)
				for !cts.rateLimiter.Pour(kb) {
					slog.Debug("Rate limit reached for tail (history), waiting for capacity", "nodeID", cts.nodeID)
					time.Sleep(100 * time.Millisecond) // Wait for bucket to drain
				}
				
				err := cts.ws.Write(websocket.TextMessage, []byte(lineText))
				if err != nil {
					slog.Error("Failed to send lines", "error", err, "nodeID", cts.nodeID)
					cts.ws.Write(websocket.CloseMessage,
						websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "Error sending console log"))
					cts.close()
					return
				}
			}

			seekOffset = currentPos

		} else if errors.Is(err, os.ErrNotExist) {
			slog.Warn("Console log not found; no history available", "filename", filename, "follow", follow, "nodeID", cts.nodeID)
			if !follow {
				err := cts.ws.Write(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, "Console log not available yet"))
				if err != nil {
					slog.Error("Failed to send close message", "error", err, "nodeID", cts.nodeID)
				}
				
				cts.close()
				return
			}
		} else {
			slog.Error("Failed to read last N lines", "numLines", numLines, "filename", filename, "error", err, "nodeID", cts.nodeID)
			err = cts.ws.Write(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "Error reading console log"))
			if err != nil {
				slog.Error("Failed to send close message", "error", err, "nodeID", cts.nodeID)
			}
			cts.close()
			return
		}
	}

	// Configuration for tail function
	conf := tail.Config{
		Follow:      follow,
		MustExist:   false, // If file doesn't exist keep trying
		Poll:        true,  // Poll instead of using inotify -- inotify may not work on all filesystems
		MaxLineSize: 1024 * 1024, // 1MB max line size (well below 10MB bucket capacity)
		Logger:      tail.DiscardingLogger,
	}

	// Only set ReOpen to true if we are following the file
	if follow {
		conf.ReOpen = true // If the file is deleted or moved, reopen original file
	}

	// When following after sending last N lines, start from where we left off
	// The tail library will handle rotation: if file is reopened, it starts from beginning
	// If the file hasn't been rotated, we continue from our saved offset

	if numLines > 0 && follow && seekOffset > 0 {
		conf.Location = &tail.SeekInfo{Offset: seekOffset, Whence: io.SeekStart}
	}

	var err error
	cts.tail, err = tail.TailFile(filename, conf)
	if err != nil {
		slog.Error("Failed to tail file", "filename", filename, "error", err, "nodeID", cts.nodeID)
		err = cts.ws.Write(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "Error starting console tail session"))
		if err != nil {
			slog.Error("Failed to send close message", "error", err, "nodeID", cts.nodeID)
		}
		cts.close()
		return
	}

	slog.Info("Tailing console file", "filename", filename, "nodeID", cts.nodeID)

	cts.streamConsoleTail(follow)
}

func doTailConsole(consoleLogsPath string, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Make sure the request is cleaned up
	defer drainAndCloseRequestBody(r)

	nodeID, err := extractNodeId(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	slog.Info("Tailing console for node", "nodeID", nodeID)

	// Make sure we are monitoring a valid node
	if exists := validateNode(nodeID); !exists {
		http.Error(w, "Node not found", http.StatusNotFound)
		return
	}

	// Parse and validate query parameters BEFORE upgrading
	params := r.URL.Query()

	follow := false
	if followParam := params.Get("follow"); followParam != "" {
		follow, err = strconv.ParseBool(followParam)
		if err != nil {
			http.Error(w, "Follow parameter must be a boolean value", http.StatusBadRequest)
			return
		}
	}

	numLines := -1
	if numLinesParam := params.Get("lines"); numLinesParam != "" {
		numLines, err = strconv.Atoi(numLinesParam)
		if err != nil {
			http.Error(w, "Lines parameter must be a valid integer", http.StatusBadRequest)
			return
		}
	}

	// Upgrade HTTP connection to WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("Error upgrading to WebSocket", "error", err)
		// Can't send HTTP error after upgrade attempt
		return
	}

	slog.Info("Client connected for node tail", "remoteAddr", conn.RemoteAddr().String(), "nodeID", nodeID)

	// From here on, errors must be sent via WebSocket close frames
	session := newConsoleTailSession(ctx, consoleLogsPath, nodeID, conn)
	if session == nil {
		conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "Error starting console tail session"))
		conn.Close()
		return
	}

	go session.waitForClientClose()

	slog.Info("Started tailing console log", "nodeID", nodeID)

	// Start streaming the console output
	session.tailConsole(follow, numLines)

	slog.Info("Console tail session ended", "nodeID", nodeID)
}
