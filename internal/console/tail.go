package console

import (
	"bufio"
	"container/ring"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	//"time"

	"github.com/gorilla/websocket"
	"github.com/nxadm/tail"
	//"github.com/nxadm/tail/ratelimiter"
)

type consoleTailSession struct {
	nodeID          string
	tail            *tail.Tail
	ctx             context.Context
	cancel          context.CancelFunc
	consoleLogsPath string
	closeOnce       sync.Once
	ws              *webSocketSession
}

func newConsoleTailSession(ctx context.Context, consoleLogsPath string, nodeID string, conn *websocket.Conn) *consoleTailSession {
	sessionCtx, cancel := context.WithCancel(ctx)
	cts := &consoleTailSession{
		nodeID:          nodeID,
		ctx:             sessionCtx,
		cancel:          cancel,
		consoleLogsPath: consoleLogsPath,
	}

	cts.ws = newWebSocketSession(conn, fmt.Sprintf("tail session %s", nodeID), cts.close)
	cts.ws.start(sessionCtx)

	return cts
}

func (cts *consoleTailSession) close() {
	cts.closeOnce.Do(func() {
		if cts.tail != nil {
			log.Printf("cleanup tail for %s", cts.nodeID)
			// Print out the last 10 lines of the file before closing
			filename := fmt.Sprintf("%s/console.%s", cts.consoleLogsPath, cts.nodeID)
			lastLines, _, err := readLastNLines(filename, 10)
			if err != nil {
				log.Printf("Error reading last lines from console log: %v", err)
			} else {
				for _, line := range lastLines {
					log.Printf("Last line: %s", line)
				}
			}
			// end debug

			cts.tail.Config.Poll = false
			cts.tail.Cleanup()
			err = cts.tail.Stop()
			if err != nil {
				log.Printf("Error stopping tail: %v", err)
			}
		}

		log.Printf("Closing console tail session for: %s", cts.nodeID)

		cts.cancel()
		log.Printf("Cancelled context for console tail session: %s", cts.nodeID)

		cts.ws.close()
	
		log.Printf("Close completed for console tail session: %s", cts.nodeID)
	})
}

func (cts *consoleTailSession) writeMessage(messageType int, data []byte) error {
	return cts.ws.write(cts.ctx, messageType, data)
}

func (cts *consoleTailSession) waitForClientClose() {
	log.Printf("Waiting for client close on tail session '%s'", cts.nodeID)
	for {
		cts.ws.configureReadDeadlines()
		_, _, err := cts.ws.readMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket closed normally for tail session '%s'", cts.nodeID)
			} else {
				log.Printf("WebSocket closed for tail session '%s', error: %v", cts.nodeID, err)
			}

			cts.close()
			break
		}
	}
}

func (cts *consoleTailSession) streamConsoleTail(follow bool) {
	fmt.Printf("streamConsoleTail called for node: %s, follow=%v\n", cts.nodeID, follow)
	// Read the lines of the tail output while looking for a cancel signal
	for {
		select {
		case <-cts.ctx.Done():
			// done tailing this file - exit
			log.Printf("Tailing console for '%s' exiting", cts.nodeID)
			cts.tail.Config.Poll = false
			cts.tail.Cleanup()
			cts.tail.Stop()
			return
		case line := <-cts.tail.Lines:
			log.Printf("got line: %v", line)

			// Stream the line to the websocket
			if line == nil {
				log.Printf("Tailing console for '%s' complete (follow=%v)", cts.nodeID, follow)

				cts.tail.Config.Poll = false
				cts.tail.Cleanup()
				cts.tail.Stop()
				log.Printf("Tail loop exiting for '%s' (follow=%v)", cts.nodeID, follow)
				return
			}

			// Add newline back (tail library strips it)
			lineText := line.Text + "\n"
			log.Printf("before write")
			log.Printf("Sending line:  follow: %v, %s", follow, lineText)
			err := cts.writeMessage(websocket.TextMessage, []byte(lineText))
			log.Printf("after write")
			if err != nil {
				log.Printf("Failed to write message to websocket: %s", err)
				cts.close()
				cts.tail.Config.Poll = false
				cts.tail.Cleanup()
				cts.tail.Stop()
				return
			}
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

	fmt.Printf("Starting to tail console log for node: %s, follow=%v, numLines=%d\n", cts.nodeID, follow, numLines)
	log.Printf("Tail session starting for node=%s follow=%v numLines=%d", cts.nodeID, follow, numLines)

	filename := fmt.Sprintf("%s/console.%s", cts.consoleLogsPath, cts.nodeID)

	var seekOffset int64
	// If numLines is specified, send last N lines first
	if numLines > 0 {
		fmt.Printf("Reading last %d lines from console log: %s\n", numLines, filename)
		lines, currentPos, err := readLastNLines(filename, numLines)
		fmt.Printf("Read %d lines from console log\n", len(lines))
		fmt.Printf("CurrentPos=%d\n", currentPos)

		if err == nil {
			for _, line := range lines {
				fmt.Printf("Sending line: follow: %v: %s\n", follow, line)
				if err := cts.writeMessage(websocket.TextMessage, []byte(line+"\n")); err != nil {
					log.Printf("Failed to send lines: %v", err)
					cts.writeMessage(websocket.CloseMessage,
						websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "Error sending console log"))
					cts.close()
					return
				}
			}

			seekOffset = currentPos

			// If not following, we're done
			if !follow {
				fmt.Printf("Not following console log, ending session\n")
				return
			}
		} else if errors.Is(err, os.ErrNotExist) {
			log.Printf("Console log %s not found; no history available (follow=%v)", filename, follow)
			if !follow {
				cts.writeMessage(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, "Console log not available yet"))
				cts.close()
				return
			}
		} else {
			log.Printf("Failed to read last %d lines from %s: %v", numLines, filename, err)
			cts.writeMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "Error reading console log"))
			cts.close()
			return
		}
	}

	// Configuration for tail function
	conf := tail.Config{
		Follow:      follow,
		MustExist:   false, // If file doesn't exist keep trying
		Poll:        true,  // Poll instead of using inotify -- inotify may not work on all filesystems
		Logger:      tail.DiscardingLogger,
		//RateLimiter: ratelimiter.NewLeakyBucket(1000, 1*time.Millisecond), // Rate limit to 1000 lines per second using leaky bucket
	}

	// Only set ReOpen to true if we are following the file
	if follow {
		conf.ReOpen = true // If the file is deleted or moved, reopen original file
	}

	// When following after sending last N lines, start from where we left off
	// The tail library will handle rotation: if file is reopened, it starts from beginning
	// If the file hasn't been rotated, we continue from our saved offset

	fmt.Printf("seekOffset=%d", seekOffset)

	if numLines > 0 && follow && seekOffset > 0 {
		conf.Location = &tail.SeekInfo{Offset: seekOffset, Whence: io.SeekStart}
	}

	var err error
	cts.tail, err = tail.TailFile(filename, conf)
	if err != nil {
		log.Printf("Failed to tail file %s with error:%s", filename, err)
		cts.writeMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "Error starting console tail session"))
		cts.close()
		return
	}

	log.Printf("Tailing console file: %s", filename)

	cts.streamConsoleTail(follow)
}

func doTailConsole(consoleLogsPath string, w http.ResponseWriter, r *http.Request) {
	log.Printf("doTailConsole called path=%s", r.URL.RequestURI())

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
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("Tailing console for node: %s", nodeID)

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

	log.Printf("Starting console tail session for node: %s", nodeID)
	log.Printf("Tail parameters: follow=%s lines=%s", params.Get("follow"), params.Get("lines"))

	// Upgrade HTTP connection to WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Error upgrading to WebSocket: %v", err)
		// Can't send HTTP error after upgrade attempt
		return
	}

	log.Printf("WebSocket path: %s", consoleLogsPath)
	log.Printf("Client %s connected for node %s tail", conn.RemoteAddr(), nodeID)

	// From here on, errors must be sent via WebSocket close frames
	session := newConsoleTailSession(ctx, consoleLogsPath, nodeID, conn)
	if session == nil {
		conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "Error starting console tail session"))
		conn.Close()
		return
	}

	go session.waitForClientClose()

	log.Printf("Started tailing console log for: %s", nodeID)

	// Start streaming the console output
	session.tailConsole(follow, numLines)

	log.Printf("Console tail session ended for: %s", nodeID)
}
