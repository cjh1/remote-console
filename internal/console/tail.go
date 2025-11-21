package console

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/websocket"
	"github.com/nxadm/tail"
)

type consoleTailSession struct {
	nodeID          string
	conn            *websocket.Conn
	tail            *tail.Tail
	ctx             context.Context
	cancel          context.CancelFunc
	consoleLogsPath string
}

func newConsoleTailSession(ctx context.Context, consoleLogsPath string, nodeID string, conn *websocket.Conn) *consoleTailSession {
	sessionCtx, cancel := context.WithCancel(ctx)
	return &consoleTailSession{
		nodeID:          nodeID,
		conn:            conn,
		ctx:             sessionCtx,
		cancel:          cancel,
		consoleLogsPath: consoleLogsPath,
	}
}

func (cts *consoleTailSession) close() {
	if cts.tail != nil {
		log.Printf("cleanup tail")
		cts.tail.Config.Poll = false
		cts.tail.Cleanup()
		err := cts.tail.Stop()
		if err != nil {
			log.Printf("Error stopping tail: %v", err)
		}
	}

	log.Printf("Closing console tail session for: %s", cts.nodeID)

	cts.cancel()
	log.Printf("Cancelled context for console tail session: %s", cts.nodeID)

	// Close WebSocket connection gracefully
	if cts.conn != nil {
		// Send close message first
		cts.conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		// Then close the connection
		cts.conn.Close()
	}

	log.Printf("Close completed for console tail session: %s", cts.nodeID)
}

func (cts *consoleTailSession) keepAlive() {
	keepAlive(cts.ctx, cts.conn)
}

func (cts *consoleTailSession) waitForClientClose() {
	for {
		log.Printf("before")
		_, _, err := cts.conn.ReadMessage()
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
			log.Printf("got line")
			// Stream the line to the websocket
			if line == nil {
				log.Printf("Tailing console for '%s' complete", cts.nodeID)

				cts.tail.Config.Poll = false
				cts.tail.Cleanup()
				cts.tail.Stop()
				return
			}

			// Add newline back (tail library strips it)
			lineText := line.Text + "\n"
			log.Printf("before write")
			err := cts.conn.WriteMessage(websocket.TextMessage, []byte(lineText))
			log.Printf("after write")
			if err != nil {
				log.Printf("Failed to write message to websocket: %s", err)

				cts.tail.Config.Poll = false
				cts.tail.Cleanup()
				cts.tail.Stop()
				return
			}
		}
	}
}

func (cts *consoleTailSession) tailConsole(follow bool, numLines int) {

	// Configuration for tail function
	conf := tail.Config{
		Follow:    follow,
		MustExist: false, // If file doesn't exist keep trying
		Poll:      true,  // Poll instead of using inotify -- inotify may not work on all filesystems
		Logger:    tail.DiscardingLogger,
	}

	// Only set ReOpen to true if we are following the file
	if follow {
		conf.ReOpen = true // If the files is deleted or moved, reopen original file
	}

	// If numLines is set to a positive number, we start reading from that many lines back
	if numLines > 0 {
		conf.Location = &tail.SeekInfo{Offset: int64(-1 * numLines), Whence: io.SeekEnd}
	}

	filename := fmt.Sprintf("%s/console.%s", cts.consoleLogsPath, cts.nodeID)
	var err error
	cts.tail, err = tail.TailFile(filename, conf)
	if err != nil {
		log.Printf("Failed to tail file %s with error:%s", filename, err)
		cts.conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "Error starting console tail session"))
		cts.conn.Close()
		return
	}

	log.Printf("Tailing console file: %s", filename)

	cts.streamConsoleTail(follow)
}

func doTailConsole(consoleLogsPath string, w http.ResponseWriter, r *http.Request) {
	log.Printf("doTailConsole called")

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
		http.Error(w, err.Error(), http.StatusNotFound)
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

	// Upgrade HTTP connection to WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Error upgrading to WebSocket: %v", err)
		// Can't send HTTP error after upgrade attempt
		return
	}

	log.Printf("WebSocket path: %s", consoleLogsPath)

	// From here on, errors must be sent via WebSocket close frames
	session := newConsoleTailSession(ctx, consoleLogsPath, nodeID, conn)
	if session == nil {
		conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "Error starting console tail session"))
		conn.Close()
		return
	}

	go session.keepAlive()
	go session.waitForClientClose()

	log.Printf("Started tailing console log for: %s", nodeID)

	// Start streaming the console output
	session.tailConsole(follow, numLines)

	log.Printf("Console tail session ended for: %s", nodeID)
}
