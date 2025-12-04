package console

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/OpenCHAMI/remote-console/internal/nodes"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
)

// keepAlive manages WebSocket ping/pong to maintain connection for any session
func keepAlive(ctx context.Context, conn *websocket.Conn) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		select {
		case <-ticker.C:
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-ctx.Done():
			fmt.Println("done")
			return
		}
	}
}

func drainAndCloseRequestBody(req *http.Request) {
	if req != nil && req.Body != nil {
		_, _ = io.Copy(io.Discard, req.Body) // ok even if already drained
		req.Body.Close()                     // ok even if already closed
	}
}

func validateNode(id string) bool {
	// make sure this is a valid node
	if !nodes.IsCurrentNode(id) {
		log.Printf("%s is not a valid node.", id)
		return false
	}
	return true
}

func extractNodeId(w http.ResponseWriter, r *http.Request) (string, error) {
	nodeID := chi.URLParam(r, "nodeID")
	if nodeID == "" {
		log.Printf("There was an error reading the node ID from the request %s", r.URL.Path)
		return "", fmt.Errorf("Unable to extract Node ID")
	}

	return nodeID, nil
}
