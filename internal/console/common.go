package console

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/OpenCHAMI/remote-console/internal/nodes"

	"github.com/go-chi/chi/v5"
)

// Rate limiter constants for console output
const (
	// Rate limit in KB units: 10MB burst, 1MB/sec sustained
	rateLimitBurstKB    = 10240 // 10MB burst capacity
	rateLimitInterval = 1*time.Millisecond     // Drain 1KB per millisecond = 1MB/sec
)

func drainAndCloseRequestBody(req *http.Request) {
	if req != nil && req.Body != nil {
		_, _ = io.Copy(io.Discard, req.Body) // ok even if already drained
		req.Body.Close()                     // ok even if already closed
	}
}

func validateNode(id string) bool {
	// make sure this is a valid node
	if !nodes.IsCurrentNode(id) {
		slog.Error("Invalid node ID", "nodeID", id)
		return false
	}
	return true
}

func extractNodeId(w http.ResponseWriter, r *http.Request) (string, error) {
	nodeID := chi.URLParam(r, "nodeID")
	if nodeID == "" {
		slog.Error("Failed to extract node ID from request", "path", r.URL.Path)
		return "", fmt.Errorf("Unable to extract Node ID")
	}

	return nodeID, nil
}
