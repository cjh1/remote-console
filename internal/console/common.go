package console

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
)

// Rate limiter constants for console output
const (
	// Rate limit in KB units: 10MB burst, 1MB/sec sustained
	rateLimitBurstKB  = 10240                // 10MB burst capacity
	rateLimitInterval = 1 * time.Millisecond // Drain 1KB per millisecond = 1MB/sec
)

func drainAndCloseRequestBody(req *http.Request) {
	if req != nil && req.Body != nil {
		_, _ = io.Copy(io.Discard, req.Body) // ok even if already drained
		if err := req.Body.Close(); err != nil {
			slog.Debug("Failed to close request body", "error", err)
		}
	}
}

func extractNodeId(r *http.Request) (string, error) {
	nodeID := chi.URLParam(r, "nodeID")
	if nodeID == "" {
		slog.Error("Failed to extract node ID from request", "path", r.URL.Path)
		return "", fmt.Errorf("unable to extract node ID")
	}

	return nodeID, nil
}
