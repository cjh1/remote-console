package test

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"
)

func (s *IntegrationTestSuite) waitForAggLogFile(timeout time.Duration) (string, error) {
	rcsContainer, ok := s.containers["remote-console"]
	if !ok {
		return "", fmt.Errorf("remote-console container not found")
	}

	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		// Poll for the first aggregation log path in the container
		exitCode, reader, err := rcsContainer.Exec(context.Background(), []string{"sh", "-c", "find /tmp -maxdepth 2 -name 'consoleAgg-*.log' | head -n 1"})
		if err == nil {
			data, _ := io.ReadAll(reader)
			// Parse and validate the path
			raw := strings.TrimSpace(string(data))
			path := raw
			if idx := strings.Index(raw, "/"); idx >= 0 {
				path = raw[idx:]
			}
			if exitCode == 0 && path != "" && strings.HasSuffix(path, ".log") {
				return path, nil
			}
			// Not found yet
			lastErr = fmt.Errorf("exit code %d, path %q", exitCode, path)
		} else {
			// Exec error
			lastErr = err
		}
		time.Sleep(2 * time.Second)
	}

	return "", fmt.Errorf("aggregation log file not found: %w", lastErr)
}

func (s *IntegrationTestSuite) waitForAggLogEntry(aggPath string, msg string, timeout time.Duration) (string, error) {
	rcsContainer, ok := s.containers["remote-console"]
	if !ok {
		return "", fmt.Errorf("remote-console container not found")
	}

	// Escape single quotes in the message for safe shell usage
	safeMsg := strings.ReplaceAll(msg, "'", "'\"'\"'")
	deadline := time.Now().Add(timeout)
	var lastOutput string
	var lastErr error

	// Poll for the log entry until timeout
	for time.Now().Before(deadline) {
		// Use grep to search for the message in the aggregation log
		cmd := fmt.Sprintf("grep -nF '%s' %s || true", safeMsg, aggPath)
		exitCode, reader, err := rcsContainer.Exec(context.Background(), []string{"sh", "-c", cmd})
		if err == nil {
			data, _ := io.ReadAll(reader)
			output := string(data)
			lastOutput = output
			if exitCode == 0 && strings.TrimSpace(output) != "" {
				return output, nil
			}
		} else {
			lastErr = err
		}
		time.Sleep(2 * time.Second)
	}

	if lastErr != nil {
		return "", fmt.Errorf("aggregation log entry not found for %q; last error: %v; last output: %s", msg, lastErr, lastOutput)
	}

	return "", fmt.Errorf("aggregation log entry not found for %q; last output: %s", msg, lastOutput)
}

func (s *IntegrationTestSuite) TestLogAggregation() {
	aggPath, err := s.waitForAggLogFile(1 * time.Minute)
	s.Require().NoError(err, "expected aggregation log file to be present")
	s.T().Logf("Aggregation log path: %s", aggPath)

	console := consoleFixtures["ssh-password"]

	params := url.Values{}
	params.Set("follow", "true")
	followURL, err := s.tailWebSocketURL(console.nodeID, params)
	s.Require().NoError(err)

	tailConn, tailResp, err := s.dialWebSocket(followURL)
	s.Require().NoError(err)
	defer func() {
		if err := tailResp.Body.Close(); err != nil {
			s.T().Logf("Warning: failed to close tail response body: %v", err)
		}
		if err := tailConn.Close(); err != nil {
			s.T().Logf("Warning: failed to close tail websocket: %v", err)
		}
	}()

	if console.readyLogMarker != "" {
		_, err = s.readWebSocketUntil(tailConn, console.readyLogMarker, tailMessageTimeout)
		s.Require().NoError(err, "Expected console readiness marker")
	}

	// Send a unique message to be aggregated
	msg := makeUnique("log-agg" + console.name)
	exitCode, output, err := s.broadcastConsoleMessage(console, msg)
	s.Require().NoError(err)
	s.T().Logf("Sent aggregation message to %s (exit code %d): %s", console.name, exitCode, output)

	_, err = s.readWebSocketUntil(tailConn, msg, tailMessageTimeout)
	s.Require().NoError(err, "tail should see broadcast message")

	// Now verify the aggregation log contains the message
	entry, err := s.waitForAggLogEntry(aggPath, msg, 90*time.Second)
	s.Require().NoErrorf(err, "expected aggregation log to contain %q", msg)
	s.Require().Contains(entry, console.nodeID, "aggregation log entry should include node id")
	s.Require().Contains(entry, msg, "aggregation log entry should include message")

}
