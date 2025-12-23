package test

import (
	"fmt"
	"io"
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
		exitCode, reader, err := rcsContainer.Exec(s.ctx, []string{"sh", "-c", "find /tmp -maxdepth 2 -name 'consoleAgg-*.log' | head -n 1"})
		if err == nil {
			data, _ := io.ReadAll(reader)
			raw := strings.TrimSpace(string(data))
			path := raw
			if idx := strings.Index(raw, "/"); idx >= 0 {
				path = raw[idx:]
			}
			if exitCode == 0 && path != "" && strings.HasSuffix(path, ".log") {
				return path, nil
			}
			lastErr = fmt.Errorf("exit code %d, path %q", exitCode, path)
		} else {
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

	safeMsg := strings.ReplaceAll(msg, "'", "'\"'\"'")
	deadline := time.Now().Add(timeout)
	var lastOutput string

	for time.Now().Before(deadline) {
		cmd := fmt.Sprintf("grep -nF '%s' %s || true", safeMsg, aggPath)
		exitCode, reader, err := rcsContainer.Exec(s.ctx, []string{"sh", "-c", cmd})
		if err == nil {
			data, _ := io.ReadAll(reader)
			output := string(data)
			lastOutput = output
			if exitCode == 0 && strings.TrimSpace(output) != "" {
				return output, nil
			}
		} else {
			lastOutput = err.Error()
		}
		time.Sleep(2 * time.Second)
	}

	tailCmd := fmt.Sprintf("tail -n 20 %s || true", aggPath)
	_, tailReader, _ := rcsContainer.Exec(s.ctx, []string{"sh", "-c", tailCmd})
	tailData, _ := io.ReadAll(tailReader)

	return "", fmt.Errorf("aggregation log entry not found for %q; last output: %s; tail:\n%s", msg, lastOutput, string(tailData))
}

func (s *IntegrationTestSuite) TestLogAggregation() {
	aggPath, err := s.waitForAggLogFile(1 * time.Minute)
	s.Require().NoError(err, "expected aggregation log file to be present")
	s.T().Logf("Aggregation log path: %s", aggPath)

	consoles := []consoleFixture{consoleFixtures["ssh-password"]}

	for _, console := range consoles {
		followURL, err := s.tailWebSocketURL(console.nodeID, "follow=true")
		s.Require().NoError(err)

		tailConn, tailResp, err := s.dialWebSocket(followURL)
		s.Require().NoError(err)

		if console.readyLogMarker != "" {
			_, err = s.readWebSocketUntil(tailConn, console.readyLogMarker, tailMessageTimeout)
			s.Require().NoError(err, "Expected console readiness marker")
		}

		msg := makeUnique("log-agg" + console.name)
		exitCode, output, err := s.broadcastConsoleMessage(console, msg)
		s.Require().NoError(err)
		s.T().Logf("Sent aggregation message to %s (exit code %d): %s", console.name, exitCode, output)

		_, err = s.readWebSocketUntil(tailConn, msg, tailMessageTimeout)
		s.Require().NoError(err, "tail should see aggregation message")

		entry, err := s.waitForAggLogEntry(aggPath, msg, 90*time.Second)
		s.Require().NoErrorf(err, "expected aggregation log to contain %q", msg)
		s.Require().Contains(entry, console.nodeID, "aggregation log entry should include node id")
		s.Require().Contains(entry, msg, "aggregation log entry should include message")

		tailResp.Body.Close()
		tailConn.Close()
	}
}
