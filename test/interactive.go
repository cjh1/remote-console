package test

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	consoleConnectAttempts = 8
	consoleRetryDelay      = 8 * time.Second
)

func (s *IntegrationTestSuite) TestSSHPasswordConsoleInteractive() {
	promptTimeout := 90 * time.Second

	nodeID := "x0c0s0b0"
	wsConn, resp, err := s.connectInteractiveConsole(nodeID, promptTimeout)
	s.Require().NoError(err)
	defer resp.Body.Close()
	defer wsConn.Close()

	// Execute hostname over websocket (carriage return to mimic Enter)
	testMsg := "hostname\r"
	err = wsConn.WriteMessage(websocket.TextMessage, []byte(testMsg))
	s.Require().NoError(err, "Error sending test message to console")

	expectedHostLine := nodeID + "\r\n"
	hostnameOutput, err := s.readWebSocketUntil(wsConn, expectedHostLine, promptTimeout)
	s.Require().NoError(err, "Expected hostname output from console")
	s.Require().True(strings.Contains(hostnameOutput, expectedHostLine),
		"Expected hostname command output in console output; got %q", hostnameOutput)
	s.T().Logf("Received hostname from console: %s", hostnameOutput)
}

func (s *IntegrationTestSuite) TestSSHPasswordConsoleInteractiveTail() {
	promptTimeout := 90 * time.Second

	nodeID := "x0c0s0b0"
	wsConn, resp, err := s.connectInteractiveConsole(nodeID, promptTimeout)
	s.Require().NoError(err)
	defer resp.Body.Close()
	defer wsConn.Close()

	// Execute hostname over websocket
	testMsg := "hostname\r"
	err = wsConn.WriteMessage(websocket.TextMessage, []byte(testMsg))
	s.Require().NoError(err, "Error sending test message to console")

	// Read response until the hostname appears, ensuring the command executed
	expectedHostLine := nodeID + "\r\n"
	hostnameOutput, err := s.readWebSocketUntil(wsConn, expectedHostLine, promptTimeout)
	s.Require().NoError(err, "Expected hostname output from console")
	s.Require().True(strings.Contains(hostnameOutput, expectedHostLine),
		"Expected hostname command output in console output; got %q", hostnameOutput)
	s.T().Logf("Received hostname from console: %s", hostnameOutput)

	// Broadcast a message to the console log and ensure we see it in the tail output
	msg := "only me"
	sshPasswordContainer := s.containers["ssh-password"]
	exitCode, output, err := sshPasswordContainer.Exec(s.ctx, []string{"broadcast.sh", msg})
	s.Require().NoError(err)
	s.T().Logf("Sent test message to console (exit code %d): %s", exitCode, output)

	_, err = s.readWebSocketUntil(wsConn, msg, 30*time.Second)
	s.Require().NoError(err, "Expected to find broadcast message in console output")
}

func (s *IntegrationTestSuite) waitForConsolePrompt(wsConn *websocket.Conn, searchString string, totalTimeout time.Duration) (string, error) {
	wsConn.SetReadDeadline(time.Now().Add(totalTimeout))
	defer wsConn.SetReadDeadline(time.Time{})

	const keepAliveInterval = 5 * time.Second
	ticker := time.NewTicker(keepAliveInterval)
	defer ticker.Stop()

	done := make(chan struct{})
	defer close(done)

	// Send periodic newlines to keep the console session active
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				s.T().Log("Console idle, sending newline to trigger prompt")
				if err := wsConn.WriteMessage(websocket.TextMessage, []byte("\n")); err != nil {
					s.T().Logf("Failed to send keepalive newline: %v", err)
					return
				}
			}
		}
	}()

	var output strings.Builder

	for {
		_, message, err := wsConn.ReadMessage()
		if err != nil {
			return output.String(), fmt.Errorf("waiting for prompt: %w", err)
		}

		msgStr := string(message)
		s.T().Logf("Console: %s", msgStr)
		output.WriteString(msgStr)
		if strings.Contains(output.String(), searchString) {
			return output.String(), nil
		}
	}
}

func (s *IntegrationTestSuite) connectInteractiveConsole(nodeID string, promptTimeout time.Duration) (*websocket.Conn, *http.Response, error) {
	parsedURL, err := url.Parse(s.apiURL)
	if err != nil {
		return nil, nil, fmt.Errorf("parse API URL: %w", err)
	}

	wsURL := url.URL{
		Scheme: "ws",
		Host:   parsedURL.Host,
		Path:   fmt.Sprintf("/remote-console/consoles/%s", nodeID),
	}

	var lastErr error

	for attempt := 1; attempt <= consoleConnectAttempts; attempt++ {
		s.T().Logf("Connecting to console %s (attempt %d/%d)", nodeID, attempt, consoleConnectAttempts)

		wsConn, resp, err := s.dialWebSocket(wsURL)
		if err != nil {
			lastErr = fmt.Errorf("websocket dial: %w", err)
			s.T().Logf("Console dial attempt %d/%d failed: %v", attempt, consoleConnectAttempts, err)
			time.Sleep(consoleRetryDelay)
			continue
		}

		if err := wsConn.WriteMessage(websocket.TextMessage, []byte("\n")); err != nil {
			resp.Body.Close()
			wsConn.Close()
			lastErr = fmt.Errorf("send initial newline: %w", err)
			s.T().Logf("Console newline attempt %d/%d failed: %v", attempt, consoleConnectAttempts, err)
			time.Sleep(consoleRetryDelay)
			continue
		}

		initialOutput, err := s.waitForConsolePrompt(wsConn, ":~$ ", promptTimeout)
		if err != nil {
			resp.Body.Close()
			wsConn.Close()
			lastErr = err
			s.T().Logf("Console prompt attempt %d/%d failed: %v", attempt, consoleConnectAttempts, err)
			if initialOutput != "" {
				s.T().Logf("Console output before failure (attempt %d): %q", attempt, initialOutput)
			}
			time.Sleep(consoleRetryDelay)
			continue
		}

		if !strings.Contains(initialOutput, fmt.Sprintf("%s:~$", nodeID)) {
			resp.Body.Close()
			wsConn.Close()
			lastErr = fmt.Errorf("prompt host mismatch, expected %s:~$ in console output: %q", nodeID, initialOutput)
			s.T().Logf("Prompt host mismatch on attempt %d; output: %q", attempt, initialOutput)
			time.Sleep(consoleRetryDelay)
			continue
		}

		s.T().Logf("Console %s ready: %s", nodeID, initialOutput)
		return wsConn, resp, nil
	}

	return nil, nil, fmt.Errorf("failed to establish console session after %d attempts: %w", consoleConnectAttempts, lastErr)
}
