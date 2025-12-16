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


func (s *IntegrationTestSuite) waitForConsolePrompt(wsConn *websocket.Conn, searchString string, totalTimeout time.Duration) (string, error) {
	wsConn.SetReadDeadline(time.Now().Add(totalTimeout))
	defer wsConn.SetReadDeadline(time.Time{})

	var output strings.Builder

	for {

		if err := wsConn.WriteMessage(websocket.TextMessage, []byte("\n")); err != nil {
			s.T().Logf("Failed to send keepalive newline: %v", err)
			return "", fmt.Errorf("waiting for prompt: %w", err)
		}
		// Sleep briefly to allow console to respond
		time.Sleep(500 * time.Millisecond)

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

func (s *IntegrationTestSuite) connectInteractiveConsole(nodeID string, prompt string, promptTimeout time.Duration) (*websocket.Conn, *http.Response, error) {
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

	// We try multiple times to connect to the console, as it may take a bit for conmand to be available.
	// TODO we should update this to be more deterministic way to know when the console is ready.
	for attempt := 1; attempt <= consoleConnectAttempts; attempt++ {
		s.T().Logf("Connecting to console %s (attempt %d/%d)", nodeID, attempt, consoleConnectAttempts)

		wsConn, resp, err := s.dialWebSocket(wsURL)
		if err != nil {
			lastErr = fmt.Errorf("websocket dial: %w", err)
			s.T().Logf("Console dial attempt %d/%d failed: %v", attempt, consoleConnectAttempts, err)
			time.Sleep(consoleRetryDelay)
			continue
		}

		if prompt != "" {
			initialOutput, err := s.waitForConsolePrompt(wsConn, prompt, promptTimeout)
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
		} else {
			s.T().Logf("Skipping prompt wait for console %s", nodeID)
		}
		return wsConn, resp, nil
	}

	return nil, nil, fmt.Errorf("failed to establish console session after %d attempts: %w", consoleConnectAttempts, lastErr)
}

func (s *IntegrationTestSuite) TestConsoleInteractive() {
	promptTimeout := 90 * time.Second

	for _, fixture := range consoleFixtures {
		s.Run(fixture.name, func() {
			wsConn, resp, err := s.connectInteractiveConsole(fixture.nodeID, fixture.prompt, promptTimeout)
			s.Require().NoError(err)
			defer resp.Body.Close()
			defer wsConn.Close()

			testMsg := "hostname\r"
			err = wsConn.WriteMessage(websocket.TextMessage, []byte(testMsg))
			s.Require().NoError(err, "Error sending test message to console")

			expectedHostLine := fixture.nodeID + "\r\n"
			hostnameOutput, err := s.readWebSocketUntil(wsConn, expectedHostLine, promptTimeout)
			s.Require().NoError(err, "Expected hostname output from console")
			s.Require().True(strings.Contains(hostnameOutput, expectedHostLine),
				"Expected hostname command output in console output; got %q", hostnameOutput)
			s.T().Logf("Received hostname from console %s: %s", fixture.name, hostnameOutput)
		})
	}
}

func (s *IntegrationTestSuite) TestConsoleInteractiveTail() {
	promptTimeout := 90 * time.Second

	for _, fixture := range consoleFixtures {
		s.Run(fixture.name, func() {
			wsConn, resp, err := s.connectInteractiveConsole(fixture.nodeID, fixture.prompt, promptTimeout)
			s.Require().NoError(err)
			defer resp.Body.Close()
			defer wsConn.Close()

			testMsg := "hostname\r"
			err = wsConn.WriteMessage(websocket.TextMessage, []byte(testMsg))
			s.Require().NoError(err, "Error sending test message to console")

			expectedHostLine := fixture.nodeID + "\r\n"
			hostnameOutput, err := s.readWebSocketUntil(wsConn, expectedHostLine, promptTimeout)
			s.Require().NoError(err, "Expected hostname output from console")
			s.Require().True(strings.Contains(hostnameOutput, expectedHostLine),
				"Expected hostname command output in console output; got %q", hostnameOutput)
			s.T().Logf("Received hostname from console %s: %s", fixture.name, hostnameOutput)

			msg := uniqueMessage("interactive-tail-" + fixture.name)
			exitCode, output, err := s.broadcastConsoleMessage(fixture, msg)
			s.Require().NoError(err)
			s.T().Logf("Sent test message to %s console (exit code %d): %s", fixture.name, exitCode, output)

			_, err = s.readWebSocketUntil(wsConn, msg, 30*time.Second)
			s.Require().NoError(err, "Expected to find broadcast message in console output")
		})
	}
}
