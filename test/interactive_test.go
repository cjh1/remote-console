// Copyright © 2026 OpenCHAMI a Series of LF Projects, LLC
//
// SPDX-License-Identifier: MIT

package test

import (
	"context"
	"errors"
	"fmt"
	"net"
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
	if err := wsConn.SetReadDeadline(time.Now().Add(totalTimeout)); err != nil {
		return "", fmt.Errorf("waiting for prompt: failed to set read deadline: %w", err)
	}
	defer func() {
		// Clear read deadline after function completes
		if err := wsConn.SetReadDeadline(time.Time{}); err != nil {
			s.T().Logf("Warning: failed to clear read deadline: %v", err)
		}
	}()

	var output strings.Builder
	logger := s.newConsoleMessageLogger()

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
		logger.LogChunk(msgStr)
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
	for attempt := 1; attempt <= consoleConnectAttempts; attempt++ {
		s.T().Logf("Connecting to console %s (attempt %d/%d)", nodeID, attempt, consoleConnectAttempts)

		wsConn, resp, err := s.dialWebSocket(wsURL)
		if err != nil {
			lastErr = fmt.Errorf("websocket dial: %w", err)
			s.T().Logf("Console dial attempt %d/%d failed: %v", attempt, consoleConnectAttempts, err)
			time.Sleep(consoleRetryDelay)
			continue
		}

		initialOutput, err := s.waitForConsolePrompt(wsConn, prompt, promptTimeout)
		if err != nil {
			if err := resp.Body.Close(); err != nil {
				s.T().Logf("Warning: failed to close response body: %v", err)
			}
			if err := wsConn.Close(); err != nil {
				s.T().Logf("Warning: failed to close websocket: %v", err)
			}
			lastErr = err
			s.T().Logf("Console prompt attempt %d/%d failed: %v", attempt, consoleConnectAttempts, err)
			if initialOutput != "" {
				s.T().Logf("Console output before failure (attempt %d): %q", attempt, initialOutput)
			}
			time.Sleep(consoleRetryDelay)
			continue
		}

		s.T().Logf("Console %s ready: %s", nodeID, initialOutput)

		return wsConn, resp, nil
	}

	return nil, nil, fmt.Errorf("failed to establish console session after %d attempts: %w", consoleConnectAttempts, lastErr)
}

func (s *IntegrationTestSuite) TestConsoleInteractive() {
	promptTimeout := 90 * time.Second

	for _, console := range consoleFixtureList() {
		s.Run(console.name, func() {
			wsConn, resp, err := s.connectInteractiveConsole(console.nodeID, console.prompt, promptTimeout)
			s.Require().NoError(err)
			defer func() {
				if err := resp.Body.Close(); err != nil {
					s.T().Logf("Warning: failed to close response body: %v", err)
				}
				if err := wsConn.Close(); err != nil {
					s.T().Logf("Warning: failed to close websocket: %v", err)
				}
			}()

			cmd := "hostname\r"
			err = wsConn.WriteMessage(websocket.TextMessage, []byte(cmd))
			s.Require().NoError(err, "Error sending test message to console")

			expectedHostLine := console.nodeID + "\r\n"
			hostnameOutput, err := s.readWebSocketUntil(wsConn, expectedHostLine, promptTimeout)
			s.Require().NoError(err, "Expected hostname output from console")
			s.Require().True(strings.Contains(hostnameOutput, expectedHostLine),
				"Expected hostname command output in console output; got %q", hostnameOutput)
			s.T().Logf("Received hostname from console %s: %s", console.name, hostnameOutput)
		})
	}
}

func (s *IntegrationTestSuite) TestConsoleInteractiveTail() {
	promptTimeout := 90 * time.Second

	for _, console := range consoleFixtureList() {
		s.Run(console.name, func() {
			wsConn, resp, err := s.connectInteractiveConsole(console.nodeID, console.prompt, promptTimeout)
			s.Require().NoError(err)
			defer func() {
				if err := resp.Body.Close(); err != nil {
					s.T().Logf("Warning: failed to close response body: %v", err)
				}
				if err := wsConn.Close(); err != nil {
					s.T().Logf("Warning: failed to close websocket: %v", err)
				}
			}()

			cmd := "hostname\r"
			err = wsConn.WriteMessage(websocket.TextMessage, []byte(cmd))
			s.Require().NoError(err, "Error sending test message to console")

			expectedHostLine := console.nodeID + "\r\n"
			hostnameOutput, err := s.readWebSocketUntil(wsConn, expectedHostLine, promptTimeout)
			s.Require().NoError(err, "Expected hostname output from console")
			s.Require().True(strings.Contains(hostnameOutput, expectedHostLine),
				"Expected hostname command output in console output; got %q", hostnameOutput)
			s.T().Logf("Received hostname from console %s: %s", console.name, hostnameOutput)

			msg := makeUnique("interactive-tail" + console.name)
			exitCode, output, err := s.broadcastConsoleMessage(console, msg)
			s.Require().NoError(err)
			s.T().Logf("Sent test message to %s console (exit code %d): %s", console.name, exitCode, output)

			_, err = s.readWebSocketUntil(wsConn, msg, 30*time.Second)
			s.Require().NoError(err, "Expected to find broadcast message in console output")
		})
	}
}

func (s *IntegrationTestSuite) TestConsoleInteractiveReconnect() {
	// Use existing console from SetupSuite
	console := consoleFixtures["ssh-password"]
	existingNodeID := console.nodeID
	promptTimeout := 90 * time.Second

	// Connect to interactive console
	wsConn, resp, err := s.connectInteractiveConsole(existingNodeID, console.prompt, promptTimeout)
	s.Require().NoError(err)
	defer func() {
		if err := resp.Body.Close(); err != nil {
			s.T().Logf("Warning: failed to close response body: %v", err)
		}
		if err := wsConn.Close(); err != nil {
			s.T().Logf("Warning: failed to close websocket: %v", err)
		}
	}()

	// Run hostname command and validate console is working before triggering reconnection
	s.T().Log("Running initial hostname command to verify console is working")
	err = wsConn.WriteMessage(websocket.TextMessage, []byte("hostname\r"))
	s.Require().NoError(err, "Error sending hostname command")

	expectedHostLine := existingNodeID + "\r\n"
	hostnameOutput, err := s.readWebSocketUntil(wsConn, expectedHostLine, promptTimeout)
	s.Require().NoError(err, "Expected hostname output from console")
	s.Require().Contains(hostnameOutput, expectedHostLine, "Expected hostname in output")
	s.Require().NotContains(hostnameOutput, "[Reconnecting", "Console should not be reconnecting initially")
	s.Require().NotContains(hostnameOutput, "Connection refused", "Console should be connected without errors")
	s.T().Logf("Initial hostname output: %s", hostnameOutput)

	// Give the console a moment to stabilize
	time.Sleep(2 * time.Second)

	// Stop the SSH container to trigger a disconnection
	s.T().Log("Stopping SSH container to trigger disconnection")
	stopTimeout := 10 * time.Second
	s.Require().NoError(s.containers["ssh-password"].Stop(context.Background(), &stopTimeout))

	// Verify disconnect message appears
	disconnectMsg := fmt.Sprintf("[Console %s disconnected at", existingNodeID)
	output, err := s.readWebSocketUntil(wsConn, disconnectMsg, 2*time.Minute)
	s.Require().NoError(err, "Expected disconnect message")
	s.Require().Contains(output, disconnectMsg, "Expected disconnect message in output")
	s.T().Logf("Saw disconnect message: %s", output)

	// Restart the SSH container so the node can reconnect
	s.T().Log("Starting SSH container to allow reconnection")
	s.Require().NoError(s.containers["ssh-password"].Start(context.Background()))

	// Wait for reconnection banner
	connectedMsg := fmt.Sprintf("[Console %s connected at", existingNodeID)
	output, err = s.readWebSocketUntil(wsConn, connectedMsg, 2*time.Minute)
	s.Require().NoError(err, "Expected connected message after reconnection")
	s.Require().Contains(output, connectedMsg, "Expected connected message in output")
	s.T().Logf("Saw connected message: %s", output)

	// Wait for console prompt to appear after reconnection
	s.T().Log("Waiting for console prompt after reconnection")
	_, err = s.waitForConsolePrompt(wsConn, ":~$ ", promptTimeout)
	s.Require().NoError(err, "Expected console prompt after reconnection")
	s.T().Log("Console prompt received after reconnection")

	// Rerun hostname command to verify reconnection worked
	s.T().Log("Running hostname command after reconnection")
	err = wsConn.WriteMessage(websocket.TextMessage, []byte("hostname\r"))
	s.Require().NoError(err, "Error sending hostname command after reconnect")

	hostnameOutput2, err := s.readWebSocketUntil(wsConn, expectedHostLine, promptTimeout)
	s.Require().NoError(err, "Expected hostname output after reconnection")
	s.Require().Contains(hostnameOutput2, expectedHostLine, "Expected hostname in output after reconnection")
	s.T().Logf("Hostname output after reconnection: %s", hostnameOutput2)

	s.T().Log("Reconnection test completed successfully")
}

func (s *IntegrationTestSuite) TestConsoleInteractiveNewNodeNoDisrupt() {
	// Verify that adding a new SSH node does not disrupt an existing interactive session.
	// Previously (conman era) adding a node caused conmand to restart, which briefly
	// disconnected all existing consoles. With the Go SSH backend each node runs
	// independently, so existing sessions must be unaffected.
	console := consoleFixtures["ssh-password"]
	existingNodeID := console.nodeID
	promptTimeout := 90 * time.Second

	wsConn, resp, err := s.connectInteractiveConsole(existingNodeID, console.prompt, promptTimeout)
	s.Require().NoError(err)
	defer func() {
		if err := resp.Body.Close(); err != nil {
			s.T().Logf("Warning: failed to close response body: %v", err)
		}
		if err := wsConn.Close(); err != nil {
			s.T().Logf("Warning: failed to close websocket: %v", err)
		}
	}()

	// Verify the existing console is working before adding a new node.
	err = wsConn.WriteMessage(websocket.TextMessage, []byte("hostname\r"))
	s.Require().NoError(err, "Error sending hostname command")
	expectedHostLine := existingNodeID + "\r\n"
	hostnameOutput, err := s.readWebSocketUntil(wsConn, expectedHostLine, promptTimeout)
	s.Require().NoError(err, "Expected hostname output before new node added")
	s.Require().Contains(hostnameOutput, expectedHostLine)
	s.T().Logf("Initial hostname output: %s", hostnameOutput)

	// Add a new SSH node.
	newNodeID := "x0c0s10b0"
	s.T().Logf("Adding new node %s", newNodeID)

	authConfig := defaultAuthConfig
	rfContainer, err := startRedfishEmulator(context.Background(), s.rfNetwork.Name, newNodeID, "ssh", &authConfig)
	s.Require().NoError(err)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := rfContainer.Terminate(ctx); err != nil {
			s.T().Logf("Warning: failed to terminate Redfish emulator %s: %v", newNodeID, err)
		}
	}()

	sshContainer, err := startSSHPasswordServer(context.Background(), s.consoleNetwork.Name, newNodeID, "ADMIN", "ADMIN")
	s.Require().NoError(err)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := sshContainer.Terminate(ctx); err != nil {
			s.T().Logf("Warning: failed to terminate SSH container %s: %v", newNodeID, err)
		}
	}()

	smdAPIURL, err := getSMDAPIURL(context.Background(), s.containers["smd"])
	s.Require().NoError(err)

	err = loadRedfishEndpoint(s.T(), context.Background(), smdAPIURL, redfishEndpoint{
		Host:     newNodeID,
		Username: "ADMIN",
		Password: "ADMIN",
	})
	s.Require().NoError(err, "failed to register new Redfish endpoint")

	defer func() {
		smdAPIURL, err := getSMDAPIURL(context.Background(), s.containers["smd"])
		if err != nil {
			s.T().Errorf("Warning: failed to get SMD API URL: %v", err)
			return
		}
		if err := deleteRedfishEndpoint(s.T(), context.Background(), smdAPIURL, newNodeID); err != nil {
			s.T().Logf("Warning: failed to remove Redfish endpoint %s: %v", newNodeID, err)
		}
	}()

	// Wait for remote-console to discover the new node.
	s.Require().NoError(s.waitForConsoleID(newNodeID, 3*time.Minute), "remote-console did not detect new console")
	s.T().Logf("New node %s is visible in remote-console", newNodeID)

	// The existing session must still be alive and functional — no disconnect markers,
	// no reconnect banners.
	err = wsConn.WriteMessage(websocket.TextMessage, []byte("hostname\r"))
	s.Require().NoError(err, "Error sending hostname command after new node added")
	hostnameOutput2, err := s.readWebSocketUntil(wsConn, expectedHostLine, promptTimeout)
	s.Require().NoError(err, "Expected hostname output after new node added")
	s.Require().Contains(hostnameOutput2, expectedHostLine, "Expected hostname in output after new node added")
	s.Require().NotContains(hostnameOutput2, fmt.Sprintf("[Console %s disconnected", existingNodeID),
		"Existing session must not have disconnected when a new node was added")
	s.T().Logf("Existing session still alive after new node added: %s", hostnameOutput2)
}

func (s *IntegrationTestSuite) TestConsoleInteractiveClientClose() {
	console := consoleFixtures["ssh-password"]
	promptTimeout := 90 * time.Second

	wsConn, resp, err := s.connectInteractiveConsole(console.nodeID, console.prompt, promptTimeout)
	s.Require().NoError(err)
	defer func() {
		if err := resp.Body.Close(); err != nil {
			s.T().Logf("Warning: failed to close response body: %v", err)
		}
	}()

	_ = wsConn.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "client closing"))
	if err := wsConn.Close(); err != nil {
		s.T().Logf("Warning: failed to close websocket: %v", err)
	}

	time.Sleep(2 * time.Second)

	wsConn, resp, err = s.connectInteractiveConsole(console.nodeID, console.prompt, promptTimeout)
	s.Require().NoError(err)
	defer func() {
		if err := resp.Body.Close(); err != nil {
			s.T().Logf("Warning: failed to close response body: %v", err)
		}
		if err := wsConn.Close(); err != nil {
			s.T().Logf("Warning: failed to close websocket: %v", err)
		}
	}()

	err = wsConn.WriteMessage(websocket.TextMessage, []byte("hostname\r"))
	s.Require().NoError(err, "Error sending hostname command")

	expectedHostLine := console.nodeID + "\r\n"
	hostnameOutput, err := s.readWebSocketUntil(wsConn, expectedHostLine, promptTimeout)
	s.Require().NoError(err, "Expected hostname output from console after reconnect")
	s.Require().Contains(hostnameOutput, expectedHostLine, "Expected hostname in output")
}

func (s *IntegrationTestSuite) TestConsoleInteractiveServerClose() {
	promptTimeout := 90 * time.Second
	newNodeID := "x0c0s10b1"
	console := consoleFixtures["ssh-password"]

	authConfig := defaultAuthConfig
	rfContainer, err := startRedfishEmulator(context.Background(), s.rfNetwork.Name, newNodeID, "ssh", &authConfig)
	s.Require().NoError(err)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := rfContainer.Terminate(ctx); err != nil {
			s.T().Logf("Warning: failed to terminate Redfish emulator %s: %v", newNodeID, err)
		}
	}()

	sshContainer, err := startSSHPasswordServer(context.Background(), s.consoleNetwork.Name, newNodeID, "ADMIN", "ADMIN")
	s.Require().NoError(err)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := sshContainer.Terminate(ctx); err != nil {
			s.T().Logf("Warning: failed to terminate SSH container %s: %v", newNodeID, err)
		}
	}()

	smdAPIURL, err := getSMDAPIURL(context.Background(), s.containers["smd"])
	s.Require().NoError(err)

	err = loadRedfishEndpoint(s.T(), context.Background(), smdAPIURL, redfishEndpoint{
		Host:     newNodeID,
		Username: "ADMIN",
		Password: "ADMIN",
	})
	s.Require().NoError(err, "failed to register Redfish endpoint")

	wsConn, resp, err := s.connectInteractiveConsole(newNodeID, console.prompt, promptTimeout)
	s.Require().NoError(err)
	defer func() {
		if err := resp.Body.Close(); err != nil {
			s.T().Logf("Warning: failed to close response body: %v", err)
		}
		if err := wsConn.Close(); err != nil {
			s.T().Logf("Warning: failed to close websocket: %v", err)
		}
	}()

	err = deleteRedfishEndpoint(s.T(), context.Background(), smdAPIURL, newNodeID)
	s.Require().NoError(err, "failed to remove Redfish endpoint")

	s.Require().NoError(s.waitForConsoleRemoval(newNodeID, 3*time.Minute), "remote-console did not drop removed console")

	readDeadline := time.Now().Add(5 * time.Second)
	if err := wsConn.SetReadDeadline(readDeadline); err != nil {
		s.T().Fatalf("Failed to set read deadline after endpoint removal: %v", err)
	}
	_, _, err = wsConn.ReadMessage()
	if err != nil {
		var closeErr *websocket.CloseError
		if !errors.As(err, &closeErr) && !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
			var netErr net.Error
			if !errors.As(err, &netErr) || !netErr.Timeout() {
				s.T().Fatalf("Unexpected websocket read error after endpoint removal: %v", err)
			}
		}
	}
}

func (s *IntegrationTestSuite) TestConsoleInteractiveInvalidNode() {
	parsedURL, err := url.Parse(s.apiURL)
	s.Require().NoError(err, "Failed to parse API URL")

	// Try to connect to a non-existent node
	invalidNodeID := "x9c9s9b9"
	wsURL := url.URL{
		Scheme: "ws",
		Host:   parsedURL.Host,
		Path:   fmt.Sprintf("/remote-console/consoles/%s", invalidNodeID),
	}

	_, resp, err := s.dialWebSocket(wsURL)

	// Should get an error because the WebSocket upgrade should fail with 404
	s.Require().Error(err, "Expected error when connecting to invalid node")

	if resp != nil {
		defer func() {
			if err := resp.Body.Close(); err != nil {
				s.T().Logf("Warning: failed to close response body: %v", err)
			}
		}()
		s.Require().Equal(http.StatusNotFound, resp.StatusCode,
			"Expected 404 Not Found for invalid node")
		s.T().Logf("Got expected 404 status for invalid node %s", invalidNodeID)
	}
}
