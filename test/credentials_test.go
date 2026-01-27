package test

import (
	"context"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
)

func (s *IntegrationTestSuite) TestConsoleCredentialRefresh() {
	console := consoleFixtures["ssh-password"]
	nodeID := console.nodeID
	username := console.username
	correctPassword := console.password
	invalidPassword := "wrong-password"

	promptTimeout := 90 * time.Second

	// Always restore the correct password so other tests are not affected.
	defer func() {
		if err := setConsoleCredentials(context.Background(), s.vaultContainer, nodeID, username, correctPassword); err != nil {
			s.T().Fatalf("failed to restore credentials for %s: %v", nodeID, err)
		}
	}()

	wsConn, resp, err := s.connectInteractiveConsole(nodeID, ":~$ ", promptTimeout)
	s.Require().NoError(err)
	defer func() {
		if err := resp.Body.Close(); err != nil {
			s.T().Logf("Warning: failed to close response body: %v", err)
		}
		if err := wsConn.Close(); err != nil {
			s.T().Logf("Warning: failed to close websocket: %v", err)
		}
	}()

	// Verify the console works before changing credentials.
	s.Require().NoError(wsConn.WriteMessage(websocket.TextMessage, []byte("hostname\r")), "send hostname before creds change")
	hostnameOutput, err := s.readWebSocketUntil(wsConn, nodeID+"\r\n", promptTimeout)
	s.Require().NoError(err, "expected hostname output before creds change")
	s.Require().Contains(hostnameOutput, nodeID+"\r\n")

	// Invalidate the credentials.
	s.T().Log("Setting invalid credentials to trigger authentication failure")
	s.Require().NoError(setConsoleCredentials(context.Background(), s.vaultContainer, nodeID, username, invalidPassword))

	// Wait for credential monitor to detect the change.
	// RCS_CREDS_MONITOR_INTERVAL is 10 seconds, so wait a bit longer to ensure detection.
	s.T().Log("Waiting for credential monitor to detect change...")
	time.Sleep(15 * time.Second)

	// SSH returns "Permission denied, please try again." when password auth fails.
	authOutput, err := s.readWebSocketUntil(wsConn, "Permission denied", 2*time.Minute)
	s.Require().NoError(err, "expected authentication error after credentials were set incorrectly")
	s.T().Logf("Observed auth error in console output: %s", authOutput)

	s.T().Log("Restoring valid credentials")
	s.Require().NoError(setConsoleCredentials(context.Background(), s.vaultContainer, nodeID, username, correctPassword))

	// Wait for credential monitor to detect the restored credentials.
	// RCS_CREDS_MONITOR_INTERVAL is 10 seconds, so wait a bit longer to ensure detection.
	s.T().Log("Waiting for credential monitor to detect restored credentials...")
	time.Sleep(15 * time.Second)

	reconnectMarker := fmt.Sprintf("<ConMan> Connection to console [%s] opened", nodeID)
	reconnectOutput, err := s.readWebSocketUntil(wsConn, reconnectMarker, 2*time.Minute)
	s.Require().NoError(err, "expected conman to reconnect after credentials were restored")
	s.T().Logf("Observed reconnection marker in console output: %s", reconnectOutput)

	_, err = s.waitForConsolePrompt(wsConn, ":~$ ", promptTimeout)
	s.Require().NoError(err, "expected console prompt after credentials were restored")

	s.Require().NoError(wsConn.WriteMessage(websocket.TextMessage, []byte("hostname\r")), "send hostname after creds restore")
	finalOutput, err := s.readWebSocketUntil(wsConn, nodeID+"\r\n", promptTimeout)
	s.Require().NoError(err, "expected hostname output after creds restore")
	s.Require().Contains(finalOutput, nodeID+"\r\n")
}
