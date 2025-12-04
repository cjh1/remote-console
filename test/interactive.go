package test

import (
	"net/url"
	"time"


	"github.com/gorilla/websocket"
)

func (s *IntegrationTestSuite) TestSSHPasswordConsoleInteractive() {

	parsedURL, err := url.Parse(s.apiURL)
	s.Require().NoError(err)

	// Connect and follow until see see that conman is connected to
	// the console
	wsURL := url.URL{
		Scheme:   "ws",
		Host:     parsedURL.Host,
		Path:     "/remote-console/consoles/x0c0s0b0",
	}

	wsConn, resp, err := websocket.DefaultDialer.Dial(wsURL.String(), nil)
	s.Require().NoError(err)
	defer resp.Body.Close()
	defer wsConn.Close()

	// Execute hostname over websocket
	testMsg := "hostname\n"
	err = wsConn.WriteMessage(websocket.TextMessage, []byte(testMsg))
	s.Require().NoError(err, "Error sending test message to console")

	// Read response until we see the hostname
	hostname, err := s.readWebSocketUntil(wsConn, "x0c0s0b0", 30*time.Second)
	s.Require().NoError(err, "Expected to find hostname in console output")
	s.T().Logf("Received hostname from console: %s", hostname)
}


func (s *IntegrationTestSuite) TestSSHPasswordConsoleInteractiveTail() {

	parsedURL, err := url.Parse(s.apiURL)
	s.Require().NoError(err)

	// Connect and follow until see see that conman is connected to
	// the console
	wsURL := url.URL{
		Scheme:   "ws",
		Host:     parsedURL.Host,
		Path:     "/remote-console/consoles/x0c0s0b0",
	}

	wsConn, resp, err := websocket.DefaultDialer.Dial(wsURL.String(), nil)
	s.Require().NoError(err)
	defer resp.Body.Close()
	defer wsConn.Close()

	// Execute hostname over websocket
	testMsg := "hostname\n"
	err = wsConn.WriteMessage(websocket.TextMessage, []byte(testMsg))
	s.Require().NoError(err, "Error sending test message to console")

	// Read response until we see the hostname
	hostname, err := s.readWebSocketUntil(wsConn, "x0c0s0b0", 30*time.Second)
	s.Require().NoError(err, "Expected to find hostname in console output")
	s.T().Logf("Received hostname from console: %s", hostname)

	// Broadcast a message to the console log and ensure we see it in the tail output
	msg := "only me"
	sshPasswordContainer := s.containers["ssh-password"]
	exitCode, output, err := sshPasswordContainer.Exec(s.ctx, []string{"broadcast.sh", msg})
	s.Require().NoError(err)
	s.T().Logf("Sent test message to console (exit code %d): %s", exitCode, output)

	_, err = s.readWebSocketUntil(wsConn, msg, 30*time.Second)
	s.Require().NoError(err, "Expected to find broadcast message in console output")
}

