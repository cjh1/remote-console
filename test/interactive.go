package test

import (
	"net/url"
	"regexp"
	"time"

	"github.com/gorilla/websocket"
)

var promptHostRegexp = regexp.MustCompile(`([A-Za-z0-9_.-]+):~\$`)

// extractPromptHost returns the shell hostname portion preceding ":~$".
func extractPromptHost(output string) string {
	match := promptHostRegexp.FindStringSubmatch(output)
	if len(match) > 1 {
		return match[1]
	}
	return ""
}



func (s *IntegrationTestSuite) TestSSHPasswordConsoleInteractive() {

	parsedURL, err := url.Parse(s.apiURL)
	s.Require().NoError(err)

	// Connect and follow until see see that conman is connected to
	// the console
	wsURL := url.URL{
		Scheme: "ws",
		Host:   parsedURL.Host,
		Path:   "/remote-console/consoles/x0c0s0b0",
	}

	wsConn, resp, err := websocket.DefaultDialer.Dial(wsURL.String(), nil)
	s.Require().NoError(err)
	defer resp.Body.Close()
	defer wsConn.Close()

	// Wait for the shell prompt to ensure the PTY is fully ready before sending commands.
	initialOutput, err := s.readWebSocketUntil(wsConn, ":~$ ", 30*time.Second)
	s.Require().NoError(err, "Expected to see shell prompt before sending commands")
	promptHost := extractPromptHost(initialOutput)
	s.Require().NotEmpty(promptHost, "Expected to extract prompt host from console output")

	// Execute hostname over websocket
	testMsg := "hostname\n"
	err = wsConn.WriteMessage(websocket.TextMessage, []byte(testMsg))
	s.Require().NoError(err, "Error sending test message to console")

	// Read response until we see the hostname echoed back (matches shell prompt host)
	hostnameOutput, err := s.readWebSocketUntil(wsConn, promptHost, 30*time.Second)
	s.Require().NoError(err, "Expected to find hostname in console output")
	s.T().Logf("Received hostname from console: %s", hostnameOutput)
}

func (s *IntegrationTestSuite) TestSSHPasswordConsoleInteractiveTail() {

	parsedURL, err := url.Parse(s.apiURL)
	s.Require().NoError(err)

	// Connect and follow until see see that conman is connected to
	// the console
	wsURL := url.URL{
		Scheme: "ws",
		Host:   parsedURL.Host,
		Path:   "/remote-console/consoles/x0c0s0b0",
	}

	wsConn, resp, err := websocket.DefaultDialer.Dial(wsURL.String(), nil)
	s.Require().NoError(err)
	defer resp.Body.Close()
	defer wsConn.Close()

	// Wait for the shell prompt to ensure the PTY is fully ready before sending commands.
	initialOutput, err := s.readWebSocketUntil(wsConn, ":~$ ", 30*time.Second)
	s.Require().NoError(err, "Expected to see shell prompt before sending commands")
	promptHost := extractPromptHost(initialOutput)
	s.Require().NotEmpty(promptHost, "Expected to extract prompt host from console output")
	s.T().Logf("Console ready (host %s): %s", promptHost, initialOutput)

	// Execute hostname over websocket
	testMsg := "hostname\n"
	err = wsConn.WriteMessage(websocket.TextMessage, []byte(testMsg))
	s.Require().NoError(err, "Error sending test message to console")

	// Read response until we see the hostname (matches shell prompt host)
	hostnameOutput, err := s.readWebSocketUntil(wsConn, promptHost, 30*time.Second)
	s.Require().NoError(err, "Expected to find hostname in console output")
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

