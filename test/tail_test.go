package test

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func (s *IntegrationTestSuite) TestConsoleTail() {
	for _, console := range consoleFixtureList() {
		s.Run(console.name, func() {
			// First, wait for the console to be ready
			followParams := url.Values{}
			followParams.Set("follow", "true")
			followURL, err := s.tailWebSocketURL(console.nodeID, followParams)
			s.Require().NoError(err)

			followConn, followResp, err := s.dialWebSocket(followURL)
			s.Require().NoError(err)
			defer func() {
				if err := followResp.Body.Close(); err != nil {
					s.T().Logf("Warning: failed to close response body: %v", err)
				}
				if err := followConn.Close(); err != nil {
					s.T().Logf("Warning: failed to close websocket: %v", err)
				}
			}()

			if console.readyLogMarker != "" {
				_, err = s.readWebSocketUntil(followConn, console.readyLogMarker, tailMessageTimeout)
				s.Require().NoError(err, "Expected console readiness marker for %s", console.name)
			}

			// Send a message to the console and verify it's seen in the tail
			wsURL, err := s.tailWebSocketURL(console.nodeID, nil)
			s.Require().NoError(err)

			msg := makeUnique("tail-basic" + console.name)
			exitCode, output, err := s.broadcastConsoleMessage(console, msg)
			s.Require().NoError(err)
			s.T().Logf("%s console echo to pts (exit code %d): %s", console.name, exitCode, output)

			_, err = s.readWebSocketUntil(followConn, msg, tailMessageTimeout)
			s.Require().NoError(err, "Expected to find '%s' in follow console output", msg)

			// Now, connect to the console and verify we can read the message
			wsConn, resp, err := s.dialWebSocket(wsURL)
			s.Require().NoError(err)
			defer func() {
				if err := resp.Body.Close(); err != nil {
					s.T().Logf("Warning: failed to close response body: %v", err)
				}
				if err := wsConn.Close(); err != nil {
					s.T().Logf("Warning: failed to close websocket: %v", err)
				}
			}()

			tailOutput, err := s.readWebSocketMessages(wsConn, 5*time.Minute)
			s.Require().NoError(err)
			s.Require().Contains(tailOutput, msg, fmt.Sprintf("Expected to find '%s' in console output", msg))
		})
	}
}

func (s *IntegrationTestSuite) TestConsoleTailFollow() {
	for _, console := range consoleFixtureList() {
		s.Run(console.name, func() {
			params := url.Values{}
			params.Set("follow", "true")
			wsURL, err := s.tailWebSocketURL(console.nodeID, params)
			s.Require().NoError(err)

			wsConn, resp, err := s.dialWebSocket(wsURL)
			s.Require().NoError(err)
			defer func() {
				if err := resp.Body.Close(); err != nil {
					s.T().Logf("Warning: failed to close response body: %v", err)
				}
				if err := wsConn.Close(); err != nil {
					s.T().Logf("Warning: failed to close websocket: %v", err)
				}
			}()

			if console.readyLogMarker != "" {
				_, err = s.readWebSocketUntil(wsConn, console.readyLogMarker, tailMessageTimeout)
				s.Require().NoError(err, "Expected console readiness marker for %s", console.name)
			}

			// Send a message to the console and verify it's seen in the tail
			testMsg := makeUnique("tail-follow" + console.name)
			exitCode, output, err := s.broadcastConsoleMessage(console, testMsg)
			s.Require().NoError(err)
			s.T().Logf("Sent test message to %s console (exit code %d): %s", console.name, exitCode, output)

			_, err = s.readWebSocketUntil(wsConn, testMsg, tailMessageTimeout)
			s.Require().NoError(err, fmt.Sprintf("Expected to find '%s' in live console output", testMsg))
		})
	}
}

func (s *IntegrationTestSuite) TestConsoleTailConcurrent() {
	for _, console := range consoleFixtureList() {
		s.Run(console.name, func() {
			params := url.Values{}
			params.Set("follow", "true")
			wsURL, err := s.tailWebSocketURL(console.nodeID, params)
			s.Require().NoError(err)

			firstConn, firstResp, err := s.dialWebSocket(wsURL)
			s.Require().NoError(err)
			defer func() {
				if err := firstResp.Body.Close(); err != nil {
					s.T().Logf("Warning: failed to close response body: %v", err)
				}
				if err := firstConn.Close(); err != nil {
					s.T().Logf("Warning: failed to close websocket: %v", err)
				}
			}()

			secondConn, secondResp, err := s.dialWebSocket(wsURL)
			s.Require().NoError(err)
			defer func() {
				if err := secondResp.Body.Close(); err != nil {
					s.T().Logf("Warning: failed to close response body: %v", err)
				}
				if err := secondConn.Close(); err != nil {
					s.T().Logf("Warning: failed to close websocket: %v", err)
				}
			}()

			// Ensure both connections see the readiness marker
			if console.readyLogMarker != "" {
				_, err = s.readWebSocketUntil(firstConn, console.readyLogMarker, tailMessageTimeout)
				s.Require().NoError(err, "first follow connection did not see initial marker")

				_, err = s.readWebSocketUntil(secondConn, console.readyLogMarker, tailMessageTimeout)
				s.Require().NoError(err, "second follow connection did not see initial marker")
			}

			// Send a message to the console and verify both tails see it
			msg := makeUnique("tail-concurrent" + console.name)
			exitCode, output, err := s.broadcastConsoleMessage(console, msg)
			s.Require().NoError(err)
			s.T().Logf("Sent test message to %s console (exit code %d): %s", console.name, exitCode, output)

			_, err = s.readWebSocketUntil(firstConn, msg, tailMessageTimeout)
			s.Require().NoError(err, "first follow connection did not see broadcast message")

			_, err = s.readWebSocketUntil(secondConn, msg, tailMessageTimeout)
			s.Require().NoError(err, "second follow connection did not see broadcast message")
		})
	}
}

func (s *IntegrationTestSuite) TestConsoleTailHistoryFollowConcurrent() {
	for _, console := range consoleFixtureList() {
		s.Run(console.name, func() {
			// Set first url with lines=50&follow=true
			historyParams := url.Values{}
			historyParams.Set("lines", "50")
			historyParams.Set("follow", "true")
			historyURL, err := s.tailWebSocketURL(console.nodeID, historyParams)
			s.Require().NoError(err)

			// Set second url with just follow=true
			followParams := url.Values{}
			followParams.Set("follow", "true")
			followURL, err := s.tailWebSocketURL(console.nodeID, followParams)
			s.Require().NoError(err)

			// Dial both connections
			historyConn, historyResp, err := s.dialWebSocket(historyURL)
			s.Require().NoError(err)
			defer func() {
				if err := historyResp.Body.Close(); err != nil {
					s.T().Logf("Warning: failed to close response body: %v", err)
				}
				if err := historyConn.Close(); err != nil {
					s.T().Logf("Warning: failed to close websocket: %v", err)
				}
			}()

			followConn, followResp, err := s.dialWebSocket(followURL)
			s.Require().NoError(err)
			defer func() {
				if err := followResp.Body.Close(); err != nil {
					s.T().Logf("Warning: failed to close response body: %v", err)
				}
				if err := followConn.Close(); err != nil {
					s.T().Logf("Warning: failed to close websocket: %v", err)
				}
			}()

			// Ensure both connections see the readiness marker
			if console.readyLogMarker != "" {
				_, err = s.readWebSocketUntil(historyConn, console.readyLogMarker, tailMessageTimeout)
				s.Require().NoError(err, "history+follow connection did not see initial marker")

				_, err = s.readWebSocketUntil(followConn, console.readyLogMarker, tailMessageTimeout)
				s.Require().NoError(err, "follow-only connection did not see initial marker")
			}

			// Send a message to the console and verify both tails see it
			msg := makeUnique("tail-history-follow" + console.name)
			exitCode, output, err := s.broadcastConsoleMessage(console, msg)
			s.Require().NoError(err)
			s.T().Logf("Sent test message to %s console (exit code %d): %s", console.name, exitCode, output)

			_, err = s.readWebSocketUntil(historyConn, msg, 30*time.Second)
			s.Require().NoError(err, "history+follow connection did not see broadcast message")

			_, err = s.readWebSocketUntil(followConn, msg, 30*time.Second)
			s.Require().NoError(err, "follow-only connection did not see broadcast message")
		})
	}
}

func (s *IntegrationTestSuite) TestConsoleTailLines() {
	for _, console := range consoleFixtureList() {
		s.Run(console.name, func() {

			followParams := url.Values{}
			followParams.Set("follow", "true")
			followURL, err := s.tailWebSocketURL(console.nodeID, followParams)
			s.Require().NoError(err)

			followConn, followResp, err := s.dialWebSocket(followURL)
			s.Require().NoError(err)
			defer func() {
				if err := followResp.Body.Close(); err != nil {
					s.T().Logf("Warning: failed to close response body: %v", err)
				}
				if err := followConn.Close(); err != nil {
					s.T().Logf("Warning: failed to close websocket: %v", err)
				}
			}()

			if console.readyLogMarker != "" {
				_, err = s.readWebSocketUntil(followConn, console.readyLogMarker, tailMessageTimeout)
				s.Require().NoError(err, "Expected console readiness marker for %s", console.name)
			}

			// Send a message to the console to ensure some recent log content
			msg := makeUnique("tail-lines" + console.name)

			exitCode, output, err := s.broadcastConsoleMessage(console, msg)
			s.Require().NoError(err)
			s.T().Logf("Sent test message to %s console (exit code %d): %s", console.name, exitCode, output)

			// Now connect with lines=2 and verify we get the message
			params := url.Values{}
			params.Set("lines", "2")
			linesURL, err := s.tailWebSocketURL(console.nodeID, params)
			s.Require().NoError(err)

			wsConn, resp, err := s.dialWebSocket(linesURL)
			s.Require().NoError(err)
			defer func() {
				if err := resp.Body.Close(); err != nil {
					s.T().Logf("Warning: failed to close response body: %v", err)
				}
				if err := wsConn.Close(); err != nil {
					s.T().Logf("Warning: failed to close websocket: %v", err)
				}
			}()

			tailOutput, err := s.readWebSocketMessages(wsConn, 5*time.Minute)
			s.Require().NoError(err)
			lines := strings.Split(strings.TrimSpace(tailOutput), "\n")
			s.Require().Contains(tailOutput, msg, "Test message not found in console output")
			s.Require().Len(lines, 2, "Expected exactly one line from tail with lines=1")
		})
	}
}

func (s *IntegrationTestSuite) TestConsoleTailLinesFollow() {
	for _, console := range consoleFixtureList() {
		s.Run(console.name, func() {
			// Use separate follow connection to ensure we get the initial message before the lines=1 connection
			followParams := url.Values{}
			followParams.Set("follow", "true")
			followURL, err := s.tailWebSocketURL(console.nodeID, followParams)
			s.Require().NoError(err)

			followConn, followResp, err := s.dialWebSocket(followURL)
			s.Require().NoError(err)
			defer func() {
				if err := followResp.Body.Close(); err != nil {
					s.T().Logf("Warning: failed to close response body: %v", err)
				}
				if err := followConn.Close(); err != nil {
					s.T().Logf("Warning: failed to close websocket: %v", err)
				}
			}()

			if console.readyLogMarker != "" {
				_, err = s.readWebSocketUntil(followConn, console.readyLogMarker, tailMessageTimeout)
				s.Require().NoError(err, "Expected console readiness marker for %s", console.name)
			}

			msg := makeUnique("tail-lines-initial" + console.name)
			exitCode, output, err := s.broadcastConsoleMessage(console, msg)
			s.Require().NoError(err)
			s.T().Logf("Sent test message to %s console (exit code %d): %s", console.name, exitCode, output)

			// Wait for the initial message to appear in the follow connection
			_, err = s.readWebSocketUntil(followConn, msg, tailMessageTimeout)
			s.Require().NoError(err, "follow connection did not see initial test message in output")

			// Now use the lines=2&follow=true connection to ensure we get the initial message and then follow
			params := url.Values{}
			params.Set("lines", "2")
			params.Set("follow", "true")
			linesFollowURL, err := s.tailWebSocketURL(console.nodeID, params)
			s.Require().NoError(err)

			followLinesConn, followLinesResp, err := s.dialWebSocket(linesFollowURL)
			s.Require().NoError(err)
			defer func() {
				if err := followLinesResp.Body.Close(); err != nil {
					s.T().Logf("Warning: failed to close response body: %v", err)
				}
				if err := followLinesConn.Close(); err != nil {
					s.T().Logf("Warning: failed to close websocket: %v", err)
				}
			}()

			// Ensure we get the initial message in the lines=2&follow=true connection
			tailOutput, err := s.readNWebSocketMessages(followLinesConn, 2, 30*time.Second)
			s.Require().NoError(err, "follow lines connection did not see initial test message in output")
			s.Require().Len(strings.Split(strings.TrimSpace(tailOutput), "\n"), 2, "Expected exactly two lines from tail with lines=2")
			s.Require().Contains(tailOutput, msg, "Test message not found in console output")

			// Now send another message and ensure we see it in the lines=2&follow=true connection
			followMsg := makeUnique("tail-lines-follow" + console.name)
			exitCode, output, err = s.broadcastConsoleMessage(console, followMsg)
			s.Require().NoError(err)
			s.T().Logf("Sent follow-up message to %s console (exit code %d): %s", console.name, exitCode, output)

			// Verify we see it in the lines=2&follow=true connection
			_, err = s.readWebSocketUntil(followLinesConn, followMsg, 200*time.Second)
			s.Require().NoError(err, fmt.Sprintf("Expected to find '%s' in live console output", followMsg))
		})
	}
}

func (s *IntegrationTestSuite) TestConsoleTailEntryCommand() {
	// Test that the entry command is executed when connecting to the console
	console := consoleFixture{
		name:           "ssh-entry-cmd",
		nodeID:         "x0c0s0b0n0",
		containerKey:   "remote-console",
		username:       "ADMIN",
		password:       "ADMIN",
		readyLogMarker: "Welcome to OpenSSH Server",
		prompt:         ":~$ ",
		// We use 'expect' here as with the entry command there may not be a pty that
		// broadcast.sh can write to, so we spawn conman in expect to echo a message
		broadcastCmd: func(msg string) []string {
			return []string{"expect", "-c", fmt.Sprintf("spawn conman x0c0s0b0n0; expect \"password:\"; expect \":~$ \"; send \"echo '%s'\\r\"; send \"&.\\r\"; expect eof", msg)}
		},
	}

	params := url.Values{}
	params.Set("follow", "true")
	wsURL, err := s.tailWebSocketURL(console.nodeID, params)
	s.Require().NoError(err)

	wsConn, resp, err := s.dialWebSocket(wsURL)
	s.Require().NoError(err)
	defer func() {
		if err := resp.Body.Close(); err != nil {
			s.T().Logf("Warning: failed to close response body: %v", err)
		}
		if err := wsConn.Close(); err != nil {
			s.T().Logf("Warning: failed to close websocket: %v", err)
		}
	}()

	// Check for the echo from the entry command
	_, err = s.readWebSocketUntil(wsConn, "Hello n0", tailMessageTimeout)
	s.Require().NoError(err, "Expected shell prompt for %s", console.name)

	// Test that we can broadcast to the console (verify entry command shell is functional)
	testMsg := makeUnique("entry-cmd-test")
	exitCode, output, err := s.broadcastConsoleMessage(console, testMsg)
	s.Require().NoError(err)
	s.T().Logf("Sent test message to %s console (exit code %d): %s", console.name, exitCode, output)

	_, err = s.readWebSocketUntil(wsConn, testMsg, tailMessageTimeout)
	s.Require().NoError(err, "Expected to find broadcast message '%s' in console with entry command", testMsg)

}

func (s *IntegrationTestSuite) TestConsoleTailInvalidNode() {
	parsedURL, err := url.Parse(s.apiURL)
	s.Require().NoError(err, "Failed to parse API URL")

	// Try to tail a non-existent node
	invalidNodeID := "x9c9s9b9"

	params := url.Values{}
	params.Set("mode", "tail")

	wsURL := url.URL{
		Scheme:   "ws",
		Host:     parsedURL.Host,
		Path:     fmt.Sprintf("/remote-console/consoles/%s", invalidNodeID),
		RawQuery: params.Encode(),
	}

	_, resp, err := s.dialWebSocket(wsURL)

	// Should get an error because the WebSocket upgrade should fail with 404
	s.Require().Error(err, "Expected error when tailing invalid node")

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
