package test

import (
	"fmt"
	"strings"
	"time"
)

func (s *IntegrationTestSuite) TestConsoleTail() {
	for _, fixture := range consoleFixtures {
		s.Run(fixture.name, func() {
			// First, wait for the console to be ready
			followURL, err := s.tailWebSocketURL(fixture.nodeID, "follow=true")
			s.Require().NoError(err)

			followConn, followResp, err := s.dialWebSocket(followURL)
			s.Require().NoError(err)
			defer followResp.Body.Close()
			defer followConn.Close()

			if fixture.readyLogMarker != "" {
				_, err = s.readWebSocketUntil(followConn, fixture.readyLogMarker, tailMessageTimeout)
				s.Require().NoError(err, "Expected console readiness marker for %s", fixture.name)
			}

			// Send a message to the console and verify it's seen in the tail
			wsURL, err := s.tailWebSocketURL(fixture.nodeID, "")
			msg := uniqueMessage("tail-basic-" + fixture.name)
			exitCode, output, err := s.broadcastConsoleMessage(fixture, msg)
			s.Require().NoError(err)
			s.T().Logf("%s console echo to pts (exit code %d): %s", fixture.name, exitCode, output)

			s.readWebSocketUntil(followConn, msg, tailMessageTimeout)

			// Now, connect to the console and verify we can read the message
			wsConn, resp, err := s.dialWebSocket(wsURL)
			s.Require().NoError(err)
			defer resp.Body.Close()
			defer wsConn.Close()

			tailOutput := s.readWebSocketMessages(wsConn, 30*time.Second)
			s.T().Logf("Console tail output: %s", tailOutput)
			s.Require().Contains(tailOutput, msg, fmt.Sprintf("Expected to find '%s' in console output", msg))
		})
	}
}

func (s *IntegrationTestSuite) TestConsoleTailFollow() {
	for _, fixture := range consoleFixtures {
		s.Run(fixture.name, func() {
			wsURL, err := s.tailWebSocketURL(fixture.nodeID, "follow=true")
			s.Require().NoError(err)

			wsConn, resp, err := s.dialWebSocket(wsURL)
			s.Require().NoError(err)
			defer resp.Body.Close()
			defer wsConn.Close()

			if fixture.readyLogMarker != "" {
				_, err = s.readWebSocketUntil(wsConn, fixture.readyLogMarker, tailMessageTimeout)
				s.Require().NoError(err, "Expected console readiness marker for %s", fixture.name)
			}

			testMsg := uniqueMessage("tail-follow-" + fixture.name)
			exitCode, output, err := s.broadcastConsoleMessage(fixture, testMsg)
			s.Require().NoError(err)
			s.T().Logf("Sent test message to %s console (exit code %d): %s", fixture.name, exitCode, output)

			_, err = s.readWebSocketUntil(wsConn, testMsg, tailMessageTimeout)
			s.Require().NoError(err, fmt.Sprintf("Expected to find '%s' in live console output", testMsg))
		})
	}
}

func (s *IntegrationTestSuite) TestConsoleTailConcurrent() {
	for _, fixture := range consoleFixtures {
		s.Run(fixture.name, func() {
			wsURL, err := s.tailWebSocketURL(fixture.nodeID, "follow=true")
			s.Require().NoError(err)

			firstConn, firstResp, err := s.dialWebSocket(wsURL)
			s.Require().NoError(err)
			defer firstResp.Body.Close()
			defer firstConn.Close()

			secondConn, secondResp, err := s.dialWebSocket(wsURL)
			s.Require().NoError(err)
			defer secondResp.Body.Close()
			defer secondConn.Close()

			if fixture.readyLogMarker != "" {
				_, err = s.readWebSocketUntil(firstConn, fixture.readyLogMarker, tailMessageTimeout)
				s.Require().NoError(err, "first follow connection did not see initial marker")

				_, err = s.readWebSocketUntil(secondConn, fixture.readyLogMarker, tailMessageTimeout)
				s.Require().NoError(err, "second follow connection did not see initial marker")
			}

			msg := uniqueMessage("tail-concurrent-" + fixture.name)
			exitCode, output, err := s.broadcastConsoleMessage(fixture, msg)
			s.Require().NoError(err)
			s.T().Logf("Sent test message to %s console (exit code %d): %s", fixture.name, exitCode, output)

			_, err = s.readWebSocketUntil(firstConn, msg, 30*time.Second)
			s.Require().NoError(err, "first follow connection did not see broadcast message")

			_, err = s.readWebSocketUntil(secondConn, msg, 30*time.Second)
			s.Require().NoError(err, "second follow connection did not see broadcast message")
		})
	}
}

func (s *IntegrationTestSuite) TestConsoleTailHistoryFollowConcurrent() {
	for _, fixture := range consoleFixtures {
		s.Run(fixture.name, func() {
			historyURL, err := s.tailWebSocketURL(fixture.nodeID, "lines=50&follow=true")
			s.Require().NoError(err)
			followURL, err := s.tailWebSocketURL(fixture.nodeID, "follow=true")
			s.Require().NoError(err)

			historyConn, historyResp, err := s.dialWebSocket(historyURL)
			s.Require().NoError(err)
			defer historyResp.Body.Close()
			defer historyConn.Close()

			followConn, followResp, err := s.dialWebSocket(followURL)
			s.Require().NoError(err)
			defer followResp.Body.Close()
			defer followConn.Close()

			if fixture.readyLogMarker != "" {
				_, err = s.readWebSocketUntil(historyConn, fixture.readyLogMarker, tailMessageTimeout)
				s.Require().NoError(err, "history+follow connection did not see initial marker")

				_, err = s.readWebSocketUntil(followConn, fixture.readyLogMarker, tailMessageTimeout)
				s.Require().NoError(err, "follow-only connection did not see initial marker")
			}

			msg := uniqueMessage("tail-history-follow-" + fixture.name)
			exitCode, output, err := s.broadcastConsoleMessage(fixture, msg)
			s.Require().NoError(err)
			s.T().Logf("Sent test message to %s console (exit code %d): %s", fixture.name, exitCode, output)

			_, err = s.readWebSocketUntil(historyConn, msg, 30*time.Second)
			s.Require().NoError(err, "history+follow connection did not see broadcast message")

			_, err = s.readWebSocketUntil(followConn, msg, 30*time.Second)
			s.Require().NoError(err, "follow-only connection did not see broadcast message")
		})
	}
}

func (s *IntegrationTestSuite) TestConsoleTailLines() {
	for _, fixture := range consoleFixtures {
		s.Run(fixture.name, func() {
			// First, wait for the console to be ready
			followURL, err := s.tailWebSocketURL(fixture.nodeID, "follow=true")
			s.Require().NoError(err)

			followConn, followResp, err := s.dialWebSocket(followURL)
			s.Require().NoError(err)
			defer followResp.Body.Close()
			defer followConn.Close()

			if fixture.readyLogMarker != "" {
				_, err = s.readWebSocketUntil(followConn, fixture.readyLogMarker, tailMessageTimeout)
				s.Require().NoError(err, "Expected console readiness marker for %s", fixture.name)
			}

			msg := uniqueMessage("tail-lines-" + fixture.name)

			exitCode, output, err := s.broadcastConsoleMessage(fixture, msg)
			s.Require().NoError(err)
			s.T().Logf("Sent test message to %s console (exit code %d): %s", fixture.name, exitCode, output)
			
			linesURL, err := s.tailWebSocketURL(fixture.nodeID, "lines=2")
			s.Require().NoError(err)

			wsConn, resp, err := s.dialWebSocket(linesURL)
			s.Require().NoError(err)
			defer resp.Body.Close()
			defer wsConn.Close()

			tailOutput := s.readWebSocketMessages(wsConn, 30*time.Second)
			lines := strings.Split(strings.TrimSpace(tailOutput), "\n")
			s.Require().Contains(tailOutput, msg, "Test message not found in console output")
			s.Require().Len(lines, 2, "Expected exactly one line from tail with lines=1")
		})
	}
}

func (s *IntegrationTestSuite) TestConsoleTailLinesFollow() {
	for _, fixture := range consoleFixtures {
		s.Run(fixture.name, func() {
			// Use separate follow connection to ensure we get the initial message before the lines=1 connection 
			followURL, err := s.tailWebSocketURL(fixture.nodeID, "follow=true")
			s.Require().NoError(err)

			followConn, followResp, err := s.dialWebSocket(followURL)
			s.Require().NoError(err)
			defer followResp.Body.Close()
			defer followConn.Close()

			if fixture.readyLogMarker != "" {
				_, err = s.readWebSocketUntil(followConn, fixture.readyLogMarker, tailMessageTimeout)
				s.Require().NoError(err, "Expected console readiness marker for %s", fixture.name)
			}

			msg := uniqueMessage("tail-lines-initial-" + fixture.name)
			exitCode, output, err := s.broadcastConsoleMessage(fixture, msg)
			s.Require().NoError(err)
			s.T().Logf("Sent test message to %s console (exit code %d): %s", fixture.name, exitCode, output)

			// Wait for the initial message to appear in the follow connection
			_, err = s.readWebSocketUntil(followConn, msg, tailMessageTimeout)
			s.Require().NoError(err, "follow connection did not see initial test message in output")

			// Now use the lines=2&follow=true connection to ensure we get the initial message and then follow
			linesFollowURL, err := s.tailWebSocketURL(fixture.nodeID, "lines=2&follow=true")
			s.Require().NoError(err)

			followLinesConn, followLinesResp, err := s.dialWebSocket(linesFollowURL)
			s.Require().NoError(err)
			defer followLinesResp.Body.Close()
			defer followLinesConn.Close()

			tailOutput, err := s.readNWebSocketMessages(followLinesConn, 2, 30*time.Second)
			fmt.Printf("tailOuput: %s", tailOutput)
			s.Require().NoError(err, "follow lines connection did not see initial test message in output")
			s.Require().Len(strings.Split(strings.TrimSpace(tailOutput), "\n"), 2, "Expected exactly two lines from tail with lines=2")
			s.Require().Contains(tailOutput, msg, "Test message not found in console output")

			followMsg := uniqueMessage("tail-lines-follow-" + fixture.name)
			exitCode, output, err = s.broadcastConsoleMessage(fixture, followMsg)
			s.Require().NoError(err)
			s.T().Logf("Sent follow-up message to %s console (exit code %d): %s", fixture.name, exitCode, output)

			_, err = s.readWebSocketUntil(followLinesConn, followMsg, 200*time.Second)
			s.Require().NoError(err, fmt.Sprintf("Expected to find '%s' in live console output", followMsg))
		})
	}
}
