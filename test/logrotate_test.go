package test

import (
	"fmt"
	"io"
	"strings"
	"time"
)

// TestConsoleLogRotation tests that console log rotation works correctly
// and that tailing WebSocket connections remain open, continuing to receiving
// data after log rotation
func (s *IntegrationTestSuite) TestConsoleLogRotation() {
	fixture := consoleFixtures[0] // Use first fixture, we only need todo if for one

	// Start a tailing connection with follow=true
	followURL, err := s.tailWebSocketURL(fixture.nodeID, "follow=true")
	s.Require().NoError(err)

	tailConn, tailResp, err := s.dialWebSocket(followURL)
	s.Require().NoError(err)
	defer tailResp.Body.Close()
	defer tailConn.Close()

	// Wait for readiness marker
	if fixture.readyLogMarker != "" {
		_, err = s.readWebSocketUntil(tailConn, fixture.readyLogMarker, tailMessageTimeout)
		s.Require().NoError(err, "Expected console readiness marker")
	}

	// Send messages before rotation to generate log content
	preRotateMsg := uniqueMessage("pre-rotation")
	exitCode, output, err := s.broadcastConsoleMessage(fixture, preRotateMsg)
	s.Require().NoError(err)
	s.T().Logf("Sent pre-rotation message (exit code %d): %s", exitCode, output)

	// Verify the tail connection sees it
	_, err = s.readWebSocketUntil(tailConn, preRotateMsg, tailMessageTimeout)
	s.Require().NoError(err, "Tail should see pre-rotation message")

	// Trigger log rotation
	// Note: In a real integration test, you would:
	// - Configure the remote-console service with a very small log size (e.g., 1K)
	// - Send enough data to exceed that size
	// - Wait for the log rotation check interval
	//
	// For now, we'll send a large amount of data and wait for rotation
	s.T().Log("Generating large log content to trigger rotation...")
	
	// Send multiple messages to fill up the log
	// With RCS_CONSOLE_LOGS_FILE_SIZE=2K, we need to write more than 2KB
	largeData := strings.Repeat("A", 512) // 512 bytes per message
	for i := 0; i < 8; i++ { // 8 * 512 = 4KB, enough to exceed 2KB threshold
		msg := fmt.Sprintf("%s-bulk-%d", uniqueMessage("rotation-trigger"), i)
		exitCode, _, err := s.broadcastConsoleMessage(fixture, msg+" "+largeData)
		s.Require().NoError(err)
		s.T().Logf("Sent bulk message %d (exit code %d)", i, exitCode)
		
		// Read the message from tail
		_, err = s.readWebSocketUntil(tailConn, msg, tailMessageTimeout)
		s.Require().NoError(err, "Tail should see bulk message %d", i)
	}

	// Wait for log rotation to potentially occur
	// Log rotation checks every 5 seconds (RCS_LOG_ROTATE_CHECK_FREQUENCY=5)
	s.T().Log("Waiting for log rotation check to run and trigger rotation...")
	time.Sleep(10 * time.Second)

	// Debug: Check logrotate configuration and try running it manually
	s.T().Log("Checking logrotate configuration...")
	rcsContainer, ok := s.containers["remote-console"]
	s.Require().True(ok, "remote-console container should exist")

	debugCmd := []string{"sh", "-c", "ls -la /usr/sbin/logrotate /sbin/logrotate 2>&1 && logrotate --version 2>&1"}
	exitCodeDebug, readerDebug, err := rcsContainer.Exec(s.ctx, debugCmd)
	s.Require().NoError(err)
	debugOutput, err := io.ReadAll(readerDebug)
	s.Require().NoError(err)
	s.T().Logf("Logrotate debug (exit code %d):\n%s", exitCodeDebug, string(debugOutput))

	// Verify log rotation occurred by checking for rotated files in the container
	s.T().Log("Checking for rotated log files in container...")

	// Check for current and rotated log files (conman uses /tmp/conman/ as base directory)
	checkCmd := []string{"sh", "-c", "ls -la /tmp/conman/ /tmp/conman.old/ 2>&1"}
	exitCode, reader, err := rcsContainer.Exec(s.ctx, checkCmd)
	s.Require().NoError(err)
	logOutput, err := io.ReadAll(reader)
	s.Require().NoError(err)
	s.T().Logf("Log files check (exit code %d):\n%s", exitCode, string(logOutput))

	// Verify rotated log exists in backup directory
	s.Require().Contains(string(logOutput), fmt.Sprintf("console.%s.1", fixture.nodeID),
		"Should find rotated log file console.%s.1 in /tmp/conman.old/", fixture.nodeID)

	// 5. Send a message after rotation to verify tail still works
	postRotateMsg := uniqueMessage("post-rotation")
	exitCode, output, err = s.broadcastConsoleMessage(fixture, postRotateMsg)
	s.Require().NoError(err)
	s.T().Logf("Sent post-rotation message (exit code %d): %s", exitCode, output)

	// 6. Verify the tail connection is still alive and receives the new message
	s.T().Log("Verifying tail connection still receives data after rotation...")
	_, err = s.readWebSocketUntil(tailConn, postRotateMsg, tailMessageTimeout)
	s.Require().NoError(err, "Tail connection should remain open and see post-rotation message")

	s.T().Log("Log rotation test passed: tail connection remained alive through rotation")
}
