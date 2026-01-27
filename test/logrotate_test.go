package test

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"
)

// TestConsoleLogRotation tests that console log rotation works correctly
// and that tailing WebSocket connections remain open, continuing to receiving
// data after log rotation
func (s *IntegrationTestSuite) TestConsoleLogRotation() {
	console := consoleFixtures["ssh-key"] // Use the SSH key console; only need to test one

	// This test requires specific log rotation settings, so we have to stop to remote-console
	// container and restart it with custom environment variables. This is not ideal, but more
	// practical than having a separate test suite just for log rotation.

	// Stop the default remote-console container so we don't have two instances fighting over ports
	defaultRC, ok := s.containers["remote-console"]
	s.Require().True(ok, "default remote-console container should exist")
	stopCtx, cancelStop := context.WithTimeout(context.Background(), time.Minute)
	defer cancelStop()
	s.T().Log("Terminating default remote-console container for log rotation test...")
	s.Require().NoError(defaultRC.Terminate(stopCtx), "failed to terminate default remote-console container")
	delete(s.containers, "remote-console")

	env := map[string]string{
		"RCS_LOG_ROTATE_CHECK_FREQUENCY": "5",  // Check every 5 seconds
		"RCS_CONSOLE_LOGS_FILE_SIZE":     "2K", // Small size to trigger rotation easily
		"RCS_CONSOLE_LOGS_NUM_ROTATE":    "2",  // Keep 2 rotated files
	}

	s.T().Log("Starting remote-console container with log rotation settings...")
	logRotateRemoteConsoleContainer, err := startRemoteConsoleWithEnv(context.Background(), env, s.rcsNetwork.Name, s.consoleNetwork.Name)
	if err != nil {
		s.T().Logf("Failed to start remote-console container: %v", err)
		// Try to get logs if the container was created but failed to start
		if logRotateRemoteConsoleContainer != nil {
			if logs, logErr := logRotateRemoteConsoleContainer.Logs(context.Background()); logErr == nil {
				logBytes, _ := io.ReadAll(logs)
				s.T().Logf("Container logs:\n%s", string(logBytes))
			}
		}
	}
	s.Require().NoError(err, "Failed to start remote-console container with log rotation settings")

	// Ensure we clean up and restore state
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := logRotateRemoteConsoleContainer.Terminate(ctx); err != nil {
			s.T().Logf("Warning: failed to terminate logrotate remote-console container: %v", err)
		}

		// Recreate the default remote-console container to avoid lingering state
		s.T().Log("Recreating default remote-console container...")
		rc, err := startRemoteConsole(context.Background(), s.rcsNetwork.Name, s.consoleNetwork.Name)
		s.Require().NoError(err, "failed to recreate default remote-console container")
		s.containers["remote-console"] = rc

		// Restore API URL
		s.apiURL, err = s.getRemoteConsoleAPIURL(context.Background(), rc)
		s.Require().NoError(err, "failed to get API URL of default remote-console container")

		// Wait for it to discover consoles again
		s.T().Log("Waiting for default remote-console to discover consoles again...")
		if err := s.waitForConsoles(5, 5*time.Minute); err != nil {
			s.Require().NoError(err, "default remote-console did not rediscover consoles")
		}
	}()

	// Update API URL to point to new container
	s.apiURL, err = s.getRemoteConsoleAPIURL(context.Background(), logRotateRemoteConsoleContainer)
	s.Require().NoError(err)

	// Wait for the new container to discover consoles
	s.T().Log("Waiting for remote-console to discover consoles...")
	s.Require().NoError(s.waitForConsoles(5, 5*time.Minute), "remote-console did not discover expected consoles")

	// Start a tailing connection with follow=true
	params := url.Values{}
	params.Set("follow", "true")
	followURL, err := s.tailWebSocketURL(console.nodeID, params)
	s.Require().NoError(err)

	tailConn, tailResp, err := s.dialWebSocket(followURL)
	s.Require().NoError(err)
	defer func() {
		if err := tailResp.Body.Close(); err != nil {
			s.T().Logf("Warning: failed to close response body: %v", err)
		}
		if err := tailConn.Close(); err != nil {
			s.T().Logf("Warning: failed to close websocket: %v", err)
		}
	}()

	// Wait for readiness marker
	if console.readyLogMarker != "" {
		_, err = s.readWebSocketUntil(tailConn, console.readyLogMarker, tailMessageTimeout)
		s.Require().NoError(err, "Expected console readiness marker")
	}

	// Send messages before rotation to generate log content
	preRotateMsg := makeUnique("pre-rotation")
	exitCode, output, err := s.broadcastConsoleMessage(console, preRotateMsg)
	s.Require().NoError(err)
	s.T().Logf("Sent pre-rotation message (exit code %d): %s", exitCode, output)

	// Verify the tail connection sees it
	_, err = s.readWebSocketUntil(tailConn, preRotateMsg, tailMessageTimeout)
	s.Require().NoError(err, "Tail should see pre-rotation message")

	// For now, we'll send a large amount of data and wait for rotation
	s.T().Log("Generating large log content to trigger rotation...")

	// Send multiple messages to fill up the log
	// With RCS_CONSOLE_LOGS_FILE_SIZE=2K, we need to write more than 2KB
	largeData := strings.Repeat("A", 512) // 512 bytes per message
	for i := 0; i < 8; i++ {              // 8 * 512 = 4KB, enough to exceed 2KB threshold
		msg := fmt.Sprintf("%s-bulk-%d", makeUnique("rotation-trigger"), i)
		exitCode, _, err := s.broadcastConsoleMessage(console, msg+" "+largeData)
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

	// Verify log rotation occurred by checking for rotated files in the container
	s.T().Log("Checking for rotated log files in container...")

	// Check for current and rotated log files (conman uses /tmp/conman/ as base directory)
	checkCmd := []string{"sh", "-c", "ls -la /tmp/conman/ /tmp/conman.old/ 2>&1"}
	exitCode, reader, err := logRotateRemoteConsoleContainer.Exec(context.Background(), checkCmd)
	s.Require().NoError(err)
	logOutput, err := io.ReadAll(reader)
	s.Require().NoError(err)
	s.T().Logf("Log files check (exit code %d):\n%s", exitCode, string(logOutput))

	// Verify rotated log exists in backup directory
	s.Require().Contains(string(logOutput), fmt.Sprintf("console.%s.1", console.nodeID),
		"Should find rotated log file console.%s.1 in /tmp/conman.old/", console.nodeID)

	// Send a message after rotation to verify tail still works
	postRotateMsg := makeUnique("post-rotation")
	exitCode, output, err = s.broadcastConsoleMessage(console, postRotateMsg)
	s.Require().NoError(err)
	s.T().Logf("Sent post-rotation message (exit code %d): %s", exitCode, output)

	// Verify the tail connection is still alive and receives the new message
	s.T().Log("Verifying tail connection still receives data after rotation...")
	_, err = s.readWebSocketUntil(tailConn, postRotateMsg, tailMessageTimeout)
	s.Require().NoError(err, "Tail connection should remain open and see post-rotation message")
}
