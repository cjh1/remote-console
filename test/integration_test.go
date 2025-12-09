package test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"

	"github.com/OpenCHAMI/remote-console/internal/console"
	"github.com/OpenCHAMI/remote-console/internal/nodes"
)

const tailMessageTimeout = 2 * time.Minute

func uniqueMessage(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// IntegrationTestSuite is the test suite for remote-console integration tests
type IntegrationTestSuite struct {
	suite.Suite
	ctx        context.Context
	apiURL     string
	networks   []*testcontainers.DockerNetwork
	containers map[string]testcontainers.Container
}

// SetupSuite runs once before all tests in the suite
func (s *IntegrationTestSuite) SetupSuite() {
	if testing.Short() {
		s.T().Skip("Skipping integration test in short mode")
	}

	s.ctx = context.Background()
	s.containers = make(map[string]testcontainers.Container)

	// Create networks
	rcsNet, err := network.New(s.ctx, network.WithCheckDuplicate())
	require.NoError(s.T(), err)
	s.networks = append(s.networks, rcsNet)

	rcsRfNet, err := network.New(s.ctx, network.WithCheckDuplicate())
	require.NoError(s.T(), err)
	s.networks = append(s.networks, rcsRfNet)

	rcsConsoleNet, err := network.New(s.ctx, network.WithCheckDuplicate())
	require.NoError(s.T(), err)
	s.networks = append(s.networks, rcsConsoleNet)

	// Start Vault
	s.T().Log("Starting Vault...")
	vaultContainer, err := startVault(s.ctx, rcsNet.Name, rcsConsoleNet.Name)
	require.NoError(s.T(), err)
	s.containers["vault"] = vaultContainer

	// Enable KV store in Vault
	s.T().Log("Enabling KV store in Vault...")
	err = enableVaultKV(s.ctx, rcsNet.Name)
	require.NoError(s.T(), err)

	// Load SSH keys into Vault (if available)
	s.T().Log("Loading SSH keys into Vault...")
	sshKeyPath := getDefaultSSHKeyPath()
	if _, err := os.Stat(sshKeyPath); err == nil {
		err = loadSSHKeysIntoVault(s.ctx, rcsNet.Name, sshKeyPath)
		require.NoError(s.T(), err)
	} else {
		s.T().Log("SSH key not found, skipping key loading")
	}

	// Start Postgres
	s.T().Log("Starting Postgres...")
	postgresContainer, err := startPostgres(s.ctx, rcsNet.Name)
	require.NoError(s.T(), err)
	s.containers["postgres"] = postgresContainer

	// Initialize SMD database
	s.T().Log("Initializing SMD database...")
	err = initSMDDatabase(s.ctx, rcsNet.Name)
	require.NoError(s.T(), err)

	// Start SMD
	s.T().Log("Starting SMD...")
	smdContainer, err := startSMD(s.ctx, rcsNet.Name, rcsRfNet.Name)
	require.NoError(s.T(), err)
	s.containers["smd"] = smdContainer

	// Start Redfish Emulators
	s.T().Log("Starting Redfish emulators...")
	authConfig := "ADMIN:ADMIN:Administrator;operator:operator_password:Operator;guest:guest_password:ReadOnly"
	rfEmulator0, err := startRedfishEmulator(s.ctx, rcsRfNet.Name, "x0c0s0b0", "ssh", &authConfig)
	require.NoError(s.T(), err)
	s.containers["rf-x0c0s0b0"] = rfEmulator0

	rfEmulator1, err := startRedfishEmulator(s.ctx, rcsRfNet.Name, "x0c0s1b0", "ssh", nil)
	require.NoError(s.T(), err)
	s.containers["rf-x0c0s1b0"] = rfEmulator1

	rfEmulator2, err := startRedfishEmulator(s.ctx, rcsRfNet.Name, "x0c0s2b0", "ipmi", nil)
	require.NoError(s.T(), err)
	s.containers["rf-x0c0s2b0"] = rfEmulator2

	// Load Redfish endpoints into SMD
	redfishEndpoints := []redfishEndpoint{
		{
			Host:     "x0c0s0b0",
			Username: "ADMIN",
			Password: "ADMIN",
		},
		{
			Host:     "x0c0s1b0",
			Username: "operator",
			Password: "operator_password",
		},
		{
			Host:     "x0c0s2b0",
			Username: "guest",
			Password: "guest_password",
		},
	}

	s.T().Log("Loading Redfish endpoints into SMD...")
	time.Sleep(5 * time.Second) // Give RF emulators time to fully start
	err = loadRedfishEndpoints(s.ctx, rcsRfNet.Name, redfishEndpoints)
	require.NoError(s.T(), err)

	// Start SSH password server
	s.T().Log("Starting SSH password server...")
	sshPasswordServer, err := startSSHPasswordServer(s.ctx, rcsConsoleNet.Name, "x0c0s0b0", "ADMIN", "ADMIN")
	require.NoError(s.T(), err)
	s.containers["ssh-password"] = sshPasswordServer

	// Start SSH key server
	s.T().Log("Starting SSH key server...")
	publicKey := os.Getenv("PUBLIC_KEY")
	if publicKey == "" {
		s.T().Log("Warning: PUBLIC_KEY not set, SSH key server may not work properly")
	}
	sshKeyServer, err := startSSHKeyServer(s.ctx, rcsConsoleNet.Name, "x0c0s1b0", "n0", publicKey)
	require.NoError(s.T(), err)
	s.containers["ssh-key"] = sshKeyServer

	// Start IPMI server
	s.T().Log("Starting IPMI server...")
	ipmiServer, err := startIPMIServer(s.ctx, rcsConsoleNet.Name, "x0c0s2b0")
	require.NoError(s.T(), err)
	s.containers["ipmi"] = ipmiServer

	// Build and start remote-console
	s.T().Log("Starting remote-console...")
	remoteConsole, err := startRemoteConsole(s.ctx, rcsNet.Name, rcsConsoleNet.Name)
	require.NoError(s.T(), err)
	s.containers["remote-console"] = remoteConsole

	if os.Getenv("STREAM_REMOTE_CONSOLE_LOGS") == "1" {
		logPath := filepath.Join(os.TempDir(), fmt.Sprintf("remote-console-%d.log", time.Now().UnixNano()))
		s.T().Logf("Streaming remote-console logs to %s", logPath)
		s.streamContainerLogs("remote-console", remoteConsole, logPath)
	}

	// Get remote-console endpoint
	host, err := remoteConsole.Host(s.ctx)
	require.NoError(s.T(), err)
	port, err := remoteConsole.MappedPort(s.ctx, "26776")
	require.NoError(s.T(), err)
	s.apiURL = fmt.Sprintf("http://%s:%s", host, port.Port())

	s.T().Logf("Remote console API available at: %s", s.apiURL)
	s.T().Log("Waiting for remote-console to discover consoles...")
	s.Require().NoError(s.waitForConsoles(6, 5*time.Minute), "remote-console did not discover expected consoles")
}

// TearDownSuite runs once after all tests in the suite
func (s *IntegrationTestSuite) TearDownSuite() {
	// Create a context with timeout for cleanup
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	keepRemoteConsole := os.Getenv("KEEP_REMOTE_CONSOLE") == "1"

	// Clean up containers
	for name, container := range s.containers {
		if keepRemoteConsole && name == "remote-console" {
			s.T().Log("KEEP_REMOTE_CONSOLE=1, preserving remote-console container for debugging")
			continue
		}
		if err := container.Terminate(cleanupCtx); err != nil {
			s.T().Logf("Warning: failed to terminate container %s: %v", name, err)
		}
	}

	// Clean up networks
	for i := len(s.networks) - 1; i >= 0; i-- {
		if err := s.networks[i].Remove(cleanupCtx); err != nil {
			s.T().Logf("Warning: failed to remove network: %v", err)
		}
	}
}

func (s *IntegrationTestSuite) TestHealthCheck() {
	var healthResponse console.HealthResponse

	resp, err := http.Get(s.apiURL + "/remote-console/health")
	s.Require().NoError(err)
	defer resp.Body.Close()
	s.Equal(http.StatusOK, resp.StatusCode)

	err = json.NewDecoder(resp.Body).Decode(&healthResponse)
	s.Require().NoError(err)
	s.Equal("6", healthResponse.NumberConsoles)
}

func (s *IntegrationTestSuite) TestReadinessCheck() {
	resp, err := http.Get(s.apiURL + "/remote-console/readiness")
	s.Require().NoError(err)
	defer resp.Body.Close()
	s.Equal(http.StatusNoContent, resp.StatusCode)
}

func (s *IntegrationTestSuite) TestLivenessCheck() {
	resp, err := http.Get(s.apiURL + "/remote-console/liveness")
	s.Require().NoError(err)
	defer resp.Body.Close()
	s.Equal(http.StatusNoContent, resp.StatusCode)
}

// sortByID sorts a slice of any type that has an ID field
func sortByID[T any](slice []T, getID func(T) string) {
	sort.Slice(slice, func(i, j int) bool {
		return getID(slice[i]) < getID(slice[j])
	})
}

// TestConsoleNodesDiscovered verifies nodes are discovered from SMD
func (s *IntegrationTestSuite) TestConsoles() {
	resp, err := http.Get(s.apiURL + "/remote-console/consoles")
	s.Require().NoError(err)
	defer resp.Body.Close()
	s.Equal(http.StatusOK, resp.StatusCode)

	var consolesResponse console.ConsolesResponse
	err = json.NewDecoder(resp.Body).Decode(&consolesResponse)
	s.Require().NoError(err)

	s.Require().Equal(len(consolesResponse.Consoles), 6, "Expected 6 consoles")

	fmt.Println(consolesResponse.Consoles)

	consoles := []nodes.NodeConsoleInfo{
		{
			ID:             "x0c0s0b0",
			ConnectionType: "ssh",
			ConnectionHost: "x0c0s0b0",
			ConnectionPort: 0,
		},
		{
			ID:             "x0c0s0b0n0",
			ConnectionType: "ssh",
			ConnectionHost: "x0c0s0b0",
			ConnectionPort: 0,
		},
		{
			ID:             "x0c0s1b0",
			ConnectionType: "ssh",
			ConnectionHost: "x0c0s1b0",
			ConnectionPort: 0,
		},
		{
			ID:             "x0c0s1b0n0",
			ConnectionType: "ssh",
			ConnectionHost: "x0c0s1b0",
			ConnectionPort: 0,
		},
		{
			ID:             "x0c0s2b0",
			ConnectionType: "ipmi",
			ConnectionHost: "x0c0s2b0",
			ConnectionPort: 0,
		},
		{
			ID:             "x0c0s2b0n0",
			ConnectionType: "ipmi",
			ConnectionHost: "x0c0s2b0",
			ConnectionPort: 0,
		},
	}

	// Sort both slices for comparison
	sortByID(consolesResponse.Consoles, func(n nodes.NodeConsoleInfo) string { return n.ID })
	sortByID(consoles, func(n nodes.NodeConsoleInfo) string { return n.ID })

	s.Equal(consoles, consolesResponse.Consoles, "Consoles do not match expected consoles")

}

func (s *IntegrationTestSuite) websocketConnect(path string) (*websocket.Conn, *http.Response, error) {
	// Parse the HTTP API URL to get host and port
	parsedURL, err := url.Parse(s.apiURL)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse API URL: %w", err)
	}

	wsURL := url.URL{
		Scheme:   "ws",
		Host:     parsedURL.Host,
		Path:     path,
		RawQuery: "follow=true",
	}

	wsConn, resp, err := s.dialWebSocket(wsURL)
	if err != nil {
		return nil, resp, fmt.Errorf("WebSocket dial error: %v", err)
	}

	return wsConn, resp, nil
}

func (s *IntegrationTestSuite) readWebSocketMessages(wsConn *websocket.Conn, timeout time.Duration) string {
	wsConn.SetReadDeadline(time.Now().Add(timeout))

	var output strings.Builder
	for {
		_, message, err := wsConn.ReadMessage()
		if err != nil {
			s.T().Logf("WebSocket read ended: %v", err)
			break
		}
		msgStr := string(message)
		s.T().Logf("Console: %s", msgStr)
		output.WriteString(msgStr)
	}
	return output.String()
}

func (s *IntegrationTestSuite) readWebSocketUntil(wsConn *websocket.Conn, searchString string, timeout time.Duration) (string, error) {
	wsConn.SetReadDeadline(time.Now().Add(timeout))

	var output strings.Builder
	for {
		_, message, err := wsConn.ReadMessage()
		if err != nil {
			// TODO this should be an error
			s.T().Logf("WebSocket read ended: %v", err)
			break
		}
		msgStr := string(message)
		s.T().Logf("Console: %s", msgStr)
		output.WriteString(msgStr)
		if strings.Contains(output.String(), searchString) {
			return output.String(), nil
		}
	}
	return output.String(), fmt.Errorf("string %q not found in output", searchString)
}

func (s *IntegrationTestSuite) dialWebSocket(wsURL url.URL) (*websocket.Conn, *http.Response, error) {
	wsConn, resp, err := websocket.DefaultDialer.Dial(wsURL.String(), nil)
	if err != nil {
		return nil, resp, err
	}
	return wsConn, resp, nil
}

func (s *IntegrationTestSuite) streamContainerLogs(name string, container testcontainers.Container, destPath string) {
	go func() {
		file, err := os.Create(destPath)
		if err != nil {
			s.T().Logf("Warning: unable to create log file %s: %v", destPath, err)
			return
		}

		logConsumer := &fileLogConsumer{writer: file}
		container.FollowOutput(logConsumer)

		if err := container.StartLogProducer(s.ctx); err != nil {
			s.T().Logf("Warning: unable to start log producer for %s: %v", name, err)
			file.Close()
			return
		}

		if errCh := container.GetLogProductionErrorChannel(); errCh != nil {
			if err := <-errCh; err != nil && err != context.Canceled && err != context.DeadlineExceeded {
				s.T().Logf("Warning: log producer for %s stopped with error: %v", name, err)
			}
		}

		file.Close()
	}()
}

type fileLogConsumer struct {
	writer io.Writer
	mu     sync.Mutex
}

func (c *fileLogConsumer) Accept(log testcontainers.Log) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.writer != nil {
		if _, err := c.writer.Write(log.Content); err != nil {
			// We cannot use s.T().Logf from here, so best effort print to stderr
			fmt.Fprintf(os.Stderr, "fileLogConsumer write error: %v\n", err)
		}
	}
}

func (s *IntegrationTestSuite) waitForConsoles(expected int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(s.apiURL + "/remote-console/consoles")
		if err == nil {
			var consolesResp console.ConsolesResponse
			if decodeErr := json.NewDecoder(resp.Body).Decode(&consolesResp); decodeErr == nil {
				if len(consolesResp.Consoles) == expected {
					resp.Body.Close()
					s.T().Logf("remote-console discovered %d consoles", expected)
					return nil
				}
			}
			resp.Body.Close()
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timed out waiting for %d consoles", expected)
}

// TestSSHPasswordConsoleConnection verifies SSH password-based console connection
func (s *IntegrationTestSuite) TestSSHPasswordConsoleTail() {
	path := "/remote-console/consoles/x0c0s0b0/tail"
	wsConn, resp, err := s.websocketConnect(path)
	s.Require().NoError(err)
	defer resp.Body.Close()
	defer wsConn.Close()

	tailOutput := s.readWebSocketMessages(wsConn, 30*time.Second)

	s.Require().Contains(tailOutput, "Welcome to OpenSSH Server", "Expected to find 'Welcome to OpenSSH Server' in console output")

	msg := uniqueMessage("tail-basic")
	sshPasswordContainer := s.containers["ssh-password"]
	exitCode, output, err := sshPasswordContainer.Exec(s.ctx, []string{"broadcast.sh", msg})
	s.Require().NoError(err)
	s.T().Logf("SSH Password container echo to pts (exit code %d): %s", exitCode, output)

	// Connect again to read the message
	wsConn, resp, err = s.websocketConnect(path)
	s.Require().NoError(err)
	defer resp.Body.Close()
	defer wsConn.Close()

	tailOutput = s.readWebSocketMessages(wsConn, 30*time.Second)
	s.Require().Contains(tailOutput, msg, fmt.Sprintf("Expected to find '%s' in console output", msg))
}

// TestSSHPasswordConsoleTailFollow verifies tail with follow=true for live updates
func (s *IntegrationTestSuite) TestSSHPasswordConsoleTailFollow() {
	// Parse the HTTP API URL to get host and port
	parsedURL, err := url.Parse(s.apiURL)
	s.Require().NoError(err)

	wsURL := url.URL{
		Scheme:   "ws",
		Host:     parsedURL.Host,
		Path:     "/remote-console/consoles/x0c0s0b0/tail",
		RawQuery: "follow=true",
	}

	wsConn, resp, err := s.dialWebSocket(wsURL)
	s.Require().NoError(err)
	defer resp.Body.Close()
	defer wsConn.Close()

	_, err = s.readWebSocketUntil(wsConn, "Welcome to OpenSSH Server", tailMessageTimeout)
	s.Require().NoError(err, "Expected to find 'Welcome to OpenSSH Server' in initial output")
	s.T().Log("Found welcome message")

	testMsg := uniqueMessage("tail-follow")
	sshPasswordContainer := s.containers["ssh-password"]
	exitCode, output, err := sshPasswordContainer.Exec(s.ctx, []string{"broadcast.sh", testMsg})
	s.Require().NoError(err)
	s.T().Logf("Sent test message to console (exit code %d): %s", exitCode, output)

	_, err = s.readWebSocketUntil(wsConn, testMsg, tailMessageTimeout)
	s.Require().NoError(err, fmt.Sprintf("Expected to find '%s' in live console output", testMsg))
	s.T().Log("Found test message in live stream!")
}

// TestSSHPasswordConsoleTailLines verifies tail with lines=N for last N lines
func (s *IntegrationTestSuite) TestSSHPasswordConsoleTailLines() {
	// Parse the HTTP API URL to get host and port
	parsedURL, err := url.Parse(s.apiURL)
	s.Require().NoError(err)

	// Connect and follow until see see that conman is connected to
	// the console
	wsURL := url.URL{
		Scheme:   "ws",
		Host:     parsedURL.Host,
		Path:     "/remote-console/consoles/x0c0s0b0/tail",
		RawQuery: "follow=true",
	}

	msg := uniqueMessage("tail-lines")
	sshPasswordContainer := s.containers["ssh-password"]

	// Use an anonymous function to scope the first connection
	// so we can close it before reconnecting
	func() {
		wsConn, resp, err := s.dialWebSocket(wsURL)
		s.Require().NoError(err)
		defer resp.Body.Close()
		defer wsConn.Close()

		_, err = s.readWebSocketUntil(wsConn, "Welcome to OpenSSH Server", tailMessageTimeout)
		s.Require().NoError(err, "Expected to find 'Welcome to OpenSSH Server' in initial output")

		exitCode, output, err := sshPasswordContainer.Exec(s.ctx, []string{"broadcast.sh", msg})
		s.Require().NoError(err)
		s.T().Logf("Sent test message to console (exit code %d): %s", exitCode, output)
	}()

	// Now connect again to read last line
	wsURL = url.URL{
		Scheme:   "ws",
		Host:     parsedURL.Host,
		Path:     "/remote-console/consoles/x0c0s0b0/tail",
		RawQuery: "lines=1",
	}

	wsConn, resp, err := s.dialWebSocket(wsURL)
	s.Require().NoError(err)
	defer resp.Body.Close()
	defer wsConn.Close()

	tailOutput := s.readWebSocketMessages(wsConn, 30*time.Minute)

	// We expect to see one line, split on newlines
	lines := strings.Split(strings.TrimSpace(tailOutput), "\n")

	fmt.Println("Tail output lines:")
	for _, line := range lines {
		fmt.Printf(">> %s\n", line)
	}

	s.Require().Len(lines, 1, "Expected exactly one line from tail with lines=1")

	s.Require().Contains(lines[0], msg, "Test message not found in console output")
}

// TestSSHPasswordConsoleTailLines verifies tail with lines=N for last N lines and then follow
func (s *IntegrationTestSuite) TestSSHPasswordConsoleTailLinesFollow() {
	// Parse the HTTP API URL to get host and port
	parsedURL, err := url.Parse(s.apiURL)
	s.Require().NoError(err)

	// Connect and follow until see see that conman is connected to
	// the console
	wsURL := url.URL{
		Scheme:   "ws",
		Host:     parsedURL.Host,
		Path:     "/remote-console/consoles/x0c0s0b0/tail",
		RawQuery: "follow=true",
	}

	fmt.Println("Connecting to WebSocket for tail with follow=true")

	msg := uniqueMessage("tail-lines-initial")
	sshPasswordContainer := s.containers["ssh-password"]

	func() {
		wsConn, resp, err := s.dialWebSocket(wsURL)
		s.Require().NoError(err)
		defer resp.Body.Close()
		defer wsConn.Close()

		_, err = s.readWebSocketUntil(wsConn, "Welcome to OpenSSH Server", tailMessageTimeout)
		s.Require().NoError(err, "Expected to find 'Welcome to OpenSSH Server' in initial output")

		exitCode, output, err := sshPasswordContainer.Exec(s.ctx, []string{"broadcast.sh", msg})
		s.Require().NoError(err)
		s.T().Logf("Sent test message to console (exit code %d): %s", exitCode, output)
	}()

	// Now connect again to read last line and stay in follow mode
	linesFollowURL := url.URL{
		Scheme:   "ws",
		Host:     parsedURL.Host,
		Path:     "/remote-console/consoles/x0c0s0b0/tail",
		RawQuery: "lines=1&follow=true",
	}

	fmt.Println("Connecting to WebSocket for tail with lines=1&follow=true")

	followConn, followResp, err := s.dialWebSocket(linesFollowURL)
	s.Require().NoError(err)
	defer followResp.Body.Close()
	defer followConn.Close()

	tailOutput, err := s.readWebSocketUntil(followConn, msg, 30*time.Minute)
	s.Require().NoError(err, "Expected to find initial test message in tail output")

	lines := strings.Split(strings.TrimSpace(tailOutput), "\n")
	s.Require().Len(lines, 1, "Expected exactly one line from tail with lines=1")
	s.Require().Contains(lines[0], msg, "Test message not found in console output")

	// Now send another message and verify we get it
	followMsg := uniqueMessage("tail-lines-follow")
	exitCode, output, err := sshPasswordContainer.Exec(s.ctx, []string{"broadcast.sh", followMsg})
	s.Require().NoError(err)
	s.T().Logf("Sent follow-up message to console (exit code %d): %s", exitCode, output)

	fmt.Printf("Starting read until")

	// Continue reading from the same connection to get the new message
	tailOutput, err = s.readWebSocketUntil(followConn, followMsg, 200*time.Second)
	s.Require().NoError(err, fmt.Sprintf("Expected to find '%s' in live console output", followMsg))
	lines = strings.Split(strings.TrimSpace(tailOutput), "\n")
	s.Require().Len(lines, 1, "Expected exactly one line from tail with follow after sending follow-up message")
	s.T().Log("Found follow-up message in live stream!")
}

// // TestSSHKeyConsoleConnection verifies SSH key-based console connection
// func (s *IntegrationTestSuite) TestSSHKeyConsoleConnection() {
// 	s.T().Skip("TODO: Implement SSH key console test")

// 	// Test connection to SSH key-based console (x0c0s1b0)
// 	resp, err := http.Get(s.apiURL + "/console/x0c0s1b0")
// 	s.Require().NoError(err)
// 	defer resp.Body.Close()
// 	s.T().Logf("SSH key console response status: %d", resp.StatusCode)
// }

// // TestIPMIConsoleConnection verifies IPMI console connection
// func (s *IntegrationTestSuite) TestIPMIConsoleConnection() {
// 	s.T().Skip("TODO: Implement IPMI console test")

// 	// Test connection to IPMI console (x0c0s2b0)
// 	resp, err := http.Get(s.apiURL + "/console/x0c0s2b0")
// 	s.Require().NoError(err)
// 	defer resp.Body.Close()
// 	s.T().Logf("IPMI console response status: %d", resp.StatusCode)
// }

// TestIntegrationSuite runs the integration test suite
func TestIntegrationSuite(t *testing.T) {
	suite.Run(t, new(IntegrationTestSuite))
}
