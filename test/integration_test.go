package test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"encoding/pem"
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
	"golang.org/x/crypto/ssh"

	"github.com/OpenCHAMI/remote-console/internal/console"
	"github.com/OpenCHAMI/remote-console/internal/nodes"
)

const (
	tailMessageTimeout = 2 * time.Minute
	dynamicTestXname   = "x0c0s8b9"
	defaultAuthConfig  = "ADMIN:ADMIN:Administrator;operator:operator_password:Operator;guest:guest_password:ReadOnly"
)

func makeUnique(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// IntegrationTestSuite is the test suite for remote-console integration tests
type IntegrationTestSuite struct {
	suite.Suite
	apiURL         string
	containers     map[string]testcontainers.Container
	vaultContainer testcontainers.Container
	rcsNetwork     *testcontainers.DockerNetwork
	rfNetwork      *testcontainers.DockerNetwork
	consoleNetwork *testcontainers.DockerNetwork
}

// generateTempSSHKeyPair creates a temporary keypair for testing and returns the private key path and public key string.
func (s *IntegrationTestSuite) generateTempSSHKeyPair() (keyPath string, pubKey string, err error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", "", fmt.Errorf("generate ed25519 key: %w", err)
	}

	// Marshal private key to OpenSSH format
	privPEM, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		return "", "", fmt.Errorf("marshal private key: %w", err)
	}

	// Create key file in temp directory (automatically cleaned up)
	tempDir := s.T().TempDir()
	keyPath = filepath.Join(tempDir, "ssh-test-key")
	privFile, err := os.Create(keyPath)
	if err != nil {
		return "", "", fmt.Errorf("create temp private key: %w", err)
	}
	defer func() {
		if cerr := privFile.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("close private key file: %w", cerr)
		}
	}()

	if err := os.Chmod(keyPath, 0600); err != nil {
		return "", "", fmt.Errorf("chmod private key: %w", err)
	}

	// Write properly formatted SSH private key
	if err := pem.Encode(privFile, privPEM); err != nil {
		return "", "", fmt.Errorf("write private key: %w", err)
	}

	// Generate public key in authorized_keys format
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		return "", "", fmt.Errorf("create ssh public key: %w", err)
	}
	pubKey = strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sshPub)))

	return keyPath, pubKey, nil
}

// SetupSuite runs once before all tests in the suite
func (s *IntegrationTestSuite) SetupSuite() {
	if testing.Short() {
		s.T().Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	s.containers = make(map[string]testcontainers.Container)

	// Create networks
	rcsNet, err := network.New(ctx)
	require.NoError(s.T(), err)
	s.rcsNetwork = rcsNet

	rcsRfNet, err := network.New(ctx)
	require.NoError(s.T(), err)
	s.rfNetwork = rcsRfNet

	rcsConsoleNet, err := network.New(ctx)
	require.NoError(s.T(), err)
	s.consoleNetwork = rcsConsoleNet

	// Start Vault
	s.T().Log("Starting Vault...")
	vaultContainer, err := startVault(ctx, s.rcsNetwork.Name, s.consoleNetwork.Name)
	require.NoError(s.T(), err)
	s.vaultContainer = vaultContainer
	s.containers["vault"] = vaultContainer

	// Enable KV store in Vault
	s.T().Log("Enabling KV store in Vault...")
	err = enableVaultKV(ctx, s.rcsNetwork.Name)
	require.NoError(s.T(), err)

	// Set initial console credentials for SSH key auth nodes
	s.T().Log("Setting initial console credentials in Vault...")
	err = setConsoleCredentials(ctx, s.vaultContainer, "x0c0s1b0", "ADMIN", "")
	require.NoError(s.T(), err)
	err = setConsoleCredentials(ctx, s.vaultContainer, "x0c0s1b0n0", "ADMIN", "")
	require.NoError(s.T(), err)

	// Load SSH keys into Vault (if available)
	s.T().Log("Loading SSH keys into Vault...")
	s.T().Log("Generating temporary SSH key pair for tests")
	sshKeyPath, publicKey, genErr := s.generateTempSSHKeyPair()
	require.NoError(s.T(), genErr)
	err = loadSSHKeysIntoVault(ctx, s.vaultContainer, sshKeyPath)
	require.NoError(s.T(), err)

	// Start Postgres
	s.T().Log("Starting Postgres...")
	postgresContainer, err := startPostgres(ctx, s.rcsNetwork.Name)
	require.NoError(s.T(), err)
	s.containers["postgres"] = postgresContainer

	// Initialize SMD database
	s.T().Log("Initializing SMD database...")
	err = initSMDDatabase(ctx, s.rcsNetwork.Name)
	require.NoError(s.T(), err)

	// Start SMD
	s.T().Log("Starting SMD...")
	smdContainer, err := startSMD(ctx, s.rcsNetwork.Name, s.rfNetwork.Name)
	require.NoError(s.T(), err)
	s.containers["smd"] = smdContainer

	sshPasswordFixture := consoleFixtures["ssh-password"]
	sshKeyFixture := consoleFixtures["ssh-key"]
	ipmiFixture := consoleFixtures["ipmi"]

	// Start Redfish Emulators
	s.T().Log("Starting Redfish emulators...")
	authConfig := defaultAuthConfig
	rfEmulator0, err := startRedfishEmulator(ctx, s.rfNetwork.Name, sshPasswordFixture.nodeID, "ssh", &authConfig)
	require.NoError(s.T(), err)
	s.containers[fmt.Sprintf("rf-%s", sshPasswordFixture.nodeID)] = rfEmulator0

	keyAuthConfig := "ADMIN::Administrator;operator:operator_password:Operator;guest:guest_password:ReadOnly"
	rfEmulator1, err := startRedfishEmulator(ctx, s.rfNetwork.Name, sshKeyFixture.nodeID, "ssh", &keyAuthConfig)
	require.NoError(s.T(), err)
	s.containers[fmt.Sprintf("rf-%s", sshKeyFixture.nodeID)] = rfEmulator1

	keyAuthConfig = "root:root_password:Administrator;operator:operator_password:Operator;guest:guest_password:ReadOnly"
	rfEmulator2, err := startRedfishEmulator(ctx, s.rfNetwork.Name, ipmiFixture.nodeID, "ipmi", &keyAuthConfig)
	require.NoError(s.T(), err)
	s.containers[fmt.Sprintf("rf-%s", ipmiFixture.nodeID)] = rfEmulator2

	// Load Redfish endpoints into SMD
	redfishEndpoints := []redfishEndpoint{
		{
			Host:     sshPasswordFixture.nodeID,
			Username: sshPasswordFixture.username,
			Password: sshPasswordFixture.password,
		},
		{
			Host:     sshKeyFixture.nodeID,
			Username: sshKeyFixture.username,
			Password: sshKeyFixture.password,
		},
		{
			Host:     ipmiFixture.nodeID,
			Username: ipmiFixture.username,
			Password: ipmiFixture.password,
		},
	}

	s.T().Log("Loading Redfish endpoints into SMD...")
	time.Sleep(5 * time.Second) // Give RF emulators time to fully start
	smdAPIURL, err := getSMDAPIURL(ctx, smdContainer)
	require.NoError(s.T(), err)

	err = loadRedfishEndpoints(s.T(), ctx, smdAPIURL, redfishEndpoints)
	require.NoError(s.T(), err)

	// Start SSH password server
	s.T().Log("Starting SSH password server...")
	sshPasswordServer, err := startSSHPasswordServer(ctx, s.consoleNetwork.Name, sshPasswordFixture.nodeID, sshPasswordFixture.username, sshPasswordFixture.password)
	require.NoError(s.T(), err)
	s.containers["ssh-password"] = sshPasswordServer

	// Start SSH key server
	s.T().Log("Starting SSH key server...")
	sshKeyServer, err := startSSHKeyServer(ctx, s.consoleNetwork.Name, sshKeyFixture.nodeID, sshKeyFixture.username, publicKey)
	require.NoError(s.T(), err)
	s.containers["ssh-key"] = sshKeyServer

	// Start IPMI server
	s.T().Log("Starting IPMI server...")
	ipmiServer, err := startIPMIServer(ctx, s.consoleNetwork.Name, ipmiFixture.nodeID)
	require.NoError(s.T(), err)
	s.containers["ipmi"] = ipmiServer

	// Build and start remote-console
	s.T().Log("Starting remote-console...")
	remoteConsole, err := startRemoteConsole(ctx, s.rcsNetwork.Name, s.consoleNetwork.Name)
	require.NoError(s.T(), err)
	s.containers["remote-console"] = remoteConsole

	// Optionally stream remote-console logs to a temp file for debugging
	if os.Getenv("STREAM_REMOTE_CONSOLE_LOGS") == "1" {
		logPath := filepath.Join(os.TempDir(), fmt.Sprintf("remote-console-%d.log", time.Now().UnixNano()))
		s.T().Logf("Streaming remote-console logs to %s", logPath)
		s.streamContainerLogs(ctx, "remote-console", remoteConsole, logPath)
	}

	// Get remote-console endpoint
	s.apiURL, err = s.getRemoteConsoleAPIURL(ctx, remoteConsole)
	require.NoError(s.T(), err)

	s.T().Logf("Remote console API available at: %s", s.apiURL)
	s.T().Log("Waiting for remote-console to discover consoles...")
	s.Require().NoError(s.waitForConsoles(5, 5*time.Minute), "remote-console did not discover expected consoles")
}

// TearDownSuite runs once after all tests in the suite
func (s *IntegrationTestSuite) TearDownSuite() {
	// Create a context with timeout for cleanup
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Clean up containers
	for name, container := range s.containers {
		if err := container.Terminate(cleanupCtx); err != nil {
			s.T().Logf("Warning: failed to terminate container %s: %v", name, err)
		}
	}

	// Clean up networks
	for _, net := range []*testcontainers.DockerNetwork{s.consoleNetwork, s.rfNetwork, s.rcsNetwork} {
		if net == nil {
			continue
		}
		if err := net.Remove(cleanupCtx); err != nil {
			s.T().Logf("Warning: failed to remove network %s: %v", net.Name, err)
		}
	}
}

func (s *IntegrationTestSuite) TestHealthCheck() {
	var healthResponse console.HealthResponse

	resp, err := http.Get(s.apiURL + "/remote-console/health")
	s.Require().NoError(err)
	defer func() {
		if err := resp.Body.Close(); err != nil {
			s.T().Logf("Warning: failed to close response body: %v", err)
		}
	}()
	s.Equal(http.StatusOK, resp.StatusCode)

	err = json.NewDecoder(resp.Body).Decode(&healthResponse)
	s.Require().NoError(err)
	s.Equal("5", healthResponse.NumberConsoles)
}

func (s *IntegrationTestSuite) TestReadinessCheck() {
	resp, err := http.Get(s.apiURL + "/remote-console/readiness")
	s.Require().NoError(err)
	defer func() {
		if err := resp.Body.Close(); err != nil {
			s.T().Logf("Warning: failed to close response body: %v", err)
		}
	}()
	s.Equal(http.StatusNoContent, resp.StatusCode)
}

func (s *IntegrationTestSuite) TestLivenessCheck() {
	resp, err := http.Get(s.apiURL + "/remote-console/liveness")
	s.Require().NoError(err)
	defer func() {
		if err := resp.Body.Close(); err != nil {
			s.T().Logf("Warning: failed to close response body: %v", err)
		}
	}()
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
	defer func() {
		if err := resp.Body.Close(); err != nil {
			s.T().Logf("Warning: failed to close response body: %v", err)
		}
	}()
	s.Equal(http.StatusOK, resp.StatusCode)

	var consolesResponse console.ConsolesResponse
	err = json.NewDecoder(resp.Body).Decode(&consolesResponse)
	s.Require().NoError(err)

	// 2 SSH password nodes, 2 SSH key nodes, 1 IPMI node
	s.Require().Equal(len(consolesResponse.Consoles), 5, "Expected 5 consoles")

	sshPasswordFixture := consoleFixtures["ssh-password"]
	sshKeyFixture := consoleFixtures["ssh-key"]
	ipmiFixture := consoleFixtures["ipmi"]
	entryCmd := "echo 'Hello n0' && /bin/sh"

	consoles := []nodes.NodeConsoleInfo{
		{
			ID:             sshPasswordFixture.nodeID,
			ConnectionType: "ssh",
			ConnectionHost: sshPasswordFixture.nodeID,
			ConnectionPort: 0,
		},
		{
			ID:                  sshPasswordFixture.nodeID + "n0",
			ConnectionType:      "ssh",
			ConnectionHost:      sshPasswordFixture.nodeID,
			ConnectionPort:      0,
			ConsoleEntryCommand: entryCmd,
		},
		{
			ID:             sshKeyFixture.nodeID,
			ConnectionType: "ssh",
			ConnectionHost: sshKeyFixture.nodeID,
			ConnectionPort: 0,
		},
		{
			ID:                  sshKeyFixture.nodeID + "n0",
			ConnectionType:      "ssh",
			ConnectionHost:      sshKeyFixture.nodeID,
			ConnectionPort:      0,
			ConsoleEntryCommand: entryCmd,
		},
		{
			ID:             ipmiFixture.nodeID,
			ConnectionType: "ipmi",
			ConnectionHost: ipmiFixture.nodeID,
			ConnectionPort: 0,
		},
	}

	// Sort both slices for comparison
	sortByID(consolesResponse.Consoles, func(n nodes.NodeConsoleInfo) string { return n.ID })
	sortByID(consoles, func(n nodes.NodeConsoleInfo) string { return n.ID })

	s.Equal(consoles, consolesResponse.Consoles, "Consoles do not match expected consoles")

}

// tailWebSocketURL constructs the WebSocket URL for tailing console output
func (s *IntegrationTestSuite) tailWebSocketURL(nodeID string, params url.Values) (url.URL, error) {
	parsedURL, err := url.Parse(s.apiURL)
	if err != nil {
		return url.URL{}, fmt.Errorf("failed to parse API URL: %w", err)
	}

	// Start with mode=tail, add any additional parameters
	if params == nil {
		params = url.Values{}
	}
	params.Set("mode", "tail")

	wsURL := url.URL{
		Scheme:   "ws",
		Host:     parsedURL.Host,
		Path:     fmt.Sprintf("/remote-console/consoles/%s", nodeID),
		RawQuery: params.Encode(),
	}

	return wsURL, nil
}

// consoleMessageLogger buffers partial WebSocket chunks and logs complete lines.
type consoleMessageLogger struct {
	t      *testing.T
	buffer string
}

func (s *IntegrationTestSuite) newConsoleMessageLogger() *consoleMessageLogger {
	return &consoleMessageLogger{t: s.T()}
}

func (l *consoleMessageLogger) LogChunk(chunk string) {
	l.buffer += chunk
	for {
		idx := strings.IndexByte(l.buffer, '\n')
		if idx == -1 {
			return
		}
		line := strings.TrimSuffix(l.buffer[:idx], "\r")
		l.t.Logf("Console: %s", line)
		l.buffer = l.buffer[idx+1:]
	}
}

// readWebSocketMessages reads messages from the WebSocket connection until it times out or encounters an error.
func (s *IntegrationTestSuite) readWebSocketMessages(wsConn *websocket.Conn, timeout time.Duration) (string, error) {
	if err := wsConn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return "", fmt.Errorf("set read deadline: %w", err)
	}

	var output strings.Builder
	logger := s.newConsoleMessageLogger()
	messageCount := 0
	start := time.Now()
	for {
		_, message, err := wsConn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return output.String(), nil
			}
			return output.String(), fmt.Errorf("websocket read ended after %d messages (%s elapsed): %w", messageCount, time.Since(start), err)
		}

		msgStr := string(message)
		logger.LogChunk(msgStr)
		output.WriteString(msgStr)
		messageCount++
	}
}

// readWebSocketUntil reads messages from the WebSocket connection until the specified search string is found or it times out.
func (s *IntegrationTestSuite) readWebSocketUntil(wsConn *websocket.Conn, searchString string, timeout time.Duration) (string, error) {
	if err := wsConn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return "", fmt.Errorf("set read deadline: %w", err)
	}

	var output strings.Builder
	logger := s.newConsoleMessageLogger()
	for {
		_, message, err := wsConn.ReadMessage()
		if err != nil {
			s.T().Logf("WebSocket read ended: %v", err)
			return output.String(), fmt.Errorf("websocket read error while searching for %q: %w", searchString, err)
		}

		msgStr := string(message)
		logger.LogChunk(msgStr)
		output.WriteString(msgStr)
		if strings.Contains(output.String(), searchString) {
			return output.String(), nil
		}
	}
}

// readNWebSocketMessages reads a specific number of messages from the WebSocket connection.
func (s *IntegrationTestSuite) readNWebSocketMessages(wsConn *websocket.Conn, count int, timeout time.Duration) (string, error) {
	if err := wsConn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return "", fmt.Errorf("set read deadline: %w", err)
	}

	var output strings.Builder
	for i := range count {
		_, message, err := wsConn.ReadMessage()
		if err != nil {
			s.T().Logf("WebSocket read ended after %d of %d messages: %v", i, count, err)
			return output.String(), fmt.Errorf("failed to read message %d of %d: %w", i+1, count, err)
		}
		output.WriteString(string(message))
	}

	return output.String(), nil
}

// dialWebSocket dials a WebSocket connection to the specified URL.
func (s *IntegrationTestSuite) dialWebSocket(wsURL url.URL) (*websocket.Conn, *http.Response, error) {
	wsConn, resp, err := websocket.DefaultDialer.Dial(wsURL.String(), nil)
	if err != nil {
		return nil, resp, err
	}
	return wsConn, resp, nil
}

// fileLogConsumer is a LogConsumer that writes logs to a file
type fileLogConsumer struct {
	writer io.Writer
	mu     sync.Mutex
}

// Accept writes the log content to the file
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

// streamContainerLogs streams the logs of the specified container to a file at destPath, used for debugging
func (s *IntegrationTestSuite) streamContainerLogs(ctx context.Context, name string, container testcontainers.Container, destPath string) {
	go func() {
		file, err := os.Create(destPath)
		if err != nil {
			s.T().Logf("Warning: unable to create log file %s: %v", destPath, err)
			return
		}

		logConsumer := &fileLogConsumer{writer: file}
		container.FollowOutput(logConsumer)

		if err := container.StartLogProducer(ctx); err != nil {
			s.T().Logf("Warning: unable to start log producer for %s: %v", name, err)
			if closeErr := file.Close(); closeErr != nil {
				s.T().Logf("Warning: failed to close log file %s: %v", destPath, closeErr)
			}
			return
		}

		if errCh := container.GetLogProductionErrorChannel(); errCh != nil {
			if err := <-errCh; err != nil && err != context.Canceled && err != context.DeadlineExceeded {
				s.T().Logf("Warning: log producer for %s stopped with error: %v", name, err)
			}
		}

		if closeErr := file.Close(); closeErr != nil {
			s.T().Logf("Warning: failed to close log file %s: %v", destPath, closeErr)
		}
	}()
}

// waitForConsoles waits until the expected number of consoles are discovered or times out
func (s *IntegrationTestSuite) waitForConsoles(expected int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(s.apiURL + "/remote-console/consoles")
		if err == nil {
			var consolesResp console.ConsolesResponse
			if decodeErr := json.NewDecoder(resp.Body).Decode(&consolesResp); decodeErr == nil {
				if len(consolesResp.Consoles) == expected {
					if err := resp.Body.Close(); err != nil {
						s.T().Logf("Warning: failed to close response body: %v", err)
					}
					s.T().Logf("remote-console discovered %d consoles", expected)
					return nil
				}
			}
			if err := resp.Body.Close(); err != nil {
				s.T().Logf("Warning: failed to close response body: %v", err)
			}
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timed out waiting for %d consoles", expected)
}

// waitForConsoleID waits until a console with the specified nodeID is discovered or times out
func (s *IntegrationTestSuite) waitForConsoleID(nodeID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(s.apiURL + "/remote-console/consoles")
		if err == nil {
			var consolesResp console.ConsolesResponse
			if decodeErr := json.NewDecoder(resp.Body).Decode(&consolesResp); decodeErr == nil {
				for _, consoleInfo := range consolesResp.Consoles {
					if consoleInfo.ID == nodeID {
						if err := resp.Body.Close(); err != nil {
							s.T().Logf("Warning: failed to close response body: %v", err)
						}
						return nil
					}
				}
			}
			if err := resp.Body.Close(); err != nil {
				s.T().Logf("Warning: failed to close response body: %v", err)
			}
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timed out waiting for console %s", nodeID)
}

// waitForConsoleRemoval waits until a console with the specified nodeID is removed or times out
func (s *IntegrationTestSuite) waitForConsoleRemoval(nodeID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(s.apiURL + "/remote-console/consoles")
		if err == nil {
			var consolesResp console.ConsolesResponse
			if decodeErr := json.NewDecoder(resp.Body).Decode(&consolesResp); decodeErr == nil {
				found := false

				for _, consoleInfo := range consolesResp.Consoles {
					if consoleInfo.ID == nodeID {
						found = true
						break
					}
				}
				if !found {
					if err := resp.Body.Close(); err != nil {
						s.T().Logf("Warning: failed to close response body: %v", err)
					}
					return nil
				}
			}
			if err := resp.Body.Close(); err != nil {
				s.T().Logf("Warning: failed to close response body: %v", err)
			}
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timed out waiting for console %s removal", nodeID)
}

// getRemoteConsoleAPIURL constructs the API URL for a container exposing port 26776
func (s *IntegrationTestSuite) getRemoteConsoleAPIURL(ctx context.Context, container testcontainers.Container) (string, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get container host: %w", err)
	}
	port, err := container.MappedPort(ctx, "26776")
	if err != nil {
		return "", fmt.Errorf("failed to get container mapped port: %w", err)
	}
	return fmt.Sprintf("http://%s:%s", host, port.Port()), nil
}

func (s *IntegrationTestSuite) TestDynamicConsoleDiscovery() {
	ctx := context.Background()
	newNodeID := dynamicTestXname

	// Start new Redfish emulator and SSH console
	s.T().Logf("Starting dynamic Redfish emulator and SSH console for %s", newNodeID)
	authConfig := defaultAuthConfig
	rfContainer, err := startRedfishEmulator(ctx, s.rfNetwork.Name, newNodeID, "ssh", &authConfig)
	s.Require().NoError(err)
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := rfContainer.Terminate(cleanupCtx); err != nil {
			s.T().Logf("Warning: failed to terminate dynamic Redfish emulator %s: %v", newNodeID, err)
		}
	}()

	sshContainer, err := startSSHPasswordServer(ctx, s.consoleNetwork.Name, newNodeID, "ADMIN", "ADMIN")
	s.Require().NoError(err)
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := sshContainer.Terminate(cleanupCtx); err != nil {
			s.T().Logf("Warning: failed to terminate dynamic SSH container %s: %v", newNodeID, err)
		}
	}()

	smdAPIURL, err := getSMDAPIURL(ctx, s.containers["smd"])
	s.Require().NoError(err)

	// Register new Redfish endpoint in SMD
	err = loadRedfishEndpoint(s.T(), ctx, smdAPIURL, redfishEndpoint{
		Host:     newNodeID,
		Username: "ADMIN",
		Password: "ADMIN",
	})
	s.Require().NoError(err, "failed to register dynamic Redfish endpoint")

	// Wait for remote-console to discover new console
	s.Require().NoError(s.waitForConsoleID(newNodeID, 3*time.Minute), "remote-console did not detect new console")

	// Connect to new console and verify we can run commands
	wsConn, resp, err := s.connectInteractiveConsole(newNodeID, ":~$ ", 90*time.Second)
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

	expectedHostLine := newNodeID + "\r\n"
	hostnameOutput, err := s.readWebSocketUntil(wsConn, expectedHostLine, 90*time.Second)
	s.Require().NoError(err, "Expected hostname output from console")
	s.Require().True(strings.Contains(hostnameOutput, expectedHostLine),
		"Expected hostname command output in console output; got %q", hostnameOutput)

	s.T().Log("Removing dynamic console registration")
	smdAPIURL, err = getSMDAPIURL(ctx, s.containers["smd"])
	s.Require().NoError(err)
	err = deleteRedfishEndpoint(s.T(), ctx, smdAPIURL, newNodeID)
	s.Require().NoError(err, "failed to remove dynamic Redfish endpoint")
	s.Require().NoError(s.waitForConsoleRemoval(newNodeID, 3*time.Minute), "remote-console did not drop dynamic console")

	// Try to connect again, should fail immediately (no retries needed)
	wsURL, err := s.tailWebSocketURL(newNodeID, nil)
	s.Require().NoError(err)

	_, resp, err = s.dialWebSocket(wsURL)
	s.Require().Error(err, "Expected error connecting to removed console")
	if resp != nil {
		s.T().Logf("Console removal connection response status: %d", resp.StatusCode)
		defer func() {
			if err := resp.Body.Close(); err != nil {
				s.T().Logf("Warning: failed to close response body: %v", err)
			}
		}()
	}

}

// TestIntegrationSuite runs the integration test suite
func TestIntegrationSuite(t *testing.T) {
	suite.Run(t, new(IntegrationTestSuite))
}
