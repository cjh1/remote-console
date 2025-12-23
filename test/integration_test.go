package test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
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

// generateTempSSHKeyPair creates a temporary keypair for testing and returns the private key path and public key string.
func (s *IntegrationTestSuite) generateTempSSHKeyPair() (string, string, error) {
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
	keyPath := filepath.Join(tempDir, "ssh-test-key")
	privFile, err := os.Create(keyPath)
	if err != nil {
		return "", "", fmt.Errorf("create temp private key: %w", err)
	}
	defer privFile.Close()

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
	pubKey := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sshPub)))

	return keyPath, pubKey, nil
}

// IntegrationTestSuite is the test suite for remote-console integration tests
type IntegrationTestSuite struct {
	suite.Suite
	// TODO we shouldn' store the context here?
	ctx            context.Context
	apiURL         string
	containers     map[string]testcontainers.Container
	vaultContainer testcontainers.Container
	rcsNetwork     *testcontainers.DockerNetwork
	rfNetwork      *testcontainers.DockerNetwork
	consoleNetwork *testcontainers.DockerNetwork
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
	s.rcsNetwork = rcsNet

	rcsRfNet, err := network.New(s.ctx, network.WithCheckDuplicate())
	require.NoError(s.T(), err)
	s.rfNetwork = rcsRfNet

	rcsConsoleNet, err := network.New(s.ctx, network.WithCheckDuplicate())
	require.NoError(s.T(), err)
	s.consoleNetwork = rcsConsoleNet

	// Start Vault
	s.T().Log("Starting Vault...")
	vaultContainer, err := startVault(s.ctx, s.rcsNetwork.Name, s.consoleNetwork.Name)
	require.NoError(s.T(), err)
	s.vaultContainer = vaultContainer
	s.containers["vault"] = vaultContainer

	// Enable KV store in Vault
	s.T().Log("Enabling KV store in Vault...")
	err = enableVaultKV(s.ctx, s.rcsNetwork.Name)
	require.NoError(s.T(), err)

	// Set initial console credentials for SSH key auth nodes
	s.T().Log("Setting initial console credentials in Vault...")
	err = setConsoleCredentials(s.ctx, s.vaultContainer, "x0c0s1b0", "ADMIN", "")
	require.NoError(s.T(), err)
	err = setConsoleCredentials(s.ctx, s.vaultContainer, "x0c0s1b0n0", "ADMIN", "")
	require.NoError(s.T(), err)

	// Load SSH keys into Vault (if available)
	s.T().Log("Loading SSH keys into Vault...")
	s.T().Log("Generating temporary SSH key pair for tests")
	sshKeyPath, publicKey, genErr := s.generateTempSSHKeyPair()
	require.NoError(s.T(), genErr)
	err = loadSSHKeysIntoVault(s.ctx, s.vaultContainer, sshKeyPath)
	require.NoError(s.T(), err)

	// Start Postgres
	s.T().Log("Starting Postgres...")
	postgresContainer, err := startPostgres(s.ctx, s.rcsNetwork.Name)
	require.NoError(s.T(), err)
	s.containers["postgres"] = postgresContainer

	// Initialize SMD database
	s.T().Log("Initializing SMD database...")
	err = initSMDDatabase(s.ctx, s.rcsNetwork.Name)
	require.NoError(s.T(), err)

	// Start SMD
	s.T().Log("Starting SMD...")
	smdContainer, err := startSMD(s.ctx, s.rcsNetwork.Name, s.rfNetwork.Name)
	require.NoError(s.T(), err)
	s.containers["smd"] = smdContainer

	// Start Redfish Emulators
	s.T().Log("Starting Redfish emulators...")
	authConfig := defaultAuthConfig
	rfEmulator0, err := startRedfishEmulator(s.ctx, s.rfNetwork.Name, "x0c0s0b0", "ssh", &authConfig)
	require.NoError(s.T(), err)
	s.containers["rf-x0c0s0b0"] = rfEmulator0

	keyAuthConfig := "ADMIN::Administrator;operator:operator_password:Operator;guest:guest_password:ReadOnly"
	rfEmulator1, err := startRedfishEmulator(s.ctx, s.rfNetwork.Name, "x0c0s1b0", "ssh", &keyAuthConfig)
	require.NoError(s.T(), err)
	s.containers["rf-x0c0s1b0"] = rfEmulator1

	keyAuthConfig = "root:root_password:Administrator;operator:operator_password:Operator;guest:guest_password:ReadOnly"
	rfEmulator2, err := startRedfishEmulator(s.ctx, s.rfNetwork.Name, "x0c0s2b0", "ipmi", &keyAuthConfig)
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
			Username: "ADMIN",
			Password: "",
		},
		{
			Host:     "x0c0s2b0",
			Username: "root",
			Password: "root_password",
		},
	}

	s.T().Log("Loading Redfish endpoints into SMD...")
	time.Sleep(5 * time.Second) // Give RF emulators time to fully start
	smdAPIURL, err := getSMDAPIURL(s.ctx, smdContainer)
	require.NoError(s.T(), err)
	err = loadRedfishEndpoints(s.ctx, smdAPIURL, redfishEndpoints)
	require.NoError(s.T(), err)

	// Start SSH password server
	s.T().Log("Starting SSH password server...")
	sshPasswordServer, err := startSSHPasswordServer(s.ctx, s.consoleNetwork.Name, "x0c0s0b0", "ADMIN", "ADMIN")
	require.NoError(s.T(), err)
	s.containers["ssh-password"] = sshPasswordServer

	// Start SSH key server
	s.T().Log("Starting SSH key server...")
	sshKeyServer, err := startSSHKeyServer(s.ctx, s.consoleNetwork.Name, "x0c0s1b0", "ADMIN", publicKey)
	require.NoError(s.T(), err)
	s.containers["ssh-key"] = sshKeyServer

	// Start IPMI server
	s.T().Log("Starting IPMI server...")
	ipmiServer, err := startIPMIServer(s.ctx, s.consoleNetwork.Name, "x0c0s2b0")
	require.NoError(s.T(), err)
	s.containers["ipmi"] = ipmiServer

	// Build and start remote-console
	s.T().Log("Starting remote-console...")
	remoteConsole, err := startRemoteConsole(s.ctx, s.rcsNetwork.Name, s.consoleNetwork.Name)
	require.NoError(s.T(), err)
	s.containers["remote-console"] = remoteConsole

	if os.Getenv("STREAM_REMOTE_CONSOLE_LOGS") == "1" {
		logPath := filepath.Join(os.TempDir(), fmt.Sprintf("remote-console-%d.log", time.Now().UnixNano()))
		s.T().Logf("Streaming remote-console logs to %s", logPath)
		s.streamContainerLogs("remote-console", remoteConsole, logPath)
	}

	// Get remote-console endpoint
	s.apiURL, err = s.getRemoteConsoleAPIURL(remoteConsole)
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
	defer resp.Body.Close()
	s.Equal(http.StatusOK, resp.StatusCode)

	err = json.NewDecoder(resp.Body).Decode(&healthResponse)
	s.Require().NoError(err)
	s.Equal("5", healthResponse.NumberConsoles)
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

	s.Require().Equal(len(consolesResponse.Consoles), 5, "Expected 5 consoles")

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
	}

	// Sort both slices for comparison
	sortByID(consolesResponse.Consoles, func(n nodes.NodeConsoleInfo) string { return n.ID })
	sortByID(consoles, func(n nodes.NodeConsoleInfo) string { return n.ID })

	s.Equal(consoles, consolesResponse.Consoles, "Consoles do not match expected consoles")

}

func (s *IntegrationTestSuite) tailWebSocketURL(nodeID string, rawQuery string) (url.URL, error) {
	parsedURL, err := url.Parse(s.apiURL)
	if err != nil {
		return url.URL{}, fmt.Errorf("failed to parse API URL: %w", err)
	}
	return url.URL{
		Scheme:   "ws",
		Host:     parsedURL.Host,
		Path:     fmt.Sprintf("/remote-console/consoles/%s/tail", nodeID),
		RawQuery: rawQuery,
	}, nil
}

func (s *IntegrationTestSuite) readWebSocketMessages(wsConn *websocket.Conn, timeout time.Duration) string {
	wsConn.SetReadDeadline(time.Now().Add(timeout))

	var output strings.Builder
	messageCount := 0
	start := time.Now()
	for {
		_, message, err := wsConn.ReadMessage()
		if err != nil {
			var closeErr *websocket.CloseError
			isTimeout := false
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				isTimeout = true
			}
			if errors.As(err, &closeErr) {
				s.T().Logf("WebSocket read saw close frame: %v", closeErr)
			}
			s.T().Logf("WebSocket read ended after %d messages (%s elapsed). timeout=%v err=%v", messageCount, time.Since(start), isTimeout, err)
			break
		}
		msgStr := string(message)
		s.T().Logf("Console: %s", msgStr)
		s.T().Logf("Console (raw): %q", message)
		output.WriteString(msgStr)
		messageCount++
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

func (s *IntegrationTestSuite) readNWebSocketMessages(wsConn *websocket.Conn, count int, timeout time.Duration) (string, error) {
	wsConn.SetReadDeadline(time.Now().Add(timeout))

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

func (s *IntegrationTestSuite) dialWebSocket(wsURL url.URL) (*websocket.Conn, *http.Response, error) {
	wsConn, resp, err := websocket.DefaultDialer.Dial(wsURL.String(), nil)
	if err != nil {
		return nil, resp, err
	}
	return wsConn, resp, nil
}

type fileLogConsumer struct {
	writer io.Writer
	mu     sync.Mutex
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

func (s *IntegrationTestSuite) waitForConsoleID(nodeID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(s.apiURL + "/remote-console/consoles")
		if err == nil {
			var consolesResp console.ConsolesResponse
			if decodeErr := json.NewDecoder(resp.Body).Decode(&consolesResp); decodeErr == nil {
				for _, consoleInfo := range consolesResp.Consoles {
					if consoleInfo.ID == nodeID {
						resp.Body.Close()
						return nil
					}
				}
			}
			resp.Body.Close()
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timed out waiting for console %s", nodeID)
}

func (s *IntegrationTestSuite) waitForConsoleRemoval(nodeID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(s.apiURL + "/remote-console/consoles")
		if err == nil {
			var consolesResp console.ConsolesResponse
			if decodeErr := json.NewDecoder(resp.Body).Decode(&consolesResp); decodeErr == nil {
				found := false

				s.T().Logf("len: %d", len(consolesResp.Consoles))

				for _, consoleInfo := range consolesResp.Consoles {
					if consoleInfo.ID == nodeID {
						found = true
						break
					}
				}
				if !found {
					resp.Body.Close()
					return nil
				}
			}
			resp.Body.Close()
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timed out waiting for console %s removal", nodeID)
}

// getRemoteConsoleAPIURL constructs the API URL for a container exposing port 26776
func (s *IntegrationTestSuite) getRemoteConsoleAPIURL(container testcontainers.Container) (string, error) {
	host, err := container.Host(s.ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get container host: %w", err)
	}
	port, err := container.MappedPort(s.ctx, "26776")
	if err != nil {
		return "", fmt.Errorf("failed to get container mapped port: %w", err)
	}
	return fmt.Sprintf("http://%s:%s", host, port.Port()), nil
}

func (s *IntegrationTestSuite) TestDynamicConsoleDiscovery() {
	newNodeID := dynamicTestXname

	s.T().Logf("Starting dynamic Redfish emulator and SSH console for %s", newNodeID)
	authConfig := defaultAuthConfig
	rfContainer, err := startRedfishEmulator(s.ctx, s.rfNetwork.Name, newNodeID, "ssh", &authConfig)
	s.Require().NoError(err)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := rfContainer.Terminate(ctx); err != nil {
			s.T().Logf("Warning: failed to terminate dynamic Redfish emulator %s: %v", newNodeID, err)
		}
	}()

	sshContainer, err := startSSHPasswordServer(s.ctx, s.consoleNetwork.Name, newNodeID, "ADMIN", "ADMIN")
	s.Require().NoError(err)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := sshContainer.Terminate(ctx); err != nil {
			s.T().Logf("Warning: failed to terminate dynamic SSH container %s: %v", newNodeID, err)
		}
	}()

	smdAPIURL, err := getSMDAPIURL(s.ctx, s.containers["smd"])
	s.Require().NoError(err)

	err = loadRedfishEndpoints(s.ctx, smdAPIURL, []redfishEndpoint{{
		Host:     newNodeID,
		Username: "ADMIN",
		Password: "ADMIN",
	}})
	s.Require().NoError(err, "failed to register dynamic Redfish endpoint")

	s.Require().NoError(s.waitForConsoleID(newNodeID, 3*time.Minute), "remote-console did not detect new console")

	wsConn, resp, err := s.connectInteractiveConsole(newNodeID, ":~$ ", 90*time.Second)
	s.Require().NoError(err)
	defer resp.Body.Close()
	defer wsConn.Close()

	testMsg := "hostname\r"
	err = wsConn.WriteMessage(websocket.TextMessage, []byte(testMsg))
	s.Require().NoError(err, "Error sending test message to console")

	expectedHostLine := newNodeID + "\r\n"
	hostnameOutput, err := s.readWebSocketUntil(wsConn, expectedHostLine, 90*time.Second)
	s.Require().NoError(err, "Expected hostname output from console")
	s.Require().True(strings.Contains(hostnameOutput, expectedHostLine),
		"Expected hostname command output in console output; got %q", hostnameOutput)

	s.T().Log("Removing dynamic console registration")
	smdAPIURL, err = getSMDAPIURL(s.ctx, s.containers["smd"])
	s.Require().NoError(err)
	err = deleteRedfishEndpoint(s.ctx, smdAPIURL, newNodeID)
	s.Require().NoError(err, "failed to remove dynamic Redfish endpoint")
	s.Require().NoError(s.waitForConsoleRemoval(newNodeID, 3*time.Minute), "remote-console did not drop dynamic console")

	// Try to connect again, should fail
	// TODO main this fail faster, we probably don't need to do the retries here
	_, resp, err = s.connectInteractiveConsole(newNodeID, ":~$ ", 30*time.Second)
	s.Require().Error(err, "Expected error connecting to removed console")
	if resp != nil {
		s.T().Logf("Console removal connection response status: %d", resp.StatusCode)
		defer resp.Body.Close()
	}

}

// TestIntegrationSuite runs the integration test suite
func TestIntegrationSuite(t *testing.T) {
	suite.Run(t, new(IntegrationTestSuite))
}
