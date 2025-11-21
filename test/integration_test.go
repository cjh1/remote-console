package test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"testing"
	"time"
	"net/url"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/gorilla/websocket"

	"github.com/OpenCHAMI/remote-console/internal/console"
	"github.com/OpenCHAMI/remote-console/internal/nodes"
)

// IntegrationTestSuite is the test suite for remote-console integration tests
type IntegrationTestSuite struct {
	suite.Suite
	ctx           context.Context
	apiURL        string
	networks      []*testcontainers.DockerNetwork
	containers    []testcontainers.Container
}

// SetupSuite runs once before all tests in the suite
func (s *IntegrationTestSuite) SetupSuite() {
	if testing.Short() {
		s.T().Skip("Skipping integration test in short mode")
	}

	s.ctx = context.Background()

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
	s.containers = append(s.containers, vaultContainer)

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
	s.containers = append(s.containers, postgresContainer)

	// Initialize SMD database
	s.T().Log("Initializing SMD database...")
	err = initSMDDatabase(s.ctx, rcsNet.Name)
	require.NoError(s.T(), err)

	// Start SMD
	s.T().Log("Starting SMD...")
	smdContainer, err := startSMD(s.ctx, rcsNet.Name, rcsRfNet.Name)
	require.NoError(s.T(), err)
	s.containers = append(s.containers, smdContainer)

	// Start Redfish Emulators
	s.T().Log("Starting Redfish emulators...")
	authConfig := "ADMIN:ADMIN:Administrator;operator:operator_password:Operator;guest:guest_password:ReadOnly"
	rfEmulator0, err := startRedfishEmulator(s.ctx, rcsRfNet.Name, "x0c0s0b0", "ssh", &authConfig)
	require.NoError(s.T(), err)
	s.containers = append(s.containers, rfEmulator0)

	rfEmulator1, err := startRedfishEmulator(s.ctx, rcsRfNet.Name, "x0c0s1b0", "ssh", nil)
	require.NoError(s.T(), err)
	s.containers = append(s.containers, rfEmulator1)

	rfEmulator2, err := startRedfishEmulator(s.ctx, rcsRfNet.Name, "x0c0s2b0", "ipmi", nil)
	require.NoError(s.T(), err)
	s.containers = append(s.containers, rfEmulator2)

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
	s.containers = append(s.containers, sshPasswordServer)

	// Start SSH key server
	s.T().Log("Starting SSH key server...")
	publicKey := os.Getenv("PUBLIC_KEY")
	if publicKey == "" {
		s.T().Log("Warning: PUBLIC_KEY not set, SSH key server may not work properly")
	}
	sshKeyServer, err := startSSHKeyServer(s.ctx, rcsConsoleNet.Name, "x0c0s1b0", "n0", publicKey)
	require.NoError(s.T(), err)
	s.containers = append(s.containers, sshKeyServer)

	// Start IPMI server
	s.T().Log("Starting IPMI server...")
	ipmiServer, err := startIPMIServer(s.ctx, rcsConsoleNet.Name, "x0c0s2b0")
	require.NoError(s.T(), err)
	s.containers = append(s.containers, ipmiServer)

	// Build and start remote-console
	s.T().Log("Starting remote-console...")
	remoteConsole, err := startRemoteConsole(s.ctx, rcsNet.Name, rcsConsoleNet.Name)
	require.NoError(s.T(), err)
	s.containers = append(s.containers, remoteConsole)

	// Get remote-console endpoint
	host, err := remoteConsole.Host(s.ctx)
	require.NoError(s.T(), err)
	port, err := remoteConsole.MappedPort(s.ctx, "26776")
	require.NoError(s.T(), err)
	s.apiURL = fmt.Sprintf("http://%s:%s", host, port.Port())

	s.T().Logf("Remote console API available at: %s", s.apiURL)
}

// TearDownSuite runs once after all tests in the suite
func (s *IntegrationTestSuite) TearDownSuite() {
	// Create a context with timeout for cleanup
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Clean up containers in reverse order
	for i := len(s.containers) - 1; i >= 0; i-- {
		if err := s.containers[i].Terminate(cleanupCtx); err != nil {
			s.T().Logf("Warning: failed to terminate container: %v", err)
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

	wsConn, resp, err := websocket.DefaultDialer.Dial(wsURL.String(), nil)
	if err != nil {
		return nil, resp, fmt.Errorf("WebSocket dial error: %v", err)
	}

	return wsConn, resp, nil
}


// TestSSHPasswordConsoleConnection verifies SSH password-based console connection
func (s *IntegrationTestSuite) TestSSHPasswordConsoleTail() {
	wsConn, resp, err := s.websocketConnect("/remote-console/consoles/x0c0s0b0/tail")
	s.Require().NoError(err)
	defer resp.Body.Close()
	defer wsConn.Close()

	// Read a few messages from the console
	for i := 0; i < 1; i++ {
		_, message, err := wsConn.ReadMessage()
		s.Require().NoError(err)
		s.T().Logf("Received console message: %s", string(message))
	}
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
