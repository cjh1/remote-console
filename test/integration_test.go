package test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/network"
)

func TestRemoteConsoleIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Create networks
	rcsNet, err := network.New(ctx, network.WithCheckDuplicate())
	require.NoError(t, err)
	defer rcsNet.Remove(ctx)

	rcsRfNet, err := network.New(ctx, network.WithCheckDuplicate())
	require.NoError(t, err)
	defer rcsRfNet.Remove(ctx)

	rcsConsoleNet, err := network.New(ctx, network.WithCheckDuplicate())
	require.NoError(t, err)
	defer rcsConsoleNet.Remove(ctx)

	// Start Vault
	t.Log("Starting Vault...")
	vaultContainer, err := startVault(ctx, rcsNet.Name, rcsConsoleNet.Name)
	require.NoError(t, err)
	defer vaultContainer.Terminate(ctx)

	// Enable KV store in Vault
	t.Log("Enabling KV store in Vault...")
	err = enableVaultKV(ctx, rcsNet.Name)
	require.NoError(t, err)

	// Load SSH keys into Vault (if available)
	t.Log("Loading SSH keys into Vault...")
	sshKeyPath := getDefaultSSHKeyPath()
	if _, err := os.Stat(sshKeyPath); err == nil {
		err = loadSSHKeysIntoVault(ctx, rcsNet.Name, sshKeyPath)
		require.NoError(t, err)
	} else {
		t.Log("SSH key not found, skipping key loading")
	}

	// Start Postgres
	t.Log("Starting Postgres...")
	postgresContainer, err := startPostgres(ctx, rcsNet.Name)
	require.NoError(t, err)
	defer postgresContainer.Terminate(ctx)

	// Initialize SMD database
	t.Log("Initializing SMD database...")
	err = initSMDDatabase(ctx, rcsNet.Name)
	require.NoError(t, err)

	// Start SMD
	t.Log("Starting SMD...")
	smdContainer, err := startSMD(ctx, rcsNet.Name, rcsRfNet.Name)
	require.NoError(t, err)
	defer smdContainer.Terminate(ctx)

	// Start Redfish Emulators
	t.Log("Starting Redfish emulators...")
	rfEmulator0, err := startRedfishEmulator(ctx, rcsRfNet.Name, "x0c0s0b0")
	require.NoError(t, err)
	defer rfEmulator0.Terminate(ctx)

	rfEmulator1, err := startRedfishEmulator(ctx, rcsRfNet.Name, "x0c0s1b0")
	require.NoError(t, err)
	defer rfEmulator1.Terminate(ctx)

	rfEmulator2, err := startRedfishEmulator(ctx, rcsRfNet.Name, "x0c0s2b0")
	require.NoError(t, err)
	defer rfEmulator2.Terminate(ctx)

	// Load Redfish endpoints into SMD
	t.Log("Loading Redfish endpoints into SMD...")
	time.Sleep(5 * time.Second) // Give RF emulators time to fully start
	err = loadRedfishEndpoints(ctx, rcsRfNet.Name, []string{"x0c0s0b0", "x0c0s1b0", "x0c0s2b0"})
	require.NoError(t, err)

	// Start SSH password server
	t.Log("Starting SSH password server...")
	sshPasswordServer, err := startSSHPasswordServer(ctx, rcsConsoleNet.Name, "x0c0s0b0", "admin", "admin")
	require.NoError(t, err)
	defer sshPasswordServer.Terminate(ctx)

	// Start SSH key server
	t.Log("Starting SSH key server...")
	publicKey := os.Getenv("PUBLIC_KEY")
	if publicKey == "" {
		t.Log("Warning: PUBLIC_KEY not set, SSH key server may not work properly")
	}
	sshKeyServer, err := startSSHKeyServer(ctx, rcsConsoleNet.Name, "x0c0s1b0", "n0", publicKey)
	require.NoError(t, err)
	defer sshKeyServer.Terminate(ctx)

	// Start IPMI server
	t.Log("Starting IPMI server...")
	ipmiServer, err := startIPMIServer(ctx, rcsConsoleNet.Name, "x0c0s2b0")
	require.NoError(t, err)
	defer ipmiServer.Terminate(ctx)

	// Build and start remote-console
	t.Log("Starting remote-console...")
	remoteConsole, err := startRemoteConsole(ctx, rcsNet.Name, rcsConsoleNet.Name)
	require.NoError(t, err)
	defer remoteConsole.Terminate(ctx)

	// Get remote-console endpoint
	host, err := remoteConsole.Host(ctx)
	require.NoError(t, err)
	port, err := remoteConsole.MappedPort(ctx, "26776")
	require.NoError(t, err)
	apiURL := fmt.Sprintf("http://%s:%s", host, port.Port())

	t.Logf("Remote console API available at: %s", apiURL)

	// Run integration tests
	t.Run("HealthCheck", func(t *testing.T) {
		resp, err := http.Get(apiURL + "/remote-console/health")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	// t.Run("ConsoleNodesDiscovered", func(t *testing.T) {
	// 	// Give some time for node discovery
	// 	time.Sleep(10 * time.Second)

	// 	resp, err := http.Get(apiURL + "/console-nodes")
	// 	require.NoError(t, err)
	// 	defer resp.Body.Close()
	// 	require.Equal(t, http.StatusOK, resp.StatusCode)

	// 	// TODO: Parse response and verify nodes are present
	// })

	// t.Run("SSHPasswordConsoleConnection", func(t *testing.T) {
	// 	// Test connection to SSH password-based console (x0c0s0b0)
	// 	resp, err := http.Get(apiURL + "/console/x0c0s0b0")
	// 	require.NoError(t, err)
	// 	defer resp.Body.Close()
	// 	// Status code may vary depending on actual connection state
	// 	t.Logf("SSH password console response status: %d", resp.StatusCode)
	// })

	// t.Run("SSHKeyConsoleConnection", func(t *testing.T) {
	// 	// Test connection to SSH key-based console (x0c0s1b0)
	// 	resp, err := http.Get(apiURL + "/console/x0c0s1b0")
	// 	require.NoError(t, err)
	// 	defer resp.Body.Close()
	// 	t.Logf("SSH key console response status: %d", resp.StatusCode)
	// })

	// t.Run("IPMIConsoleConnection", func(t *testing.T) {
	// 	// Test connection to IPMI console (x0c0s2b0)
	// 	resp, err := http.Get(apiURL + "/console/x0c0s2b0")
	// 	require.NoError(t, err)
	// 	defer resp.Body.Close()
	// 	t.Logf("IPMI console response status: %d", resp.StatusCode)
	// })

	t.Log("Integration tests completed successfully")
}
