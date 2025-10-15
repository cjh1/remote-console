package test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/vault"
	"github.com/testcontainers/testcontainers-go/wait"
)

type redfishEndpoint struct {
	Host     string
	Username string
	Password string
}

// startVault starts a Vault container with development mode enabled
func startVault(ctx context.Context, networks ...string) (testcontainers.Container, error) {

	// Base options for Vault container, including dev token.
	opts := []testcontainers.ContainerCustomizer{
		vault.WithToken("hms"),
	}

	// Add networks and additional configuration if specified.
	networkAliases := make(map[string][]string)
	for _, network := range networks {
		networkAliases[network] = []string{"vault"}
	}

	opts = append(opts, testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Hostname:       "vault",
			Networks:       networks,
			NetworkAliases: networkAliases,
			Env: map[string]string{
				"VAULT_DEV_LISTEN_ADDRESS": "0.0.0.0:8200",
				"VAULT_ADDR":               "http://127.0.0.1:8200",
			},
		},
	}))

	return vault.Run(ctx, "vault:1.5.5", opts...)
}

// enableVaultKV enables KV store in Vault
func enableVaultKV(ctx context.Context, network string) error {
	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    "..",
			Dockerfile: "vault-kv-enabler.dockerfile",
		},
		Networks: []string{network},
		Env: map[string]string{
			"VAULT_ADDR":  "http://vault:8200",
			"VAULT_TOKEN": "hms",
			"KV_STORES":   "hms-creds",
		},
		WaitingFor: wait.ForExit().WithExitTimeout(30 * time.Second),
	}

	_, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	return err
}

// loadSSHKeysIntoVault loads SSH keys from the local filesystem into Vault
func loadSSHKeysIntoVault(ctx context.Context, vaultContainer testcontainers.Container, sshKeyPath string) error {
	if _, err := os.Stat(sshKeyPath); err != nil {
		return fmt.Errorf("SSH key not found at %s: %w", sshKeyPath, err)
	}

	// Copy the key file into the vault container
	err := vaultContainer.CopyFileToContainer(ctx, sshKeyPath, "/tmp/bmc-console-key", 0400)
	if err != nil {
		return fmt.Errorf("failed to copy SSH key to vault container: %w", err)
	}

	// Execute vault command to store the key
	cmd := []string{
		"vault", "kv", "put",
		"hms-creds/bmc-console-keys",
		"PrivateKey=@/tmp/bmc-console-key",
	}

	exitCode, reader, err := vaultContainer.Exec(ctx, cmd)
	if err != nil {
		return fmt.Errorf("failed to exec vault command: %w", err)
	}

	output, _ := io.ReadAll(reader)
	if exitCode != 0 {
		return fmt.Errorf("failed to store SSH key in vault: exit code %d, output: %s", exitCode, string(output))
	}

	return nil
}

// setConsoleCredentials sets console credentials for a given xname in Vault
func setConsoleCredentials(ctx context.Context, vaultContainer testcontainers.Container, xname, username, password string) error {

	// Setup vault command to store credentials
	cmd := []string{
		"vault", "kv", "put",
		fmt.Sprintf("hms-creds/%s", xname),
		fmt.Sprintf("Username=%s", username),
		fmt.Sprintf("Password=%s", password),
		fmt.Sprintf("Xname=%s", xname),
	}

	// Execute vault command
	exitCode, reader, err := vaultContainer.Exec(ctx, cmd)
	if err != nil {
		return fmt.Errorf("failed to exec vault command: %w", err)
	}

	output, _ := io.ReadAll(reader)
	if exitCode != 0 {
		return fmt.Errorf("failed to store credentials for %s: exit code %d, output: %s", xname, exitCode, string(output))
	}

	return nil
}

// startPostgres starts a Postgre container
func startPostgres(ctx context.Context, network string) (testcontainers.Container, error) {
	return postgres.Run(ctx,
		"postgres:11-alpine",
		postgres.WithDatabase("hmsds"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithStartupTimeout(60*time.Second),
		),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Hostname: "postgres",
				Networks: []string{network},
				NetworkAliases: map[string][]string{
					network: {"postgres"},
				},
			},
		}),
	)
}

// initSMDDatabase initializes the SMD database schema
func initSMDDatabase(ctx context.Context, network string) error {
	req := testcontainers.ContainerRequest{
		Image:    "ghcr.io/openchami/smd:v2.19.2",
		Networks: []string{network},
		Env: map[string]string{
			"SMD_DBHOST": "postgres",
			"SMD_DBPORT": "5432",
			"SMD_DBNAME": "hmsds",
			"SMD_DBUSER": "postgres",
			"SMD_DBPASS": "postgres",
			"SMD_DBOPTS": "sslmode=disable",
		},
		Cmd:        []string{"/smd-init"},
		WaitingFor: wait.ForExit().WithExitTimeout(60 * time.Second),
	}

	_, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	return err
}

// startSMD starts SMD
func startSMD(ctx context.Context, networks ...string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:          "ghcr.io/openchami/smd:v2.19.2",
		Hostname:       "smd",
		Networks:       networks,
		NetworkAliases: map[string][]string{},
		Env: map[string]string{
			"SMD_DBHOST":           "postgres",
			"SMD_DBPORT":           "5432",
			"SMD_DBNAME":           "hmsds",
			"SMD_DBUSER":           "postgres",
			"SMD_DBPASS":           "postgres",
			"SMD_DBOPTS":           "sslmode=disable",
			"SMD_JWKS_URL":         "",
			"RF_MSG_HOST":          "kafka:9092:cray-dmtf-resource-event",
			"CRAY_VAULT_AUTH_PATH": "auth/token/create",
			"CRAY_VAULT_ROLE_FILE": "configs/namespace",
			"CRAY_VAULT_JWT_FILE":  "configs/token",
			"VAULT_ADDR":           "http://vault:8200",
			"VAULT_TOKEN":          "hms",
			"VAULT_KEYPATH":        "hms-creds",
			"SMD_WVAULT":           "true",
			"SMD_RVAULT":           "true",
			"SMD_SLS_HOST":         "",
			"SMD_HBTD_HOST":        "",
			"ENABLE_DISCOVERY":     "true", // Enable discovery to find Redfish endpoints
		},
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      "../configs/namespace",
				ContainerFilePath: "/configs/namespace",
				FileMode:          0644,
			},
			{
				HostFilePath:      "../configs/token",
				ContainerFilePath: "/configs/token",
				FileMode:          0644,
			},
		},
		ExposedPorts: []string{"27779/tcp"},
		WaitingFor:   wait.ForHTTP("/hsm/v2/service/ready").WithPort("27779/tcp").WithStartupTimeout(120 * time.Second),
	}

	for _, network := range networks {
		req.NetworkAliases[network] = []string{"smd"}
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

// getSMDAPIURL returns the base API URL for the SMD container
func getSMDAPIURL(ctx context.Context, smdContainer testcontainers.Container) (string, error) {
	host, err := smdContainer.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get SMD host: %w", err)
	}
	port, err := smdContainer.MappedPort(ctx, "27779")
	if err != nil {
		return "", fmt.Errorf("failed to get SMD port: %w", err)
	}
	return fmt.Sprintf("http://%s:%s/hsm/v2", host, port.Port()), nil
}

// startRedfishEmulator starts a Redfish emulator for a specific xname
func startRedfishEmulator(ctx context.Context, network string, xname string, mock string, authConfig *string) (testcontainers.Container, error) {
	env := map[string]string{
		"MOCKUPFOLDER": mock,
		"MAC_SCHEMA":   "Mountain",
		"XNAME":        xname,
		"PORT":         "443",
	}

	if authConfig != nil {
		env["AUTH_CONFIG"] = *authConfig
	}

	mocksDirectory, err := filepath.Abs(filepath.Join(".", "redfish-emulator-mocks"))
	if err != nil {
		return nil, fmt.Errorf("unable to determine absolute path for mocks directory: %w", err)
	}

	req := testcontainers.ContainerRequest{
		Image:    "ghcr.io/openchami/csm-rie:v1.6.7",
		Hostname: xname,
		Networks: []string{network},
		NetworkAliases: map[string][]string{
			network: {xname},
		},
		Env:        env,
		WaitingFor: wait.ForLog("Running on all addresses").WithStartupTimeout(60 * time.Second),
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      filepath.Join(mocksDirectory, "ssh"),
				ContainerFilePath: "/app/api_emulator/redfish/static/",
			},
			{
				HostFilePath:      filepath.Join(mocksDirectory, "ipmi"),
				ContainerFilePath: "/app/api_emulator/redfish/static/",
			},
		},
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

// loadRedfishEndpoints loads Redfish endpoint information into SMD
func loadRedfishEndpoints(t testing.TB, ctx context.Context, smdAPIURL string, endpoints []redfishEndpoint) error {
	if len(endpoints) == 0 {
		return nil
	}

	type redfishEndpoint struct {
		ID                 string `json:"ID"`
		FQDN               string `json:"FQDN"`
		RediscoverOnUpdate bool   `json:"RediscoverOnUpdate"`
		User               string `json:"User"`
		Password           string `json:"Password"`
	}
	type redfishEndpoints struct {
		RedfishEndpoints []redfishEndpoint `json:"RedfishEndpoints"`
	}

	payload := redfishEndpoints{
		RedfishEndpoints: make([]redfishEndpoint, 0, len(endpoints)),
	}

	for _, endpoint := range endpoints {
		payload.RedfishEndpoints = append(payload.RedfishEndpoints, redfishEndpoint{
			ID:                 endpoint.Host,
			FQDN:               endpoint.Host,
			RediscoverOnUpdate: true,
			User:               endpoint.Username,
			Password:           endpoint.Password,
		})
	}

	jsonBody, err := json.Marshal(payload)

	if err != nil {
		return fmt.Errorf("failed to build Redfish endpoints payload: %w", err)
	}

	url := fmt.Sprintf("%s/Inventory/RedfishEndpoints", smdAPIURL)

	resp, err := http.Post(url,
		"application/json",
		bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("failed to post Redfish endpoints: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Logf("Warning: failed to close response body: %v", err)
		}
	}()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to load Redfish endpoints: status %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}

func loadRedfishEndpoint(t testing.TB, ctx context.Context, smdAPIURL string, endpoint redfishEndpoint) error {
	return loadRedfishEndpoints(t, ctx, smdAPIURL, []redfishEndpoint{endpoint})
}

func deleteRedfishEndpoint(t testing.TB, ctx context.Context, smdAPIURL string, endpointID string) error {
	url := fmt.Sprintf("%s/Inventory/RedfishEndpoints/%s", smdAPIURL, endpointID)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create DELETE request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to delete Redfish endpoint: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Logf("Warning: failed to close response body: %v", err)
		}
	}()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete Redfish endpoint %s: status %d, body: %s", endpointID, resp.StatusCode, string(body))
	}

	return nil
}

// startSSHPasswordServer starts an SSH server with password authentication
func startSSHPasswordServer(ctx context.Context, network string, host string, username string, password string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:    "linuxserver/openssh-server:latest",
		Hostname: host,
		Networks: []string{network},
		NetworkAliases: map[string][]string{
			network: {host},
		},
		Env: map[string]string{
			"PASSWORD_ACCESS": "true",
			"USER_NAME":       username,
			"USER_PASSWORD":   password,
			"LISTEN_PORT":     "22",
		},
		ExposedPorts: []string{"22/tcp"},
		WaitingFor:   wait.ForLog("done.").WithStartupTimeout(60 * time.Second),
		// Copy broadcast.sh into the container
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      "broadcast.sh",
				ContainerFilePath: "/usr/local/bin/broadcast.sh",
				FileMode:          0755,
			},
		},
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

// startSSHKeyServer starts an SSH server with public key authentication
func startSSHKeyServer(ctx context.Context, network string, host string, username string, publicKey string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:    "linuxserver/openssh-server:latest",
		Hostname: host,
		Networks: []string{network},
		NetworkAliases: map[string][]string{
			network: {host},
		},
		Env: map[string]string{
			"USER_NAME":   username,
			"PUBLIC_KEY":  publicKey,
			"LISTEN_PORT": "22",
		},
		ExposedPorts: []string{"22/tcp"},
		WaitingFor:   wait.ForLog("done.").WithStartupTimeout(60 * time.Second),
		// Copy broadcast.sh into the container
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      "broadcast.sh",
				ContainerFilePath: "/usr/local/bin/broadcast.sh",
				FileMode:          0755,
			},
		},
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

// startIPMIServer starts an IPMI server
func startIPMIServer(ctx context.Context, network string, hostname string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    "../ipmi_sim",
			Dockerfile: "Dockerfile",
		},
		Hostname: hostname,
		Networks: []string{network},
		NetworkAliases: map[string][]string{
			network: {hostname},
		},
		ExposedPorts: []string{"623/udp"},
		WaitingFor:   wait.ForLog("Opened UDP port 623").WithStartupTimeout(30 * time.Second),
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

// startRemoteConsoleWithEnv starts the remote-console service with optional env overrides
func startRemoteConsoleWithEnv(ctx context.Context, envOverrides map[string]string, networks ...string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    "..",
			Dockerfile: "Dockerfile",
		},
		Networks:       networks,
		NetworkAliases: map[string][]string{},
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      "../configs/namespace",
				ContainerFilePath: "/app/configs/namespace",
				FileMode:          0644,
			},
			{
				HostFilePath:      "../configs/token",
				ContainerFilePath: "/app/configs/token",
				FileMode:          0644,
			},
		},
		Env: map[string]string{
			"RCS_SMD_URL":                            "http://smd:27779",
			"SMS_SERVER":                             "http://smd:27779",
			"CRAY_VAULT_AUTH_PATH":                   "auth/token/create",
			"CRAY_VAULT_ROLE_FILE":                   "/app/configs/namespace",
			"CRAY_VAULT_JWT_FILE":                    "/app/configs/token",
			"VAULT_ADDR":                             "http://vault:8200",
			"VAULT_TOKEN":                            "hms",
			"VAULT_BASE_PATH":                        "hms-creds",
			"VAULT_SKIP_VERIFY":                      "true",
			"VAULT_ENABLED":                          "true",
			"LOG_LEVEL":                              "DEBUG",
			"RCS_NEW_NODE_LOOKUP":                    "10",
			"RCS_CREDS_MONITOR_INTERVAL":             "10",
			"RCS_CREDS_SECURE_STORAGE_SSH_KEYS_PATH": "hms-creds/bmc-console-keys",
			"RCS_CONMAN_PID_FILE_PATH":               "/app/remote-console.pid",
			"RCS_CONMAN_LOGS_PATH":                   "/tmp",
			// Log rotation settings for testing
			"RCS_LOG_ROTATE_CHECK_FREQUENCY": "5",  // Check every 5 seconds
			"RCS_CONSOLE_LOGS_FILE_SIZE":     "5M", // Small size to trigger rotation easily
			"RCS_CONSOLE_LOGS_NUM_ROTATE":    "2",  // Keep 2 rotated files
			"RCS_CONSOLE_LOGS_BACKUP_PATH":   "/tmp/conman.old",
			"RCS_LOG_ROTATE_FILE_PATH":       "/tmp/logrotate.conman",
			"RCS_LOG_ROTATE_STATE_FILE_PATH": "/tmp/rot_conman.state",
		},
		ExposedPorts: []string{"26776/tcp"},
		WaitingFor: wait.ForHTTP("/remote-console/readiness").
			WithPort("26776/tcp").
			WithStatusCodeMatcher(func(status int) bool {
				return status == http.StatusNoContent
			}).
			WithStartupTimeout(120 * time.Second),
	}

	for _, network := range networks {
		req.NetworkAliases[network] = []string{"remote-console"}
	}

	// Apply overrides if provided
	for k, v := range envOverrides {
		req.Env[k] = v
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

// startRemoteConsole starts the remote-console service with default test env
func startRemoteConsole(ctx context.Context, networks ...string) (testcontainers.Container, error) {
	return startRemoteConsoleWithEnv(ctx, nil, networks...)
}
