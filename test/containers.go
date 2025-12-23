package test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type redfishEndpoint struct {
	Host     string
	Username string
	Password string
}

// startVault starts a Vault container with development mode enabled
func startVault(ctx context.Context, networks ...string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:          "docker.io/library/vault:1.5.5",
		Hostname:       "vault",
		Networks:       networks,
		NetworkAliases: map[string][]string{},
		Env: map[string]string{
			"VAULT_DEV_ROOT_TOKEN_ID":  "hms",
			"VAULT_DEV_LISTEN_ADDRESS": "0.0.0.0:8200",
			"VAULT_ADDR":               "http://127.0.0.1:8200",
		},
		ExposedPorts: []string{"8200/tcp"},
		WaitingFor:   wait.ForHTTP("/v1/sys/health").WithPort("8200/tcp").WithStartupTimeout(60 * time.Second),
		CapAdd:       []string{"IPC_LOCK"},
	}

	for _, network := range networks {
		req.NetworkAliases[network] = []string{"vault"}
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
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
func loadSSHKeysIntoVault(ctx context.Context, network string, sshKeyPath string) error {
	if _, err := os.Stat(sshKeyPath); err != nil {
		return fmt.Errorf("SSH key not found at %s: %w", sshKeyPath, err)
	}

	req := testcontainers.ContainerRequest{
		Image:    "docker.io/library/vault:1.5.5",
		Networks: []string{network},
		Env: map[string]string{
			"VAULT_ADDR":      "http://vault:8200",
			"VAULT_TOKEN":     "hms",
			"VAULT_BASE_PATH": "hms-creds",
		},
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      sshKeyPath,
				ContainerFilePath: "/tmp/bmc-console-key",
				FileMode:          0400,
			},
		},
		Cmd: []string{
			"sh", "-c",
			"until vault status 2>/dev/null; do echo 'Waiting for vault...'; sleep 2; done && " +
				"vault kv put $VAULT_BASE_PATH/bmc-console-keys PrivateKey=@/tmp/bmc-console-key",
		},
		WaitingFor: wait.ForExit().WithExitTimeout(30 * time.Second),
	}

	_, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	return err
}

// TODO I think this can be done by exec into the existing vault container? Would that be better?
func setConsoleCredentials(ctx context.Context, network, xname, username, password string) error {
	cmd := fmt.Sprintf("vault kv put hms-creds/%s Username=%s Password='%s' Xname=%s",
		xname, username, password, xname)

	req := testcontainers.ContainerRequest{
		Image:    "docker.io/library/vault:1.5.5",
		Networks: []string{network},
		Env: map[string]string{
			"VAULT_ADDR":  "http://vault:8200",
			"VAULT_TOKEN": "hms",
		},
		Cmd:        []string{"sh", "-c", cmd},
		WaitingFor: wait.ForExit().WithExitTimeout(30 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return err
	}

	state, err := container.State(ctx)
	if err != nil {
		return fmt.Errorf("failed to get container state: %w", err)
	}

	if state.ExitCode != 0 {
		return fmt.Errorf("failed to store credentials for %s: exit code %d", xname, state.ExitCode)
	}

	return nil
}

// startPostgres starts a PostgreSQL container
func startPostgres(ctx context.Context, network string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:    "docker.io/library/postgres:11-alpine",
		Hostname: "postgres",
		Networks: []string{network},
		NetworkAliases: map[string][]string{
			network: {"postgres"},
		},
		Env: map[string]string{
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_USER":     "postgres",
			"POSTGRES_DB":       "hmsds",
		},
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor:   wait.ForLog("database system is ready to accept connections").WithStartupTimeout(60 * time.Second),
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

// initSMDDatabase initializes the SMD database schema
func initSMDDatabase(ctx context.Context, network string) error {
	req := testcontainers.ContainerRequest{
		Image:    "docker.io/openchami/smd:rf",
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

// startSMD starts the State Management Database service
func startSMD(ctx context.Context, networks ...string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:          "docker.io/openchami/smd:rf",
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
			"ENABLE_DISCOVERY":     "true",
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

	fmt.Printf("Using mocks directory: %s\n", mocksDirectory)

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
func loadRedfishEndpoints(ctx context.Context, smdAPIURL string, endpoints []redfishEndpoint) error {
	if len(endpoints) == 0 {
		return nil
	}

	// Build JSON payload
	jsonPayload := `{"RedfishEndpoints":[`
	for i, endpoint := range endpoints {
		if i > 0 {
			jsonPayload += ","
		}
		jsonPayload += fmt.Sprintf(`{"ID":"%s","FQDN":"%s","RediscoverOnUpdate":true,"User":"%s","Password":"%s"}`,
			endpoint.Host, endpoint.Host, endpoint.Username, endpoint.Password)
	}
	jsonPayload += `]}`

	url := fmt.Sprintf("%s/Inventory/RedfishEndpoints", smdAPIURL)
	resp, err := http.Post(url,
		"application/json",
		strings.NewReader(jsonPayload))
	if err != nil {
		return fmt.Errorf("failed to post Redfish endpoints: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to load Redfish endpoints: status %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}

func deleteRedfishEndpoint(ctx context.Context, smdAPIURL string, endpointID string) error {
	url := fmt.Sprintf("%s/Inventory/RedfishEndpoints/%s", smdAPIURL, endpointID)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create DELETE request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to delete Redfish endpoint: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete Redfish endpoint %s: status %d, body: %s", endpointID, resp.StatusCode, string(body))
	}

	return nil
}

// startSSHPasswordServer starts an SSH server with password authentication
func startSSHPasswordServer(ctx context.Context, network string, alias string, username string, password string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:    "linuxserver/openssh-server:latest",
		Hostname: alias,
		Networks: []string{network},
		NetworkAliases: map[string][]string{
			network: {alias},
		},
		Env: map[string]string{
			"PUID":            "1000",
			"PGID":            "1000",
			"PASSWORD_ACCESS": "true",
			"USER_NAME":       username,
			"USER_PASSWORD":   password,
			"LISTEN_PORT":     "22",
		},
		ExposedPorts: []string{"22/tcp"},
		WaitingFor:   wait.ForLog("done.").WithStartupTimeout(60 * time.Second),
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
func startSSHKeyServer(ctx context.Context, network string, alias string, username string, publicKey string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:    "linuxserver/openssh-server:latest",
		Hostname: alias,
		Networks: []string{network},
		NetworkAliases: map[string][]string{
			network: {alias},
		},
		Env: map[string]string{
			"PUID":        "1000",
			"PGID":        "1000",
			"USER_NAME":   username,
			"PUBLIC_KEY":  publicKey,
			"LISTEN_PORT": "22",
		},
		ExposedPorts: []string{"22/tcp"},
		WaitingFor:   wait.ForLog("done.").WithStartupTimeout(60 * time.Second),
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

// startIPMIServer starts an IPMI server TODO alias => hostname
func startIPMIServer(ctx context.Context, network string, alias string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    "../ipmi_sim",
			Dockerfile: "Dockerfile",
		},
		Hostname: alias,
		Networks: []string{network},
		NetworkAliases: map[string][]string{
			network: {alias},
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
			"RCS_LOG_ROTATE_CHECK_FREQUENCY": "5", // Check every 5 seconds
			"RCS_CONSOLE_LOGS_FILE_SIZE":     "5M", // Small size to trigger rotation easily
			"RCS_CONSOLE_LOGS_NUM_ROTATE":    "2", // Keep 2 rotated files
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