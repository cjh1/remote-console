package test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type redfishEndpoint struct {
	Host	 string 
	Username string 
	Password string 
}


// startVault starts a Vault container with development mode enabled
func startVault(ctx context.Context, networks ...string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:    "docker.io/library/vault:1.5.5",
		Hostname: "vault",
		Networks: networks,
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
		Image:    "docker.io/openchami/smd:rf",
		Hostname: "smd",
		Networks: networks,
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
		Env: env,
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
func loadRedfishEndpoints(ctx context.Context, network string, endpoints []redfishEndpoint) error {
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

	curlCmd := fmt.Sprintf("apk add curl && sleep 10 && curl -X POST -d '%s' http://smd:27779/hsm/v2/Inventory/RedfishEndpoints",
		jsonPayload)

	req := testcontainers.ContainerRequest{
		Image:      "library/golang:1.24-alpine",
		Networks:   []string{network},
		Cmd:        []string{"sh", "-c", curlCmd},
		WaitingFor: wait.ForExit().WithExitTimeout(60 * time.Second),
	}

	_, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	return err
}

// startSSHPasswordServer starts an SSH server with password authentication
func startSSHPasswordServer(ctx context.Context, network string, alias string, username string, password string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:    "linuxserver/openssh-server:latest",
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
			"LISTEN_PORT":       "22",
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
		Networks: []string{network},
		NetworkAliases: map[string][]string{
			network: {alias},
		},
		Env: map[string]string{
			"PUID":       "1000",
			"PGID":       "1000",
			"USER_NAME":  username,
			"PUBLIC_KEY": publicKey,
		},
		ExposedPorts: []string{"2222/tcp"},
		WaitingFor:   wait.ForLog("done.").WithStartupTimeout(60 * time.Second),
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

// startIPMIServer starts an IPMI server
func startIPMIServer(ctx context.Context, network string, alias string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    "../ipmi_sim",
			Dockerfile: "Dockerfile",
		},
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

// startRemoteConsole starts the remote-console service
func startRemoteConsole(ctx context.Context, networks ...string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    "..",
			Dockerfile: "Dockerfile",
		},
		Networks: networks,
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
			"RCS_SMD_URL":			   "http://smd:27779",
			"SMS_SERVER":           "http://smd:27779",
			"CRAY_VAULT_AUTH_PATH": "auth/token/create",
			"CRAY_VAULT_ROLE_FILE": "/app/configs/namespace",
			"CRAY_VAULT_JWT_FILE":  "/app/configs/token",
			"VAULT_ADDR":           "http://vault:8200",
			"VAULT_TOKEN":          "hms",
			"VAULT_BASE_PATH":      "hms-creds",
			"VAULT_SKIP_VERIFY":    "true",
			"VAULT_ENABLED":        "true",
			"LOG_LEVEL":            "DEBUG",
			"RCS_CONMAN_PID_FILE_PATH":    "/app/remote-console.pid",
			"RCS_CONMAN_LOGS_PATH":    "/tmp",

		},
		ExposedPorts: []string{"26776/tcp"},
		WaitingFor:   wait.ForLog("Listening on port 7890").WithStartupTimeout(120 * time.Second),
	}

	for _, network := range networks {
		req.NetworkAliases[network] = []string{"remote-console"}
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

// getDefaultSSHKeyPath returns the default SSH key path for testing
func getDefaultSSHKeyPath() string {
	return filepath.Join(os.Getenv("HOME"), ".ssh", "console-test")
}
