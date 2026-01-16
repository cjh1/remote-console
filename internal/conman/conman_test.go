package conman

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Cray-HPE/hms-compcredentials"
	"github.com/stretchr/testify/require"

	"github.com/OpenCHAMI/remote-console/internal/nodes"
)

func TestGenerateBaseConfig(t *testing.T) {
	baseDir := "/tmp/conman_test"

	config := DefaultConmanConfig()
	config.BaseConfFilePath = "../../scripts/conman.conf.tmpl"
	config.ConfFilePath = filepath.Join(baseDir, "conman.conf")
	config.LogsPath = filepath.Join(baseDir, "logs")
	config.PidFilePath = filepath.Join(baseDir, "conman.pid")

	baseConfig, err := generateBaseConfig(config)
	require.NoError(t, err)
	require.NotEmpty(t, baseConfig)

	expected := `# UPDATE_CONFIG=TRUE
SERVER keepalive=ON
SERVER logdir="/tmp/conman_test/logs"
SERVER logfile="conman.log"
SERVER loopback=ON
SERVER pidfile="/tmp/conman_test/conman.pid"
SERVER resetcmd="powerman -0 %N; sleep 3; powerman -1 %N"
SERVER timestamp=1h
GLOBAL seropts="115200,8n1"
GLOBAL log="conman/console.%N"
GLOBAL logopts="sanitize,timestamp"
`

	require.Equal(t, []byte(expected), baseConfig)
}

func TestConfigureConman(t *testing.T) {
	tempDir := t.TempDir()

	config := DefaultConmanConfig()
	config.BaseConfFilePath = "../../scripts/conman.conf.tmpl"
	config.ConfFilePath = filepath.Join(tempDir, "conman.conf")
	config.LogsPath = filepath.Join(tempDir, "logs")
	config.PidFilePath = filepath.Join(tempDir, "conman.pid")

	nodes := map[string]*nodes.NodeConsoleInfo{
		"x0c0s1b0": {
			ID:             "x0c0s1b0",
			ConnectionType: nodes.IPMI,
			ConnectionHost: "x0c0s1b0",
		},
		"x0c0s2b0": {
			ID:             "x0c0s2b0",
			ConnectionType: nodes.SSH,
			ConnectionHost: "x0c0s2b0",
			ConnectionPort: 2222,
		},
		"x0c0s3b0": {
			ID:             "x0c0s3b0",
			ConnectionType: nodes.SSH,
			ConnectionHost: "x0c0s3b0",
		},
	}

	passwords := map[string]compcredentials.CompCredentials{
		"x0c0s1b0": {
			Username: "admin",
			Password: "password1",
		},
		"x0c0s2b0": {
			Username: "admin",
			Password: "",
		},
		"x0c0s3b0": {
			Username: "admin",
			Password: "password3",
		},
	}
	service := NewConmanService(config)

	// First call should create the config file
	updated, err := service.ConfigureConman(nodes, passwords, "/tmp/ssh_console_key")
	require.NoError(t, err)
	require.True(t, updated)

	// Read the generated config file
	generatedConfig, err := os.ReadFile(config.ConfFilePath)
	require.NoError(t, err)
	require.NotEmpty(t, generatedConfig)

	expected := `# UPDATE_CONFIG=TRUE
SERVER keepalive=ON
SERVER logdir="/logs"
SERVER logfile="conman.log"
SERVER loopback=ON
SERVER pidfile="/conman.pid"
SERVER resetcmd="powerman -0 %N; sleep 3; powerman -1 %N"
SERVER timestamp=1h
GLOBAL seropts="115200,8n1"
GLOBAL log="conman/console.%N"
GLOBAL logopts="sanitize,timestamp"
console name="x0c0s1b0" dev="ipmi:x0c0s1b0" ipmiopts="U:admin,P:password1,W:solpayloadsize"
console name="x0c0s2b0" dev="/usr/bin/ssh-key-console x0c0s2b0 2222 admin /tmp/ssh_console_key"
console name="x0c0s3b0" dev="/usr/bin/ssh-pwd-console x0c0s3b0 0 admin password3"
`
	// Remove temporary directory path from generated config for comparison
	generatedConfigStr := string(generatedConfig)
	generatedConfigStr = strings.ReplaceAll(generatedConfigStr, tempDir, "")

	require.Equal(t, expected, generatedConfigStr)
}
