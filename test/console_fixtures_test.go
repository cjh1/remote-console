package test

import (
	"context"
	"fmt"
	"io"
	"sort"
)

// consoleFixture defines the parameters for a console test fixture
type consoleFixture struct {
	name           string
	nodeID         string
	containerKey   string
	username       string
	password       string
	readyLogMarker string
	prompt         string
	broadcastCmd   func(msg string) []string
}

// consoleFixtures defines the console test fixtures
var consoleFixtures = map[string]consoleFixture{
	// SSH console accessed via password authentication
	"ssh-password": {
		name:           "ssh-password",
		nodeID:         "x0c0s0b0",
		containerKey:   "ssh-password",
		username:       "ADMIN",
		password:       "ADMIN",
		readyLogMarker: "Welcome to OpenSSH Server",
		prompt:         ":~$ ",
		broadcastCmd: func(msg string) []string {
			return []string{"broadcast.sh", msg}
		},
	},
	// SSH console accessed via key-based authentication
	"ssh-key": {
		name:           "ssh-key",
		nodeID:         "x0c0s1b0",
		containerKey:   "ssh-key",
		username:       "ADMIN",
		password:       "",
		readyLogMarker: "Welcome to OpenSSH Server",
		prompt:         ":~$ ",
		broadcastCmd: func(msg string) []string {
			return []string{"broadcast.sh", msg}
		},
	},
	// IPMI console accessed via serial over LAN
	"ipmi": {
		name:           "ipmi",
		nodeID:         "x0c0s2b0",
		containerKey:   "ipmi",
		username:       "root",
		password:       "root_password",
		readyLogMarker: "<ConMan> Console [x0c0s2b0] connected",
		prompt:         "/ # ",
		broadcastCmd: func(msg string) []string {
			return []string{"sh", "-c", fmt.Sprintf("printf \"echo %s\n\" > /dev/vtty", msg)}
		},
	},
}

// consoleFixtureList returns a sorted list of console fixtures
func consoleFixtureList() []consoleFixture {
	consoles := make([]consoleFixture, 0, len(consoleFixtures))
	for _, f := range consoleFixtures {
		consoles = append(consoles, f)
	}
	// sort to make iteration deterministic
	sort.Slice(consoles, func(i, j int) bool {
		return consoles[i].name < consoles[j].name
	})
	return consoles
}

// broadcastConsoleMessage broadcasts a message to all logged-in users' TTYs in the specified console fixture
func (s *IntegrationTestSuite) broadcastConsoleMessage(f consoleFixture, msg string) (int, string, error) {
	container, ok := s.containers[f.containerKey]
	if !ok {
		return 0, "", fmt.Errorf("container %s not found", f.containerKey)
	}
	if f.broadcastCmd == nil {
		return 0, "", fmt.Errorf("console %s does not support broadcasting", f.name)
	}
	cmd := f.broadcastCmd(msg)
	if len(cmd) == 0 {
		return 0, "", fmt.Errorf("console %s does not support broadcasting", f.name)
	}
	exitCode, reader, err := container.Exec(context.Background(), cmd)
	if err != nil {
		return exitCode, "", err
	}
	data, readErr := io.ReadAll(reader)
	if readErr != nil {
		return exitCode, "", fmt.Errorf("read broadcast output: %w", readErr)
	}
	return exitCode, string(data), nil
}
