package test

import (
	"fmt"
	"io"
)

type consoleFixture struct {
	name           string
	nodeID         string
	containerKey   string
	readyLogMarker string
	prompt         string
	broadcastCmd   func(msg string) []string
}

var consoleFixtures = []consoleFixture{
	{
		name:           "ssh-password",
		nodeID:         "x0c0s0b0",
		containerKey:   "ssh-password",
		readyLogMarker: "Welcome to OpenSSH Server",
		prompt:         ":~$ ",
		broadcastCmd: func(msg string) []string {
			return []string{"broadcast.sh", msg}
		},
	},
	{
		name:           "ssh-key",
		nodeID:         "x0c0s1b0",
		containerKey:   "ssh-key",
		readyLogMarker: "Welcome to OpenSSH Server",
		prompt:         ":~$ ",
		broadcastCmd: func(msg string) []string {
			return []string{"broadcast.sh", msg}
		},
	},
	{
		name:           "ipmi",
		nodeID:         "x0c0s2b0",
		containerKey:   "ipmi",
		readyLogMarker: "<ConMan> Console [x0c0s2b0] connected",
		prompt:         "",
		broadcastCmd: func(msg string) []string {
			return []string{"sh", "-c", fmt.Sprintf("printf \"echo %s\n\" > /dev/vtty", msg)}
		},
	},
}

func (s *IntegrationTestSuite) broadcastConsoleMessage(f consoleFixture, msg string) (int, string, error) {
	container, ok := s.containers[f.containerKey]
	if !ok {
		return 0, "", fmt.Errorf("container %s not found", f.containerKey)
	}
	if f.broadcastCmd == nil {
		return 0, "", fmt.Errorf("fixture %s does not support broadcasting", f.name)
	}
	cmd := f.broadcastCmd(msg)
	if len(cmd) == 0 {
		return 0, "", fmt.Errorf("fixture %s does not support broadcasting", f.name)
	}
	exitCode, reader, err := container.Exec(s.ctx, cmd)
	if err != nil {
		return exitCode, "", err
	}
	data, readErr := io.ReadAll(reader)
	if readErr != nil {
		return exitCode, "", fmt.Errorf("read broadcast output: %w", readErr)
	}
	return exitCode, string(data), nil
}
