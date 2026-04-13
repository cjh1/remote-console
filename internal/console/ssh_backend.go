// Copyright © 2026 OpenCHAMI a Series of LF Projects, LLC
//
// SPDX-License-Identifier: MIT

package console

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/OpenCHAMI/remote-console/internal/ssh"
)

// clientIDCounter generates unique client IDs within a process.
var clientIDCounter atomic.Uint64

func generateClientID() string {
	return strconv.FormatUint(clientIDCounter.Add(1), 10)
}

// sshConsoleIO implements consoleIO backed by an SSHConsoleNode attachment.
type sshConsoleIO struct {
	nodeID    string
	clientID  string
	out       chan []byte
	manager   *ssh.SSHConsoleManager
	closeOnce sync.Once
}

func newSSHConsoleIO(nodeID string, manager *ssh.SSHConsoleManager) (*sshConsoleIO, error) {
	clientID := generateClientID()
	ch, err := manager.Attach(nodeID, clientID)
	if err != nil {
		return nil, fmt.Errorf("attach SSH console %s: %w", nodeID, err)
	}
	return &sshConsoleIO{
		nodeID:   nodeID,
		clientID: clientID,
		out:      ch,
		manager:  manager,
	}, nil
}

func (s *sshConsoleIO) Output() <-chan []byte { return s.out }

func (s *sshConsoleIO) Write(p []byte) (int, error) {
	return s.manager.Write(s.nodeID, p)
}

func (s *sshConsoleIO) Close() error {
	s.closeOnce.Do(func() {
		s.manager.Detach(s.nodeID, s.clientID)
	})
	return nil
}
