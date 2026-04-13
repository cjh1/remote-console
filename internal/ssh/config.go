// Copyright © 2026 OpenCHAMI a Series of LF Projects, LLC
//
// SPDX-License-Identifier: MIT

package ssh

import "time"

// SSHConfig holds configuration for the SSH console manager.
type SSHConfig struct {
	TerminalType         string        `desc:"Terminal type for SSH PTY requests"`
	TCPKeepAlive         time.Duration `desc:"TCP keepalive interval for SSH connections (0 disables)"`
	ReconnectMinInterval time.Duration `desc:"Minimum interval between SSH reconnect attempts"`
	ReconnectMaxInterval time.Duration `desc:"Maximum interval between SSH reconnect attempts"`
}

func DefaultSSHConfig() SSHConfig {
	return SSHConfig{
		TerminalType:         "xterm-256color",
		TCPKeepAlive:         180 * time.Second,
		ReconnectMinInterval: 1 * time.Second,
		ReconnectMaxInterval: 30 * time.Second,
	}
}
