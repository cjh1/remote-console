// MIT License
// (C) Copyright 2025 Hewlett Packard Enterprise Development LP
//
// This file contains the interfaces and dependency injection points for conman management.

package conman

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"syscall"
	"text/template"
	"time"

	"github.com/Cray-HPE/hms-compcredentials"

	"github.com/OpenCHAMI/remote-console/internal/nodes"
)

type conmanService struct {
	config  ConmanConfig
	mutex   sync.Mutex
	command *exec.Cmd
}

func NewConmanService(config ConmanConfig) *conmanService {
	return &conmanService{
		config:  config,
		mutex:   sync.Mutex{},
		command: nil,
	}
}

func (cs *conmanService) ConfigureConman(nodeMap map[string]*nodes.NodeConsoleInfo, passwords map[string]compcredentials.CompCredentials, sshConsoleKeyPath string) (bool, error) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()

	return cs.updateConfigFile(nodeMap, passwords, sshConsoleKeyPath, true)
}

func generateBaseConfig(config ConmanConfig) ([]byte, error) {

	// Read template file
	slog.Debug("Opening base configuration file", "path", config.BaseConfFilePath)
	tmplContent, err := os.ReadFile(config.BaseConfFilePath)
	if err != nil {
		return nil, fmt.Errorf("error opening base config template: %w", err)
	}

	// Parse and execute template
	tmpl, err := template.New("conman").Parse(string(tmplContent))
	if err != nil {
		return nil, fmt.Errorf("error templating base config: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, config); err != nil {
		return nil, fmt.Errorf("error templating base config: %w", err)
	}

	return buf.Bytes(), nil
}

func (cs *conmanService) updateConfigFile(nodeMap map[string]*nodes.NodeConsoleInfo, passwords map[string]compcredentials.CompCredentials, sshConsoleKeyPath string, forceUpdate bool) (bool, error) {
	slog.Info("Updating conman configuration file")

	bs, err := generateBaseConfig(cs.config)
	if err != nil {
		return false, fmt.Errorf("Unable to template base config file: %w", err)
	}

	if !forceUpdate && !willUpdateConfig(bs) {
		slog.Debug("Skipping update due to base config file flag")
		return false, nil
	}

	slog.Debug("Opening conman configuration file for output", "path", cs.config.ConfFilePath)
	cf, err := os.OpenFile(cs.config.ConfFilePath, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return false, fmt.Errorf("Unable to open config file to write: %w", err)
	}
	defer cf.Close()

	_, err = cf.Write(bs)
	if err != nil {
		return false, fmt.Errorf("Unable to write base config into file: %w", err)
	}

	slog.Info("Populating conman configuration with nodes", "nodeCount", len(nodeMap))

	consoles := make([]string, 0, len(nodeMap))

	for _, nci := range nodeMap {
		// IPMI connection
		if nci.ConnectionType == nodes.IPMI {
			creds, ok := passwords[nci.ID]
			if !ok {
				slog.Warn("No credentials found for node", "nodeID", nci.ID)
			}

			slog.Debug("Configuring IPMI console", "nodeID", nci.ID, "host", nci.ConnectionHost, "username", creds.Username)
			output := fmt.Sprintf("console name=\"%s\" dev=\"ipmi:%s\" ipmiopts=\"U:%s,P:%s,W:solpayloadsize\"\n",
				nci.ID, nci.ConnectionHost, creds.Username, creds.Password)
			consoles = append(consoles, output)

			// SSH connection
		} else if nci.ConnectionType == nodes.SSH {
			creds, ok := passwords[nci.ID]
			if !ok {
				slog.Warn("No credentials found for node", "nodeID", nci.ID)
			}

			// If we have password creds, use those, otherwise use key-based
			if creds.Password != "" {
				slog.Debug("Configuring SSH console with password", "nodeID", nci.ID, "host", nci.ConnectionHost, "port", nci.ConnectionPort, "username", creds.Username)
				output := fmt.Sprintf("console name=\"%s\" dev=\"%s/ssh-pwd-console %s %d %s %s\"\n",
					nci.ID, cs.config.ConsoleScriptsPath, nci.ConnectionHost, nci.ConnectionPort, creds.Username, creds.Password)
				consoles = append(consoles, output)
			} else {
				// Key based auth, note that we still use the username from the secure store
				slog.Debug("Configuring SSH console with key", "nodeID", nci.ID, "host", nci.ConnectionHost, "port", nci.ConnectionPort, "username", creds.Username, "keyPath", sshConsoleKeyPath)
				output := fmt.Sprintf("console name=\"%s\" dev=\"%s/ssh-key-console %s %d %s %s\"\n",
					nci.ID, cs.config.ConsoleScriptsPath, nci.ConnectionHost, nci.ConnectionPort, creds.Username, sshConsoleKeyPath)
				consoles = append(consoles, output)
			}
		}
	}

	// Sort consoles for consistent output
	sort.Strings(consoles)
	for _, output := range consoles {
		if _, err = cf.WriteString(output); err != nil {
			return false, fmt.Errorf("Unable to write console entry into file: %w", err)
		}
	}

	return len(nodeMap) > 0, nil
}

func willUpdateConfig(baseConfig []byte) bool {
	buff := make([]byte, 50)
	n := len(baseConfig)
	if n < 50 {
		slog.Warn("Base configuration truncated")
		return false
	}

	s := string(buff[:n])
	retVal := false
	ss := "UPDATE_CONFIG="
	pos := strings.Index(s, ss)
	if pos > 0 {
		valPos := pos + len(ss)
		retVal = s[valPos] != 'F' && s[valPos] != 'f'
	}

	return retVal
}

// SignalConmanTERM sends SIGTERM to running conmand process
func (cs *conmanService) SignalConmanTERM() {
	if cs.command != nil {
		slog.Info("Signaling conman with SIGTERM")
		cs.command.Process.Signal(syscall.SIGTERM)
	} else {
		slog.Warn("Attempting to signal conman process when nil")
	}
}

// SignalConmanHUP sends SIGHUP to running conmand process
func (cs *conmanService) SignalConmanHUP() {
	if cs.command != nil {
		slog.Info("Signaling conman with SIGHUP")
		cs.command.Process.Signal(syscall.SIGHUP)
	} else {
		slog.Warn("Attempting to signal conman process when nil")
	}
}

// LogPipeOutput takes the output of a pipe and logs it
func logPipeOutput(readPipe *io.ReadCloser, desc string) {
	slog.Debug("Starting conmand pipe logging", "pipe", desc)
	er := bufio.NewReader(*readPipe)
	for {
		// read the next line
		line, err := er.ReadString('\n')
		if err != nil {
			slog.Debug("Ending pipe logging", "pipe", desc, "error", err)
			break
		}
		slog.Debug("conmand output", "pipe", desc, "output", line)
	}
}

func (cs *conmanService) ExecuteConman() error {
	slog.Info("Starting new instance of conmand")
	if cs.command != nil {
		return fmt.Errorf("command not nil on entry to executeConman!!")
	}
	cs.command = exec.Command("conmand", "-F", "-v", "-c", cs.config.ConfFilePath)
	cmdStdErr, err := cs.command.StderrPipe()
	if err != nil {
		return fmt.Errorf("Unable to connect to conmand stderr pipe: %w", err)
	}
	cmdStdOut, err := cs.command.StdoutPipe()
	if err != nil {
		return fmt.Errorf("Unable to connect to conmand stdout pipe: %w", err)
	}
	go logPipeOutput(&cmdStdErr, "stderr")
	go logPipeOutput(&cmdStdOut, "stdout")
	slog.Info("Starting conmand process")
	if err = cs.command.Start(); err != nil {
		return fmt.Errorf("Unable to start the command: %w", err)
	}
	if err = cs.command.Wait(); err != nil {
		slog.Error("Conmand process exited with error", "error", err)
		time.Sleep(15 * time.Second)
	}
	cs.command = nil
	slog.Info("Conmand process has exited")

	return nil
}
