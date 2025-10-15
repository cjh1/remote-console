// MIT License
// (C) Copyright 2025 Hewlett Packard Enterprise Development LP
//
// This file contains the interfaces and dependency injection points for conman management.

package conman

import (
	"bufio"
	"bytes"
	"encoding/base64"
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

type ConmanService struct {
	config  ConmanConfig
	mutex   sync.Mutex
	command *exec.Cmd
}

func NewConmanService(config ConmanConfig) *ConmanService {
	return &ConmanService{
		config:  config,
		mutex:   sync.Mutex{},
		command: nil,
	}
}

func (cs *ConmanService) ConfigureConman(nodeMap map[string]*nodes.NodeConsoleInfo, passwords map[string]compcredentials.CompCredentials, sshConsoleKeyPath string) (bool, error) {
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

func willUpdateConfig(baseConfig []byte) bool {
	// if the first line of the base configuration file has '# UPDATE_CONFIG=FALSE'
	// then bail on the update
	// NOTE: only reading first 50 bytes of file, should be at least that many
	//  present if this is a valid base configuration file and don't need to read more.
	const key = "UPDATE_CONFIG="
	configStr := string(baseConfig)

	keyPosition := strings.Index(configStr, key)
	if keyPosition == -1 {
		return false
	}

	valuePosition := keyPosition + len(key)
	if valuePosition >= len(configStr) {
		slog.Warn("Base configuration missing UPDATE_CONFIG value")
		return false
	}

	value := configStr[valuePosition]
	return value != 'F' && value != 'f'
}

func generateIPMIConsoleConfig(nci *nodes.NodeConsoleInfo, creds compcredentials.CompCredentials) string {
	slog.Debug("Configuring IPMI console", "nodeID", nci.ID, "host", nci.ConnectionHost, "username", creds.Username)
	return fmt.Sprintf("console name=\"%s\" dev=\"ipmi:%s\" ipmiopts=\"U:%s,P:%s,W:solpayloadsize\"\n",
		nci.ID, nci.ConnectionHost, creds.Username, creds.Password)
}

func (cs *ConmanService) generateSSHConsoleConfig(nci *nodes.NodeConsoleInfo, creds compcredentials.CompCredentials, sshConsoleKeyPath string) string {
	var devArgs string

	// If we have password creds, use those, otherwise use key-based.
	if creds.Password != "" {
		slog.Debug("Configuring SSH console with password", "nodeID", nci.ID, "host", nci.ConnectionHost, "port", nci.ConnectionPort, "username", creds.Username, "entryCmd", nci.ConsoleEntryCommand)
		devArgs = fmt.Sprintf("%s/ssh-pwd-console %s %d %s %s", cs.config.ConsoleScriptsPath, nci.ConnectionHost, nci.ConnectionPort, creds.Username, creds.Password)
	} else {
		// Key based auth, note that we still use the username from the secure store.
		slog.Debug("Configuring SSH console with key", "nodeID", nci.ID, "host", nci.ConnectionHost, "port", nci.ConnectionPort, "username", creds.Username, "keyPath", sshConsoleKeyPath, "entryCmd", nci.ConsoleEntryCommand)
		devArgs = fmt.Sprintf("%s/ssh-key-console %s %d %s %s", cs.config.ConsoleScriptsPath, nci.ConnectionHost, nci.ConnectionPort, creds.Username, sshConsoleKeyPath)
	}

	if nci.ConsoleEntryCommand != "" {
		// Encode the entry command in base64 to avoid issues with special characters, conman can't handle escaping quotes.
		base64EncodedCmd := base64.StdEncoding.EncodeToString([]byte(nci.ConsoleEntryCommand))
		devArgs = fmt.Sprintf("%s %s", devArgs, base64EncodedCmd)
	}

	return fmt.Sprintf("console name=\"%s\" dev=\"%s\"\n", nci.ID, devArgs)
}

func (cs *ConmanService) updateConfigFile(nodeMap map[string]*nodes.NodeConsoleInfo, passwords map[string]compcredentials.CompCredentials, sshConsoleKeyPath string, forceUpdate bool) (bool, error) {
	slog.Info("Updating conman configuration file")

	bs, err := generateBaseConfig(cs.config)
	if err != nil {
		return false, fmt.Errorf("unable to template base config file: %w", err)
	}

	if !forceUpdate && !willUpdateConfig(bs) {
		slog.Debug("Skipping update due to base config file flag")
		return false, nil
	}

	slog.Debug("Opening conman configuration file for output", "path", cs.config.ConfFilePath)
	cf, err := os.OpenFile(cs.config.ConfFilePath, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return false, fmt.Errorf("unable to open config file to write: %w", err)
	}
	defer func() {
		if err := cf.Close(); err != nil {
			slog.Warn("Failed to close conman config file", "path", cs.config.ConfFilePath, "error", err)
		}
	}()

	_, err = cf.Write(bs)
	if err != nil {
		return false, fmt.Errorf("unable to write base config into file: %w", err)
	}

	slog.Info("Populating conman configuration with nodes", "nodeCount", len(nodeMap))

	consoles := make([]string, 0, len(nodeMap))

	for _, nci := range nodeMap {
		creds, ok := passwords[nci.ID]
		if !ok {
			slog.Warn("No credentials found for node", "nodeID", nci.ID)
		}

		switch nci.ConnectionType {
		// IPMI connection
		case nodes.IPMI:
			output := generateIPMIConsoleConfig(nci, creds)
			consoles = append(consoles, output)

		// SSH connection
		case nodes.SSH:
			output := cs.generateSSHConsoleConfig(nci, creds, sshConsoleKeyPath)
			consoles = append(consoles, output)
		}
	}

	// Sort consoles for consistent output
	sort.Strings(consoles)
	for _, output := range consoles {
		if _, err = cf.WriteString(output); err != nil {
			return false, fmt.Errorf("unable to write console entry into file: %w", err)
		}
	}

	return len(nodeMap) > 0, nil
}

// SignalConmanTERM sends SIGTERM to running conmand process
func (cs *ConmanService) SignalConmanTERM() error {
	if cs.command != nil {
		slog.Info("Signaling conman with SIGTERM")
		if err := cs.command.Process.Signal(syscall.SIGTERM); err != nil {
			return fmt.Errorf("failed to signal conman with SIGTERM: %w", err)
		}
	} else {
		slog.Warn("Attempting to signal conman process when nil")
	}
		
	return nil
}

// SignalConmanHUP sends SIGHUP to running conmand process
func (cs *ConmanService) SignalConmanHUP() error {
	if cs.command != nil {
		slog.Info("Signaling conman with SIGHUP")
		if err := cs.command.Process.Signal(syscall.SIGHUP); err != nil {
			return fmt.Errorf("failed to signal conman with SIGHUP: %w", err)
		}
		return nil
	} else {
		slog.Warn("Attempting to signal conman process when nil")
	}

	return nil
}

// logPipeOutput takes the output of a pipe and logs it
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

// ExecuteConman starts conmand and waits for it to exit
func (cs *ConmanService) ExecuteConman() error {
	slog.Info("Starting new instance of conmand")
	if cs.command != nil {
		return fmt.Errorf("command not nil on entry to executeConman")
	}
	cs.command = exec.Command("conmand", "-F", "-v", "-c", cs.config.ConfFilePath)
	cmdStdErr, err := cs.command.StderrPipe()
	if err != nil {
		return fmt.Errorf("unable to connect to conmand stderr pipe: %w", err)
	}
	cmdStdOut, err := cs.command.StdoutPipe()
	if err != nil {
		return fmt.Errorf("unable to connect to conmand stdout pipe: %w", err)
	}
	go logPipeOutput(&cmdStdErr, "stderr")
	go logPipeOutput(&cmdStdOut, "stdout")

	slog.Info("Starting conmand process")
	if err = cs.command.Start(); err != nil {
		return fmt.Errorf("unable to start command: %w", err)
	}

	if err = cs.command.Wait(); err != nil {
		slog.Error("Conmand process exited with error", "error", err)
		time.Sleep(15 * time.Second)
	}

	cs.command = nil
	slog.Info("Conmand process has exited")

	return nil
}
