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
	"log"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"syscall"
	"text/template"
	"time"

	"github.com/Cray-HPE/hms-compcredentials"

	"github.com/OpenCHAMI/remote-console/internal/types"
)

type ConmanService interface {
	ConfigureConman(nodes map[string]*types.NodeConsoleInfo, passwords map[string]compcredentials.CompCredentials, sshConsoleKeyPath string) (bool, error)
	ExecuteConman() error
	SignalConmanTERM()
	SignalConmanHUP()
}

type conmanService struct {
	config  ConmanConfig
	mutex   sync.Mutex
	command *exec.Cmd
}

type ConmanConfig struct {
	BaseConfFilePath   string `desc:"Path to the base conman configuration template file."`
	ConfFilePath       string `desc:"Path to the generated conman configuration file."`
	LogsPath           string `desc:"Path to conman log files."`
	PidFilePath        string `desc:"Path to the conman PID file."`
	ConsoleScriptsPath string `desc:"Path to console helper scripts."`
}

func DefaultConmanConfig() ConmanConfig {
	return ConmanConfig{
		BaseConfFilePath:   "/app/conman_base.conf.tmpl",
		ConfFilePath:       "/etc/conman.conf",
		LogsPath:           "/var/log/conman",
		PidFilePath:        "/var/run/conman.pid",
		ConsoleScriptsPath: "/usr/bin",
	}
}

func NewConmanService(config ConmanConfig) ConmanService {
	return &conmanService{
		config:  config,
		mutex:   sync.Mutex{},
		command: nil,
	}
}

func (cs *conmanService) ConfigureConman(nodes map[string]*types.NodeConsoleInfo, passwords map[string]compcredentials.CompCredentials, sshConsoleKeyPath string) (bool, error) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()

	return cs.updateConfigFile(nodes, passwords, sshConsoleKeyPath, true)
}

func generateBaseConfig(config ConmanConfig) ([]byte, error) {

	// Read template file
	log.Printf("Opening base configuration file: %s", config.BaseConfFilePath)
	tmplContent, err := os.ReadFile(config.BaseConfFilePath)
	if err != nil {
		return nil, fmt.Errorf("error opening base config template: %s", err)
	}

	// Parse and execute template
	tmpl, err := template.New("conman").Parse(string(tmplContent))
	if err != nil {
		return nil, fmt.Errorf("error templating base config: %s", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, config); err != nil {
		return nil, fmt.Errorf("error templating base config: %s", err)
	}

	return buf.Bytes(), nil
}

func (cs *conmanService) updateConfigFile(nodes map[string]*types.NodeConsoleInfo, passwords map[string]compcredentials.CompCredentials, sshConsoleKeyPath string, forceUpdate bool) (bool, error) {
	log.Print("Updating the configuration file")

	bs, err := generateBaseConfig(cs.config)
	if err != nil {
		return false, fmt.Errorf("Unable to template base config file: %v", err)
	}

	if !forceUpdate && !willUpdateConfig(bs) {
		log.Print("Skipping update due to base config file flag")
		return false, nil
	}

	log.Printf("Opening conman configuration file for output: %s", cs.config.ConfFilePath)
	cf, err := os.OpenFile(cs.config.ConfFilePath, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return false, fmt.Errorf("Unable to open config file to write: %v", err)
	}
	defer cf.Close()

	_, err = cf.Write(bs)
	if err != nil {
		return false, fmt.Errorf("Unable to write base config into file: %v", err)
	}

	log.Printf("Getting current nodes to populate conman configuration")

	consoles := make([]string, 0, len(nodes))

	for _, nci := range nodes {
		// IPMI connection
		if nci.ConnectionType == types.IPMI {
			creds, ok := passwords[nci.ID]
			if !ok {
				log.Printf("No creds record returned for %s", nci.ID)
			}

			log.Printf("console name=\"%s\" dev=\"ipmi:%s\" ipmiopts=\"U:%s,P:REDACTED,W:solpayloadsize\"\n",
				nci.ID, nci.ConnectionHost, creds.Username)
			output := fmt.Sprintf("console name=\"%s\" dev=\"ipmi:%s\" ipmiopts=\"U:%s,P:%s,W:solpayloadsize\"\n",
				nci.ID, nci.ConnectionHost, creds.Username, creds.Password)
			consoles = append(consoles, output)

		// SSH connection
		} else if nci.ConnectionType == types.SSH {
			creds, ok := passwords[nci.ID]
			if !ok {
				log.Printf("No creds record returned for %s", nci.ID)
			}

			// If we have password creds, use those, otherwise use key-based
			if creds.Password != "" {
				log.Printf("console name=\"%s\" dev=\"%s/ssh-pwd-console %s %d %s REDACTED\"\n",
					nci.ID, cs.config.ConsoleScriptsPath, nci.ConnectionHost, nci.ConnectionPort, creds.Username)
				output := fmt.Sprintf("console name=\"%s\" dev=\"%s/ssh-pwd-console %s %d %s %s\"\n",
					nci.ID, cs.config.ConsoleScriptsPath, nci.ConnectionHost, nci.ConnectionPort, creds.Username, creds.Password)
				consoles = append(consoles, output)
			} else {
				// Key based auth, note that we still use the username from the secure store
				log.Printf("console name=\"%s\" dev=\"%s/ssh-key-console %s %d %s %s\"\n",
					nci.ID, cs.config.ConsoleScriptsPath, nci.ConnectionHost, nci.ConnectionPort, creds.Username, sshConsoleKeyPath)
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
			return false, fmt.Errorf("Unable to write console entry into file: %v", err)
		}
	}

	return len(nodes) > 0, nil
}

func willUpdateConfig(baseConfig []byte) bool {
	buff := make([]byte, 50)
	n := len(baseConfig)
	if n < 50 {
		log.Printf("Base configuration truncated")
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
		log.Print("Signaling conman with SIGTERM")
		cs.command.Process.Signal(syscall.SIGTERM)
	} else {
		log.Print("Warning: Attempting to signal conman process when nil.")
	}
}

// SignalConmanHUP sends SIGHUP to running conmand process
func (cs *conmanService) SignalConmanHUP() {
	if cs.command != nil {
		log.Print("Signaling conman with SIGHUP")
		cs.command.Process.Signal(syscall.SIGHUP)
	} else {
		log.Print("Warning: Attempting to signal conman process when nil.")
	}
}

// LogPipeOutput takes the output of a pipe and logs it
func logPipeOutput(readPipe *io.ReadCloser, desc string) {
	log.Printf("Starting log of conmand %s output", desc)
	er := bufio.NewReader(*readPipe)
	for {
		// read the next line
		line, err := er.ReadString('\n')
		if err != nil {
			log.Printf("Ending %s logging from error:%s", desc, err)
			break
		}
		log.Print(line)
	}
}

func (cs *conmanService) ExecuteConman() error {
	log.Print("Starting a new instance of conmand")
	if cs.command != nil {
		return fmt.Errorf("command not nil on entry to executeConman!!")
	}
	cs.command = exec.Command("conmand", "-F", "-v", "-c", cs.config.ConfFilePath)
	cmdStdErr, err := cs.command.StderrPipe()
	if err != nil {
		return fmt.Errorf("Unable to connect to conmand stderr pipe: %s", err)
	}
	cmdStdOut, err := cs.command.StdoutPipe()
	if err != nil {
		return fmt.Errorf("Unable to connect to conmand stdout pipe: %s", err)
	}
	go logPipeOutput(&cmdStdErr, "stderr")
	go logPipeOutput(&cmdStdOut, "stdout")
	log.Print("Starting conmand process")
	if err = cs.command.Start(); err != nil {
		return fmt.Errorf("Unable to start the command: %s", err)
	}
	if err = cs.command.Wait(); err != nil {
		log.Printf("Error from command wait: %s", err)
		time.Sleep(15 * time.Second)
	}
	cs.command = nil
	log.Print("Conmand process has exited")

	return nil
}