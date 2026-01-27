//  MIT License
//
//  (C) Copyright 2021-2023 Hewlett Packard Enterprise Development LP
//
//  Permission is hereby granted, free of charge, to any person obtaining a
//  copy of this software and associated documentation files (the "Software"),
//  to deal in the Software without restriction, including without limitation
//  the rights to use, copy, modify, merge, publish, distribute, sublicense,
//  and/or sell copies of the Software, and to permit persons to whom the
//  Software is furnished to do so, subject to the following conditions:
//
//  The above copyright notice and this permission notice shall be included
//  in all copies or substantial portions of the Software.
//
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
//  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
//  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
//  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
//  OTHER DEALINGS IN THE SOFTWARE.
//

// This file contains the code needed to handle log rotation inside the console pod.

package logs

import (
	"bufio"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/OpenCHAMI/remote-console/internal/nodes"
)

// LogRotate initializes and starts log rotation
func (ls *LogsService) initLogRotate() error {
	slog.Debug("Setting up log rotation")
	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	// Set up the 'backups' directory for logrotation to use
	slog.Info("Ensuring console log backup directory present", "path", ls.config.ConsoleLogsBackupPath)
	err := os.MkdirAll(ls.config.ConsoleLogsBackupPath, 0755)
	if err != nil {
		return fmt.Errorf("error ensuring console logs backup directory: %w", err)
	}

	return nil
}

func writeConfigEntry(lrf *os.File, fileName string, oldDir string, numRotate int, fileSize string) error {
	var b strings.Builder
	fmt.Fprintf(&b, "%s { \n", fileName)
	fmt.Fprintln(&b, "  nocompress")
	fmt.Fprintln(&b, "  missingok")
	fmt.Fprintln(&b, "  nocopytruncate")
	fmt.Fprintln(&b, "  nocreate")
	fmt.Fprintln(&b, "  nodelaycompress")
	fmt.Fprintln(&b, "  nomail")
	fmt.Fprintln(&b, "  notifempty")
	fmt.Fprintf(&b, "  olddir %s\n", oldDir)
	fmt.Fprintf(&b, "  rotate %d\n", numRotate)
	fmt.Fprintf(&b, "  size=%s\n", fileSize)
	fmt.Fprintln(&b, "}")

	_, err := lrf.WriteString(b.String())

	return err
}

// UpdateLogRotateConf updates the log rotation configuration file
func (ls *LogsService) UpdateLogRotateConf(consoleLogsPath string, nodes map[string]*nodes.NodeConsoleInfo) (err error) {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	// Open the file for writing
	slog.Debug("Opening log rotation configuration file", "path", ls.config.LogRotateFilePath)
	lrf, err := os.Create(ls.config.LogRotateFilePath)
	if err != nil {
		return fmt.Errorf("unable to create log rotation config file %q: %w", ls.config.LogRotateFilePath, err)
	}
	defer func() {
		if cerr := lrf.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("failed to close log rotation config file %q: %w", ls.config.LogRotateFilePath, cerr)
		}
	}()

	slog.Info("Writing log rotation configuration file", "nodeCount", len(nodes))

	// Write out the contents of the file
	if _, err := fmt.Fprintln(lrf, "# Auto-generated conman log rotation configuration file."); err != nil {
		return fmt.Errorf("failed to write log rotation header: %w", err)
	}

	// Add the aggregation file
	if ls.conAggLogFile != "" {
		conAggLogDir := filepath.Dir(ls.conAggLogFile)
		if len(conAggLogDir) > 0 {
			if err := writeConfigEntry(lrf, ls.conAggLogFile, conAggLogDir, ls.config.AggLogsNumRotate, ls.config.AggLogsFileSize); err != nil {
				return fmt.Errorf("failed to write aggregation log rotate entry for %q: %w", ls.conAggLogFile, err)
			}
		} else {
			return fmt.Errorf("invalid aggregation file name/dir for log rotation (file=%q dir=%q)", ls.conAggLogFile, conAggLogDir)
		}
	}

	// Add all nodes
	for _, cni := range nodes {
		id := cni.ID
		fn := filepath.Join(consoleLogsPath, fmt.Sprintf("console.%s", id))
		if err := writeConfigEntry(lrf, fn, ls.config.ConsoleLogsBackupPath, ls.config.ConsoleLogsNumRotate, ls.config.ConsoleLogsFileSize); err != nil {
			return fmt.Errorf("failed to write console log rotate entry for %q: %w", fn, err)
		}
	}

	if _, err := fmt.Fprintln(lrf, ""); err != nil {
		return fmt.Errorf("failed to write log rotation footer: %w", err)
	}

	slog.Debug("Completed writing log rotation configuration file")
	return nil
}

func parseTimestamp(consoleLogsPath string, conAggLogFile string, line string) (string, time.Time, bool, bool) {
	var nodeName string
	var fd time.Time
	isCon := false
	isAgg := false

	filePrefix := filepath.Join(consoleLogsPath, "console.")
	timeStampStr := ""
	pos := strings.Index(line, filePrefix)
	nodeStPos := 0
	if pos != -1 {
		nodeStPos = pos + len(filePrefix)
		posQ2 := strings.Index(line[nodeStPos:], "\"")
		if posQ2 == -1 {
			slog.Error("Unexpected file format - expected quote to close filename")
			return nodeName, fd, isCon, isAgg
		}
		posQ2 += nodeStPos
		nodeName = line[nodeStPos:posQ2]
		timeStampStr = line[posQ2+2:]
		isCon = true
	} else {
		if conAggLogFile == "" {
			return nodeName, fd, isCon, isAgg
		}

		pos = strings.Index(line, conAggLogFile)
		if pos == -1 {
			return nodeName, fd, isCon, isAgg
		}
		nodeName = "consoleAgg.log"
		isAgg = true
		timeStampStr = line[len(conAggLogFile)+pos+2:]
	}

	var year, month, day, hour, min, sec int
	_, err := fmt.Sscanf(timeStampStr, "%d-%d-%d-%d:%d:%d", &year, &month, &day, &hour, &min, &sec)
	if err != nil {
		slog.Error("Error parsing timestamp", "timestamp", timeStampStr, "error", err)
		return nodeName, fd, false, false
	}
	fd = time.Date(year, time.Month(month), day, hour, min, sec, 0, time.Local)

	return nodeName, fd, isCon, isAgg
}

func readLogRotTimestamps(config LogConfig, consoleLogsPath string, conAggLogFile string, fileStamp map[string]time.Time) (conChanged, aggChanged bool) {
	conChanged = false
	aggChanged = false

	sf, err := os.Open(config.LogRotateStateFilePath)
	if err != nil {
		slog.Error("Unable to open log rotation state file", "path", config.LogRotateStateFilePath, "error", err)
		return false, false
	}
	defer func() {
		if err := sf.Close(); err != nil {
			slog.Warn("Failed to close log rotation state file", "path", config.LogRotateStateFilePath, "error", err)
		}
	}()

	er := bufio.NewReader(sf)

	// Read the logrotate state -- version 2 line
	_, err = er.ReadString('\n')
	if err != nil {
		slog.Error("Unable to read log rotation state file", "path", config.LogRotateStateFilePath, "error", err)
		return false, false
	}

	for {
		line, err := er.ReadString('\n')
		if err != nil {
			break
		}

		if fileName, fd, isCon, isAgg := parseTimestamp(consoleLogsPath, conAggLogFile, line); isCon || isAgg {
			if _, ok := fileStamp[fileName]; ok {
				if fileStamp[fileName] != fd {
					slog.Debug("Log file rotated", "file", fileName)
					fileStamp[fileName] = fd
					if isCon {
						conChanged = true
					} else {
						aggChanged = true
					}
				}
			} else {
				slog.Debug("New log file detected", "file", fileName)
				fileStamp[fileName] = fd
				if isCon {
					conChanged = true
				} else {
					aggChanged = true
				}
			}
		}
	}

	return conChanged, aggChanged
}

// LogRotate performs a log rotation check and rotation if needed
func (ls *LogsService) LogRotate(consoleLogsPath string) bool {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	consoleLogChanged := false
	if ls.logRotateFileStamp == nil {
		ls.logRotateFileStamp = make(map[string]time.Time)
	}
	readLogRotTimestamps(ls.config, consoleLogsPath, ls.conAggLogFile, ls.logRotateFileStamp)

	if ls.config.LogRotateEnabled {
		consoleLogChanged = ls.rotateLogsOnce(ls.config, consoleLogsPath)
	}

	return consoleLogChanged
}

func (ls *LogsService) rotateLogsOnce(config LogConfig, consoleLogsPath string) bool {
	conChanged := false
	aggChanged := false
	slog.Info("Starting logrotate")
	cmd := exec.Command("logrotate", "-s", config.LogRotateStateFilePath, config.LogRotateFilePath)
	exitCode := -1
	if err := cmd.Run(); err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			exitCode = ee.ExitCode()
			slog.Warn("Logrotate exited with error", "exitCode", exitCode, "error", ee)
		}
	} else {
		exitCode = 0
	}
	slog.Info("Log rotation completed", "exitCode", exitCode)

	if conChanged, aggChanged = readLogRotTimestamps(config, consoleLogsPath, ls.conAggLogFile, ls.logRotateFileStamp); aggChanged {
		time.Sleep(5 * time.Second)

		if aggChanged {
			// Reopen the aggregation log file after rotation
			ls.reopenAggLog()
		}
	} else {
		slog.Debug("No log files changed with logrotate")
	}

	return conChanged
}
