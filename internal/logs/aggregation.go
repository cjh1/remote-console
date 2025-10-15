//  MIT License
//
//  (C) Copyright 2020-2022, 2024 Hewlett Packard Enterprise Development LP
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

// This file contains the code needed to handle aggregation of log files

package logs

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/nxadm/tail"

	"github.com/OpenCHAMI/remote-console/internal/nodes"
)

// aggregateFile starts tailing a log file if not already running (idempotent)
func (ls *LogsService) aggregateFile(consoleLogsPath string, xname string) {
	if _, ok := ls.tailCancelByNode[xname]; !ok {

		// set up a context and a cancel function for this thread
		ctx, cancel := context.WithCancel(context.Background())
		ls.tailCancelByNode[xname] = &cancel

		// record being tracked and forward log file contents
		go ls.watchConsoleLogFile(ctx, consoleLogsPath, xname)
	}
}

// stopTailing stops tailing a specific node's console log
func (ls *LogsService) stopTailing(xname string) {
	if cancel, ok := ls.tailCancelByNode[xname]; ok {
		(*cancel)()
		delete(ls.tailCancelByNode, xname)
	}
}

// watchConsoleLogFile tails a console log file and writes to aggregation log
func (ls *LogsService) watchConsoleLogFile(ctx context.Context, consoleLogsPath string, xname string) {
	filename := fmt.Sprintf("%s/console.%s", consoleLogsPath, xname)
	slog.Info("Setting up console log tail", "filename", filename, "xname", xname)

	// set up a tail operation on the console file
	t, err := tail.TailFile(filename, tail.Config{
		Follow:    true,
		ReOpen:    true,
		MustExist: false,
		Poll:      true, // Avoid missing updates when files are recreated/rotated.
	})
	if err != nil {
		slog.Error("Failed to setup tail on file", "filename", filename, "error", err)
		return
	}

	slog.Debug("Starting tail process loop", "xname", xname)
	for {
		select {
		case <-ctx.Done():
			slog.Debug("Cancelling tail", "xname", xname)
			if err := t.Stop(); err != nil {
				slog.Error("Failed to stop tail", "xname", xname, "error", err)
			}
			return
		case line, ok := <-t.Lines:
			// This channel is closed while waiting for the file to appear
			if !ok {
				return
			}
			if line.Err != nil {
				slog.Error("Error reading line from console", "xname", xname, "error", line.Err)
				continue
			}
			ls.writeToAggLog(xname, line.Text)
		}
	}
}

// writeToAggLog writes a line to the aggregation log with proper locking
func (ls *LogsService) writeToAggLog(xname, line string) {
	ls.conAggMutex.Lock()
	defer ls.conAggMutex.Unlock()

	if ls.conAggLogger == nil {
		return
	}

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	ls.conAggLogger.Printf("%s [%s] %s", timestamp, xname, line)
}

func (ls *LogsService) openAggLogLocked() {
	if ls.conAggLogFile == "" {
		hostname, err := os.Hostname()
		if err != nil {
			slog.Warn("Failed to get hostname, using 'unknown'", "error", err)
			hostname = "unknown"
		}
		ls.conAggLogFile = fmt.Sprintf("%s/consoleAgg-%s.log", ls.config.AggLogsPath, hostname)
	}

	dir := filepath.Dir(ls.conAggLogFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		slog.Error("Failed to create aggregation log directory", "directory", dir, "error", err)
		return
	}

	calf, err := os.OpenFile(ls.conAggLogFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		slog.Error("Failed to open aggregation log file", "file", ls.conAggLogFile, "error", err)
		return
	}

	ls.conAggFile = calf
	slog.Info("Started aggregation log file", "file", ls.conAggLogFile)
	ls.conAggLogger = log.New(calf, "", 0)
	ls.conAggLogger.Print("Starting aggregation log")
}

// EnsureAggLog opens the aggregation log file if not already open.
func (ls *LogsService) EnsureAggLog() {
	ls.conAggMutex.Lock()
	defer ls.conAggMutex.Unlock()

	if ls.conAggLogger != nil {
		return
	}

	ls.openAggLogLocked()
}

// reopenAggLog closes and reopens the aggregation log file (used after rotation).
func (ls *LogsService) reopenAggLog() {
	ls.conAggMutex.Lock()
	defer ls.conAggMutex.Unlock()

	if ls.conAggFile != nil {
		if err := ls.conAggFile.Close(); err != nil {
			slog.Error("Failed to close aggregation log file", "error", err)
		}
		ls.conAggFile = nil
	}
	ls.conAggLogger = nil
	ls.openAggLogLocked()
}

func (ls *LogsService) AggregateFiles(consoleLogsPath string, nodes map[string]*nodes.NodeConsoleInfo) {
	slog.Info("Starting aggregation of console log files", "nodeCount", len(nodes), "path", consoleLogsPath)

	// Ensure the aggregation log file is ready before we start tailing console logs.
	ls.EnsureAggLog()

	// Start tailing any new nodes
	for xname := range nodes {
		ls.aggregateFile(consoleLogsPath, xname)
	}

	// Stop tailing nodes that are no longer in the list
	for xname := range ls.tailCancelByNode {
		if _, exists := nodes[xname]; !exists {
			slog.Info("Stopping tail for removed node", "xname", xname)
			ls.stopTailing(xname)
		}
	}
}
