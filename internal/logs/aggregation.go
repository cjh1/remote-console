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
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hpcloud/tail"

	"github.com/OpenCHAMI/remote-console/internal/types"
)

// Global vars
var conAggMutex = &sync.Mutex{}
var conAggLogger *log.Logger = nil

var conAggLogFile string = ""

// map to cancel threads tailing log files
var tailThreads map[string]*context.CancelFunc = make(map[string]*context.CancelFunc)

// aggregateFile sets up tailing a log file to add to the aggregation file
func (ls *logsService) aggregateFile(consoleLogsPath string, xname string) bool {
	newFile := false
	if _, ok := tailThreads[xname]; !ok {
		// indicate we are starting to watch this one
		newFile = true
		// set up a context and a cancel function for this thread
		ctx, cancel := context.WithCancel(context.Background())
		tailThreads[xname] = &cancel

		// record being tracked and forward log file contents
		go ls.watchConsoleLogFile(ctx, consoleLogsPath, xname)
	}
	return newFile
}

// StopTailing stops tailing a console log file
func StopTailing(xname string) {
	if cancel, ok := tailThreads[xname]; ok {
		(*cancel)()
		delete(tailThreads, xname)
	}
}

// watchConsoleLogFile tails a console log file and writes to aggregation log
func (ls *logsService) watchConsoleLogFile(ctx context.Context, consoleLogsPath string, xname string) {
	filename := fmt.Sprintf("%s/console.%s", consoleLogsPath, xname)
	log.Printf("Setting up tail of %s", filename)

	// set up a tail operation on the console file
	t, err := tail.TailFile(filename, tail.Config{Follow: true, ReOpen: true})
	if err != nil {
		log.Printf("Error setting up tail on file %s:%s", filename, err)
		return
	}

	log.Printf("Starting tail process loop for %s", xname)
	for {
		select {
		case <-ctx.Done():
			log.Printf("Cancelling tail of %s", xname)
			t.Stop()
			return
		case line := <-t.Lines:
			if line.Err != nil {
				log.Printf("Error reading line from %s:%s", xname, line.Err)
				continue
			}
			writeToAggLog(xname, line.Text)
		}
	}
}

// writeToAggLog writes a line to the aggregation log with proper locking
func writeToAggLog(xname, line string) {
	conAggMutex.Lock()
	defer conAggMutex.Unlock()

	if conAggLogger == nil {
		return
	}

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	conAggLogger.Printf("%s [%s] %s", timestamp, xname, line)
}

// RespinAggLog reopens the aggregation log file (after rotation)
func (ls *logsService) respinAggLog() {
	conAggMutex.Lock()
	defer conAggMutex.Unlock()

	if conAggLogFile == "" {
		// build up the aggregation file name
		hostname, err := os.Hostname()
		if err != nil {
			log.Printf("Error getting hostname:%s", err)
			hostname = "unknown"
		}
		conAggLogFile = fmt.Sprintf("%s/consoleAgg-%s.log", ls.config.AggLogsPath, hostname)
	}

	// ensure the directory exists
	dir := filepath.Dir(conAggLogFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("Error creating aggregation log directory %s:%s", dir, err)
		return
	}

	// open/create the aggregation log file
	calf, err := os.OpenFile(conAggLogFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		log.Printf("Error opening aggregation log file %s:%s", conAggLogFile, err)
		return
	}

	if conAggLogger == nil {
		log.Printf("Started aggregation log file: %s", conAggLogFile)
	} else {
		log.Printf("Restarted aggregation log file: %s", conAggLogFile)
	}

	conAggLogger = log.New(calf, "", 0)
	conAggLogger.Print("Starting aggregation log")
}

func (ls *logsService) AggregateFiles(consoleLogsPath string, nodes map[string]*types.NodeConsoleInfo) {
	fmt.Printf("AggregateFiles: Starting aggregation of console log files\n")
	fmt.Printf("AggregateFiles: Starting aggregation of console log files: %d\n", len(nodes))

	for xname := range nodes {
		// make sure the node is being aggregated - no-op if already being done
		ls.aggregateFile(consoleLogsPath, xname)
	}
}
