package logs

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/OpenCHAMI/remote-console/internal/nodes"
)

func TestWriteToAggLog(t *testing.T) {
	tempDir := t.TempDir()

	// Create a logs service
	config := DefaultLogConfig()
	config.AggLogsPath = tempDir
	service := NewLogsService(config)

	// Initialize aggregation log
	service.conAggLogFile = filepath.Join(tempDir, "consoleAgg-test.log")
	lf, err := os.OpenFile(service.conAggLogFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	require.NoError(t, err)

	// Initialize logger
	service.conAggMutex.Lock()
	service.conAggLogger = log.New(lf, "", log.LstdFlags)
	service.conAggMutex.Unlock()

	testLines := map[string]string{
		"x0c0s1b0": "First log line",
		"x0c0s1b1": "Second log line",
		"x0c0s1b2": "Third log line",
	}

	for xname, line := range testLines {
		service.writeToAggLog(xname, line)
	}

	// Read back the aggregation log file and verify contents
	data, err := os.ReadFile(service.conAggLogFile)
	require.NoError(t, err)

	logContents := string(data)
	fmt.Println(logContents)
	for xname, line := range testLines {
		require.Contains(t, logContents, line)
		require.Contains(t, logContents, xname)
	}
}

func TestWatchConsoleLogFile(t *testing.T) {
	tempDir := t.TempDir()
	
	config := DefaultLogConfig()
	config.AggLogsPath = tempDir
	service := NewLogsService(config)

	// Set up aggregation log file path
	service.conAggLogFile = filepath.Join(tempDir, "consoleAgg-test.log")
	lf, err := os.OpenFile(service.conAggLogFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	require.NoError(t, err)

	// Initialize logger
	service.conAggMutex.Lock()
	service.conAggLogger = log.New(lf, "", log.LstdFlags)
	service.conAggMutex.Unlock()

	// Set up a console log file
	testXname := "x0c0s1b0"
	consoleLogFileName := fmt.Sprintf("console.%s", testXname)
	consoleLogFile := filepath.Join(tempDir, consoleLogFileName)
	lf, err = os.OpenFile(consoleLogFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	require.NoError(t, err)
	defer lf.Close()

	// Start watching the console log file
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go service.watchConsoleLogFile(ctx, tempDir, testXname)

	// Write some lines to the console log file
	testLines := []string{
		"First console log line",
		"Second console log line",
		"Third console log line",
	}

	for _, line := range testLines {
		_, err := lf.WriteString(line + "\n")
		require.NoError(t, err)
	}

	// Give some time for the watcher to process the lines
	time.Sleep(1 * time.Second)

	cancel()

	// Read back the aggregation log file and verify contents
	data, err := os.ReadFile(service.conAggLogFile)
	require.NoError(t, err)

	logContents := string(data)
	fmt.Println(logContents)
	for _, line := range testLines {
		require.Contains(t, logContents, line)
	}
}

func TestAggregateFiles(t *testing.T) {
	tempDir := t.TempDir()

	config := DefaultLogConfig()
	config.AggLogsPath = tempDir
	service := NewLogsService(config)

	// Set up aggregation log file path
	service.conAggLogFile = filepath.Join(tempDir, "consoleAgg-test.log")
	lf, err := os.OpenFile(service.conAggLogFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	require.NoError(t, err)

	// Initialize logger
	service.conAggMutex.Lock()
	service.conAggLogger = log.New(lf, "", log.LstdFlags)
	service.conAggMutex.Unlock()

	// Set up console log files
	testXnames := []string{"x0c0s1b0", "x0c0s1b1"}
	consoleLogFiles := make(map[string]string)
	for _, xname := range testXnames {
		consoleLogFileName := fmt.Sprintf("console.%s", xname)
		consoleLogFile := filepath.Join(tempDir, consoleLogFileName)
		lf, err := os.OpenFile(consoleLogFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		require.NoError(t, err)
		defer lf.Close()
		consoleLogFiles[xname] = consoleLogFile
	}

	// Prepare node console info map
	nodeMap := make(map[string]*nodes.NodeConsoleInfo)
	for _, xname := range testXnames {
		nodeMap[xname] = &nodes.NodeConsoleInfo{
			ID: xname,
		}
	}

	// Start aggregating files
	service.AggregateFiles(tempDir, nodeMap)

	// Write some lines to the console log files
	testLines := map[string][]string{
		"x0c0s1b0": {"First log line from x0c0s1b0", "Second log line from x0c0s1b0"},
		"x0c0s1b1": {"First log line from x0c0s1b1", "Second log line from x0c0s1b1"},
	}

	for xname, lines := range testLines {
		lf, err := os.OpenFile(consoleLogFiles[xname], os.O_APPEND|os.O_WRONLY, 0600)
		require.NoError(t, err)
		for _, line := range lines {
			_, err := lf.WriteString(line + "\n")
			require.NoError(t, err)
		}
		lf.Close()
	}

	// Give some time for the aggregation to process the lines
	time.Sleep(1 * time.Second)

	// Read back the aggregation log file and verify contents
	data, err := os.ReadFile(service.conAggLogFile)
	require.NoError(t, err)

	logContents := string(data)
	fmt.Println(logContents)
	for xname, lines := range testLines {
		for _, line := range lines {
			require.Contains(t, logContents, line)
			require.Contains(t, logContents, xname)
		}
	}
}

func TestAggregationLogReopen(t *testing.T) {
	tempDir := t.TempDir()

	config := DefaultLogConfig()
	config.AggLogsPath = tempDir
	service := NewLogsService(config)

	// Set up aggregation log file path
	service.conAggLogFile = filepath.Join(tempDir, "consoleAgg-test.log")

	// First open
	service.EnsureAggLog()
	require.NotNil(t, service.conAggLogger)

	// Write a test line
	service.writeToAggLog("x0c0s1b0", "Test line before respin")

	// Capture the current logger pointer
	firstLogger := service.conAggLogger

	// Respin again
	service.reopenAggLog()
	require.NotNil(t, service.conAggLogger)

	// Ensure the logger pointer has changed
	require.NotEqual(t, firstLogger, service.conAggLogger)

	// Write another test line
	service.writeToAggLog("x0c0s1b0", "Test line after respin")

	// Read back the aggregation log file and verify contents
	data, err := os.ReadFile(service.conAggLogFile)
	require.NoError(t, err)

	logContents := string(data)
	require.Contains(t, logContents, "Test line before respin")
	require.Contains(t, logContents, "Test line after respin")
}
