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

	"github.com/OpenCHAMI/remote-console/internal/types"
)

func TestWriteToAggLog(t *testing.T) {
	tempDir := t.TempDir()

	// Set up aggregation log file path
	conAggLogFile = filepath.Join(tempDir, "consoleAgg-test.log")
	lf, err := os.OpenFile(conAggLogFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	require.NoError(t, err)

	// Initialize logger
	conAggMutex.Lock()
	conAggLogger = log.New(lf, "", log.LstdFlags)
	conAggMutex.Unlock()

	testLines := map[string]string{
		"x0c0s1b0": "First log line",
		"x0c0s1b1": "Second log line",
		"x0c0s1b2": "Third log line",
	}

	for xname, line := range testLines {
		writeToAggLog(xname, line)
	}

	// Read back the aggregation log file and verify contents
	data, err := os.ReadFile(conAggLogFile)
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
	// Set up aggregation log file path
	conAggLogFile = filepath.Join(tempDir, "consoleAgg-test.log")
	lf, err := os.OpenFile(conAggLogFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	require.NoError(t, err)

	// Initialize logger
	conAggMutex.Lock()
	conAggLogger = log.New(lf, "", log.LstdFlags)
	conAggMutex.Unlock()

	// Set up a console log file
	testXname := "x0c0s1b0"
	consoleLogFileName := fmt.Sprintf("console.%s", testXname)
	consoleLogFile := filepath.Join(tempDir, consoleLogFileName)
	lf, err = os.OpenFile(consoleLogFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	require.NoError(t, err)
	defer lf.Close()

	config := DefaultLogConfig()

	// Start watching the console log file
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	service := NewLogsService(config)

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
	data, err := os.ReadFile(conAggLogFile)
	require.NoError(t, err)

	logContents := string(data)
	fmt.Println(logContents)
	for _, line := range testLines {
		require.Contains(t, logContents, line)
	}
}

func TestAggregateFiles(t *testing.T) {
	tempDir := t.TempDir()

	// Set up aggregation log file path
	conAggLogFile = filepath.Join(tempDir, "consoleAgg-test.log")
	lf, err := os.OpenFile(conAggLogFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	require.NoError(t, err)

	// Initialize logger
	conAggMutex.Lock()
	conAggLogger = log.New(lf, "", log.LstdFlags)
	conAggMutex.Unlock()

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

	config := DefaultLogConfig()

	// Prepare node console info map
	nodes := make(map[string]*types.NodeConsoleInfo)
	for _, xname := range testXnames {
		nodes[xname] = &types.NodeConsoleInfo{
			ID: xname,
		}
	}

	service := NewLogsService(config)
	// Start aggregating files
	service.AggregateFiles(tempDir, nodes)

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
	data, err := os.ReadFile(conAggLogFile)
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

func TestRespinAggLog(t *testing.T) {
	tempDir := t.TempDir()

	// Set up aggregation log file path
	conAggLogFile = filepath.Join(tempDir, "consoleAgg-test.log")

	service := NewLogsService(DefaultLogConfig())

	// First respin
	service.respinAggLog()
	require.NotNil(t, conAggLogger)

	// Write a test line
	writeToAggLog("x0c0s1b0", "Test line before respin")

	// Capture the current logger pointer
	firstLogger := conAggLogger

	// Respin again
	service.respinAggLog()
	require.NotNil(t, conAggLogger)

	// Ensure the logger pointer has changed
	require.NotEqual(t, firstLogger, conAggLogger)

	// Write another test line
	writeToAggLog("x0c0s1b0", "Test line after respin")

	// Read back the aggregation log file and verify contents
	data, err := os.ReadFile(conAggLogFile)
	require.NoError(t, err)

	logContents := string(data)
	require.Contains(t, logContents, "Test line before respin")
	require.Contains(t, logContents, "Test line after respin")
}
