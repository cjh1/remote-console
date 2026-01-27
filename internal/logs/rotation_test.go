package logs

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/OpenCHAMI/remote-console/internal/nodes"
	"github.com/stretchr/testify/require"
)

func TestInitNewLogsService(t *testing.T) {
	tempDir := t.TempDir()

	config := DefaultLogConfig()
	config.ConsoleLogsBackupPath = filepath.Join(tempDir, "conman.old")

	_, err := NewLogsService(config)
	require.NoError(t, err)

	// Verify that the backup directory was created
	_, err = os.Stat(config.ConsoleLogsBackupPath)
	require.NoError(t, err, "Backup directory should exist")
}

func TestUpdateLogRotateConf(t *testing.T) {
	tempDir := t.TempDir()

	config := DefaultLogConfig()
	config.LogRotateFilePath = filepath.Join(tempDir, "logrotate.test")
	config.AggLogsPath = tempDir
	config.ConsoleLogsBackupPath = filepath.Join(tempDir, "conman.old")

	service, err := NewLogsService(config)
	require.NoError(t, err)
	service.conAggLogFile = "/tmp/consoleAgg-test.log"

	nodes := map[string]*nodes.NodeConsoleInfo{
		"x0c0s1b0": {ID: "x0c0s1b0"},
		"x0c0s1b1": {ID: "x0c0s1b1"},
	}

	require.NoError(t, service.UpdateLogRotateConf("/var/log/conman", nodes))

	// Verify that the log rotation configuration file was created
	data, err := os.ReadFile(config.LogRotateFilePath)
	require.NoError(t, err, "Log rotation config file should exist")

	logConfigContents := string(data)

	// Verify header
	require.Contains(t, logConfigContents, "# Auto-generated conman log rotation configuration file.")

	// Verify aggregation log entry
	aggLogEntry := `/tmp/consoleAgg-test.log { 
  nocompress
  missingok
  nocopytruncate
  nocreate
  nodelaycompress
  nomail
  notifempty
  olddir /tmp
  rotate 1
  size=20M
}`
	require.Contains(t, logConfigContents, aggLogEntry, "Aggregation log entry should be present")

	// Verify console log entry for x0c0s1b0
	consoleLogEntry0 := fmt.Sprintf(`/var/log/conman/console.x0c0s1b0 { 
  nocompress
  missingok
  nocopytruncate
  nocreate
  nodelaycompress
  nomail
  notifempty
  olddir %s
  rotate 2
  size=5M
}`, config.ConsoleLogsBackupPath)
	require.Contains(t, logConfigContents, consoleLogEntry0, "Console log entry for x0c0s1b0 should be present")

	// Verify console log entry for x0c0s1b1
	consoleLogEntry1 := fmt.Sprintf(`/var/log/conman/console.x0c0s1b1 { 
  nocompress
  missingok
  nocopytruncate
  nocreate
  nodelaycompress
  nomail
  notifempty
  olddir %s
  rotate 2
  size=5M
}`, config.ConsoleLogsBackupPath)
	require.Contains(t, logConfigContents, consoleLogEntry1, "Console log entry for x0c0s1b1 should be present")
}

func TestReadLogRotTimestamps(t *testing.T) {
	// Prepare a temporary log rotation timestamp file
	tempDir := t.TempDir()
	logRotTimestampsFile := filepath.Join(tempDir, "logrot.timestamps")

	console1Timestamp := time.Date(2023, 11, 14, 12, 26, 40, 0, time.Local)
	console2Timestamp := time.Date(2023, 11, 14, 12, 34, 60, 0, time.Local)

	content := fmt.Sprintf(`# Log rotation timestamps
"/var/log/conman/console.x0c0s1b0" %s
"/var/log/conman/console.x0c0s1b1" %s
`, console1Timestamp.Format("2006-01-02-15:04:05"), console2Timestamp.Format("2006-01-02-15:04:05"))

	config := DefaultLogConfig()
	config.LogRotateStateFilePath = logRotTimestampsFile
	config.LogRotateFilePath = filepath.Join(tempDir, "logrotate.test")

	err := os.WriteFile(logRotTimestampsFile, []byte(content), 0600)
	require.NoError(t, err)

	fileStamp := make(map[string]time.Time)
	// Read the timestamps
	conChanged, aggChanged := readLogRotTimestamps(config, "/var/log/conman", "", fileStamp)
	require.True(t, conChanged, "Console logs should show changes")
	require.False(t, aggChanged, "Aggregation log should not show changes")

	// Verify the contents
	expected := map[string]time.Time{
		"x0c0s1b0": console1Timestamp,
		"x0c0s1b1": console2Timestamp,
	}

	require.Equal(t, expected, fileStamp, "Timestamps should match expected values")
}

func TestRotateLogsOnce(t *testing.T) {
	tempDir := t.TempDir()
	logRotateStateFilePath := filepath.Join(tempDir, "logrot.state")
	logBackupDir := filepath.Join(tempDir, "logbackups")
	err := os.MkdirAll(logBackupDir, 0755)
	require.NoError(t, err)

	config := DefaultLogConfig()
	config.LogRotateStateFilePath = logRotateStateFilePath
	config.LogRotateFilePath = filepath.Join(tempDir, "logrotate.test")
	config.ConsoleLogsBackupPath = logBackupDir
	config.LogRotateEnabled = true
	config.ConsoleLogsFileSize = "1K"

	nodes := map[string]*nodes.NodeConsoleInfo{
		"x0c0s1b0": {ID: "x0c0s1b0"},
		"x0c0s1b1": {ID: "x0c0s1b1"},
	}
	service, err := NewLogsService(config)
	require.NoError(t, err)

	require.NoError(t, service.UpdateLogRotateConf(tempDir, nodes))

	// Perform log rotation check
	service.logRotateFileStamp = make(map[string]time.Time)
	changed := service.rotateLogsOnce(config, tempDir)
	// TODO This is the current behavior, but seems wrong - should be false if no files exist?
	require.True(t, changed, "Change should be detected")

	changed = service.rotateLogsOnce(config, tempDir)
	require.False(t, changed, "Nothing should have changed")

	// Now create some log files to trigger rotation
	consoleLog1 := filepath.Join(tempDir, "console.x0c0s1b0")
	consoleLog2 := filepath.Join(tempDir, "console.x0c0s1b1")

	// Writ over 1KB of data to each log file
	largeData := make([]byte, 2048)
	for i := range largeData {
		largeData[i] = 'A'
	}

	err = os.WriteFile(consoleLog1, largeData, 0600)
	require.NoError(t, err)
	err = os.WriteFile(consoleLog2, largeData, 0600)
	require.NoError(t, err)

	// Update timestamps to force rotation
	service.logRotateFileStamp["x0c0s1b0"] = time.Now().Add(-2 * time.Hour)
	service.logRotateFileStamp["x0c0s1b1"] = time.Now().Add(-2 * time.Hour)

	// Perform log rotation check again
	changed = service.rotateLogsOnce(config, tempDir)
	require.True(t, changed, "Log rotations should be detected")

	// Verify that the log files have been rotated (moved to backup directory)
	backupFiles, err := os.ReadDir(logBackupDir)
	fmt.Println(backupFiles)
	require.NoError(t, err)

	var found1, found2 bool
	for _, f := range backupFiles {
		if f.Name() == "console.x0c0s1b0.1" {
			found1 = true
		}
		if f.Name() == "console.x0c0s1b1.1" {
			found2 = true
		}
	}
	require.True(t, found1, "console.x0c0s1b0.1 should be in backup directory")
	require.True(t, found2, "console.x0c0s1b1.1 should be in backup directory")
}

func TestReadLogRotTimestampsWithAggregationEntry(t *testing.T) {
	// Prepare a temporary log rotation timestamp file containing an aggregation entry
	tempDir := t.TempDir()
	logRotTimestampsFile := filepath.Join(tempDir, "logrot.timestamps")

	consoleTimestamp := time.Date(2023, 11, 14, 12, 26, 40, 0, time.Local)
	aggTimestamp := time.Date(2023, 11, 15, 8, 30, 0, 0, time.Local)

	content := fmt.Sprintf(`# Log rotation timestamps
"/var/log/conman/console.x0c0s1b0" %s
"/tmp/consoleAgg-test.log" %s
`, consoleTimestamp.Format("2006-01-02-15:04:05"), aggTimestamp.Format("2006-01-02-15:04:05"))

	config := DefaultLogConfig()
	config.LogRotateStateFilePath = logRotTimestampsFile
	config.LogRotateFilePath = filepath.Join(tempDir, "logrotate.test")

	err := os.WriteFile(logRotTimestampsFile, []byte(content), 0600)
	require.NoError(t, err)

	fileStamp := make(map[string]time.Time)
	aggPath := "/tmp/consoleAgg-test.log"

	conChanged, aggChanged := readLogRotTimestamps(config, "/var/log/conman", aggPath, fileStamp)

	require.True(t, conChanged, "Console logs should show changes")
	require.True(t, aggChanged, "Aggregation log should show changes")

	expected := map[string]time.Time{
		"x0c0s1b0":       consoleTimestamp,
		"consoleAgg.log": aggTimestamp,
	}
	require.Equal(t, expected, fileStamp, "Timestamps should match expected values including aggregation log")
}

func TestReadLogRotTimestampsNoEntries(t *testing.T) {
	// Prepare a temporary log rotation timestamp file with no entries
	tempDir := t.TempDir()
	logRotTimestampsFile := filepath.Join(tempDir, "logrot.timestamps")

	content := "logrotate state -- version 2\n"

	config := DefaultLogConfig()
	config.LogRotateStateFilePath = logRotTimestampsFile
	config.LogRotateFilePath = filepath.Join(tempDir, "logrotate.test")

	err := os.WriteFile(logRotTimestampsFile, []byte(content), 0600)
	require.NoError(t, err)

	fileStamp := make(map[string]time.Time)
	// Read the timestamps
	conChanged, aggChanged := readLogRotTimestamps(config, "/var/log/conman", "", fileStamp)
	require.False(t, conChanged, "Console logs should not show changes")
	require.False(t, aggChanged, "Aggregation log should not show changes")

	// Verify the contents
	expected := map[string]time.Time{}

	require.Equal(t, expected, fileStamp, "Timestamps should be empty")
}
