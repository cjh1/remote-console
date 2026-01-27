package logs

type LogConfig struct {
	ConsoleLogsFileSize     string `desc:"Maximum size of console log files before rotation."`
	ConsoleLogsNumRotate    int    `desc:"Number of rotated console log files to keep."`
	ConsoleLogsBackupPath   string `desc:"Path to rotated console log files."`
	AggLogsFileSize         string `desc:"Maximum size of aggregation log file before rotation."`
	AggLogsNumRotate        int    `desc:"Number of rotated aggregation log files to keep."`
	AggLogsPath             string `desc:"Path to aggregation log files."`
	LogRotateEnabled        bool   `desc:"Enable log rotation."`
	LogRotateCheckFrequency int    `desc:"Frequency in seconds to check for log rotation."`
	LogRotateFilePath       string `desc:"Path to logrotate configuration file."`
	LogRotateStateFilePath  string `desc:"Path to logrotate state file."`
}

func DefaultLogConfig() LogConfig {
	return LogConfig{
		LogRotateEnabled:        true,
		ConsoleLogsFileSize:     "5M",
		ConsoleLogsNumRotate:    2,
		ConsoleLogsBackupPath:   "/var/log/conman.old",
		AggLogsFileSize:         "20M",
		AggLogsNumRotate:        1,
		AggLogsPath:             "/tmp/consoleAgg",
		LogRotateCheckFrequency: 600,
		LogRotateFilePath:       "/tmp/logrotate.conman",
		LogRotateStateFilePath:  "/tmp/rot_conman.state",
	}
}
