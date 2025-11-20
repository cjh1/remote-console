package conman

type ConmanConfig struct {
	BaseConfFilePath   string `desc:"Path to the base conman configuration template file."`
	ConfFilePath       string `desc:"Path to the generated conman configuration file."`
	LogsPath           string `desc:"Path to conman log files."`
	PidFilePath        string `desc:"Path to the conman PID file."`
	ConsoleScriptsPath string `desc:"Path to console helper scripts."`
}

func DefaultConmanConfig() ConmanConfig {
	return ConmanConfig{
		BaseConfFilePath:   "/app/conman.conf.tmpl",
		ConfFilePath:       "/app/conman.conf",
		LogsPath:           "/var/log/conman",
		PidFilePath:        "/var/run/conman.pid",
		ConsoleScriptsPath: "/usr/bin",
	}
}
