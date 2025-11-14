package logs

import (
	"sync"

	"github.com/OpenCHAMI/remote-console/internal/types"
)

type LogsService interface {
	UpdateLogRotateConf(consoleLogsPath string, nodes map[string]*types.NodeConsoleInfo)
	LogRotate(consoleLogsPath string) bool
	AggregateFiles(consoleLogsPath string, nodes map[string]*types.NodeConsoleInfo)
}

type logsService struct {
	config LogConfig
	mutex  sync.Mutex
}

func NewLogsService(config LogConfig) LogsService {
	service := &logsService{
		config: config,
	}

	service.initLogRotate()

	return service
}
