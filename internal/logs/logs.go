package logs

import (
	"sync"
)

type logsService struct {
	config LogConfig
	mutex  sync.Mutex
}

func NewLogsService(config LogConfig) *logsService {
	service := &logsService{
		config: config,
	}

	service.initLogRotate()

	return service
}
