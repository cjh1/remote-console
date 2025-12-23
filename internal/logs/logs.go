package logs

import (
	"context"
	"log"
	"os"
	"sync"
)

type logsService struct {
	config LogConfig
	mutex  sync.Mutex

	// Aggregation log fields
	conAggMutex   sync.Mutex
	conAggLogger  *log.Logger
	conAggLogFile string
	conAggFile    *os.File
	tailThreads   map[string]*context.CancelFunc
}

func NewLogsService(config LogConfig) *logsService {
	service := &logsService{
		config:      config,
		tailThreads: make(map[string]*context.CancelFunc),
	}

	service.initLogRotate()

	return service
}
