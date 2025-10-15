package logs

import (
	"context"
	"log"
	"os"
	"sync"
	"time"
)

type LogsService struct {
	config LogConfig
	mutex  sync.Mutex

	// Aggregation log fields
	conAggMutex        sync.Mutex
	conAggLogger       *log.Logger
	conAggLogFile      string
	conAggFile         *os.File
	tailCancelByNode   map[string]*context.CancelFunc // nodeID -> cancel func
	logRotateFileStamp map[string]time.Time           // filename -> last mod time
}

func NewLogsService(config LogConfig) (*LogsService, error) {
	service := &LogsService{
		config:             config,
		tailCancelByNode:   make(map[string]*context.CancelFunc),
		logRotateFileStamp: make(map[string]time.Time),
	}

	if err := service.initLogRotate(); err != nil {
		return nil, err
	}

	return service, nil
}
