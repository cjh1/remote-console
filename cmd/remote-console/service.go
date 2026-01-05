package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	compcreds "github.com/Cray-HPE/hms-compcredentials"
	"github.com/OpenCHAMI/remote-console/internal/conman"
	"github.com/OpenCHAMI/remote-console/internal/console"
	"github.com/OpenCHAMI/remote-console/internal/creds"
	"github.com/OpenCHAMI/remote-console/internal/logs"
	"github.com/OpenCHAMI/remote-console/internal/nodes"
)

// ConmanService defines the interface for conman service operations
type ConmanService interface {
	ConfigureConman(nodes map[string]*nodes.NodeConsoleInfo, passwords map[string]compcreds.CompCredentials, sshConsoleKeyPath string) (bool, error)
	ExecuteConman() error
	SignalConmanTERM()
	SignalConmanHUP()
}

// CredsService defines the interface for credentials service operations
type CredsService interface {
	GetPasswordsWithRetries(bmcXNames []string, maxTries, waitSecs int) map[string]compcreds.CompCredentials
	EnsureConsoleKeysPresent() (bool, error)
	CheckForUpdates() (bool, error)
}

// LogsService defines the interface for logs service operations
type LogsService interface {
	UpdateLogRotateConf(consoleLogsPath string, nodes map[string]*nodes.NodeConsoleInfo)
	LogRotate(consoleLogsPath string) bool
	AggregateFiles(consoleLogsPath string, nodes map[string]*nodes.NodeConsoleInfo)
}

// Watch for node updates and signal conman and log rotation as needed
func watchForNodesUpdates(ctx context.Context, config remoteConsoleConfig, conmanService ConmanService, logsService LogsService) {
	if conmanService == nil {
		slog.Error("Conman service is nil")
		panic("Conman service is nil")
	}

	// conman will add the conman directory, so we point the logs service their
	conmanLogsPath := filepath.Join(config.Conman.LogsPath, "conman")

	ticker := time.NewTicker(time.Duration(config.NewNodeLookup) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Exiting node watch loop due to shutdown")
			return
		case <-ticker.C:
			changed := nodes.CheckForUpdates(config.SmdURL)

			if changed {
				slog.Info("Node changes detected, signaling conman to restart")
				conmanService.SignalConmanTERM()

				nodes := nodes.CurrentNodes()

				// also update log rotation configuration
				slog.Info("Updating log rotation configuration for node changes")
				logsService.UpdateLogRotateConf(conmanLogsPath, nodes)

				// make sure we are aggregating any new console log files
				slog.Info("Updating log aggregation configuration for node changes")
				logsService.AggregateFiles(conmanLogsPath, nodes)
			}
		}
	}
}

// Watch for credential updates and signal conman as needed
func watchForCredUpdates(ctx context.Context, config remoteConsoleConfig, credsService CredsService, conmanService ConmanService) {
	ticker := time.NewTicker(time.Duration(config.CredsMonitorInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Exiting credential watch loop due to shutdown")
			return
		case <-ticker.C:
			changed, err := credsService.CheckForUpdates()
			if err != nil {
				slog.Error("Failed to check for credential updates", "error", err)
			}

			if changed {
				slog.Info("Credential changes detected, signaling conman to restart")
				conmanService.SignalConmanTERM()
			}
		}
	}
}

// Log rotation setup and loop
func logRotate(ctx context.Context, config remoteConsoleConfig, conmanService ConmanService, logsService LogsService) {
	logConfig := config.Log
	// log the log rotation parameters
	slog.Info("Log rotation configuration",
		"enabled", logConfig.LogRotateEnabled,
		"checkFrequencySec", logConfig.LogRotateCheckFrequency,
		"consoleFileSize", logConfig.ConsoleLogsFileSize,
		"consoleNumRotate", logConfig.ConsoleLogsNumRotate,
		"aggFileSize", logConfig.AggLogsFileSize,
		"aggNumRotate", logConfig.AggLogsNumRotate)

	// conman will add the conman directory, so we point the logs service their
	conmanLogsPath := filepath.Join(config.Conman.LogsPath, "conman")

	// Create the log rotation configuration file
	logsService.UpdateLogRotateConf(conmanLogsPath, nodes.CurrentNodes())

	sleepSecs := time.Duration(300) * time.Second
	logRotCheckFreqSec := logConfig.LogRotateCheckFrequency
	if logRotCheckFreqSec > 0 {
		sleepSecs = time.Duration(logRotCheckFreqSec) * time.Second
	} else {
		slog.Warn("Log rotation frequency invalid, defaulting to 5 min", "inputValue", logRotCheckFreqSec)
	}

	ticker := time.NewTicker(sleepSecs)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Exiting log rotation loop due to shutdown")
			return
		case <-ticker.C:
			restartConman := logsService.LogRotate(conmanLogsPath)
			if restartConman {
				slog.Info("Log files rotated, signaling conmand")
				conmanService.SignalConmanHUP()
			}
		}
	}
}

func runConman(ctx context.Context, config remoteConsoleConfig, conmanService ConmanService, credService CredsService) {
	if conmanService == nil {
		slog.Error("Conman service is nil")
		panic("Conman service is nil")
	}

	for {
		// Check for shutdown before processing
		select {
		case <-ctx.Done():
			slog.Info("Exiting conman loop due to shutdown")
			return
		default:
		}

		nodes := nodes.CurrentNodes()

		var requireCredentials []string
		for _, nci := range nodes {
			requireCredentials = append(requireCredentials, nci.ID)
		}

		passwords := credService.GetPasswordsWithRetries(requireCredentials, 15, 10)
		hasNodes, err := conmanService.ConfigureConman(nodes, passwords, config.Creds.SshConsoleKeyPath)
		if err != nil {
			slog.Error("Failed to configure conman", "error", err)
			panic(fmt.Sprintf("Failed to configure conman: %s", err))
		}

		if !hasNodes {
			slog.Info("No console nodes found - trying again")
			select {
			case <-ctx.Done():
				slog.Info("Exiting conman loop due to shutdown")
				return
			case <-time.After(30 * time.Second):
			}
		} else {
			err := conmanService.ExecuteConman()
			if err != nil {
				slog.Error("Failed to execute conman", "error", err)
				panic(fmt.Sprintf("Failed to execute conman: %s", err))
			}
		}
		select {
		case <-ctx.Done():
			slog.Info("Exiting conman loop due to shutdown")
			return
		case <-time.After(10 * time.Second):
		}
	}
}

func runService(config remoteConsoleConfig) error {

	slog.Info("Remote console service starting")
	// Set up the zombie killer
	slog.Info("Starting zombie killer")
	go conman.WatchForZombies()

	conmanService := conman.NewConmanService(config.Conman)

	// then we set up the goroutine that controls conman
	conmanLogsPath := filepath.Join(config.Conman.LogsPath, "conman")
	if err := os.MkdirAll(conmanLogsPath, 0755); err != nil {
		log.Fatal(err)
	}

	credsService := creds.NewCredsService(config.Creds)

	// I am not sure that we need this, so I am leaving it out for
	// now, I think that normal logging will work now that we only
	// have one container
	// respinAggLog()

	logsService := logs.NewLogsService(config.Log)
	// Initialize aggregation log early so it is present in the first logrotate config.
	logsService.EnsureAggLog()

	if _, err := credsService.EnsureConsoleKeysPresent(); err != nil {
		slog.Warn("Failed to ensure console SSH keys present", "error", err)
	}

	// Create service context for coordinating shutdown of background goroutines
	serviceCtx, serviceStopCtx := context.WithCancel(context.Background())

	// Start log rotation with callback to signal conman
	go logRotate(serviceCtx, config, conmanService, logsService)

	// spin a thread that watches for changes in console configuration
	go watchForNodesUpdates(serviceCtx, config, conmanService, logsService)

	// start up the thread that runs conman
	go runConman(serviceCtx, config, conmanService, credsService)

	// start the thread that will make sure that the conman creds are correct
	go watchForCredUpdates(serviceCtx, config, credsService, conmanService)

	// Setup a channel to wait for the os to tell us to stop.
	// NOTE - This must be set up before initializing anything that needs
	//  to be cleaned up.  This will trap any signals and wait to
	//  process them until the channel is read.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	// Conman will append "conman" to this path for its logs, so we
	// need to pass that full path to service monitoring the logs
	console.SetupRoutes(conmanLogsPath)

	slog.Info("Starting HTTP server", "address", config.HttpListen)
	server := &http.Server{Addr: config.HttpListen, Handler: console.RequestRouter}

	// signal to cleanly shut down
	go func() {

		// NOTE: do not use log.Fatal as that will immediately exit
		// the program and short-circuit the shutdown logic below
		slog.Info("Server started", "result", server.ListenAndServe())
	}()

	serverCtx, serverStopCtx := context.WithCancel(context.Background())

	// Listen for syscall signals for process to interrupt/quit
	go func() {
		sig := <-sigs
		inShutdown = true
		slog.Info("Detected signal to close service", "signal", sig)

		// Cancel service context to stop background goroutines
		serviceStopCtx()

		// Shutdown signal with grace period of 30 seconds
		shutdownCtx, shutdownCtxCancel := context.WithTimeout(context.Background(), 30*time.Second)

		go func() {
			<-shutdownCtx.Done()
			if shutdownCtx.Err() == context.DeadlineExceeded {
				shutdownCtxCancel()
				log.Fatal("graceful shutdown timed out.. forcing exit.")
			}
		}()

		// Trigger graceful shutdown
		err := server.Shutdown(shutdownCtx)
		if err != nil {
			log.Fatal(err)
		}
		serverStopCtx()
	}()

	// Wait for server context to be stopped
	<-serverCtx.Done()
	slog.Info("Shutdown complete")

	return nil
}
