package main

import (
	"context"
	"log"
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
func watchForNodesUpdates(config remoteConsoleConfig, conmanService ConmanService, logsService LogsService) {
	if conmanService == nil {
		log.Panicf("Conman service is nil")
	}

	// conman will add the conman directory, so we point the logs service their
	conmanLogsPath := filepath.Join(config.Conman.LogsPath, "conman")

	for {
		// look for new nodes once
		if isShuttingDown() {
			log.Printf("Info: Exiting node watch loop due to shutdown")
			return
		}
		changed := nodes.CheckForUpdates(config.SmdURL)

		if changed {
			log.Printf("Info: Node changes detected, signaling conman to restart")
			conmanService.SignalConmanTERM()

			nodes := nodes.CurrentNodes()

			// also update log rotation configuration
			log.Printf("Info: Node changes detected, updating log rotation configuration")
			logsService.UpdateLogRotateConf(conmanLogsPath, nodes)

			// make sure we are aggregating any new console log files
			log.Printf("Info: Node changes detected, updating log aggregation configuration")
			logsService.AggregateFiles(conmanLogsPath, nodes)
		}

		// Wait for the correct polling interval
		time.Sleep(time.Duration(config.NewNodeLookup) * time.Second)
	}
}

// Watch for credential updates and signal conman as needed
func watchForCredUpdates(config remoteConsoleConfig, credsService CredsService, conmanService ConmanService) {
	time.Sleep(time.Duration(config.CredsMonitorInterval) * time.Second)
	for {
		changed, err := credsService.CheckForUpdates()
		if err != nil {
			log.Printf("Error checking for credential updates: %s", err)
		}

		if changed {
			log.Printf("Info: Credential changes detected, signaling conman to restart")
			conmanService.SignalConmanTERM()
		}

		time.Sleep(time.Duration(config.CredsMonitorInterval) * time.Second)
	}
}

// Log rotation setup and loop
func logRotate(config remoteConsoleConfig, conmanService ConmanService, logsService LogsService) {
	logConfig := config.Log
	// log the log rotation parameters
	log.Printf("LOG ROTATE: Log rotation enabled: %v, Check Freq Sec: %d", logConfig.LogRotateEnabled, logConfig.LogRotateCheckFrequency)
	log.Printf("LOG ROTATE: Log rotation console file size: %s, num rotate: %d", logConfig.ConsoleLogsFileSize, logConfig.ConsoleLogsNumRotate)
	log.Printf("LOG ROTATE: Log rotation aggregation file size: %s, num rotate: %d", logConfig.AggLogsFileSize, logConfig.AggLogsNumRotate)

	// conman will add the conman directory, so we point the logs service their
	conmanLogsPath := filepath.Join(config.Conman.LogsPath, "conman")

	// Create the log rotation configuration file
	logsService.UpdateLogRotateConf(conmanLogsPath, nodes.CurrentNodes())

	sleepSecs := time.Duration(300) * time.Second
	logRotCheckFreqSec := logConfig.LogRotateCheckFrequency
	if logRotCheckFreqSec > 0 {
		sleepSecs = time.Duration(logRotCheckFreqSec) * time.Second
	} else {
		log.Printf("Log rotation frequency invalid, defaulting to 5 min. Input value:%d", logRotCheckFreqSec)
	}

	for {
		restartConman := logsService.LogRotate(conmanLogsPath)
		if restartConman {
			log.Print("LOG ROTATE: Log files rotated, signaling conmand")
			conmanService.SignalConmanHUP()
		}

		time.Sleep(sleepSecs)
	}
}

func runConman(config remoteConsoleConfig, conmanService ConmanService, credService CredsService) {
	if conmanService == nil {
		log.Panicf("Conman service is nil")
	}

	for {
		nodes := nodes.CurrentNodes()

		var requireCredentials []string
		for _, nci := range nodes {
			requireCredentials = append(requireCredentials, nci.ID)
		}

		passwords := credService.GetPasswordsWithRetries(requireCredentials, 15, 10)
		hasNodes, err := conmanService.ConfigureConman(nodes, passwords, config.Creds.SshConsoleKeyPath)
		if err != nil {
			log.Panicf("Error configuring conman: %s", err)
		}

		if !hasNodes {
			log.Printf("No console nodes found - trying again")
			time.Sleep(30 * time.Second)
		} else {
			err := conmanService.ExecuteConman()
			if err != nil {
				log.Panicf("Error executing conman: %s", err)
			}
		}
		time.Sleep(10 * time.Second)
	}
}

func runService(config remoteConsoleConfig) error {

	log.Printf("Remote console service starting")
	// Set up the zombie killer
	log.Printf("Starting zombie killer...")
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
		log.Printf("Error ensuring console SSH keys present: %v", err)
	}

	// Start log rotation with callback to signal conman
	go logRotate(config, conmanService, logsService)

	// spin a thread that watches for changes in console configuration
	go watchForNodesUpdates(config, conmanService, logsService)

	// start up the thread that runs conman
	go runConman(config, conmanService, credsService)

	// start the thread that will make sure that the conman creds are correct

	go watchForCredUpdates(config, credsService, conmanService)

	// Setup a channel to wait for the os to tell us to stop.
	// NOTE - This must be set up before initializing anything that needs
	//  to be cleaned up.  This will trap any signals and wait to
	//  process them until the channel is read.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	// Conman will append "conman" to this path for its logs, so we
	// need to pass that full path to service monitoring the logs
	console.SetupRoutes(conmanLogsPath)

	log.Printf("Spinning up http server...")
	server := &http.Server{Addr: config.HttpListen, Handler: console.RequestRouter}

	// signal to cleanly shut down
	go func() {

		// NOTE: do not use log.Fatal as that will immediately exit
		// the program and short-circuit the shutdown logic below
		log.Printf("Info: Server %s\n", server.ListenAndServe())
	}()

	serverCtx, serverStopCtx := context.WithCancel(context.Background())

	// Listen for syscall signals for process to interrupt/quit
	go func() {
		sig := <-sigs
		inShutdown = true
		log.Printf("Info: Detected signal to close service: %s", sig)

		// Shutdown signal with grace period of 30 seconds
		shutdownCtx, shutdownCtxCancel := context.WithTimeout(serverCtx, 30*time.Second)

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

	// // Wait for server context to be stopped
	<-serverCtx.Done()
	log.Printf("Info: Shutdown complete.")

	return nil
}
