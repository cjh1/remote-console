package main

import (
	"context"
	"errors"
	"fmt"
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
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

const smdHTTPTimeout = 15 * time.Second

// ConmanService defines the interface for conman service operations
type ConmanService interface {
	ConfigureConman(nodes map[string]*nodes.NodeConsoleInfo, passwords map[string]compcreds.CompCredentials, sshConsoleKeyPath string) (bool, error)
	ExecuteConman() error
	SignalConmanTERM() error
	SignalConmanHUP() error
}

// CredsService defines the interface for credentials service operations
type CredsService interface {
	GetPasswordsWithRetries(ctx context.Context, bmcXNames []string, maxTries, waitSecs int) (map[string]compcreds.CompCredentials, error)
	EnsureConsoleKeysPresent() (bool, error)
	CheckForUpdates() (bool, error)
}

// LogsService defines the interface for logs service operations
type LogsService interface {
	UpdateLogRotateConf(consoleLogsPath string, nodes map[string]*nodes.NodeConsoleInfo) error
	LogRotate(consoleLogsPath string) bool
	AggregateFiles(consoleLogsPath string, nodes map[string]*nodes.NodeConsoleInfo)
}

// Watch for node updates and signal conman and log rotation as needed
func watchForNodesUpdates(ctx context.Context, config remoteConsoleConfig, httpClient *http.Client, conmanService ConmanService, logsService LogsService) {
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
			changed := nodes.CheckForUpdates(ctx, httpClient, config.SmdURL)

			if changed {
				slog.Info("Node changes detected, signaling conman to restart")
				if err := conmanService.SignalConmanTERM(); err != nil {
					slog.Error("Failed to signal conman with SIGTERM", "error", err)
				}

				nodes := nodes.CurrentNodes()

				// also update log rotation configuration
				slog.Info("Updating log rotation configuration for node changes")
				if err := logsService.UpdateLogRotateConf(conmanLogsPath, nodes); err != nil {
					slog.Error("Failed to update log rotation configuration for node changes", "error", err)
				}

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
				if err := conmanService.SignalConmanTERM(); err != nil {
					slog.Error("Failed to signal conman with SIGTERM", "error", err)
				}
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
	if err := logsService.UpdateLogRotateConf(conmanLogsPath, nodes.CurrentNodes()); err != nil {
		slog.Error("Failed to update log rotation configuration", "error", err)
	}

	sleepDuration := 300 * time.Second
	logRotCheckFreqSec := logConfig.LogRotateCheckFrequency
	if logRotCheckFreqSec > 0 {
		sleepDuration = time.Duration(logRotCheckFreqSec) * time.Second
	} else {
		slog.Warn("Log rotation frequency invalid, defaulting to 5 min", "inputValue", logRotCheckFreqSec)
	}

	ticker := time.NewTicker(sleepDuration)
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
				if err := conmanService.SignalConmanHUP(); err != nil {
					slog.Error("Failed to signal conman with SIGHUP", "error", err)
				}
			}
		}
	}
}

func runConman(ctx context.Context, config remoteConsoleConfig, conmanService ConmanService, credService CredsService) {
	waitWithContext := func(d time.Duration) bool {
		select {
		case <-ctx.Done():
			slog.Info("Exiting conman loop due to shutdown")
			return true
		case <-time.After(d):
			return false
		}
	}

	for {
		// Check for shutdown before processing
		select {
		case <-ctx.Done():
			slog.Info("Exiting conman loop due to shutdown")
			return
		default:
		}

		currentNodes := nodes.CurrentNodes()

		var requireCredentials []string
		for _, nci := range currentNodes {
			requireCredentials = append(requireCredentials, nci.ID)
		}

		passwords, err := credService.GetPasswordsWithRetries(ctx, requireCredentials, 15, 10)
		if err != nil {
			slog.Warn("Credential retrieval ended early", "error", err)
			if errors.Is(err, context.Canceled) {
				return
			}
		}

		hasNodes, err := conmanService.ConfigureConman(currentNodes, passwords, config.Creds.SshConsoleKeyPath)
		if err != nil {
			slog.Error("Failed to configure conman", "error", err)
			if waitWithContext(5 * time.Second) {
				return
			}
			continue
		}

		if !hasNodes {
			slog.Info("No console nodes found - trying again")
			if waitWithContext(30 * time.Second) {
				return
			}
		} else {
			err := conmanService.ExecuteConman()
			if err != nil {
				slog.Error("Failed to execute conman", "error", err)
			}
		}
		if waitWithContext(10 * time.Second) {
			return
		}
	}
}

func runService(config remoteConsoleConfig) error {

	slog.Info("Remote console service starting")
	// Set up the zombie killer
	slog.Info("Starting zombie killer")
	go conman.WatchForZombies()

	conmanService := conman.NewConmanService(config.Conman)

	// Conman will append "conman" to this path for its logs, so we
	// need to pass that full path to service monitoring the logs
	conmanLogsPath := filepath.Join(config.Conman.LogsPath, "conman")
	if err := os.MkdirAll(conmanLogsPath, 0755); err != nil {
		slog.Error("Failed to create console logs directory", "path", conmanLogsPath, "error", err)
		return err
	}

	credsService := creds.NewCredsService(config.Creds)

	logsService, err := logs.NewLogsService(config.Log)
	if err != nil {
		return fmt.Errorf("failed to initialize logs service: %w", err)
	}
	// Initialize aggregation log early so it is present in the first logrotate config.
	logsService.EnsureAggLog()

	if _, err := credsService.EnsureConsoleKeysPresent(); err != nil {
		slog.Warn("Failed to ensure console SSH keys present", "error", err)
	}

	// Create service context for coordinating shutdown of background goroutines
	serviceCtx, serviceStopCtx := context.WithCancel(context.Background())

	// Configure HTTP client for SMD requests
	var smdHTTPClient *http.Client
	if config.Oauth2.TokenURL != "" {
		slog.Info("Configuring OAuth2 client for SMD authentication")

		clientConfig := &clientcredentials.Config{
			ClientID:     config.Oauth2.ClientID,
			ClientSecret: config.Oauth2.ClientSecret,
			TokenURL:     config.Oauth2.TokenURL,
			Scopes:       config.Oauth2.Scopes,
			AuthStyle:    oauth2.AuthStyleInHeader,
		}

		ctx := context.Background()
		ts := clientConfig.TokenSource(ctx)

		// Create HTTP client with OAuth2 transport
		smdHTTPClient = &http.Client{
			Transport: &oauth2.Transport{
				Source: ts,
				Base:   http.DefaultTransport,
			},
			Timeout: smdHTTPTimeout,
		}
		slog.Info("OAuth2 client configured for SMD requests")
	} else {
		// Use default HTTP client without OAuth2
		smdHTTPClient = &http.Client{
			Timeout: smdHTTPTimeout,
		}
	}

	// goroutine for log rotation
	go logRotate(serviceCtx, config, conmanService, logsService)

	// goroutine to watches for changes in console configuration
	go watchForNodesUpdates(serviceCtx, config, smdHTTPClient, conmanService, logsService)

	// goroutine to run conman
	go runConman(serviceCtx, config, conmanService, credsService)

	// goroutine watch for credential updates
	go watchForCredUpdates(serviceCtx, config, credsService, conmanService)

	// Initialize JWT token authorization if JWKS URL is provided
	if config.JwksURL != "" {
		slog.Info("Fetching public key from JWKS URL", "url", config.JwksURL)
		maxRetries := 5
		var lastErr error
		for i := 0; i <= maxRetries; i++ {
			err := console.FetchPublicKeyFromURL(config.JwksURL)
			if err != nil {
				lastErr = err
				slog.Error("Failed to fetch public key from JWKS URL",
					"url", config.JwksURL,
					"attempt", i+1,
					"maxRetries", maxRetries+1,
					"error", err)
				if i < maxRetries {
					time.Sleep(time.Duration(config.JwksFetchInterval) * time.Second)
					continue
				}
			} else {
				slog.Info("Successfully initialized JWT authentication")
				lastErr = nil
				break
			}
		}
		if lastErr != nil {
			// JWKS URL was explicitly provided but we couldn't fetch it
			// This is a fatal error - don't start with unprotected endpoints
			slog.Error("Failed to initialize JWT authentication after all retries - refusing to start with unprotected endpoints")
			serviceStopCtx()
			return fmt.Errorf("failed to fetch JWKS from %s: %w", config.JwksURL, lastErr)
		}
	} else {
		slog.Warn("No JWKS URL provided - JWT authentication is disabled")
	}

	// Setup a channel to wait for the os to tell us to stop.
	// NOTE - This must be set up before initializing anything that needs
	//  to be cleaned up.  This will trap any signals and wait to
	//  process them until the channel is read.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	router := console.SetupRoutes(conmanLogsPath)

	slog.Info("Starting HTTP server", "address", config.HttpListen)
	server := &http.Server{Addr: config.HttpListen, Handler: router}

	// Signal to cleanly shut down
	go func() {

		// NOTE: do not use log.Fatal as that will immediately exit
		// the program and short-circuit the shutdown logic below
		slog.Info("Server started", "result", server.ListenAndServe())
	}()

	serverCtx, serverStopCtx := context.WithCancel(context.Background())

	// Listen for syscall signals for process to interrupt/quit
	go func() {
		sig := <-sigs
		slog.Info("Detected signal to close service", "signal", sig)

		// Cancel service context to stop background goroutines
		serviceStopCtx()

		// Shutdown signal with grace period of 30 seconds
		shutdownCtx, shutdownCtxCancel := context.WithTimeout(context.Background(), 30*time.Second)

		go func() {
			<-shutdownCtx.Done()
			if shutdownCtx.Err() == context.DeadlineExceeded {
				shutdownCtxCancel()
				slog.Error("Graceful shutdown timed out, forcing exit")
				os.Exit(1)
			}
		}()

		// Trigger graceful shutdown
		err := server.Shutdown(shutdownCtx)
		if err != nil {
			slog.Error("Failed to shutdown HTTP server gracefully", "error", err)
			os.Exit(1)
		}
		serverStopCtx()
	}()

	// Wait for server context to be stopped
	<-serverCtx.Done()
	slog.Info("Shutdown complete")

	return nil
}
