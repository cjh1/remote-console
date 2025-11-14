package main

import (
	"fmt"
	"path/filepath"

	"github.com/OpenCHAMI/remote-console/internal/conman"
	"github.com/OpenCHAMI/remote-console/internal/creds"
	"github.com/OpenCHAMI/remote-console/internal/logs"
)

type remoteConsoleConfig struct {
	Log                  logs.LogConfig `flag:"-"`
	Conman               conman.ConmanConfig
	Creds                creds.CredsConfig
	HttpListen           string `desc:"HTTP listen address"`
	NewNodeLookup        int    `desc:"Interval in seconds to look for new nodes"`
	CredsMonitorInterval int    `desc:"Interval in seconds to monitor credential updates"`
	SmdURL               string `desc:"URL for the SMD service"`
	DebugOnly            bool   `flag:"-"`
}

func DefaultConfig() remoteConsoleConfig {
	return remoteConsoleConfig{
		Log:                  logs.DefaultLogConfig(),
		Conman:               conman.DefaultConmanConfig(),
		Creds:                creds.DefaultCredsConfig(),
		HttpListen:           "0.0.0.0:8080",
		NewNodeLookup:        120,
		CredsMonitorInterval: 30,
		DebugOnly:            false,
		SmdURL:               "http://cray-smd/",
	}
}

func validateCredsConfig(config *remoteConsoleConfig) error {
	credConfig := config.Creds

	// Copy over DebugOnly
	credConfig.DebugOnly = config.DebugOnly

	if credConfig.SecureStorageAdapter != "" {
		_, err := creds.NewStorageAdapter(string(credConfig.SecureStorageAdapter))
		if err != nil {
			return fmt.Errorf("invalid secure storage adapter: %s, valid values are (vault or local)", credConfig.SecureStorageAdapter)
		}

		if credConfig.SecureStorageAdapter == creds.StorageAdapterLocal {
			if credConfig.LocalStoreFilePath == "" {
				return fmt.Errorf("a local storage path must be set when using the local secure storage adapter")
			}

			if credConfig.LocalStoreKey == "" {
				return fmt.Errorf("a local storage key must be set when using the local secure storage adapter")
			}
		}
	}

	return nil
}

func validateLogsConfig(config *remoteConsoleConfig) error {
	// Copy over ConsoleLogsPath
	conmanConfig := config.Conman

	// conman will add the conman directory, so we point the logs service their
	config.Log.ConsoleLogsPath = filepath.Join(conmanConfig.LogsPath, "conman")


	return nil
}

func validateConmanConfig(config *remoteConsoleConfig) error {
	// Copy over DebugOnly
	config.Conman.DebugOnly = config.DebugOnly
	
	return nil
}

func validateConfig(config *remoteConsoleConfig) error {
	err := validateLogsConfig(config)
	if err != nil {
		return err
	}

	err = validateConmanConfig(config)
	if err != nil {
		return err
	}

	if err := validateCredsConfig(config); err != nil {
		return err
	}

	return nil
}
