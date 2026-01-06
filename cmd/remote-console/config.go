package main

import (
	"fmt"

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
	JwksURL              string `desc:"JWKS URL for fetching public keys for JWT validation (optional)"`
	JwksFetchInterval    int    `desc:"Interval in seconds to retry fetching JWKS on failure"`
}

func DefaultConfig() remoteConsoleConfig {
	return remoteConsoleConfig{
		Log:                  logs.DefaultLogConfig(),
		Conman:               conman.DefaultConmanConfig(),
		Creds:                creds.DefaultCredsConfig(),
		HttpListen:           "0.0.0.0:26776",
		NewNodeLookup:        120,
		CredsMonitorInterval: 30,
		SmdURL:               "http://cray-smd/",
		JwksURL:              "",
		JwksFetchInterval:    5,
	}
}

func validateCredsConfig(config *remoteConsoleConfig) error {
	credConfig := config.Creds

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

func validateConfig(config *remoteConsoleConfig) error {
	if err := validateCredsConfig(config); err != nil {
		return err
	}

	return nil
}
