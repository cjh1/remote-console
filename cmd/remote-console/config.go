package main

import (
	"fmt"
	"slices"

	"github.com/OpenCHAMI/remote-console/internal/conman"
	"github.com/OpenCHAMI/remote-console/internal/creds"
	"github.com/OpenCHAMI/remote-console/internal/logs"
)

type OAuth2Config struct {
	ClientID     string   `desc:"OAuth2 client ID for SMD authentication"`
	ClientSecret string   `desc:"OAuth2 client secret for SMD authentication"`
	TokenURL     string   `desc:"OAuth2 token endpoint URL for SMD authentication"`
	Scopes       []string `desc:"OAuth2 scopes for SMD authentication"`
}

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
	Oauth2               OAuth2Config
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
		// Note: Oauth2 vs OAuth2 so the sflags generate the correct flag name
		Oauth2: OAuth2Config{},
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

	// Validate OAuth2 configuration - either all or nothing
	oauth2 := config.Oauth2

	oauth2Provided := []bool{
		oauth2.ClientID != "",
		oauth2.ClientSecret != "",
		oauth2.TokenURL != "",
		len(oauth2.Scopes) > 0,
	}

	// All
	allProvided := !slices.Contains(oauth2Provided, false)

	// Nothing
	noneProvided := !slices.Contains(oauth2Provided, true)

	if !allProvided && !noneProvided {
		return fmt.Errorf("incomplete OAuth2 configuration: all fields (oauth2-client-id, oauth2-client-secret, oauth2-token-url and oauth2-scopes) must be provided")
	}

	return nil
}
