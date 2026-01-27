//  MIT License
//
//  (C) Copyright 2020-2022, 2024 Hewlett Packard Enterprise Development LP
//
//  Permission is hereby granted, free of charge, to any person obtaining a
//  copy of this software and associated documentation files (the "Software"),
//  to deal in the Software without restriction, including without limitation
//  the rights to use, copy, modify, merge, publish, distribute, sublicense,
//  and/or sell copies of the Software, and to permit persons to whom the
//  Software is furnished to do so, subject to the following conditions:
//
//  The above copyright notice and this permission notice shall be included
//  in all copies or substantial portions of the Software.
//
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
//  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
//  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
//  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
//  OTHER DEALINGS IN THE SOFTWARE.

// This file contains the functions to configure and retrieve credentials

package creds

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"os"
	"time"

	compcreds "github.com/Cray-HPE/hms-compcredentials"
	sstorage "github.com/Cray-HPE/hms-securestorage"
)

type CredsService struct {
	config                 CredsConfig
	previousPasswords      map[string]compcreds.CompCredentials
	previousPrivateKeyHash []byte
	previousCertHash       []byte
}

func NewCredsService(config CredsConfig) *CredsService {
	return &CredsService{
		config:                 config,
		previousPasswords:      nil,
		previousPrivateKeyHash: nil,
		previousCertHash:       nil,
	}
}

func NewStorageAdapter(value string) (StorageAdapter, error) {
	adapter := StorageAdapter(value)
	if err := adapter.Validate(); err != nil {
		return "", err
	}
	return adapter, nil
}

func (s StorageAdapter) Validate() error {
	switch s {
	case StorageAdapterVault, StorageAdapterLocal:
		return nil
	default:
		return fmt.Errorf("invalid storage adapter %q", s)
	}
}

type sshKeys struct {
	PrivateKey  string  `json:"privateKey"`
	Certificate *string `json:"certificate"`
}

func createSecureStorage(config CredsConfig) (sstorage.SecureStorage, error) {
	var ss sstorage.SecureStorage
	var err error

	switch config.SecureStorageAdapter {
	case StorageAdapterVault:
		ss, err = sstorage.NewVaultAdapterAs(config.VaultBasePath, config.VaultRole)
		if err != nil {
			return nil, fmt.Errorf("unable to create vault secure storage adapter: %w", err)
		}
	case StorageAdapterLocal:
		ss, err = sstorage.NewLocalSecretStore(config.LocalStoreKey, config.LocalStoreFilePath, false)
		if err != nil {
			return nil, fmt.Errorf("unable to create local file secure storage adapter: %w", err)
		}
	default:
		return nil, fmt.Errorf("invalid secure storage adapter type: %s", config.SecureStorageAdapter)
	}

	return ss, nil
}

// Look up the creds for the input endpoints
func getPasswords(config CredsConfig, bmcXNames []string) (map[string]compcreds.CompCredentials, error) {
	ss, err := createSecureStorage(config)
	if err != nil {
		return nil, fmt.Errorf("error creating secure storage adapter: %w", err)
	}

	ccs := compcreds.NewCompCredStore(config.SecureStoragePasswordsPath, ss)
	ccreds, err := ccs.GetCompCreds(bmcXNames)
	if err != nil {
		return nil, fmt.Errorf("error creating comp creds store: %w", err)
	}

	return ccreds, nil
}

// Look up the creds for the input endpoints with retries
func (cs *CredsService) GetPasswordsWithRetries(ctx context.Context, bmcXNames []string, maxTries, waitSecs int) (map[string]compcreds.CompCredentials, error) {
	var passwords map[string]compcreds.CompCredentials = nil
	var err error = nil
	for numTries := 0; numTries < maxTries; numTries++ {
		select {
		case <-ctx.Done():
			slog.Info("Stopping credential retrieval due to context cancellation")
			return passwords, ctx.Err()
		default:
		}

		slog.Debug("Get passwords with retry", "attempt", numTries)
		passwords, err = getPasswords(cs.config, bmcXNames)

		slog.Debug("Passwords retrieved", "count", len(passwords))

		if err != nil {
			slog.Error("Error retrieving passwords", "error", err)
		}

		foundAll := true
		for _, nn := range bmcXNames {
			_, ok := passwords[nn]
			if !ok {
				slog.Warn("Missing credentials for", "xname", nn)
				foundAll = false
			}
		}
		if foundAll {
			slog.Info("Retrieved all passwords")
			break
		}
		slog.Warn("Only retrieved subset of creds from vault, waiting and trying again",
			"attempt", numTries, "retrieved", len(passwords), "total", len(bmcXNames))

		select {
		case <-ctx.Done():
			slog.Info("Stopping credential retrieval due to context cancellation")
			return passwords, ctx.Err()
		case <-time.After(time.Duration(waitSecs) * time.Second):
		}
	}
	slog.Warn("Maximum password attempts reached, configuring conman with what we have")

	cs.previousPasswords = passwords

	return passwords, err
}

func hashString(s string) ([]byte, error) {
	hasher := sha256.New()
	if _, err := hasher.Write([]byte(s)); err != nil {
		return nil, err
	}
	return hasher.Sum(nil), nil
}

func (cs *CredsService) EnsureConsoleKeysPresent() (bool, error) {
	// Skip if SSH keys path is not configured
	if cs.config.SecureStorageSshKeysPath == "" {
		return false, nil
	}

	retVal := false

	ss, err := createSecureStorage(cs.config)
	if err != nil {
		return false, fmt.Errorf("unable to create secure storage adapter: %w", err)
	}
	var consoleKeys sshKeys
	err = ss.Lookup(cs.config.SecureStorageSshKeysPath, &consoleKeys)
	if err != nil {
		return false, fmt.Errorf("unable to lookup private key: %w", err)
	}

	newHash, err := hashString(consoleKeys.PrivateKey)
	if err != nil {
		return false, fmt.Errorf("failed to hash private ssh key received from vault: %w", err)
	}

	if cs.previousPrivateKeyHash == nil || !(bytes.Equal(newHash, cs.previousPrivateKeyHash)) {
		retVal = true
		cs.previousPrivateKeyHash = newHash
		err = os.WriteFile(cs.config.SshConsoleKeyPath, []byte(consoleKeys.PrivateKey), 0600)
		if err != nil {
			return false, fmt.Errorf("failed to write private ssh key received from vault: %w", err)
		}
		slog.Info("Console ssh key file created")
	} else {
		slog.Warn("Console ssh cert file already exists")
	}

	if consoleKeys.Certificate != nil {
		newHash, err = hashString(*consoleKeys.Certificate)
		if err != nil {
			slog.Error("Failed to hash the public ssh cert received from Vault", "error", err)
		}

		if cs.previousCertHash == nil || !(bytes.Equal(newHash, cs.previousCertHash)) {
			retVal = true
			cs.previousCertHash = newHash
			sshConsoleCertPath := cs.config.SshConsoleKeyPath + "-cert.pub"
			err = os.WriteFile(sshConsoleCertPath, []byte(*consoleKeys.Certificate), 0644)
			if err != nil {
				return false, fmt.Errorf("failed to write public ssh cert: %w", err)
			}
			slog.Info("Console ssh cert file created")
		} else {
			slog.Warn("Console ssh cert file already exists")
		}
	}

	return retVal, nil
}
