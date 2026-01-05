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
	"crypto/sha256"
	"fmt"
	"log"
	"os"
	"time"

	compcreds "github.com/Cray-HPE/hms-compcredentials"
	sstorage "github.com/Cray-HPE/hms-securestorage"
)

type credsService struct {
	config                 CredsConfig
	previousPasswords      map[string]compcreds.CompCredentials
	previousPrivateKeyHash []byte
	previousCertHash       []byte
}

func NewCredsService(config CredsConfig) *credsService {
	return &credsService{
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

// Look up the creds for the input endpoints with retries
func (cs *credsService) GetPasswordsWithRetries(bmcXNames []string, maxTries, waitSecs int) map[string]compcreds.CompCredentials {
	var passwords map[string]compcreds.CompCredentials = nil
	var err error = nil
	for numTries := 0; numTries < maxTries; numTries++ {
		log.Printf("Get passwords with retry: %d", numTries)
		passwords, err = getPasswords(cs.config, bmcXNames)

		log.Printf("Passwords: %#v", passwords)

		if err != nil {
			log.Printf("Error retrieving passwords: %v", err)
		}

		foundAll := true
		for _, nn := range bmcXNames {
			_, ok := passwords[nn]
			if !ok {
				log.Printf("Missing credentials for %s", nn)
				foundAll = false
			}
		}
		if foundAll {
			log.Printf("Retrieved all passwords")
			break
		}
		log.Printf("Attempt %d - Only retrieved %d of %d creds from vault, waiting and trying again...",
			numTries, len(passwords), len(bmcXNames))
		time.Sleep(time.Duration(waitSecs) * time.Second)
	}
	log.Printf("Maximum password attempts reached, configuring conman with what we have.")

	cs.previousPasswords = passwords

	return passwords
}

func createSecureStorage(config CredsConfig) (sstorage.SecureStorage, error) {
	var ss sstorage.SecureStorage = nil
	var err error = nil
	switch config.SecureStorageAdapter {
	case StorageAdapterVault:
		ss, err = sstorage.NewVaultAdapterAs(config.VaultBasePath, config.VaultRole)
		if err != nil {
			return nil, fmt.Errorf("unable to create vault secure storage adapter: %#v\n", err)
		}
	case StorageAdapterLocal:
		ss, err = sstorage.NewLocalSecretStore(config.LocalStoreKey, config.LocalStoreFilePath, false)
		if err != nil {
			return nil, fmt.Errorf("unable to create local file secure storage adapter: %#v\n", err)
		}
	default:
		return nil, fmt.Errorf("invalid secure storage adapter type: %s\n", config.SecureStorageAdapter)

	}

	return ss, nil
}

// Look up the creds for the input endpoints
func getPasswords(config CredsConfig, bmcXNames []string) (map[string]compcreds.CompCredentials, error) {
	ss, err := createSecureStorage(config)
	if err != nil {
		return nil, fmt.Errorf("error creating secure storage adapter %#v\n", err)
	}

	ccs := compcreds.NewCompCredStore(config.SecureStoragePasswordsPath, ss)
	ccreds, err := ccs.GetCompCreds(bmcXNames)
	if err != nil {
		return nil, fmt.Errorf("error create comp creds store: %#v\n", err)
	}

	return ccreds, nil
}

func HashString(s string) ([]byte, error) {
	hasher := sha256.New()
	if _, err := hasher.Write([]byte(s)); err != nil {
		return nil, err
	}
	return hasher.Sum(nil), nil
}

func (cs *credsService) EnsureConsoleKeysPresent() (bool, error) {
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

	newHash, err := HashString(consoleKeys.PrivateKey)
	if err != nil {
		return false, fmt.Errorf("failed to hash the private ssh key received from Vault. %w", err)
	} else if cs.previousPrivateKeyHash == nil || !(bytes.Equal(newHash, cs.previousPrivateKeyHash)) {
		retVal = true
		cs.previousPrivateKeyHash = newHash
		err = os.WriteFile(cs.config.SshConsoleKeyPath, []byte(consoleKeys.PrivateKey), 0600)
		if err != nil {
			return false, fmt.Errorf("failed to write our the private ssh key received from Vault. Err: %w", err)
		}
		log.Printf("Console ssh key file created")
	} else {
		log.Printf("Console ssh key file already exists")
	}

	if consoleKeys.Certificate != nil {
		newHash, err = HashString(*consoleKeys.Certificate)
		if err != nil {
			log.Printf("Error: Failed to hash the public ssh cert received from Vault. Err: %s", err)
		} else if cs.previousCertHash == nil || !(bytes.Equal(newHash, cs.previousCertHash)) {
			retVal = true
			cs.previousCertHash = newHash
			sshConsoleCertPath := cs.config.SshConsoleKeyPath + "-cert.pub"
			err = os.WriteFile(sshConsoleCertPath, []byte(*consoleKeys.Certificate), 0644)
			if err != nil {
				return false, fmt.Errorf("failed to write our the public ssh cert %w", err)
			}
			log.Printf("Console ssh cert file created")
		} else {
			log.Printf("Console ssh cert file already exists")
		}
	}

	return retVal, nil
}
