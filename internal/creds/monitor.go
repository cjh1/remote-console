// MIT License
// (C) Copyright 2023-2024 Hewlett Packard Enterprise Development LP
//
// This file contains the functions to monitor for changes in keys and certs

package creds

import (
	"log/slog"

	"github.com/OpenCHAMI/remote-console/internal/nodes"
)

type SignalConmanTERM func()

// function to do check for credential changes
func (cs *CredsService) CheckForUpdates() (bool, error) {
	currentNodes := nodes.CurrentNodes()
	ids := make([]string, 0, len(currentNodes))
	for _, nci := range currentNodes {
		ids = append(ids, nci.ID)
	}

	keysChanged := false
	// Only check keys if SecureStorageSshKeysPath is configured
	if cs.config.SecureStorageSshKeysPath != "" {
		var err error
		keysChanged, err = cs.checkIfKeysChanged()
		if err != nil {
			return false, err
		}
	}

	passwordsChanged, err := cs.checkIfPasswordsChanged(ids)
	if err != nil {
		return false, err
	}

	return (len(ids) > 0 && passwordsChanged) || keysChanged, nil
}

func (cs *CredsService) checkIfPasswordsChanged(xnames []string) (bool, error) {
	if cs.previousPasswords == nil {
		return false, nil
	}
	currentPasswords, err := getPasswords(cs.config, xnames)

	if err != nil {
		slog.Error("Error retrieving passwords while checking for credential changes", "error", err)
		return false, err
	}
	for _, xname := range xnames {
		currentCreds, ok := currentPasswords[xname]
		if !ok {
			slog.Warn("Missing credentials detected while checking for credential changes", "xname", xname)
			continue
		}
		previousCreds := cs.previousPasswords[xname]

		if (currentCreds.Username != previousCreds.Username) || (currentCreds.Password != previousCreds.Password) {
			slog.Info("Change detected in the passwords. Conman will be reconfigured.")
			return true, nil
		}
	}

	return false, nil
}

func (cs *CredsService) checkIfKeysChanged() (bool, error) {
	return cs.EnsureConsoleKeysPresent()
}
