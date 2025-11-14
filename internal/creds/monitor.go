// MIT License
// (C) Copyright 2023-2024 Hewlett Packard Enterprise Development LP
//
// This file contains the functions to monitor for changes in keys and certs

package creds

import (
	"fmt"
	"log"

	"github.com/OpenCHAMI/remote-console/internal/nodes"
)

type SignalConmanTERM func()

// Time to wait between checking for credential changes
var MonitorIntervalSecs int = 30

// function to do check for credential changes and restart conman if necessary
func (cs *credsService) CheckForUpdates() (bool, error) {
	restartConman := false

	var ids []string = nil

	currentNodes := nodes.CurrentNodes()
	for _, nci := range currentNodes {
		ids = append(ids, nci.ID)
	}

	// Only check keys if SecureStorageSshKeysPath is configured
	if cs.config.SecureStorageSshKeysPath != "" {
		changed, err := cs.checkIfKeysChanged()
		if err != nil {
			return false, err
		}
		restartConman = changed
	}

	changed, err := cs.checkIfPasswordsChanged(ids)
	if err != nil {
		return false, err
	}

	restartConman = len(ids) > 0 && changed || restartConman

	return restartConman, nil
}

func (cs *credsService) checkIfPasswordsChanged(xnames []string) (bool, error) {
	if cs.previousPasswords == nil {
		fmt.Printf("No previous passwords stored, cannot check for changes\n")
		return false, nil
	}
	currentPasswords, err := getPasswords(cs.config, xnames)

	if err != nil {
		log.Printf("Error retrieving passwords while checking for credential changes: %v", err)
		return false, err
	}
	for _, xname := range xnames {
		currentCreds, ok := currentPasswords[xname]
		if !ok {
			log.Printf("Missing credentials detected for %s while checking for credential changes", xname)
			continue
		}
		previousCreds, _ := cs.previousPasswords[xname]

		if (currentCreds.Username != previousCreds.Username) || (currentCreds.Password != previousCreds.Password) {
			log.Printf("Change detected in the passwords.  Conman will be reconfigured.")
			return true, nil
		}
	}

	return false, nil
}

func (cs *credsService) checkIfKeysChanged() (bool, error) {
	return cs.EnsureConsoleKeysPresent()
}
