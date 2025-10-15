package creds

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Cray-HPE/hms-securestorage"
)

func TestCheckIfPasswordsChanged(t *testing.T) {
	tempDir := t.TempDir()

	// Setup a local secure storage file
	localStoreFilePath := filepath.Join(tempDir, "secure_store")
	localStoreKey, err := securestorage.GenerateMasterKey()
	require.NoError(t, err)
	ss, err := securestorage.NewLocalSecretStore(localStoreKey, localStoreFilePath, true)
	require.NoError(t, err)

	testPasswords := map[string]string{
		"x0c0s1b0": "password1",
		"x0c0s1b1": "password2",
	}
	nodes := []string{"x0c0s1b0", "x0c0s1b1"}

	for node, password := range testPasswords {
		value := map[string]string{
			"Username": "admin",
			"Password": password,
			"Xname":    node,
			"URL":      "https://" + node + "/redfish/v1/Managers/BMC",
		}
		err = ss.Store(fmt.Sprintf("hms-creds/%s", node), value)
		require.NoError(t, err)
	}

	config := DefaultCredsConfig()
	config.SecureStorageAdapter = StorageAdapterLocal
	config.LocalStoreFilePath = localStoreFilePath
	config.LocalStoreKey = localStoreKey

	service := NewCredsService(config)

	changed, err := service.checkIfPasswordsChanged(nodes)
	if err != nil {
		t.Fatalf("Error checking if passwords changed: %v", err)
	}

	require.False(t, changed, "Passwords should not have changed")

	// Call GetPasswordsWithRetry to set previousPasswords
	_, err = service.GetPasswordsWithRetries(context.Background(), nodes, 3, 1)
	require.NoError(t, err)

	// Now change a password
	value := map[string]string{
		"Username": "admin",
		"Password": "newpassword",
		"Xname":    "x0c0s1b0",
		"URL":      "https://x0c0s1b0/redfish/v1/Managers/BMC",
	}
	err = ss.Store("hms-creds/x0c0s1b0", value)
	require.NoError(t, err)

	changed, err = service.checkIfPasswordsChanged(nodes)
	if err != nil {
		t.Fatalf("Error checking if passwords changed: %v", err)
	}

	require.True(t, changed, "Passwords should have changed")
}

func TestCheckIfKeysChanged(t *testing.T) {
	tempDir := t.TempDir()

	// Setup a local secure storage file
	localStoreFilePath := filepath.Join(tempDir, "secure_store")
	localStoreKey, err := securestorage.GenerateMasterKey()
	require.NoError(t, err)
	ss, err := securestorage.NewLocalSecretStore(localStoreKey, localStoreFilePath, true)
	require.NoError(t, err)

	config := DefaultCredsConfig()
	config.SecureStorageAdapter = StorageAdapterLocal
	config.LocalStoreFilePath = localStoreFilePath
	config.LocalStoreKey = localStoreKey
	config.SshConsoleKeyPath = filepath.Join(tempDir, "conman.key")
	config.SecureStorageSshKeysPath = "bmc-console-keys"

	// Save test key
	testKey := "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC7..."
	value := map[string]string{
		"PrivateKey": testKey,
	}
	err = ss.Store(config.SecureStorageSshKeysPath, value)
	require.NoError(t, err)

	service := NewCredsService(config)

	changed, err := service.checkIfKeysChanged()
	require.NoError(t, err)
	require.True(t, changed, "Keys should be considered changed on first check")

	// Check again without changing keys
	changed, err = service.checkIfKeysChanged()
	require.NoError(t, err)
	require.False(t, changed, "Keys should not have changed")

	// Now change the key
	newTestKey := "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQD8..."
	value = map[string]string{
		"PrivateKey": newTestKey,
	}
	err = ss.Store(config.SecureStorageSshKeysPath, value)
	require.NoError(t, err)

	changed, err = service.checkIfKeysChanged()
	require.NoError(t, err)
	require.True(t, changed, "Keys should have changed after update")
}
