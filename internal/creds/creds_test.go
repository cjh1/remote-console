package creds

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Cray-HPE/hms-securestorage"
)

func TestGetPasswords(t *testing.T) {
	tempDir := t.TempDir()

	// Setup a local secure storage file
	localStoreFilePath := tempDir + "/secure_store"
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

	passwords, err := getPasswords(config, nodes)
	fmt.Println(passwords["x0c0s1b0"].Username)
	if err != nil {
		t.Fatalf("Error getting passwords: %v", err)
	}

	for node, values := range passwords {
		require.Contains(t, nodes, node)

		expectedPassword, ok := testPasswords[node]
		require.True(t, ok, "Unexpected node %s in passwords", node)

		require.Equal(t, "admin", values.Username)
		require.Equal(t, expectedPassword, values.Password)
	}

	if len(passwords) != len(nodes) {
		t.Fatalf("Expected %d passwords, got %d", len(nodes), len(passwords))
	}
}
