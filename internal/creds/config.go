package creds

type StorageAdapter string

const (
	StorageAdapterVault StorageAdapter = "vault"
	StorageAdapterLocal StorageAdapter = "local"
)

type CredsConfig struct {
	SshConsoleKeyPath          string         `desc:"Path where the SSH private key file for console access will be writen to."`
	SecureStorageAdapter       StorageAdapter `desc:"Type of secure storage adapter to use for credentials retrieval."`
	VaultBasePath              string         `desc:"Base path in Vault where credentials are stored."`
	VaultRole                  string         `desc:"Vault role to use when authenticating to Vault."`
	LocalStoreFilePath         string         `desc:"Path to local secure storage file."`
	LocalStoreKey              string         `desc:"Key to use for local secure storage decryption."`
	SecureStorageSshKeysPath   string         `desc:"Path where the SSH keys can be found in secure storage. Leave empty to skip SSH key management."`
	SecureStoragePasswordsPath string         `desc:"Path where the console credentials access can be found in secure storage."`
}

func DefaultCredsConfig() CredsConfig {
	return CredsConfig{
		SshConsoleKeyPath:          "/app/conman.key",
		VaultBasePath:              "",
		VaultRole:                  "",
		SecureStorageAdapter:       StorageAdapterVault,
		LocalStoreFilePath:         "",
		LocalStoreKey:              "",
		SecureStorageSshKeysPath:   "",
		SecureStoragePasswordsPath: "hms-creds",
	}
}
