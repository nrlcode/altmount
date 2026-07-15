package config

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPR5ConfigUpdatesCannotReintroduceEmptyProviderIdentity(t *testing.T) {
	enabled := true
	current := DefaultConfig()
	current.Providers = []ProviderConfig{{
		ID: "durable-provider", Name: "Primary", Host: "primary.invalid", Port: 119,
		Username: "synthetic-account", MaxConnections: 1, Enabled: &enabled,
	}}
	manager := NewManager(current, filepath.Join(t.TempDir(), "config.yaml"))

	updated := current.DeepCopy()
	updated.Providers[0].ID = ""
	require.NoError(t, updated.Validate(),
		"startup validation must continue accepting legacy empty IDs for one-time backfill")
	require.Error(t, manager.ValidateConfigUpdate(updated),
		"a live update must not rebuild nntppool with endpoint-derived identity")
}
