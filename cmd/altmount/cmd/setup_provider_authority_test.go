package cmd

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/pool"
	"github.com/javi11/nntppool/v4"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type setupAuthorityPool struct {
	pool.Manager
	prepared []nntppool.Provider
	commits  int
	capacity int
}

type emptyProviderIdentitySource struct{}

func (emptyProviderIdentitySource) ReadProviderIdentityRegistrySnapshot(context.Context) (config.ProviderIdentityRegistrySnapshot, error) {
	return config.ProviderIdentityRegistrySnapshot{}, nil
}

func (p *setupAuthorityPool) SetImportConnCapacity(capacity int) {
	p.capacity = capacity
}

func (p *setupAuthorityPool) HasPool() bool {
	return p.commits == 1 && len(p.prepared) > 0
}

func (p *setupAuthorityPool) PrepareProviders(_ context.Context, providers []nntppool.Provider) (config.PreparedChange, error) {
	p.prepared = append([]nntppool.Provider(nil), providers...)
	return config.PreparedChange{Commit: func() { p.commits++ }}, nil
}

func TestSetupNNTPPoolCommitsNormalizedProviderIdentity(t *testing.T) {
	dir := t.TempDir()
	configFile := filepath.Join(dir, "config.yaml")
	cfg := config.DefaultConfig(dir)
	enabled, backup := true, false
	cfg.Providers = []config.ProviderConfig{{
		Host:             "news.example",
		Port:             563,
		Username:         "account",
		MaxConnections:   2,
		Enabled:          &enabled,
		IsBackupProvider: &backup,
	}}

	manager := config.NewManager(cfg, configFile)
	manager.SetProviderIdentitySource(emptyProviderIdentitySource{})
	poolManager := &setupAuthorityPool{}
	committed, err := setupNNTPPool(context.Background(), manager, poolManager)
	require.NoError(t, err)
	require.Len(t, committed.Providers, 1)
	require.NotEmpty(t, committed.Providers[0].ID)
	require.Equal(t, 2, poolManager.capacity)
	require.Equal(t, 1, poolManager.commits)
	require.Len(t, poolManager.prepared, 1)
	require.Equal(t, committed.Providers[0].ID, poolManager.prepared[0].ID)

	snapshot, err := manager.Snapshot()
	require.NoError(t, err)
	require.Equal(t, committed.Providers[0].ID, snapshot.Config.Providers[0].ID)

	data, err := os.ReadFile(configFile)
	require.NoError(t, err)
	var persisted config.Config
	require.NoError(t, yaml.Unmarshal(data, &persisted))
	require.Equal(t, committed.Providers[0].ID, persisted.Providers[0].ID)
}
