package pool

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/nntppool/v4"
	"github.com/stretchr/testify/require"
)

func TestPR3FailedOrderedRebuildKeepsExistingPoolUsable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	manager := NewManager(ctx, nil)
	t.Cleanup(func() { _ = manager.ClearPool() })

	var calls atomic.Int32
	require.NoError(t, manager.SetProviders([]nntppool.Provider{{
		ID: "existing", Factory: responsiveStatFactory(t, &calls), Connections: 1, StatInflight: 1, SkipPing: true,
	}}))
	existing, err := manager.GetPool()
	require.NoError(t, err)

	err = manager.SetProviders([]nntppool.Provider{
		{ID: "duplicate", Factory: responsiveStatFactory(t, &calls), Connections: 1, StatInflight: 1, SkipPing: true},
		{ID: "duplicate", Factory: responsiveStatFactory(t, &calls), Connections: 1, StatInflight: 1, SkipPing: true},
	})
	require.Error(t, err)
	require.True(t, manager.HasPool(),
		"a rejected configuration must not destroy the last working transport")

	retained, err := manager.GetPool()
	require.NoError(t, err)
	require.Same(t, existing, retained)
	statCtx, statCancel := context.WithTimeout(ctx, time.Second)
	defer statCancel()
	result, err := retained.Stat(statCtx, "synthetic@test.invalid")
	require.NoError(t, err)
	require.Equal(t, "existing", result.ProviderID)
}

func TestPR3ClearingProvidersStopsQuotaWatcher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	publicManager := NewManager(ctx, nil)
	manager := publicManager.(*manager)
	t.Cleanup(func() { _ = publicManager.ClearPool() })

	var calls atomic.Int32
	require.NoError(t, publicManager.SetProviders([]nntppool.Provider{{
		ID: "existing", Factory: responsiveStatFactory(t, &calls), Connections: 1, StatInflight: 1, SkipPing: true,
	}}))
	manager.mu.RLock()
	require.NotNil(t, manager.quotaWatchCancel)
	manager.mu.RUnlock()

	require.NoError(t, publicManager.SetProviders(nil))
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	require.Nil(t, manager.quotaWatchCancel,
		"clearing the last provider must stop its orphaned quota watcher")
}
