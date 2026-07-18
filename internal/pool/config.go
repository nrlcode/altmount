package pool

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/nntppool/v4"
)

type providerPreparer interface {
	PrepareProviders(context.Context, []nntppool.Provider) (config.PreparedChange, error)
}

// RegisterConfigHandlers registers handlers for pool-related configuration changes
func RegisterConfigHandlers(ctx context.Context, configManager *config.Manager, poolManager Manager) {
	// Initial import connection budget: the pool's total connection capacity.
	poolManager.SetImportConnCapacity(configManager.GetConfig().TotalProviderConnections())
	configManager.SetPrecommit(func(prepareCtx context.Context, oldConfig, newConfig *config.Config) (config.PreparedChange, error) {
		return prepareProviderChanges(prepareCtx, oldConfig, newConfig, poolManager)
	})

	configManager.OnConfigChange(func(oldConfig, newConfig *config.Config) {
		slog.InfoContext(ctx, "Configuration updated")

		// Keep the import connection budget in sync with provider capacity.
		if capacity := newConfig.TotalProviderConnections(); capacity != oldConfig.TotalProviderConnections() {
			slog.InfoContext(ctx, "Import connection budget updated", "capacity", capacity)
			poolManager.SetImportConnCapacity(capacity)
		}

		// Log changes that still require restart
		if oldConfig.Metadata.RootPath != newConfig.Metadata.RootPath {
			slog.InfoContext(ctx, "Metadata root path changed (restart required)",
				"old", oldConfig.Metadata.RootPath,
				"new", newConfig.Metadata.RootPath)
		}
	})
}

func prepareProviderChanges(ctx context.Context, oldConfig, newConfig *config.Config, poolManager Manager) (config.PreparedChange, error) {
	newProviders := newConfig.ToNNTPProviders()
	if poolManager.HasPool() && oldConfig.ProvidersEqual(newConfig) {
		return config.PreparedChange{}, nil
	}
	preparer, ok := poolManager.(providerPreparer)
	if !ok {
		return config.PreparedChange{}, fmt.Errorf("pool manager does not support transactional provider preparation")
	}
	slog.InfoContext(ctx, "Preparing NNTP providers in configured order",
		"provider_count", len(newProviders))
	return preparer.PrepareProviders(ctx, newProviders)
}
