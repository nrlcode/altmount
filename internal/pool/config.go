package pool

import (
	"context"
	"log/slog"

	"github.com/javi11/altmount/internal/config"
)

// RegisterConfigHandlers registers handlers for pool-related configuration changes
func RegisterConfigHandlers(ctx context.Context, configManager *config.Manager, poolManager Manager) {
	// Initial ID mapping
	updateProviderIDMap(configManager.GetConfig(), poolManager)
	// Initial import connection budget: the pool's total connection capacity.
	poolManager.SetImportConnCapacity(configManager.GetConfig().TotalProviderConnections())

	configManager.OnConfigChange(func(oldConfig, newConfig *config.Config) {
		slog.InfoContext(ctx, "Configuration updated")

		updateProviderIDMap(newConfig, poolManager)
		handleProviderChanges(ctx, oldConfig, newConfig, poolManager)

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

// updateProviderIDMap provides a mapping of pool names to config IDs to the pool manager
func updateProviderIDMap(cfg *config.Config, poolManager Manager) {
	idMap := make(map[string]string)
	for _, p := range cfg.Providers {
		idMap[p.NNTPPoolName()] = p.ID
	}
	poolManager.SetProviderIDs(idMap)
}

// handleProviderChanges rebuilds the transport from the complete configured
// sequence whenever provider state changes. Incremental remove/add operations
// can append a modified provider and silently destroy primary/backup priority.
func handleProviderChanges(ctx context.Context, oldConfig, newConfig *config.Config, poolManager Manager) {
	changes := oldConfig.ProvidersDiff(newConfig)
	if changes == nil && !oldConfig.ProvidersOrderChanged(newConfig) {
		return
	}

	slog.InfoContext(ctx, "NNTP providers changed - rebuilding configured order",
		"change_count", len(changes),
		"provider_count", len(newConfig.Providers))
	if err := poolManager.SetProviders(newConfig.ToNNTPProviders()); err != nil {
		slog.ErrorContext(ctx, "Failed to rebuild NNTP connection pool", "err", err)
	}
}
