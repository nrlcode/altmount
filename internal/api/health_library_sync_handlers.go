package api

import (
	"errors"
	"log/slog"

	"github.com/gofiber/fiber/v2"
	"github.com/javi11/altmount/internal/health"
)

// LibrarySyncHandlers holds the library sync-related request handlers
type LibrarySyncHandlers struct {
	librarySyncWorker *health.LibrarySyncWorker
	configManager     ConfigManager
}

// NewLibrarySyncHandlers creates a new instance of library sync handlers
func NewLibrarySyncHandlers(librarySyncWorker *health.LibrarySyncWorker, configManager ConfigManager) *LibrarySyncHandlers {
	return &LibrarySyncHandlers{
		librarySyncWorker: librarySyncWorker,
		configManager:     configManager,
	}
}

// handleGetLibrarySyncStatus handles GET /api/health/library-sync/status
func (h *LibrarySyncHandlers) handleGetLibrarySyncStatus(c *fiber.Ctx) error {
	status := h.librarySyncWorker.GetStatus()
	return RespondSuccess(c, status)
}

// handleStartLibrarySync handles POST /api/health/library-sync/start
func (h *LibrarySyncHandlers) handleStartLibrarySync(c *fiber.Ctx) error {
	err := h.librarySyncWorker.TriggerManualSync(c.Context())
	if err != nil {
		slog.ErrorContext(c.Context(), "Failed to trigger library sync", "error", err)
		return RespondConflict(c, "Library sync already running", err.Error())
	}

	return RespondMessage(c, "Library sync triggered successfully")
}

// handleCancelLibrarySync handles POST /api/health/library-sync/cancel
func (h *LibrarySyncHandlers) handleCancelLibrarySync(c *fiber.Ctx) error {
	// Cancel only the active scan. The periodic supervisor remains available
	// for a later manual trigger and future scheduled discovery cycles.
	if err := h.librarySyncWorker.CancelActiveSync(c.Context()); err != nil {
		return RespondInternalError(c, "Failed to cancel library sync", "")
	}

	return RespondMessage(c, "Library sync cancelled successfully")
}

// handleDryRunLibrarySync handles POST /api/health/library-sync/dry-run
func (h *LibrarySyncHandlers) handleDryRunLibrarySync(c *fiber.Ctx) error {
	result, err := h.librarySyncWorker.RunLibrarySyncChecked(c.Context(), true)
	if errors.Is(err, health.ErrLibrarySyncScanAlreadyRunning) {
		return RespondConflict(c, "Library sync already running", "")
	}
	if err != nil {
		slog.ErrorContext(c.Context(), "Library sync dry run failed")
		return RespondInternalError(c, "Failed to perform dry run", "")
	}
	if result == nil {
		slog.ErrorContext(c.Context(), "Dry run returned nil result")
		return RespondInternalError(c, "Failed to perform dry run", "")
	}

	// Convert internal DryRunResult to API DryRunSyncResult
	apiResult := DryRunSyncResult{
		OrphanedMetadataCount:  result.OrphanedMetadataCount,
		OrphanedLibraryFiles:   result.OrphanedLibraryFiles,
		DatabaseRecordsToClean: result.DatabaseRecordsToClean,
		WouldCleanup:           result.WouldCleanup,
	}

	return RespondSuccess(c, apiResult)
}

// handleGetSyncNeeded handles GET /api/health/library-sync/needed
// Returns whether a library sync is needed due to configuration changes
func (h *LibrarySyncHandlers) handleGetSyncNeeded(c *fiber.Ctx) error {
	needsSync := false
	reason := ""

	if h.configManager != nil && h.configManager.NeedsLibrarySync() {
		needsSync = true
		reason = "mount_path_changed"
	}

	return RespondSuccess(c, fiber.Map{
		"needs_sync": needsSync,
		"reason":     reason,
	})
}
