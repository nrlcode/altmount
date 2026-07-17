package stremio

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/metadata"
)

// StremioCleanupService periodically removes expired Stremio-originated queue items
// along with their associated .meta files, storage directory, and persistent NZB files.
type StremioCleanupService struct {
	queueRepo       *database.Repository
	metadataService *metadata.MetadataService
	configGetter    config.ConfigGetter
}

// NewStremioCleanupService creates a new StremioCleanupService.
func NewStremioCleanupService(
	queueRepo *database.Repository,
	metadataService *metadata.MetadataService,
	configGetter config.ConfigGetter,
) *StremioCleanupService {
	return &StremioCleanupService{
		queueRepo:       queueRepo,
		metadataService: metadataService,
		configGetter:    configGetter,
	}
}

// StartCleanup launches a background goroutine that runs cleanup every hour.
// The goroutine stops when ctx is cancelled.
func (s *StremioCleanupService) StartCleanup(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.cleanupExpired(ctx)
			}
		}
	}()
}

func (s *StremioCleanupService) cleanupExpired(ctx context.Context) {
	cfg := s.configGetter()
	ttlHours := cfg.Stremio.NzbTTLHours
	if ttlHours <= 0 {
		return
	}

	items, err := s.queueRepo.GetExpiredStremioQueueItems(ctx, ttlHours)
	if err != nil {
		slog.ErrorContext(ctx, "StremioCleanup: failed to query expired items", "error", err)
		return
	}

	for _, item := range items {
		s.deleteItem(ctx, item)
	}

	if len(items) > 0 {
		slog.InfoContext(ctx, "StremioCleanup: cleaned up expired items", "count", len(items))
	}
}

func (s *StremioCleanupService) deleteItem(ctx context.Context, item *database.ImportQueueItem) {
	cfg := s.configGetter()
	storeRoot := filepath.Join(filepath.Dir(cfg.Database.Path), ".nzbs")
	if err := s.metadataService.ConfigureCleanupRoots(
		storeRoot,
		filepath.Join(os.TempDir(), ".altmount-queue"),
		storeRoot,
	); err != nil {
		slog.ErrorContext(ctx, "StremioCleanup: failed to configure cleanup authority", "error", err)
		return
	}

	storagePath := ""
	if item.StoragePath != nil {
		storagePath = *item.StoragePath
	}
	if err := s.metadataService.DeleteStoragePathWithSourceNzb(ctx, storagePath, item.NzbPath); err != nil {
		slog.ErrorContext(ctx, "StremioCleanup: failed to delete item",
			"storage_path", storagePath, "nzb_path", item.NzbPath, "error", err)
		return
	}

	if err := s.queueRepo.RemoveFromQueue(ctx, item.ID); err != nil {
		slog.ErrorContext(ctx, "StremioCleanup: failed to remove queue item", "id", item.ID, "error", err)
	}
}
