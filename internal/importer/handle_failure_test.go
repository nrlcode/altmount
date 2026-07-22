package importer

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/metadata"
)

// newMoveToFailedTestService builds a minimal *Service with a real SQLite DB so that
// MoveToFailedFolder can call UpdateQueueItemNzbPath without panicking. The config
// is wired so that GetFailedNzbFolder() returns <configDir>/.nzbs/failed/.
func newMoveToFailedTestService(t *testing.T) *Service {
	service, _ := newMoveToFailedTestServiceWithDB(t)
	return service
}

func newMoveToFailedTestServiceWithDB(t *testing.T) (*Service, *sql.DB) {
	t.Helper()

	setImporterTempRoot(t, t.TempDir())
	configDir := t.TempDir()

	// Use a per-test unique in-memory SQLite DB to avoid shared-cache collisions.
	dbDSN := "file:" + t.Name() + "_move_failed?mode=memory&cache=shared&_busy_timeout=5000"
	db, err := sql.Open("sqlite3", dbDSN)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS import_queue (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			download_id TEXT DEFAULT NULL,
			nzb_path TEXT NOT NULL,
			relative_path TEXT DEFAULT NULL,
			storage_path TEXT DEFAULT NULL,
			priority INTEGER NOT NULL DEFAULT 1,
			status TEXT NOT NULL DEFAULT 'pending'
				CHECK(status IN ('pending','processing','completed','failed','fallback','paused')),
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			started_at DATETIME DEFAULT NULL,
			completed_at DATETIME DEFAULT NULL,
			retry_count INTEGER NOT NULL DEFAULT 0,
			max_retries INTEGER NOT NULL DEFAULT 3,
			error_message TEXT DEFAULT NULL,
			batch_id TEXT DEFAULT NULL,
			metadata TEXT DEFAULT NULL,
			category TEXT DEFAULT NULL,
			file_size BIGINT DEFAULT NULL,
			target_path TEXT DEFAULT NULL,
			skip_arr_notification BOOLEAN NOT NULL DEFAULT FALSE,
			skip_post_import_links BOOLEAN NOT NULL DEFAULT FALSE,
			indexer TEXT DEFAULT NULL,
			UNIQUE(nzb_path)
		);
		CREATE INDEX IF NOT EXISTS idx_queue_nzb_path ON import_queue(nzb_path);
	`)
	require.NoError(t, err)

	repo := database.NewQueueRepository(db, database.DialectSQLite)
	dbWrapper := &database.DB{}
	dbWrapper.Repository = repo

	cfg := config.DefaultConfig(configDir)
	storeRoot := filepath.Join(configDir, ".nzbs")
	queueRoot := filepath.Join(os.TempDir(), ".altmount-queue")
	metadataService := metadata.NewMetadataService(cfg.Metadata.RootPath)
	require.NoError(t, metadataService.ConfigureCleanupRoots(storeRoot, queueRoot, storeRoot))
	cfgGetter := config.ConfigGetter(func() *config.Config { return cfg })

	return &Service{
		database:        dbWrapper,
		configGetter:    cfgGetter,
		metadataService: metadataService,
		log:             slog.Default(),
		cancelFuncs:     make(map[int64]context.CancelFunc),
		mu:              sync.RWMutex{},
	}, db
}

// TestHandleFailure_MovesToFailedDir verifies that MoveToFailedFolder moves an .nzb
// file from the OS temp queue directory (where ensurePersistentNzb places it) to
// GetFailedNzbFolder(), and updates the DB record's nzb_path accordingly.
func TestHandleFailure_MovesToFailedDir(t *testing.T) {
	svc := newMoveToFailedTestService(t)

	// Write a .nzb under a token directory, matching ensurePersistentNzb output.
	tmpQueue := filepath.Join(os.TempDir(), ".altmount-queue")
	tokenDir := filepath.Join(tmpQueue, "success-token")
	require.NoError(t, os.MkdirAll(tokenDir, 0o755))
	nzbPath := filepath.Join(tokenDir, "42-show.s01e01.nzb")
	require.NoError(t, os.WriteFile(nzbPath, []byte("<nzb/>"), 0644))
	t.Cleanup(func() { os.Remove(nzbPath) }) // safety net if test fails mid-way

	// Insert the item into the DB so UpdateQueueItemNzbPath can find it.
	ctx := context.Background()
	item := &database.ImportQueueItem{NzbPath: nzbPath, Status: database.QueueStatusPending}
	err := svc.database.Repository.AddToQueue(ctx, item)
	require.NoError(t, err)
	require.NotZero(t, item.ID, "AddToQueue should populate item.ID")

	// Act: move the NZB to the failed folder.
	moveErr := svc.MoveToFailedFolder(ctx, item)
	require.NoError(t, moveErr)

	failedDir := svc.GetFailedNzbFolder()
	destination := filepath.Join(failedDir, filepath.Base(nzbPath))
	t.Cleanup(func() { _ = os.Remove(destination) })

	assert.FileExists(t, destination, "failed .nzb should retain its basename in the failed directory")
	assert.NoFileExists(t, nzbPath, "original temp file should be removed after failure move")
	assert.NoDirExists(t, tokenDir, "the empty queue token directory should be pruned")

	assert.Equal(t, destination, item.NzbPath, "item.NzbPath should preserve the release basename")

	// Assert: DB record is updated to the new path.
	dbItem, err := svc.database.Repository.GetQueueItem(ctx, item.ID)
	require.NoError(t, err)
	assert.Equal(t, item.NzbPath, dbItem.NzbPath,
		"DB nzb_path should match the new failed-folder path")
}

// TestHandleFailure_SourceMissing_IsNoop verifies that MoveToFailedFolder is a
// no-op (no error) when the source NZB no longer exists on disk.
func TestHandleFailure_SourceMissing_IsNoop(t *testing.T) {
	svc := newMoveToFailedTestService(t)

	item := &database.ImportQueueItem{
		ID:      999,
		NzbPath: filepath.Join(os.TempDir(), ".altmount-queue", "nonexistent-handle-failure.nzb"),
	}

	err := svc.MoveToFailedFolder(context.Background(), item)
	assert.NoError(t, err, "missing source NZB should not be treated as an error")
}

func TestMoveToFailedFolderRejectsCategoryTraversal(t *testing.T) {
	svc := newMoveToFailedTestService(t)
	queueRoot := filepath.Join(os.TempDir(), ".altmount-queue")
	require.NoError(t, os.MkdirAll(queueRoot, 0o755))
	source := filepath.Join(queueRoot, fmt.Sprintf("failed-traversal-%d.nzb", time.Now().UnixNano()))
	require.NoError(t, os.WriteFile(source, []byte("<nzb/>"), 0o600))
	t.Cleanup(func() { _ = os.Remove(source) })

	category := filepath.Join("..", "..", "..", "escaped-category")
	item := &database.ImportQueueItem{
		NzbPath:    source,
		Category:   &category,
		Status:     database.QueueStatusFailed,
		MaxRetries: 3,
	}
	require.NoError(t, svc.database.Repository.AddToQueue(context.Background(), item))
	escaped := filepath.Join(svc.GetFailedNzbFolder(), category, filepath.Base(source))
	t.Cleanup(func() { _ = os.Remove(escaped) })

	err := svc.MoveToFailedFolder(context.Background(), item)

	require.Error(t, err)
	assert.FileExists(t, source)
	assert.NoFileExists(t, escaped)
	stored, getErr := svc.database.Repository.GetQueueItem(context.Background(), item.ID)
	require.NoError(t, getErr)
	require.NotNil(t, stored)
	assert.Equal(t, source, stored.NzbPath)
}

func TestMoveToFailedFolderRejectsUnownedLegacySource(t *testing.T) {
	svc := newMoveToFailedTestService(t)
	source := filepath.Join(t.TempDir(), "operator-source.nzb")
	require.NoError(t, os.WriteFile(source, []byte("keep"), 0o600))
	item := &database.ImportQueueItem{
		NzbPath: source, Status: database.QueueStatusFailed, MaxRetries: 3,
	}
	require.NoError(t, svc.database.Repository.AddToQueue(context.Background(), item))
	destination := filepath.Join(svc.GetFailedNzbFolder(), filepath.Base(source))

	err := svc.MoveToFailedFolder(context.Background(), item)

	require.Error(t, err)
	assert.FileExists(t, source)
	assert.NoFileExists(t, destination)
	stored, getErr := svc.database.Repository.GetQueueItem(context.Background(), item.ID)
	require.NoError(t, getErr)
	require.NotNil(t, stored)
	assert.Equal(t, source, stored.NzbPath)
}

func TestMoveToFailedFolderRollsBackCopyWhenPathUpdateFails(t *testing.T) {
	svc, db := newMoveToFailedTestServiceWithDB(t)
	queueRoot := filepath.Join(os.TempDir(), ".altmount-queue")
	tokenDir := filepath.Join(queueRoot, "rollback-token")
	require.NoError(t, os.MkdirAll(tokenDir, 0o755))
	source := filepath.Join(tokenDir, "update-rollback.nzb")
	require.NoError(t, os.WriteFile(source, []byte("keep"), 0o600))
	item := &database.ImportQueueItem{
		NzbPath: source, Status: database.QueueStatusFailed, MaxRetries: 3,
	}
	require.NoError(t, svc.database.Repository.AddToQueue(context.Background(), item))
	_, err := db.Exec(`
		CREATE TRIGGER reject_failed_path_update
		BEFORE UPDATE OF nzb_path ON import_queue
		BEGIN
			SELECT RAISE(ABORT, 'injected failed-path update rejection');
		END;
	`)
	require.NoError(t, err)
	destination := filepath.Join(svc.GetFailedNzbFolder(), filepath.Base(source))

	err = svc.MoveToFailedFolder(context.Background(), item)

	require.Error(t, err)
	assert.FileExists(t, source, "a rejected path publication must retain the prior owned source")
	assert.DirExists(t, tokenDir, "rollback must recreate a pruned queue token directory")
	assert.NoFileExists(t, destination, "a rejected path publication must not leak the staged destination")
	stored, getErr := svc.database.Repository.GetQueueItem(context.Background(), item.ID)
	require.NoError(t, getErr)
	require.NotNil(t, stored)
	assert.Equal(t, source, stored.NzbPath)
}

// TestCleanupFailedItems_RemovesNzbFile verifies that cleanupFailedItems removes
// an owned NZB before purging its queue row.
func TestCleanupFailedItems_RemovesNzbFile(t *testing.T) {
	svc, db := newMoveToFailedTestServiceWithDB(t)
	retentionHours := 1
	cfg := svc.configGetter()
	cfg.Import.FailedItemRetentionHours = &retentionHours
	svc.configGetter = func() *config.Config { return cfg }

	// Create a temp "failed" NZB file on disk.
	failedDir := svc.GetFailedNzbFolder()
	require.NoError(t, os.MkdirAll(failedDir, 0755))
	nzbPath := filepath.Join(failedDir, "old-failed-cleanup.nzb")
	require.NoError(t, os.WriteFile(nzbPath, []byte("<nzb/>"), 0644))
	t.Cleanup(func() { os.Remove(nzbPath) })

	ctx := context.Background()

	// Insert into DB.
	item := &database.ImportQueueItem{NzbPath: nzbPath, Status: database.QueueStatusPending}
	require.NoError(t, svc.database.Repository.AddToQueue(ctx, item))

	errMsg := "simulated failure"
	require.NoError(t, svc.database.Repository.UpdateQueueItemStatus(ctx, item.ID, database.QueueStatusFailed, &errMsg))
	_, err := db.Exec(`UPDATE import_queue SET updated_at = '2000-01-01 00:00:00' WHERE id = ?`, item.ID)
	require.NoError(t, err)

	svc.cleanupFailedItems(ctx)

	assert.NoFileExists(t, nzbPath, "failed NZB should be removed by cleanup")
	remaining, err := svc.database.Repository.GetQueueItem(ctx, item.ID)
	require.NoError(t, err)
	assert.Nil(t, remaining, "queue ownership should be removed after successful file cleanup")
}

func TestCleanupFailedItemsPreservesOwnershipWhenRootedCleanupFails(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink authority test requires Unix symlink semantics")
	}
	svc, db := newMoveToFailedTestServiceWithDB(t)
	retentionHours := 1
	cfg := svc.configGetter()
	cfg.Import.FailedItemRetentionHours = &retentionHours
	svc.configGetter = func() *config.Config { return cfg }

	failedDir := svc.GetFailedNzbFolder()
	require.NoError(t, os.MkdirAll(failedDir, 0o755))
	outside := t.TempDir()
	victim := filepath.Join(outside, "victim.nzb")
	require.NoError(t, os.WriteFile(victim, []byte("keep"), 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(failedDir, "escape")))

	ctx := context.Background()
	item := &database.ImportQueueItem{
		NzbPath:    filepath.Join(failedDir, "escape", "victim.nzb"),
		Status:     database.QueueStatusFailed,
		MaxRetries: 3,
		CreatedAt:  time.Now().Add(-2 * time.Hour),
	}
	require.NoError(t, svc.database.Repository.AddToQueue(ctx, item))
	_, err := db.Exec(`UPDATE import_queue SET updated_at = '2000-01-01 00:00:00' WHERE id = ?`, item.ID)
	require.NoError(t, err)

	svc.cleanupFailedItems(ctx)

	assert.FileExists(t, victim)
	remaining, err := svc.database.Repository.GetQueueItem(ctx, item.ID)
	require.NoError(t, err)
	require.NotNil(t, remaining, "failed file cleanup must retain the queue row for retry")
}

func TestCleanupFailedItemsPreservesOwnershipWhenSourceUnlinkFails(t *testing.T) {
	svc, db := newMoveToFailedTestServiceWithDB(t)
	retentionHours := 1
	cfg := svc.configGetter()
	cfg.Import.FailedItemRetentionHours = &retentionHours
	svc.configGetter = func() *config.Config { return cfg }

	failedDir := svc.GetFailedNzbFolder()
	nzbPath := filepath.Join(failedDir, "non-empty-source.nzb")
	require.NoError(t, os.MkdirAll(nzbPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(nzbPath, "keep"), []byte("keep"), 0o600))

	ctx := context.Background()
	item := &database.ImportQueueItem{
		NzbPath:    nzbPath,
		Status:     database.QueueStatusFailed,
		MaxRetries: 3,
		CreatedAt:  time.Now().Add(-2 * time.Hour),
	}
	require.NoError(t, svc.database.Repository.AddToQueue(ctx, item))
	_, err := db.Exec(`UPDATE import_queue SET updated_at = '2000-01-01 00:00:00' WHERE id = ?`, item.ID)
	require.NoError(t, err)

	svc.cleanupFailedItems(ctx)

	assert.DirExists(t, nzbPath, "failed source cleanup must preserve the source path")
	remaining, err := svc.database.Repository.GetQueueItem(ctx, item.ID)
	require.NoError(t, err)
	require.NotNil(t, remaining, "failed source cleanup must retain the queue row for retry")
}
