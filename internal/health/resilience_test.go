package health

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/metadata"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newResilienceDB creates an in-memory SQLite database with the required schema.
func newResilienceDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared&mode=memory")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS file_health (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			file_path TEXT NOT NULL UNIQUE,
			library_path TEXT,
			status TEXT NOT NULL,
			claim_generation INTEGER NOT NULL DEFAULT 0,
			last_checked DATETIME,
			last_error TEXT,
			retry_count INTEGER DEFAULT 0,
			max_retries INTEGER DEFAULT 3,
			repair_retry_count INTEGER DEFAULT 0,
			max_repair_retries INTEGER DEFAULT 3,
			source_nzb_path TEXT,
			error_details TEXT,
			metadata TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			release_date DATETIME,
			scheduled_check_at DATETIME,
			priority INTEGER NOT NULL DEFAULT 0,
			streaming_failure_count INTEGER DEFAULT 0,
			is_masked BOOLEAN DEFAULT FALSE,
			indexer TEXT DEFAULT NULL,
			download_id TEXT DEFAULT NULL
		);

		CREATE TABLE IF NOT EXISTS system_state (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
	`)
	require.NoError(t, err)
	return db
}

func TestRatioGuard_SkipsCleanupWhenOrphanRatioHigh(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}

	tempDir := t.TempDir()
	db := newResilienceDB(t)
	healthRepo := database.NewHealthRepository(db, database.DialectSQLite)
	metadataService := metadata.NewMetadataService(tempDir)

	// Create 4 metadata files, 2 have library entries, 2 are orphans (50% > 20%)
	for i := range 4 {
		vp := filepath.Join("movies", fmt.Sprintf("movie_%d.mkv", i))
		meta := metadataService.CreateFileMetadata(
			1024, "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
			nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
		)
		require.NoError(t, metadataService.WriteFileMetadata(vp, meta))
	}

	// Library has only 2 of the 4 files
	libraryDir := filepath.Join(tempDir, "library")
	require.NoError(t, os.MkdirAll(filepath.Join(libraryDir, "movies"), 0755))
	mountPath := "/mnt/test"
	for i := range 2 {
		target := filepath.Join(mountPath, "movies", fmt.Sprintf("movie_%d.mkv", i))
		link := filepath.Join(libraryDir, "movies", fmt.Sprintf("movie_%d.mkv", i))
		require.NoError(t, os.Symlink(target, link))
	}

	healthEnabled := true
	cleanupEnabled := true
	cfg := config.DefaultConfig()
	cfg.Health.Enabled = &healthEnabled
	cfg.Health.LibrarySyncIntervalMinutes = 60
	cfg.Health.LibrarySyncConcurrency = 1
	cfg.Health.CleanupOrphanedMetadata = &cleanupEnabled
	cfg.Health.LibraryDir = &libraryDir
	cfg.Metadata.RootPath = tempDir
	cfg.MountPath = mountPath
	cfg.Import.ImportStrategy = config.ImportStrategySYMLINK

	configManager := config.NewManager(cfg, "")

	worker := NewLibrarySyncWorker(
		metadataService,
		healthRepo,
		configManager.GetConfig,
		configManager,
		&MockRcloneClient{},
	)

	ctx := context.Background()

	// First sync — would add to pending, but ratio guard should prevent it
	worker.SyncLibrary(ctx, false)

	// Second sync — even after two passes, ratio guard should still block cleanup
	worker.SyncLibrary(ctx, false)

	// All 4 metadata files should still exist (ratio guard prevented deletion)
	for i := range 4 {
		vp := filepath.Join("movies", fmt.Sprintf("movie_%d.mkv", i))
		assert.True(t, metadataService.FileExists(vp),
			"movie_%d.mkv should NOT be deleted (ratio guard)", i)
	}

	// Pending state should be cleared because ratio guard disabled cleanup
	raw, err := healthRepo.GetSystemState(ctx, "pending_metadata_deletions")
	assert.NoError(t, err)
	assert.Empty(t, raw, "pending state should be cleared when ratio guard triggers")
}

func TestWalkErrors_SkipCleanup(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}
	if os.Getuid() == 0 {
		t.Skip("root bypasses permission checks")
	}

	tempDir := t.TempDir()
	db := newResilienceDB(t)
	healthRepo := database.NewHealthRepository(db, database.DialectSQLite)
	metadataService := metadata.NewMetadataService(tempDir)

	// Create an orphan metadata file
	vp := filepath.Join("movies", "orphan.mkv")
	meta := metadataService.CreateFileMetadata(
		1024, "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		nil, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
	)
	require.NoError(t, metadataService.WriteFileMetadata(vp, meta))

	// Create library dir with an unreadable subdirectory to cause walk errors
	libraryDir := filepath.Join(tempDir, "library")
	unreadableDir := filepath.Join(libraryDir, "movies", "unreadable")
	require.NoError(t, os.MkdirAll(unreadableDir, 0755))
	// Also create a valid symlink so library isn't empty (which triggers mount guard instead)
	mountPath := "/mnt/test"
	dummyLink := filepath.Join(libraryDir, "movies", "dummy.mkv")
	require.NoError(t, os.Symlink(filepath.Join(mountPath, "movies", "dummy.mkv"), dummyLink))

	// Make the subdirectory unreadable
	require.NoError(t, os.Chmod(unreadableDir, 0000))
	t.Cleanup(func() {
		// Restore permissions so t.TempDir() cleanup succeeds
		os.Chmod(unreadableDir, 0755)
	})

	// Also seed the pending state to simulate a previous pass marking this as orphaned
	pendingData := `{"movies/orphan.mkv":true}`
	require.NoError(t, healthRepo.UpdateSystemState(context.Background(), "pending_metadata_deletions", pendingData))

	healthEnabled := true
	cleanupEnabled := true
	cfg := config.DefaultConfig()
	cfg.Health.Enabled = &healthEnabled
	cfg.Health.LibrarySyncIntervalMinutes = 60
	cfg.Health.LibrarySyncConcurrency = 1
	cfg.Health.CleanupOrphanedMetadata = &cleanupEnabled
	cfg.Health.LibraryDir = &libraryDir
	cfg.Metadata.RootPath = tempDir
	cfg.MountPath = mountPath
	cfg.Import.ImportStrategy = config.ImportStrategySYMLINK

	configManager := config.NewManager(cfg, "")

	worker := NewLibrarySyncWorker(
		metadataService,
		healthRepo,
		configManager.GetConfig,
		configManager,
		&MockRcloneClient{},
	)

	ctx := context.Background()
	worker.SyncLibrary(ctx, false)

	// Orphan should NOT be deleted because walk errors disable cleanup
	assert.True(t, metadataService.FileExists(vp),
		"orphan should NOT be deleted when walk errors are present")
}
