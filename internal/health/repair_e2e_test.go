package health

import (
	"context"
	"database/sql"
	"errors"
	"runtime"
	"sync"
	"testing"

	"github.com/javi11/altmount/internal/arrs/model"
	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/importer"
	"github.com/javi11/altmount/internal/metadata"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/javi11/altmount/internal/pool"
	"github.com/javi11/altmount/internal/testsupport/fakepool"
	nntppool "github.com/javi11/nntppool/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockPoolManager implements pool.Manager and always fails GetPool so segment validation fails.
type mockPoolManager struct{}

func (m *mockPoolManager) GetPool() (pool.NntpClient, error) {
	return nil, errors.New("no pool available (test mock)")
}
func (m *mockPoolManager) SetProviders(_ []nntppool.Provider) error { return nil }
func (m *mockPoolManager) ClearPool() error                         { return nil }
func (m *mockPoolManager) HasPool() bool                            { return false }
func (m *mockPoolManager) GetMetrics() (pool.MetricsSnapshot, error) {
	return pool.MetricsSnapshot{}, nil
}
func (m *mockPoolManager) ResetMetrics(_ context.Context, _, _ bool) error { return nil }
func (m *mockPoolManager) ResetProviderErrors(_ context.Context) error     { return nil }
func (m *mockPoolManager) IncArticlesDownloaded()                          {}
func (m *mockPoolManager) UpdateDownloadProgress(_ string, _ int64)        {}
func (m *mockPoolManager) IncArticlesPosted()                              {}
func (m *mockPoolManager) AddProvider(_ nntppool.Provider) error           { return nil }
func (m *mockPoolManager) RemoveProvider(_ string) error                   { return nil }
func (m *mockPoolManager) ResetProviderQuota(_ context.Context, _ string) error {
	return nil
}
func (m *mockPoolManager) SetProviderIDs(_ map[string]string) {}
func (m *mockPoolManager) AcquireImportSlot(_ context.Context) (func(), error) {
	return func() {}, nil
}
func (m *mockPoolManager) SetAdmissionCap(_ int) {}
func (m *mockPoolManager) AcquireImportConnection(_ context.Context) (func(), error) {
	return func() {}, nil
}
func (m *mockPoolManager) SetImportConnCapacity(_ int)                 {}
func (m *mockPoolManager) ImportConnCapacity() int                     { return 0 }
func (m *mockPoolManager) SetStreamSource(_ pool.StreamActivitySource) {}
func (m *mockPoolManager) NotifyStreamChange()                         {}

// hardMissingPoolManager is the destructive-repair fixture: unlike a pool
// outage, it supplies conclusive hard-absence evidence for every STAT.
type hardMissingPoolManager struct {
	mockPoolManager
	client *fakepool.Client
}

func (m *hardMissingPoolManager) GetPool() (pool.NntpClient, error) { return m.client, nil }
func (m *hardMissingPoolManager) HasPool() bool                     { return true }

// mockARRsService captures TriggerFileRescan calls and returns a configurable error.
type mockARRsService struct {
	mu        sync.Mutex
	calls     []triggerCall
	returnErr error
}

type triggerCall struct {
	pathForRescan string
	relativePath  string
}

func (m *mockARRsService) TriggerFileRescan(_ context.Context, pathForRescan string, relativePath string, _ *string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, triggerCall{pathForRescan: pathForRescan, relativePath: relativePath})
	return m.returnErr
}

func (m *mockARRsService) DiscoverFileMetadata(_ context.Context, _, _, _, _ string) (*model.WebhookMetadata, error) {
	return nil, nil
}

// mockImportService implements importer.ImportService for testing.
type mockImportService struct {
	importer.ImportService
}

func (m *mockImportService) RegenerateMetadata(_ context.Context, _ string) error {
	return nil
}

// repairTestEnv holds all the pieces needed for repair e2e tests.
type repairTestEnv struct {
	db              *sql.DB
	healthRepo      *database.HealthRepository
	metadataService *metadata.MetadataService
	healthChecker   *HealthChecker
	mockARRs        *mockARRsService
	hw              *HealthWorker
}

func newRepairTestEnv(t *testing.T, tempDir string, arrsErr error, configure ...func(*config.Config)) *repairTestEnv {
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

	healthRepo := database.NewHealthRepository(db, database.DialectSQLite)
	metadataService := metadata.NewMetadataService(tempDir)

	healthEnabled := true
	cfg := config.DefaultConfig()
	cfg.Health.Enabled = &healthEnabled
	cfg.Health.MaxRetries = 3
	cfg.Metadata.RootPath = tempDir
	cfg.MountPath = "/mnt/test"
	cfg.Health.MaxConcurrentJobs = 1
	cfg.Health.CheckIntervalSeconds = 3600
	cfg.Health.SegmentSamplePercentage = 10
	cfg.Health.MaxConnectionsForHealthChecks = 1

	for _, fn := range configure {
		fn(cfg)
	}

	configManager := config.NewManager(cfg, "")

	mockARRs := &mockARRsService{returnErr: arrsErr}
	mockImporter := &mockImportService{}
	missingClient := fakepool.New()
	missingClient.SetDefaultBehavior(fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	healthChecker := NewHealthChecker(
		healthRepo,
		metadataService,
		&hardMissingPoolManager{client: missingClient},
		configManager.GetConfig,
		&MockRcloneClient{},
	)

	hw := NewHealthWorker(
		healthChecker,
		healthRepo,
		metadataService,
		mockARRs,
		mockImporter,
		configManager.GetConfig,
		nil,
	)

	return &repairTestEnv{
		db:              db,
		healthRepo:      healthRepo,
		metadataService: metadataService,
		healthChecker:   healthChecker,
		mockARRs:        mockARRs,
		hw:              hw,
	}
}

// validSegmentMeta creates a FileMetadata with one segment that covers the full fileSize,
// so CheckMetadataIntegrity passes. The test pool then returns conclusive hard
// absence, allowing repair/delete behavior to be exercised legitimately.
func validSegmentMeta(ms *metadata.MetadataService, fileSize int64) *metapb.FileMetadata {
	seg := &metapb.SegmentData{
		Id:          "test-article-001@test.example.com",
		SegmentSize: fileSize,
		StartOffset: 0,
		EndOffset:   fileSize - 1,
	}
	return ms.CreateFileMetadata(
		fileSize, "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		[]*metapb.SegmentData{seg},
		metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
	)
}

// insertFileHealth directly inserts a file_health row with the given parameters.
func insertFileHealth(t *testing.T, db *sql.DB, filePath, libraryPath string, retryCount, maxRetries int) {
	t.Helper()
	_, err := db.Exec(`
		INSERT INTO file_health
			(file_path, library_path, status, retry_count, max_retries,
			 repair_retry_count, max_repair_retries, scheduled_check_at)
		VALUES (?, ?, 'pending', ?, ?, 0, 3, datetime('now', '-1 second'))
	`, filePath, libraryPath, retryCount, maxRetries)
	require.NoError(t, err)
}

// TestE2E_RepairDisabled_NoARRRescan verifies that when health.repair.enabled is false, a
// file that exhausts its health-check retries is finalized as corrupted for visibility but
// never triggers an Arr rescan and its metadata is left in place (no safety-folder move).
func TestE2E_RepairDisabled_NoARRRescan(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}
	tempDir := t.TempDir()
	repairDisabled := false
	env := newRepairTestEnv(t, tempDir, nil, func(c *config.Config) {
		c.Health.Repair.Enabled = &repairDisabled
	})

	ctx := context.Background()
	filePath := "series/show.s01e09.mkv"
	libraryPath := "/media/library/show.s01e09.mkv"
	maxRetries := 3

	meta := validSegmentMeta(env.metadataService, 1024)
	require.NoError(t, env.metadataService.WriteFileMetadata(filePath, meta))

	// File already at its last health retry: a single failing cycle would normally
	// trigger a repair, but repair is disabled.
	insertFileHealth(t, env.db, filePath, libraryPath, maxRetries-1, maxRetries)

	require.NoError(t, env.hw.runHealthCheckCycle(ctx))

	// ARR must NOT be called when repair is disabled.
	env.mockARRs.mu.Lock()
	callCount := len(env.mockARRs.calls)
	env.mockARRs.mu.Unlock()
	assert.Equal(t, 0, callCount, "repair disabled must not trigger an ARR rescan")

	// Status finalized as corrupted for visibility.
	fh, err := env.healthRepo.GetFileHealth(ctx, filePath)
	require.NoError(t, err)
	require.NotNil(t, fh)
	assert.Equal(t, database.HealthStatusCorrupted, fh.Status)

	// Metadata must be left in place — no safety-folder move when repair is disabled.
	original, readErr := env.metadataService.ReadFileMetadata(filePath)
	require.NoError(t, readErr)
	assert.NotNil(t, original, "repair disabled must not move metadata to the corrupted folder")
}
