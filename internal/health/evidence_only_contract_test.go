package health

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/metadata"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/javi11/altmount/internal/testsupport/fakepool"
	nntppool "github.com/javi11/nntppool/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkerAdmissionFailurePerformsNoCheckEffectOrPublication(t *testing.T) {
	client := fakepool.New()
	client.SetDefaultBehavior(fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	env := newBatchTestEnv(t, t.TempDir(), client)
	const filePath = "movies/admission-failed.mkv"
	writeHealthyFile(t, env, filePath)
	insertFileHealth(t, env.db, filePath, "/library/admission-failed.mkv", 2, 3)
	_, err := env.db.Exec(`
		CREATE TRIGGER reject_health_admission BEFORE UPDATE OF status ON file_health
		WHEN NEW.status = 'checking'
		BEGIN SELECT RAISE(ABORT, 'synthetic admission failure'); END;
	`)
	require.NoError(t, err)

	_ = env.hw.runHealthCheckCycle(context.Background())

	assert.Zero(t, client.StatCalls(), "a failed admission must not start evidence gathering")
	env.mockARRs.mu.Lock()
	arrCalls := len(env.mockARRs.calls)
	env.mockARRs.mu.Unlock()
	assert.Zero(t, arrCalls, "a failed admission must not dispatch repair")
	row, err := env.healthRepo.GetFileHealth(context.Background(), filePath)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, database.HealthStatusPending, row.Status)
	assert.Equal(t, 2, row.RetryCount)
	meta, err := env.metadataService.ReadFileMetadata(filePath)
	require.NoError(t, err)
	require.NotNil(t, meta)
	assert.Equal(t, metapb.FileStatus_FILE_STATUS_HEALTHY, meta.Status)
}

func TestWorkerProcessesOnlyRowsActuallyClaimed(t *testing.T) {
	client := fakepool.New()
	env := newBatchTestEnv(t, t.TempDir(), client)
	paths := []string{"movies/claimed.mkv", "movies/omitted.mkv"}
	segmentIDs := make([]string, len(paths))
	for i, path := range paths {
		segmentIDs[i] = writeHealthyFile(t, env, path)
		insertFileHealth(t, env.db, path, "/library/"+filepath.Base(path), 0, 3)
	}
	_, err := env.db.Exec(`
		CREATE TRIGGER omit_stale_health_member BEFORE UPDATE OF status ON file_health
		WHEN OLD.file_path = 'movies/omitted.mkv' AND NEW.status = 'checking'
		BEGIN SELECT RAISE(IGNORE); END;
	`)
	require.NoError(t, err)

	require.NoError(t, env.hw.runHealthCheckCycle(context.Background()))

	assert.Equal(t, int64(1), client.PerMessageCalls(segmentIDs[0]))
	assert.Zero(t, client.PerMessageCalls(segmentIDs[1]),
		"a member omitted by current-row admission must not be checked")
	omitted, err := env.healthRepo.GetFileHealth(context.Background(), paths[1])
	require.NoError(t, err)
	require.NotNil(t, omitted)
	assert.Equal(t, database.HealthStatusPending, omitted.Status)
}

func TestWorkerPublishesCorruptionEvidenceWithoutAutomaticEffects(t *testing.T) {
	for _, action := range []string{"repair", "delete"} {
		t.Run(action, func(t *testing.T) {
			libraryRoot := t.TempDir()
			env := newRepairTestEnv(t, t.TempDir(), nil, func(cfg *config.Config) {
				cfg.Health.CorruptionAction = action
				cfg.Health.LibraryDir = &libraryRoot
			})
			const filePath = "movies/evidence-only.mkv"
			libraryPath := filepath.Join(libraryRoot, "evidence-only.mkv")
			require.NoError(t, os.WriteFile(libraryPath, []byte("current"), 0o644))
			require.NoError(t, env.metadataService.WriteFileMetadata(filePath, validSegmentMeta(env.metadataService, 1024)))
			insertFileHealth(t, env.db, filePath, libraryPath, 2, 3)

			require.NoError(t, env.hw.runHealthCheckCycle(context.Background()))

			env.mockARRs.mu.Lock()
			arrCalls := len(env.mockARRs.calls)
			env.mockARRs.mu.Unlock()
			assert.Zero(t, arrCalls, "automatic ARR repair is fail-closed")
			row, err := env.healthRepo.GetFileHealth(context.Background(), filePath)
			require.NoError(t, err)
			require.NotNil(t, row, "checked SQL evidence must be retained")
			assert.Equal(t, database.HealthStatusCorrupted, row.Status)
			assert.NotNil(t, row.LastError)
			meta, err := env.metadataService.ReadFileMetadata(filePath)
			require.NoError(t, err)
			require.NotNil(t, meta, "automatic metadata move/delete is fail-closed")
			assert.Equal(t, metapb.FileStatus_FILE_STATUS_HEALTHY, meta.Status,
				"the worker may publish SQL evidence but cannot mutate metadata")
			_, err = os.Stat(libraryPath)
			assert.NoError(t, err, "automatic library deletion is fail-closed")
		})
	}
}

func TestWorkerDoesNotDispatchRepairNotifications(t *testing.T) {
	env := newRepairTestEnv(t, t.TempDir(), nil)
	const filePath = "movies/repair-notification.mkv"
	_, err := env.db.Exec(`
		INSERT INTO file_health
			(file_path, library_path, status, repair_retry_count, max_repair_retries, scheduled_check_at)
		VALUES (?, '/library/repair-notification.mkv', 'repair_triggered', 0, 3, datetime('now', '-1 second'))
	`, filePath)
	require.NoError(t, err)

	require.NoError(t, env.hw.runHealthCheckCycle(context.Background()))

	env.mockARRs.mu.Lock()
	arrCalls := len(env.mockARRs.calls)
	env.mockARRs.mu.Unlock()
	assert.Zero(t, arrCalls, "repair-notification dispatch is generation-blind and must remain disabled")
	row, err := env.healthRepo.GetFileHealth(context.Background(), filePath)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, database.HealthStatusRepairTriggered, row.Status)
	assert.Zero(t, row.RepairRetryCount)
}

func TestLibrarySyncReportsButDoesNotDeleteOrphanRecords(t *testing.T) {
	db := newResilienceDB(t)
	repo := database.NewHealthRepository(db, database.DialectSQLite)
	_, err := db.Exec(`INSERT INTO file_health (file_path, status) VALUES ('movies/orphan.mkv', 'healthy')`)
	require.NoError(t, err)
	root := t.TempDir()
	enabled := true
	cleanup := true
	cfg := config.DefaultConfig()
	cfg.Health.Enabled = &enabled
	cfg.Health.CleanupOrphanedMetadata = &cleanup
	cfg.Metadata.RootPath = root
	cfg.Import.ImportStrategy = config.ImportStrategyNone
	manager := config.NewManager(cfg, "")
	worker := NewLibrarySyncWorker(metadata.NewMetadataService(root), repo, manager.GetConfig, manager, &MockRcloneClient{})

	dry := worker.SyncLibrary(context.Background(), true)
	require.NotNil(t, dry)
	assert.Equal(t, 1, dry.DatabaseRecordsToClean, "dry run must still report the orphan")
	require.NoError(t, func() error {
		row, getErr := repo.GetFileHealth(context.Background(), "movies/orphan.mkv")
		if getErr == nil {
			require.NotNil(t, row)
		}
		return getErr
	}())

	assert.Nil(t, worker.SyncLibrary(context.Background(), false))
	row, err := repo.GetFileHealth(context.Background(), "movies/orphan.mkv")
	require.NoError(t, err)
	assert.NotNil(t, row, "live library sync must not delete orphan authority")
}
