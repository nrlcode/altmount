package health

import (
	"context"
	"testing"

	"github.com/javi11/altmount/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLibrarySyncPreservesOrphanRowReplacedAfterObservation(t *testing.T) {
	metadataRoot := t.TempDir()
	env := newRepairTestEnv(t, metadataRoot, nil, func(cfg *config.Config) {
		cfg.Import.ImportStrategy = config.ImportStrategyNone
		cfg.Health.LibraryDir = nil
		cfg.Health.LibrarySyncConcurrency = 1
		cfg.Metadata.RootPath = metadataRoot
	})
	const observedPath = "complete/sync-observed-orphan.mkv"
	const addedPath = "complete/sync-add-trigger.mkv"
	_, err := env.db.Exec(`
		INSERT INTO file_health (file_path, status, metadata, scheduled_check_at)
		VALUES (?, 'corrupted', '{"revision":"observed"}', datetime('now'))
	`, observedPath)
	require.NoError(t, err)
	observed, err := env.healthRepo.GetFileHealth(context.Background(), observedPath)
	require.NoError(t, err)
	require.NotNil(t, observed)

	// Sync observes the orphan first. Its earlier batch-add phase then installs
	// a distinct same-path generation before automatic deletion begins.
	_, err = env.db.Exec(`
		CREATE TRIGGER replace_sync_orphan_after_observation
		AFTER INSERT ON file_health
		WHEN NEW.file_path = 'complete/sync-add-trigger.mkv'
		BEGIN
			DELETE FROM file_health WHERE file_path = 'complete/sync-observed-orphan.mkv';
			INSERT INTO file_health
				(file_path, status, metadata, scheduled_check_at, updated_at)
			VALUES
				('complete/sync-observed-orphan.mkv', 'pending',
				 '{"revision":"replacement"}', datetime('now', '+1 hour'),
				 datetime('now', '+1 second'));
		END;
	`)
	require.NoError(t, err)
	writeHealthyFile(t, env, addedPath)

	worker := NewLibrarySyncWorker(
		env.metadataService,
		env.healthRepo,
		env.hw.configGetter,
		nil,
		&MockRcloneClient{},
	)
	assert.Nil(t, worker.SyncLibrary(context.Background(), false))

	current, err := env.healthRepo.GetFileHealth(context.Background(), observedPath)
	require.NoError(t, err)
	require.NotNil(t, current,
		"automatic sync must not delete a same-path generation created after orphan observation")
	assert.NotEqual(t, observed.ID, current.ID)
	assert.Equal(t, "pending", string(current.Status))
	require.NotNil(t, current.Metadata)
	assert.JSONEq(t, `{"revision":"replacement"}`, *current.Metadata)
}
