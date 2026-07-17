package database

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// observedHealthCleanupAPI is the automatic-reconciliation boundary. Unlike
// administrative deletion, an unattended cleanup may delete only the exact
// identity and eligibility state that the orphan scan actually observed.
type observedHealthCleanupAPI interface {
	DeleteObservedHealthRecords(context.Context, []*FileHealth) (int64, error)
}

func requireObservedHealthCleanupAPI(t *testing.T, repo *HealthRepository) observedHealthCleanupAPI {
	t.Helper()
	cleanup, ok := any(repo).(observedHealthCleanupAPI)
	require.True(t, ok, "HealthRepository must expose an observed-identity automatic cleanup boundary")
	return cleanup
}

func TestObservedAutomaticCleanupDeletesOnlyUnchangedIdleRows(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	const path = "cleanup/observed-idle.mkv"
	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO file_health (file_path, status, metadata, health_claim_token)
		VALUES (?, 'corrupted', '{"revision":"observed"}', NULL)
	`, path)
	require.NoError(t, err)
	observed, err := repo.GetFileHealth(ctx, path)
	require.NoError(t, err)
	require.NotNil(t, observed)

	deleted, err := requireObservedHealthCleanupAPI(t, repo).DeleteObservedHealthRecords(ctx, []*FileHealth{observed})
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)
	current, err := repo.GetFileHealth(ctx, path)
	require.NoError(t, err)
	assert.Nil(t, current)
}

func TestObservedAutomaticCleanupPreservesSamePathReplacement(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	const path = "cleanup/observed-replacement.mkv"
	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO file_health (file_path, status, metadata)
		VALUES (?, 'corrupted', '{"revision":"observed"}')
	`, path)
	require.NoError(t, err)
	observed, err := repo.GetFileHealth(ctx, path)
	require.NoError(t, err)
	require.NotNil(t, observed)

	_, err = repo.db.ExecContext(ctx, `DELETE FROM file_health WHERE id = ?`, observed.ID)
	require.NoError(t, err)
	_, err = repo.db.ExecContext(ctx, `
		INSERT INTO file_health (file_path, status, metadata)
		VALUES (?, 'pending', '{"revision":"replacement"}')
	`, path)
	require.NoError(t, err)

	deleted, err := requireObservedHealthCleanupAPI(t, repo).DeleteObservedHealthRecords(ctx, []*FileHealth{observed})
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted)
	current, err := repo.GetFileHealth(ctx, path)
	require.NoError(t, err)
	require.NotNil(t, current)
	assert.NotEqual(t, observed.ID, current.ID)
	require.NotNil(t, current.Metadata)
	assert.JSONEq(t, `{"revision":"replacement"}`, *current.Metadata)
}

func TestObservedAutomaticCleanupPreservesSubsecondReplacement(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	const path = "cleanup/observed-subsecond-replacement.mkv"
	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO file_health (file_path, status, metadata, updated_at)
		VALUES (?, 'corrupted', '{"revision":"same"}', '2026-07-16 12:34:56.100000000')
	`, path)
	require.NoError(t, err)
	observed, err := repo.GetFileHealth(ctx, path)
	require.NoError(t, err)
	require.NotNil(t, observed)

	// Model an otherwise-identical replacement/update within the same wall-clock
	// second. Automatic cleanup must preserve it; truncating the observation to
	// seconds would consume a row the scan never actually observed.
	_, err = repo.db.ExecContext(ctx, `
		UPDATE file_health
		SET updated_at = '2026-07-16 12:34:56.900000000'
		WHERE id = ?
	`, observed.ID)
	require.NoError(t, err)

	deleted, err := requireObservedHealthCleanupAPI(t, repo).DeleteObservedHealthRecords(
		ctx, []*FileHealth{observed},
	)
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted)
	current, err := repo.GetFileHealth(ctx, path)
	require.NoError(t, err)
	require.NotNil(t, current)
	assert.Equal(t, observed.ID, current.ID)
	assert.NotEqual(t, observed.UpdatedAt, current.UpdatedAt)
}

func TestObservedAutomaticCleanupPreservesChangedOrActivelyOwnedRows(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	const changedPath = "cleanup/observed-changed.mkv"
	const ownedPath = "cleanup/observed-owned.mkv"
	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO file_health (file_path, status, metadata, health_claim_token)
		VALUES (?, 'corrupted', '{"revision":"observed"}', NULL),
		       (?, 'corrupted', '{"revision":"observed"}', NULL)
	`, changedPath, ownedPath)
	require.NoError(t, err)
	changed, err := repo.GetFileHealth(ctx, changedPath)
	require.NoError(t, err)
	require.NotNil(t, changed)
	owned, err := repo.GetFileHealth(ctx, ownedPath)
	require.NoError(t, err)
	require.NotNil(t, owned)

	_, err = repo.db.ExecContext(ctx, `
		UPDATE file_health
		SET status = 'healthy', metadata = '{"revision":"changed"}',
		    updated_at = datetime('now', '+1 second')
		WHERE id = ?
	`, changed.ID)
	require.NoError(t, err)
	_, err = repo.db.ExecContext(ctx, `
		UPDATE file_health SET health_claim_token = 'active-owner' WHERE id = ?
	`, owned.ID)
	require.NoError(t, err)

	deleted, err := requireObservedHealthCleanupAPI(t, repo).DeleteObservedHealthRecords(
		ctx, []*FileHealth{changed, owned},
	)
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted)

	currentChanged, err := repo.GetFileHealth(ctx, changedPath)
	require.NoError(t, err)
	require.NotNil(t, currentChanged)
	assert.Equal(t, HealthStatusHealthy, currentChanged.Status)
	currentOwned, err := repo.GetFileHealth(ctx, ownedPath)
	require.NoError(t, err)
	require.NotNil(t, currentOwned)
	var token sql.NullString
	require.NoError(t, repo.db.QueryRowContext(ctx, `
		SELECT health_claim_token FROM file_health WHERE id = ?
	`, currentOwned.ID).Scan(&token))
	require.True(t, token.Valid)
	assert.Equal(t, "active-owner", token.String)
}

func TestFailedImportPrefixCleanupDefersActiveHealthOwner(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	const idlePath = "complete/failed-pack/idle.mkv"
	const ownedPath = "complete/failed-pack/owned.mkv"
	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO file_health
			(file_path, status, library_path, source_nzb_path, metadata, health_claim_token)
		VALUES (?, 'pending', NULL, NULL, NULL, NULL),
		       (?, 'checking', NULL, NULL, NULL, 'active-owner')
	`, idlePath, ownedPath)
	require.NoError(t, err)

	deleted, err := repo.DeleteUnvalidatedHealthRecordsByPrefix(ctx, "complete/failed-pack")
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)
	idle, err := repo.GetFileHealth(ctx, idlePath)
	require.NoError(t, err)
	assert.Nil(t, idle)
	owned, err := repo.GetFileHealth(ctx, ownedPath)
	require.NoError(t, err)
	require.NotNil(t, owned, "failed-import cleanup must leave active health work for a later pass")
	var token sql.NullString
	require.NoError(t, repo.db.QueryRowContext(ctx, `
		SELECT health_claim_token FROM file_health WHERE id = ?
	`, owned.ID).Scan(&token))
	require.True(t, token.Valid)
	assert.Equal(t, "active-owner", token.String)
}
