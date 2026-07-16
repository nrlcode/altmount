package database

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// healthBatchClaimAPI freezes a claim-and-return boundary using only existing
// repository model types. Fresh rows and their aligned per-row tokens must be
// returned together; callers must not perform a later unlocked SELECT.
type healthBatchClaimAPI interface {
	ClaimFilesCheckingBulk(context.Context, []*FileHealth) ([]*FileHealth, []string, error)
}

type healthClaimRotationAPI interface {
	RotateHealthClaim(context.Context, int64, string, HealthStatus, string) (string, error)
}

// healthCleanupDeleteAPI freezes the administrative cleanup authority boundary.
// The row returned by each DELETE is the current row that the database actually
// removed; callers must not drive physical cleanup from an earlier unlocked read.
type healthCleanupDeleteAPI interface {
	DeleteHealthRecordByIDReturning(context.Context, int64) (*FileHealth, error)
	DeleteHealthRecordsBulkReturning(context.Context, []string) ([]*FileHealth, error)
	DeleteHealthRecordsByDateReturning(context.Context, time.Time, *HealthStatus) ([]*FileHealth, error)
}

type healthLibraryCleanupDeleteAPI interface {
	DeleteHealthRecordByLibraryPathReturning(context.Context, string) (*FileHealth, error)
}

func requireHealthBatchClaimAPI(t *testing.T, repo *HealthRepository) healthBatchClaimAPI {
	t.Helper()
	claims, ok := any(repo).(healthBatchClaimAPI)
	require.True(t, ok, "HealthRepository must expose atomic token-bound ClaimFilesCheckingBulk claim-and-return")
	return claims
}

func requireHealthClaimRotationAPI(t *testing.T, repo *HealthRepository) healthClaimRotationAPI {
	t.Helper()
	rotation, ok := any(repo).(healthClaimRotationAPI)
	require.True(t, ok, "HealthRepository must expose checked fresh-token RotateHealthClaim")
	return rotation
}

func requireHealthCleanupDeleteAPI(t *testing.T, repo *HealthRepository) healthCleanupDeleteAPI {
	t.Helper()
	cleanup, ok := any(repo).(healthCleanupDeleteAPI)
	require.True(t, ok, "HealthRepository must expose atomic delete-and-return cleanup boundaries")
	return cleanup
}

func requireHealthLibraryCleanupDeleteAPI(t *testing.T, repo *HealthRepository) healthLibraryCleanupDeleteAPI {
	t.Helper()
	cleanup, ok := any(repo).(healthLibraryCleanupDeleteAPI)
	require.True(t, ok, "HealthRepository must atomically delete and return the current library-path row")
	return cleanup
}

func TestClaimFilesCheckingBulkReturnsFreshRowsWithUniquePerRowTokens(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	paths := []string{"claim/a.mkv", "claim/b.mkv", "claim/c.mkv"}
	for _, path := range paths {
		_, err := repo.db.ExecContext(ctx, `
			INSERT INTO file_health (file_path, library_path, status, metadata, updated_at)
			VALUES (?, '/stale/library.mkv', 'pending', '{"snapshot":"stale"}', '2001-02-03 04:05:06')
		`, path)
		require.NoError(t, err)
	}

	selected := make([]*FileHealth, len(paths))
	for i, path := range paths {
		row, err := repo.GetFileHealth(ctx, path)
		require.NoError(t, err)
		require.NotNil(t, row)
		selected[i] = row
		_, err = repo.db.ExecContext(ctx, `
			UPDATE file_health
			SET library_path = ?, metadata = ?, updated_at = '2001-02-03 04:05:06'
			WHERE id = ? AND status = 'pending'
		`, "/current/"+path, `{"snapshot":"current","path":"`+path+`"}`, row.ID)
		require.NoError(t, err)
	}

	fresh, tokens, err := requireHealthBatchClaimAPI(t, repo).ClaimFilesCheckingBulk(ctx, selected)
	require.NoError(t, err)
	require.Len(t, fresh, len(paths))
	require.Len(t, tokens, len(paths))

	seen := make(map[string]struct{}, len(tokens))
	for i, row := range fresh {
		require.NotNil(t, row)
		assert.Equal(t, selected[i].ID, row.ID, "fresh rows remain aligned with admitted inputs")
		assert.Equal(t, paths[i], row.FilePath)
		assert.Equal(t, HealthStatusChecking, row.Status)
		require.NotNil(t, row.LibraryPath)
		assert.Equal(t, "/current/"+paths[i], *row.LibraryPath, "claim must return the current row, not the selection snapshot")
		require.NotNil(t, row.Metadata)
		assert.JSONEq(t, `{"snapshot":"current","path":"`+paths[i]+`"}`, *row.Metadata)

		token := tokens[i]
		assert.NotEmpty(t, token)
		_, reused := seen[token]
		assert.False(t, reused, "every claimed row receives a globally fresh token")
		seen[token] = struct{}{}

		var storedToken, storedLibrary, storedMetadata, storedStatus, storedUpdatedAt string
		require.NoError(t, repo.db.QueryRowContext(ctx, `
			SELECT health_claim_token, library_path, metadata, status, CAST(updated_at AS TEXT)
			FROM file_health WHERE id = ? AND file_path = ? AND health_claim_token = ?
		`, row.ID, row.FilePath, token).Scan(&storedToken, &storedLibrary, &storedMetadata, &storedStatus, &storedUpdatedAt))
		assert.Equal(t, token, storedToken)
		assert.Equal(t, "/current/"+paths[i], storedLibrary)
		assert.JSONEq(t, `{"snapshot":"current","path":"`+paths[i]+`"}`, storedMetadata)
		assert.Equal(t, "checking", storedStatus)
		assert.Contains(t, storedUpdatedAt, "2001-02-03 04:05:06", "claim API bookkeeping must not change business updated_at")
		assert.Equal(t, 2001, row.UpdatedAt.Year(), "the token-bound returned row must carry the unchanged business timestamp")
	}
}

func TestClaimFilesCheckingBulkTakeoverBeforePostconditionErrorsAndRollsBack(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	const path = "claim/takeover.mkv"
	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO file_health (
			file_path, library_path, status, metadata, source_nzb_path,
			scheduled_check_at, last_error, error_details, updated_at
		) VALUES (?, '/preclaim/library.mkv', 'pending', '{"owner":"preclaim"}',
		          '/preclaim/source.nzb', '2040-01-02 03:04:05', 'preclaim-error',
		          '{"owner":"preclaim","evidence":"original"}', '2001-02-03 04:05:06')
	`, path)
	require.NoError(t, err)
	selected, err := repo.GetFileHealth(ctx, path)
	require.NoError(t, err)
	require.NotNil(t, selected)

	_, err = repo.db.ExecContext(ctx, `
		CREATE TRIGGER replace_claim_before_returned_row_postcondition
		AFTER UPDATE OF health_claim_token ON file_health
		WHEN OLD.health_claim_token IS NULL AND NEW.health_claim_token IS NOT NULL
		BEGIN
			UPDATE file_health
			SET library_path = '/owner-b/library.mkv',
			    metadata = '{"owner":"b"}',
			    source_nzb_path = '/owner-b/source.nzb',
			    scheduled_check_at = '2042-03-04 05:06:07',
			    last_error = 'owner-b-error',
			    error_details = '{"owner":"b","evidence":"takeover"}',
			    health_claim_token = 'owner-b'
			WHERE id = NEW.id;
		END;
	`)
	require.NoError(t, err)

	_, _, err = requireHealthBatchClaimAPI(t, repo).ClaimFilesCheckingBulk(ctx, []*FileHealth{selected})
	assert.Error(t, err, "takeover after UPDATE but before returned-row postcondition must reject and roll back the claim")

	var status, libraryPath, metadata, sourceNZB, scheduledAt, lastError, errorDetails, updatedAt string
	var token sql.NullString
	require.NoError(t, repo.db.QueryRowContext(ctx, `
		SELECT status, library_path, metadata, source_nzb_path, health_claim_token,
		       CAST(scheduled_check_at AS TEXT), last_error, error_details, CAST(updated_at AS TEXT)
		FROM file_health WHERE id = ? AND file_path = ?
	`, selected.ID, path).Scan(
		&status, &libraryPath, &metadata, &sourceNZB, &token,
		&scheduledAt, &lastError, &errorDetails, &updatedAt,
	))
	assert.Equal(t, "pending", status)
	assert.Equal(t, "/preclaim/library.mkv", libraryPath)
	assert.JSONEq(t, `{"owner":"preclaim"}`, metadata)
	assert.Equal(t, "/preclaim/source.nzb", sourceNZB)
	assert.False(t, token.Valid, "claim and takeover must roll back together")
	assert.Contains(t, scheduledAt, "2040-01-02 03:04:05")
	assert.Equal(t, "preclaim-error", lastError)
	assert.JSONEq(t, `{"owner":"preclaim","evidence":"original"}`, errorDetails)
	assert.Contains(t, updatedAt, "2001-02-03 04:05:06")
}

func TestRotateHealthClaimUsesFreshTokenChecksStaleOwnerAndPreservesTimestamp(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	const path = "claim/rotate-api.mkv"
	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO file_health (file_path, status, metadata, updated_at)
		VALUES (?, 'pending', '{"owner":"selected"}', '2001-02-03 04:05:06')
	`, path)
	require.NoError(t, err)
	selected, err := repo.GetFileHealth(ctx, path)
	require.NoError(t, err)
	require.NotNil(t, selected)

	fresh, tokens, err := requireHealthBatchClaimAPI(t, repo).ClaimFilesCheckingBulk(ctx, []*FileHealth{selected})
	require.NoError(t, err)
	require.Len(t, fresh, 1)
	require.Len(t, tokens, 1)
	oldToken := tokens[0]
	require.NotEmpty(t, oldToken)

	newToken, err := requireHealthClaimRotationAPI(t, repo).RotateHealthClaim(
		ctx, fresh[0].ID, fresh[0].FilePath, HealthStatusChecking, oldToken,
	)
	require.NoError(t, err)
	assert.NotEmpty(t, newToken)
	assert.NotEqual(t, oldToken, newToken, "every boundary rotation must mint a globally fresh token")

	var storedToken, updatedAt string
	require.NoError(t, repo.db.QueryRowContext(ctx, `
		SELECT health_claim_token, CAST(updated_at AS TEXT)
		FROM file_health WHERE id = ? AND file_path = ? AND status = 'checking'
	`, fresh[0].ID, fresh[0].FilePath).Scan(&storedToken, &updatedAt))
	assert.Equal(t, newToken, storedToken)
	assert.Contains(t, updatedAt, "2001-02-03 04:05:06", "rotation API bookkeeping must not change business updated_at")

	staleResult, err := requireHealthClaimRotationAPI(t, repo).RotateHealthClaim(
		ctx, fresh[0].ID, fresh[0].FilePath, HealthStatusChecking, oldToken,
	)
	assert.Error(t, err, "stale old token must report a checked zero-row rotation")
	assert.Empty(t, staleResult)
	require.NoError(t, repo.db.QueryRowContext(ctx, `
		SELECT health_claim_token, CAST(updated_at AS TEXT)
		FROM file_health WHERE id = ?
	`, fresh[0].ID).Scan(&storedToken, &updatedAt))
	assert.Equal(t, newToken, storedToken, "stale rotation must preserve the current owner")
	assert.Contains(t, updatedAt, "2001-02-03 04:05:06")
}

func TestUpdateHealthStatusBulkStaleGuardRollsBackOwnedSibling(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO file_health (file_path, status, repair_retry_count)
		VALUES ('owned-sibling.mkv', 'repair_triggered', 1),
		       ('stale-sibling.mkv', 'healthy', 7)
	`)
	require.NoError(t, err)

	repairStatus := HealthStatusRepairTriggered
	next := time.Now().UTC().Add(time.Hour)
	err = repo.UpdateHealthStatusBulk(ctx, []HealthStatusUpdate{
		{
			Type:             UpdateTypeRepairRetry,
			FilePath:         "owned-sibling.mkv",
			ScheduledCheckAt: next,
			ExpectedStatus:   &repairStatus,
		},
		{
			Type:             UpdateTypeRepairRetry,
			FilePath:         "stale-sibling.mkv",
			ScheduledCheckAt: next,
			ExpectedStatus:   &repairStatus,
		},
	})
	assert.Error(t, err, "a zero-row guarded sibling must fail the whole publication")

	owned, err := repo.GetFileHealth(ctx, "owned-sibling.mkv")
	require.NoError(t, err)
	require.NotNil(t, owned)
	assert.Equal(t, HealthStatusRepairTriggered, owned.Status)
	assert.Equal(t, 1, owned.RepairRetryCount, "the earlier owned update must roll back with its stale sibling")

	stale, err := repo.GetFileHealth(ctx, "stale-sibling.mkv")
	require.NoError(t, err)
	require.NotNil(t, stale)
	assert.Equal(t, HealthStatusHealthy, stale.Status)
	assert.Equal(t, 7, stale.RepairRetryCount)
}

func TestHealthCleanupDeleteByIDReturnsFreshRelinkedRow(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	const oldPath = "cleanup/by-id-old.mkv"
	const currentPath = "cleanup/by-id-current.mkv"
	const oldLibrary = "/library/old/by-id.mkv"
	const currentLibrary = "/library/current/by-id.mkv"

	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO file_health (file_path, library_path, status, metadata)
		VALUES (?, ?, 'corrupted', '{"revision":"old"}')
	`, oldPath, oldLibrary)
	require.NoError(t, err)
	selected, err := repo.GetFileHealth(ctx, oldPath)
	require.NoError(t, err)
	require.NotNil(t, selected)

	_, err = repo.db.ExecContext(ctx, `
		UPDATE file_health
		SET file_path = ?, library_path = ?, metadata = '{"revision":"current"}'
		WHERE id = ?
	`, currentPath, currentLibrary, selected.ID)
	require.NoError(t, err)

	deleted, err := requireHealthCleanupDeleteAPI(t, repo).DeleteHealthRecordByIDReturning(ctx, selected.ID)
	require.NoError(t, err)
	require.NotNil(t, deleted)
	assert.Equal(t, selected.ID, deleted.ID)
	assert.Equal(t, currentPath, deleted.FilePath, "physical cleanup must follow the current same-ID relink")
	require.NotNil(t, deleted.LibraryPath)
	assert.Equal(t, currentLibrary, *deleted.LibraryPath)
	require.NotNil(t, deleted.Metadata)
	assert.JSONEq(t, `{"revision":"current"}`, *deleted.Metadata)

	current, err := repo.GetFileHealthByID(ctx, selected.ID)
	require.NoError(t, err)
	assert.Nil(t, current)
}

func TestHealthCleanupBulkReturningUsesCurrentPathRows(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	const stalePath = "cleanup/bulk-relinked-old.mkv"
	const relinkedPath = "cleanup/bulk-relinked-current.mkv"
	const replacedPath = "cleanup/bulk-reimported.mkv"

	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO file_health (file_path, library_path, status, metadata)
		VALUES (?, '/library/stale-relink.mkv', 'corrupted', '{"revision":"old"}'),
		       (?, '/library/stale-reimport.mkv', 'corrupted', '{"revision":"old"}')
	`, stalePath, replacedPath)
	require.NoError(t, err)

	relinked, err := repo.GetFileHealth(ctx, stalePath)
	require.NoError(t, err)
	require.NotNil(t, relinked)
	_, err = repo.db.ExecContext(ctx, `
		UPDATE file_health
		SET file_path = ?, library_path = '/library/current-relink.mkv', metadata = '{"revision":"current"}'
		WHERE id = ?
	`, relinkedPath, relinked.ID)
	require.NoError(t, err)

	replaced, err := repo.GetFileHealth(ctx, replacedPath)
	require.NoError(t, err)
	require.NotNil(t, replaced)
	_, err = repo.db.ExecContext(ctx, `DELETE FROM file_health WHERE id = ?`, replaced.ID)
	require.NoError(t, err)
	_, err = repo.db.ExecContext(ctx, `
		INSERT INTO file_health (file_path, library_path, status, metadata)
		VALUES (?, '/library/current-reimport.mkv', 'pending', '{"revision":"current"}')
	`, replacedPath)
	require.NoError(t, err)

	deleted, err := requireHealthCleanupDeleteAPI(t, repo).DeleteHealthRecordsBulkReturning(
		ctx, []string{stalePath, replacedPath},
	)
	require.NoError(t, err)
	require.Len(t, deleted, 1)
	assert.Equal(t, replacedPath, deleted[0].FilePath)
	assert.NotEqual(t, replaced.ID, deleted[0].ID, "same-path replacement must be represented by its current identity")
	require.NotNil(t, deleted[0].LibraryPath)
	assert.Equal(t, "/library/current-reimport.mkv", *deleted[0].LibraryPath)
	require.NotNil(t, deleted[0].Metadata)
	assert.JSONEq(t, `{"revision":"current"}`, *deleted[0].Metadata)

	stillRelinked, err := repo.GetFileHealth(ctx, relinkedPath)
	require.NoError(t, err)
	require.NotNil(t, stillRelinked, "a path-based request must not delete a row relinked away from the requested path")
	assert.Equal(t, relinked.ID, stillRelinked.ID)
}

func TestHealthCleanupDeleteByLibraryPathReturnsCurrentReplacement(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	const libraryPath = "/library/current/movie.mkv"
	const staleFilePath = "cleanup/library-stale.mkv"
	const currentFilePath = "cleanup/library-current.mkv"

	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO file_health (file_path, library_path, status, metadata)
		VALUES (?, ?, 'corrupted', '{"revision":"old"}')
	`, staleFilePath, libraryPath)
	require.NoError(t, err)
	stale, err := repo.GetFileHealth(ctx, staleFilePath)
	require.NoError(t, err)
	require.NotNil(t, stale)
	_, err = repo.db.ExecContext(ctx, `DELETE FROM file_health WHERE id = ?`, stale.ID)
	require.NoError(t, err)
	_, err = repo.db.ExecContext(ctx, `
		INSERT INTO file_health (file_path, library_path, status, metadata)
		VALUES (?, ?, 'pending', '{"revision":"current"}')
	`, currentFilePath, libraryPath)
	require.NoError(t, err)

	deleted, err := requireHealthLibraryCleanupDeleteAPI(t, repo).DeleteHealthRecordByLibraryPathReturning(ctx, libraryPath)
	require.NoError(t, err)
	require.NotNil(t, deleted)
	assert.NotEqual(t, stale.ID, deleted.ID)
	assert.Equal(t, currentFilePath, deleted.FilePath)
	assert.Equal(t, libraryPath, *deleted.LibraryPath)
	require.NotNil(t, deleted.Metadata)
	assert.JSONEq(t, `{"revision":"current"}`, *deleted.Metadata)
}

func TestHealthCleanupDeleteByLibraryPathSparesRelinkedRow(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	const staleLibraryPath = "/library/stale/movie.mkv"
	const currentLibraryPath = "/library/current/movie.mkv"
	const filePath = "cleanup/library-relinked.mkv"

	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO file_health (file_path, library_path, status)
		VALUES (?, ?, 'corrupted')
	`, filePath, staleLibraryPath)
	require.NoError(t, err)
	selected, err := repo.GetFileHealth(ctx, filePath)
	require.NoError(t, err)
	require.NotNil(t, selected)
	_, err = repo.db.ExecContext(ctx, `UPDATE file_health SET library_path = ? WHERE id = ?`, currentLibraryPath, selected.ID)
	require.NoError(t, err)

	deleted, err := requireHealthLibraryCleanupDeleteAPI(t, repo).DeleteHealthRecordByLibraryPathReturning(ctx, staleLibraryPath)
	require.NoError(t, err)
	assert.Nil(t, deleted)
	current, err := repo.GetFileHealthByID(ctx, selected.ID)
	require.NoError(t, err)
	require.NotNil(t, current)
	require.NotNil(t, current.LibraryPath)
	assert.Equal(t, currentLibraryPath, *current.LibraryPath)
}

func TestHealthCleanupDeleteByDateRechecksEligibilityAndReturnsFreshRows(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	const eligiblePath = "cleanup/age-current.mkv"
	const revokedPath = "cleanup/age-revoked.mkv"
	const ownedPath = "cleanup/age-owned.mkv"

	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO file_health (file_path, library_path, status, metadata, created_at, health_claim_token)
		VALUES (?, '/library/stale-age.mkv', 'corrupted', '{"revision":"old"}', datetime('now', '-30 days'), NULL),
		       (?, '/library/revoked-age.mkv', 'corrupted', '{"revision":"old"}', datetime('now', '-30 days'), NULL),
		       (?, '/library/owned-age.mkv', 'corrupted', '{"revision":"owned"}', datetime('now', '-30 days'), 'active-owner')
	`, eligiblePath, revokedPath, ownedPath)
	require.NoError(t, err)
	_, err = repo.db.ExecContext(ctx, `
		UPDATE file_health
		SET library_path = '/library/current-age.mkv', metadata = '{"revision":"current"}'
		WHERE file_path = ?
	`, eligiblePath)
	require.NoError(t, err)
	_, err = repo.db.ExecContext(ctx, `
		UPDATE file_health SET status = 'healthy', created_at = datetime('now') WHERE file_path = ?
	`, revokedPath)
	require.NoError(t, err)

	status := HealthStatusCorrupted
	deleted, err := requireHealthCleanupDeleteAPI(t, repo).DeleteHealthRecordsByDateReturning(
		ctx, time.Now().Add(-7*24*time.Hour), &status,
	)
	require.NoError(t, err)
	require.Len(t, deleted, 1)
	assert.Equal(t, eligiblePath, deleted[0].FilePath)
	require.NotNil(t, deleted[0].LibraryPath)
	assert.Equal(t, "/library/current-age.mkv", *deleted[0].LibraryPath)
	require.NotNil(t, deleted[0].Metadata)
	assert.JSONEq(t, `{"revision":"current"}`, *deleted[0].Metadata)

	revoked, err := repo.GetFileHealth(ctx, revokedPath)
	require.NoError(t, err)
	require.NotNil(t, revoked, "current age/status eligibility must be checked by the deleting statement")
	assert.Equal(t, HealthStatusHealthy, revoked.Status)
	owned, err := repo.GetFileHealth(ctx, ownedPath)
	require.NoError(t, err)
	require.NotNil(t, owned, "unattended age cleanup must defer an actively owned row")
	require.NotNil(t, owned.HealthClaimToken)
	assert.Equal(t, "active-owner", *owned.HealthClaimToken)
}

func TestAutomaticBulkCleanupSkipsActiveClaims(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	const idlePath = "cleanup/automatic-idle.mkv"
	const ownedPath = "cleanup/automatic-owned.mkv"
	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO file_health (file_path, status, health_claim_token)
		VALUES (?, 'corrupted', NULL), (?, 'checking', 'active-owner')
	`, idlePath, ownedPath)
	require.NoError(t, err)

	deleted, err := repo.DeleteHealthRecordsBulk(ctx, []string{idlePath, ownedPath})
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)
	idle, err := repo.GetFileHealth(ctx, idlePath)
	require.NoError(t, err)
	assert.Nil(t, idle)
	owned, err := repo.GetFileHealth(ctx, ownedPath)
	require.NoError(t, err)
	require.NotNil(t, owned, "automatic reconciliation must leave active ownership for a later pass")
	require.NotNil(t, owned.HealthClaimToken)
	assert.Equal(t, "active-owner", *owned.HealthClaimToken)
}

func TestDeleteHealthRecordsBulkRollsBackEarlierChunks(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	paths := make([]string, 251)
	for i := range paths {
		paths[i] = fmt.Sprintf("cleanup/rollback-%03d.mkv", i)
		_, err := repo.db.ExecContext(ctx, `
			INSERT INTO file_health (file_path, status) VALUES (?, 'corrupted')
		`, paths[i])
		require.NoError(t, err)
	}
	_, err := repo.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TRIGGER reject_later_cleanup_chunk
		BEFORE DELETE ON file_health
		WHEN OLD.file_path = '%s'
		BEGIN
			SELECT RAISE(FAIL, 'synthetic later-chunk cleanup failure');
		END
	`, paths[250]))
	require.NoError(t, err)

	deleted, err := repo.DeleteHealthRecordsBulk(ctx, paths)
	require.Error(t, err)
	assert.Equal(t, int64(0), deleted, "rolled-back chunks must not be reported as durably deleted")
	for _, path := range []string{paths[0], paths[249], paths[250]} {
		row, getErr := repo.GetFileHealth(ctx, path)
		require.NoError(t, getErr)
		assert.NotNil(t, row, "all chunks must roll back when a later chunk fails: %s", path)
	}
}
