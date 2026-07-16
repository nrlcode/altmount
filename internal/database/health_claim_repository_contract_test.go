package database

import (
	"context"
	"database/sql"
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
