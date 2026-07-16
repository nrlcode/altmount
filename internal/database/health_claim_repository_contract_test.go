package database

import (
	"context"
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

func requireHealthBatchClaimAPI(t *testing.T, repo *HealthRepository) healthBatchClaimAPI {
	t.Helper()
	claims, ok := any(repo).(healthBatchClaimAPI)
	require.True(t, ok, "HealthRepository must expose atomic token-bound ClaimFilesCheckingBulk claim-and-return")
	return claims
}

func TestClaimFilesCheckingBulkReturnsFreshRowsWithUniquePerRowTokens(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	paths := []string{"claim/a.mkv", "claim/b.mkv", "claim/c.mkv"}
	for _, path := range paths {
		_, err := repo.db.ExecContext(ctx, `
			INSERT INTO file_health (file_path, library_path, status, metadata)
			VALUES (?, '/stale/library.mkv', 'pending', '{"snapshot":"stale"}')
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
			SET library_path = ?, metadata = ?
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

		var storedToken, storedLibrary, storedMetadata, storedStatus string
		require.NoError(t, repo.db.QueryRowContext(ctx, `
			SELECT health_claim_token, library_path, metadata, status
			FROM file_health WHERE id = ? AND file_path = ? AND health_claim_token = ?
		`, row.ID, row.FilePath, token).Scan(&storedToken, &storedLibrary, &storedMetadata, &storedStatus))
		assert.Equal(t, token, storedToken)
		assert.Equal(t, "/current/"+paths[i], storedLibrary)
		assert.JSONEq(t, `{"snapshot":"current","path":"`+paths[i]+`"}`, storedMetadata)
		assert.Equal(t, "checking", storedStatus)
	}
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
