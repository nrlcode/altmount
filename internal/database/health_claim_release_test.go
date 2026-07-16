package database

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// healthClaimReleaseAPI freezes the narrow token-only release boundary without
// coupling callers to a persistent version/epoch column. ID + canonical path +
// globally fresh token are the complete ownership predicate.
type healthClaimReleaseAPI interface {
	ReleaseHealthClaim(context.Context, int64, string, string) error
}

func requireHealthClaimReleaseAPI(t *testing.T, repo *HealthRepository) healthClaimReleaseAPI {
	t.Helper()
	release, ok := any(repo).(healthClaimReleaseAPI)
	require.True(t, ok, "HealthRepository must expose token-bound ReleaseHealthClaim(ctx, id, path, token)")
	return release
}

func TestReleaseHealthClaimRejectsLostTokenAndPreservesWinner(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	const path = "claim/release-lost.mkv"
	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO file_health (
			file_path, status, metadata, health_claim_token, scheduled_check_at,
			last_error, error_details, updated_at
		) VALUES (?, 'checking', '{"owner":"b"}', 'owner-b', '2042-03-04 05:06:07',
		          'owner-b-error', '{"owner":"b","evidence":"complete"}', '2001-02-03 04:05:06')
	`, path)
	require.NoError(t, err)
	var id int64
	require.NoError(t, repo.db.QueryRowContext(ctx, `SELECT id FROM file_health WHERE file_path = ?`, path).Scan(&id))

	err = requireHealthClaimReleaseAPI(t, repo).ReleaseHealthClaim(ctx, id, path, "owner-a")
	require.Error(t, err, "zero affected rows must report stale release ownership")

	var status, metadata, token, scheduledAt, lastError, errorDetails, updatedAt string
	require.NoError(t, repo.db.QueryRowContext(ctx, `
		SELECT status, metadata, health_claim_token, CAST(scheduled_check_at AS TEXT),
		       last_error, error_details, CAST(updated_at AS TEXT)
		FROM file_health WHERE file_path = ?
	`, path).Scan(&status, &metadata, &token, &scheduledAt, &lastError, &errorDetails, &updatedAt))
	assert.Equal(t, "checking", status)
	assert.JSONEq(t, `{"owner":"b"}`, metadata)
	assert.Equal(t, "owner-b", token)
	assert.Contains(t, scheduledAt, "2042-03-04 05:06:07")
	assert.Equal(t, "owner-b-error", lastError)
	assert.JSONEq(t, `{"owner":"b","evidence":"complete"}`, errorDetails)
	assert.Contains(t, updatedAt, "2001-02-03 04:05:06")
}

func TestReleaseHealthClaimClearsOnlyTokenWithoutBusinessTimestampChange(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	const path = "claim/release-owned.mkv"
	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO file_health (
			file_path, status, metadata, health_claim_token, scheduled_check_at,
			last_error, error_details, updated_at
		) VALUES (?, 'checking', '{"owner":"a"}', 'owner-a', '2042-03-04 05:06:07',
		          'owner-a-error', '{"owner":"a","evidence":"incomplete"}', '2001-02-03 04:05:06')
	`, path)
	require.NoError(t, err)
	var id int64
	require.NoError(t, repo.db.QueryRowContext(ctx, `SELECT id FROM file_health WHERE file_path = ?`, path).Scan(&id))

	require.NoError(t, requireHealthClaimReleaseAPI(t, repo).ReleaseHealthClaim(ctx, id, path, "owner-a"))

	var status, metadata, scheduledAt, lastError, errorDetails, updatedAt string
	var token sql.NullString
	require.NoError(t, repo.db.QueryRowContext(ctx, `
		SELECT status, metadata, health_claim_token, CAST(scheduled_check_at AS TEXT),
		       last_error, error_details, CAST(updated_at AS TEXT)
		FROM file_health WHERE file_path = ?
	`, path).Scan(&status, &metadata, &token, &scheduledAt, &lastError, &errorDetails, &updatedAt))
	assert.Equal(t, "checking", status)
	assert.JSONEq(t, `{"owner":"a"}`, metadata)
	assert.False(t, token.Valid, "successful release clears exactly the owned token")
	assert.Contains(t, scheduledAt, "2042-03-04 05:06:07")
	assert.Equal(t, "owner-a-error", lastError)
	assert.JSONEq(t, `{"owner":"a","evidence":"incomplete"}`, errorDetails)
	assert.Contains(t, updatedAt, "2001-02-03 04:05:06", "token-only release must not change business updated_at")
}
