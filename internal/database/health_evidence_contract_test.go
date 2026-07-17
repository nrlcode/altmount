package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// schemaFreeHealthEvidence is the narrow repository boundary required by
// FACORE-AD-004. Identity is the existing row ID and transient checking status.
type schemaFreeHealthEvidence interface {
	ClaimFilesCheckingBulk(context.Context, []*FileHealth) ([]*FileHealth, error)
	PublishClaimedHealthStatusBulk(context.Context, []*FileHealth, []HealthStatusUpdate) error
}

func requireSchemaFreeHealthEvidence(t *testing.T, repo *HealthRepository) schemaFreeHealthEvidence {
	t.Helper()
	api, ok := any(repo).(schemaFreeHealthEvidence)
	require.True(t, ok, "HealthRepository must expose the schema-free claim/publication boundary")
	if !ok {
		return nil
	}
	return api
}

func TestClaimFilesCheckingBulkReturnsCurrentRows(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO file_health (file_path, library_path, status)
		VALUES ('movies/current.mkv', '/library/observed.mkv', 'pending')
	`)
	require.NoError(t, err)
	selected, err := repo.GetFileHealth(ctx, "movies/current.mkv")
	require.NoError(t, err)
	require.NoError(t, func() error {
		_, updateErr := repo.db.ExecContext(ctx, `
			UPDATE file_health SET library_path = '/library/current.mkv'
			WHERE id = ?
		`, selected.ID)
		return updateErr
	}())

	claimed, err := requireSchemaFreeHealthEvidence(t, repo).ClaimFilesCheckingBulk(ctx, []*FileHealth{selected})
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	assert.Equal(t, selected.ID, claimed[0].ID)
	assert.Equal(t, HealthStatusChecking, claimed[0].Status)
	require.NotNil(t, claimed[0].LibraryPath)
	assert.Equal(t, "/library/current.mkv", *claimed[0].LibraryPath,
		"the worker must receive the current database snapshot, not its earlier selection")
}

func TestClaimFilesCheckingBulkOmitsStaleMembers(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *HealthRepository, []*FileHealth)
	}{
		{
			name: "stale status",
			mutate: func(t *testing.T, repo *HealthRepository, selected []*FileHealth) {
				_, err := repo.db.ExecContext(context.Background(),
					`UPDATE file_health SET status = 'healthy' WHERE id = ?`, selected[1].ID)
				require.NoError(t, err)
			},
		},
		{
			name: "same-path replacement",
			mutate: func(t *testing.T, repo *HealthRepository, selected []*FileHealth) {
				_, err := repo.db.ExecContext(context.Background(), `
					DELETE FROM file_health WHERE id = ?;
					INSERT INTO file_health (file_path, status) VALUES ('b.mkv', 'pending');
				`, selected[1].ID)
				require.NoError(t, err)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := setupTestDB(t)
			ctx := context.Background()
			_, err := repo.db.ExecContext(ctx, `
				INSERT INTO file_health (file_path, status)
				VALUES ('a.mkv', 'pending'), ('b.mkv', 'pending')
			`)
			require.NoError(t, err)
			selected := make([]*FileHealth, 0, 2)
			for _, path := range []string{"a.mkv", "b.mkv"} {
				row, getErr := repo.GetFileHealth(ctx, path)
				require.NoError(t, getErr)
				selected = append(selected, row)
			}
			tc.mutate(t, repo, selected)

			claimed, err := requireSchemaFreeHealthEvidence(t, repo).ClaimFilesCheckingBulk(ctx, selected)
			require.NoError(t, err)
			require.Len(t, claimed, 1, "stale or replaced members are omitted from admission")
			assert.Equal(t, selected[0].ID, claimed[0].ID)
			assert.Equal(t, HealthStatusChecking, claimed[0].Status)
			a, getErr := repo.GetFileHealth(ctx, "a.mkv")
			require.NoError(t, getErr)
			assert.Equal(t, HealthStatusChecking, a.Status)
		})
	}
}

func TestClaimFilesCheckingBulkWriteFailureRollsBack(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO file_health (file_path, status)
		VALUES ('a.mkv', 'pending'), ('b.mkv', 'pending');
		CREATE TRIGGER reject_b_claim BEFORE UPDATE OF status ON file_health
		WHEN OLD.file_path = 'b.mkv' AND NEW.status = 'checking'
		BEGIN SELECT RAISE(ABORT, 'synthetic claim failure'); END;
	`)
	require.NoError(t, err)
	selected := make([]*FileHealth, 0, 2)
	for _, path := range []string{"a.mkv", "b.mkv"} {
		row, getErr := repo.GetFileHealth(ctx, path)
		require.NoError(t, getErr)
		selected = append(selected, row)
	}

	_, err = requireSchemaFreeHealthEvidence(t, repo).ClaimFilesCheckingBulk(ctx, selected)
	require.Error(t, err)
	for _, path := range []string{"a.mkv", "b.mkv"} {
		row, getErr := repo.GetFileHealth(ctx, path)
		require.NoError(t, getErr)
		assert.Equal(t, HealthStatusPending, row.Status, "SQL failure must roll back the whole transaction")
	}
}

func TestPublishClaimedHealthStatusBulkRejectsReplacementAtomically(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO file_health (file_path, status)
		VALUES ('a.mkv', 'pending'), ('b.mkv', 'pending')
	`)
	require.NoError(t, err)
	selected := make([]*FileHealth, 0, 2)
	for _, path := range []string{"a.mkv", "b.mkv"} {
		row, getErr := repo.GetFileHealth(ctx, path)
		require.NoError(t, getErr)
		selected = append(selected, row)
	}
	api := requireSchemaFreeHealthEvidence(t, repo)
	claimed, err := api.ClaimFilesCheckingBulk(ctx, selected)
	require.NoError(t, err)

	_, err = repo.db.ExecContext(ctx, `
		DELETE FROM file_health WHERE id = ?;
		INSERT INTO file_health (file_path, status) VALUES ('b.mkv', 'checking');
	`, claimed[1].ID)
	require.NoError(t, err)
	replacement, err := repo.GetFileHealth(ctx, "b.mkv")
	require.NoError(t, err)
	require.NotEqual(t, claimed[1].ID, replacement.ID)

	next := time.Now().UTC().Add(time.Hour)
	err = api.PublishClaimedHealthStatusBulk(ctx, claimed, []HealthStatusUpdate{
		{Type: UpdateTypeHealthy, FilePath: "a.mkv", ScheduledCheckAt: next},
		{Type: UpdateTypeHealthy, FilePath: "b.mkv", ScheduledCheckAt: next},
	})
	require.Error(t, err, "publication must require every claimed ID to remain checking")
	a, err := repo.GetFileHealth(ctx, "a.mkv")
	require.NoError(t, err)
	assert.Equal(t, HealthStatusChecking, a.Status, "a rejected member must roll back all evidence publication")
	currentB, err := repo.GetFileHealth(ctx, "b.mkv")
	require.NoError(t, err)
	assert.Equal(t, replacement.ID, currentB.ID)
	assert.Equal(t, HealthStatusChecking, currentB.Status)
}
