package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// generationFencedHealthEvidence is the claim/publication boundary retained
// when FACORE-CHG-014 superseded FACORE-AD-004's schema-free identity.
type generationFencedHealthEvidence interface {
	ClaimFilesCheckingBulk(context.Context, []*FileHealth) ([]*FileHealth, error)
	PublishClaimedHealthStatusBulk(context.Context, []*FileHealth, []HealthStatusUpdate) error
}

func requireGenerationFencedHealthEvidence(t *testing.T, repo *HealthRepository) generationFencedHealthEvidence {
	t.Helper()
	api, ok := any(repo).(generationFencedHealthEvidence)
	require.True(t, ok, "HealthRepository must expose the generation-fenced claim/publication boundary")
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
	currentSchedule := time.Date(2030, time.January, 2, 3, 4, 5, 0, time.UTC)
	require.NoError(t, func() error {
		_, updateErr := repo.db.ExecContext(ctx, `
			UPDATE file_health
			SET library_path = '/library/current.mkv', scheduled_check_at = ?
			WHERE id = ?
		`, currentSchedule, selected.ID)
		return updateErr
	}())

	claimed, err := requireGenerationFencedHealthEvidence(t, repo).ClaimFilesCheckingBulk(ctx, []*FileHealth{selected})
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	assert.Equal(t, selected.ID, claimed[0].ID)
	assert.Equal(t, HealthStatusChecking, claimed[0].Status)
	require.NotNil(t, claimed[0].LibraryPath)
	assert.Equal(t, "/library/current.mkv", *claimed[0].LibraryPath,
		"the worker must receive the current database snapshot, not its earlier selection")
	require.NotNil(t, claimed[0].ScheduledCheckAt)
	assert.WithinDuration(t, currentSchedule, claimed[0].ScheduledCheckAt.UTC(), time.Second,
		"the claimed snapshot must include scheduling changes made after selection")
}

func TestClaimFilesCheckingBulkOmitsAlreadyCheckingRowWithoutMutation(t *testing.T) {
	repo := setupTestDB(t)
	ctx := context.Background()
	_, err := repo.db.ExecContext(ctx, `
		INSERT INTO file_health (file_path, status, updated_at)
		VALUES ('movies/in-flight.mkv', 'checking', '2020-01-02 03:04:05')
	`)
	require.NoError(t, err)
	selected, err := repo.GetFileHealth(ctx, "movies/in-flight.mkv")
	require.NoError(t, err)
	require.Equal(t, HealthStatusChecking, selected.Status)

	claimed, err := requireGenerationFencedHealthEvidence(t, repo).ClaimFilesCheckingBulk(ctx, []*FileHealth{selected})
	require.NoError(t, err)
	assert.Empty(t, claimed, "an in-flight row cannot be admitted a second time")

	current, err := repo.GetFileHealth(ctx, "movies/in-flight.mkv")
	require.NoError(t, err)
	assert.Equal(t, selected, current, "rejecting an already-checking row must not mutate it")
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

			claimed, err := requireGenerationFencedHealthEvidence(t, repo).ClaimFilesCheckingBulk(ctx, selected)
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

	_, err = requireGenerationFencedHealthEvidence(t, repo).ClaimFilesCheckingBulk(ctx, selected)
	require.Error(t, err)
	for _, path := range []string{"a.mkv", "b.mkv"} {
		row, getErr := repo.GetFileHealth(ctx, path)
		require.NoError(t, getErr)
		assert.Equal(t, HealthStatusPending, row.Status, "SQL failure must roll back the whole transaction")
	}
}

func TestPublishClaimedHealthStatusBulkRejectsLostClaimAtomically(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *HealthRepository, *FileHealth) *FileHealth
	}{
		{
			name: "same-path replacement removes claimed ID",
			mutate: func(t *testing.T, repo *HealthRepository, claimed *FileHealth) *FileHealth {
				_, err := repo.db.ExecContext(context.Background(), `
					DELETE FROM file_health WHERE id = ?;
					INSERT INTO file_health (file_path, status) VALUES ('b.mkv', 'checking');
				`, claimed.ID)
				require.NoError(t, err)
				current, err := repo.GetFileHealth(context.Background(), "b.mkv")
				require.NoError(t, err)
				require.NotEqual(t, claimed.ID, current.ID)
				return current
			},
		},
		{
			name: "claimed ID leaves checking",
			mutate: func(t *testing.T, repo *HealthRepository, claimed *FileHealth) *FileHealth {
				_, err := repo.db.ExecContext(context.Background(),
					`UPDATE file_health SET status = 'pending' WHERE id = ?`, claimed.ID)
				require.NoError(t, err)
				current, err := repo.GetFileHealth(context.Background(), "b.mkv")
				require.NoError(t, err)
				require.Equal(t, claimed.ID, current.ID)
				require.Equal(t, HealthStatusPending, current.Status)
				return current
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
			api := requireGenerationFencedHealthEvidence(t, repo)
			claimed, err := api.ClaimFilesCheckingBulk(ctx, selected)
			require.NoError(t, err)
			beforeB := tc.mutate(t, repo, claimed[1])
			beforeA, err := repo.GetFileHealth(ctx, "a.mkv")
			require.NoError(t, err)

			next := time.Now().UTC().Add(time.Hour)
			err = api.PublishClaimedHealthStatusBulk(ctx, claimed, []HealthStatusUpdate{
				{Type: UpdateTypeHealthy, FilePath: "a.mkv", ScheduledCheckAt: next},
				{Type: UpdateTypeHealthy, FilePath: "b.mkv", ScheduledCheckAt: next},
			})
			require.Error(t, err, "publication must require every claimed ID to remain checking")
			afterA, err := repo.GetFileHealth(ctx, "a.mkv")
			require.NoError(t, err)
			assert.Equal(t, beforeA, afterA, "a rejected member must roll back all evidence publication")
			afterB, err := repo.GetFileHealth(ctx, "b.mkv")
			require.NoError(t, err)
			assert.Equal(t, beforeB, afterB, "rejected publication must not touch the current second row")
		})
	}
}
