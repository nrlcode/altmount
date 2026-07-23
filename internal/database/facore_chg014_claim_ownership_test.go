package database

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newCHG014HealthRepositories(t *testing.T) (*HealthRepository, *HealthRepository) {
	t.Helper()

	db, err := NewDB(Config{
		Type:         "sqlite",
		DatabasePath: filepath.Join(t.TempDir(), "health-claim-ownership.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	// Deliberately use separate repository values over the shared durable
	// authority. Ownership must not depend on which repository instance acts.
	return NewHealthRepository(db.Connection(), DialectSQLite),
		NewHealthRepository(db.Connection(), DialectSQLite)
}

func insertCHG014PendingHealth(t *testing.T, repo *HealthRepository, path string, retryCount int, lastError *string) {
	t.Helper()
	_, err := repo.db.ExecContext(context.Background(), `
		INSERT INTO file_health
			(file_path, status, retry_count, max_retries, last_error, scheduled_check_at)
		VALUES (?, 'pending', ?, 3, ?, datetime('now', '-1 second'))
	`, path, retryCount, lastError)
	require.NoError(t, err)
}

func claimCHG014Health(t *testing.T, repo *HealthRepository, path string) *FileHealth {
	t.Helper()
	ctx := context.Background()
	selected, err := repo.GetFileHealth(ctx, path)
	require.NoError(t, err)
	require.NotNil(t, selected)
	claimed, err := repo.ClaimFilesCheckingBulk(ctx, []*FileHealth{selected})
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.Equal(t, selected.ID, claimed[0].ID)
	require.Equal(t, HealthStatusChecking, claimed[0].Status)
	return claimed[0]
}

func rearmAndReclaimCHG014Health(t *testing.T, repo *HealthRepository, owner *FileHealth) *FileHealth {
	t.Helper()
	_, err := repo.db.ExecContext(context.Background(), `
		UPDATE file_health
		SET status = 'pending', scheduled_check_at = datetime('now')
		WHERE id = ?
	`, owner.ID)
	require.NoError(t, err)

	newOwner := claimCHG014Health(t, repo, owner.FilePath)
	require.Equal(t, owner.ID, newOwner.ID, "the regression requires a same-row reclaim")
	return newOwner
}

func TestFACORECHG014ClaimGenerationAdvancesAcrossSameRowReclaim(t *testing.T) {
	repoA, repoB := newCHG014HealthRepositories(t)
	insertCHG014PendingHealth(t, repoA, "movies/generation.mkv", 0, nil)

	ownerA := claimCHG014Health(t, repoA, "movies/generation.mkv")
	ownerB := rearmAndReclaimCHG014Health(t, repoB, ownerA)

	assert.Greater(t, ownerB.ClaimGeneration, ownerA.ClaimGeneration,
		"every successful claim must advance persisted ownership")
}

func TestFACORECHG014StaleSelectionCannotReclaimAfterNewerPublication(t *testing.T) {
	repoA, repoB := newCHG014HealthRepositories(t)
	path := "movies/stale-selection.mkv"
	insertCHG014PendingHealth(t, repoA, path, 0, nil)

	staleSelection, err := repoA.GetFileHealth(context.Background(), path)
	require.NoError(t, err)
	require.NotNil(t, staleSelection)

	claimed, err := repoB.ClaimFilesCheckingBulk(context.Background(), []*FileHealth{staleSelection})
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	newerEvidence := "newer owner deferred the check"
	require.NoError(t, repoB.PublishClaimedHealthStatusBulk(context.Background(), claimed, []HealthStatusUpdate{{
		Type:             UpdateTypeInconclusive,
		Status:           HealthStatusPending,
		FilePath:         path,
		ErrorMessage:     &newerEvidence,
		ScheduledCheckAt: time.Now().UTC().Add(2 * time.Hour),
	}}))
	published, err := repoB.GetFileHealth(context.Background(), path)
	require.NoError(t, err)
	require.Equal(t, HealthStatusPending, published.Status)
	require.Greater(t, published.ClaimGeneration, staleSelection.ClaimGeneration)

	claimed, err = repoA.ClaimFilesCheckingBulk(context.Background(), []*FileHealth{staleSelection})
	require.NoError(t, err)
	require.Empty(t, claimed, "an obsolete selection must not bypass a newer owner's evidence and backoff")

	current, err := repoA.GetFileHealth(context.Background(), path)
	require.NoError(t, err)
	assert.Equal(t, published, current, "a rejected stale selection must leave the newer publication unchanged")
}

func TestFACORECHG014StaleOwnerCannotPublishIntoSameRowReclaim(t *testing.T) {
	repoA, repoB := newCHG014HealthRepositories(t)
	insertCHG014PendingHealth(t, repoA, "movies/stale-publication.mkv", 1, nil)

	ownerA := claimCHG014Health(t, repoA, "movies/stale-publication.mkv")
	ownerB := rearmAndReclaimCHG014Health(t, repoB, ownerA)

	err := repoA.PublishClaimedHealthStatusBulk(context.Background(), []*FileHealth{ownerA}, []HealthStatusUpdate{{
		Type:             UpdateTypeHealthy,
		Status:           HealthStatusHealthy,
		FilePath:         ownerA.FilePath,
		ScheduledCheckAt: time.Now().UTC().Add(time.Hour),
	}})
	require.Error(t, err, "a stale owner must not publish through a newer same-row claim")

	current, getErr := repoB.GetFileHealth(context.Background(), ownerB.FilePath)
	require.NoError(t, getErr)
	assert.Equal(t, ownerB, current, "rejected stale publication must not mutate the new owner")
}

func TestFACORECHG014LostGenerationRollsBackWholePublication(t *testing.T) {
	repoA, repoB := newCHG014HealthRepositories(t)
	paths := []string{"movies/atomic-a.mkv", "movies/atomic-b.mkv"}
	for _, path := range paths {
		insertCHG014PendingHealth(t, repoA, path, 0, nil)
	}
	ownerA := claimCHG014Health(t, repoA, paths[0])
	staleB := claimCHG014Health(t, repoA, paths[1])
	ownerB := rearmAndReclaimCHG014Health(t, repoB, staleB)

	err := repoA.PublishClaimedHealthStatusBulk(context.Background(), []*FileHealth{ownerA, staleB}, []HealthStatusUpdate{
		{
			Type:             UpdateTypeHealthy,
			Status:           HealthStatusHealthy,
			FilePath:         ownerA.FilePath,
			ScheduledCheckAt: time.Now().UTC().Add(time.Hour),
		},
		{
			Type:             UpdateTypeHealthy,
			Status:           HealthStatusHealthy,
			FilePath:         staleB.FilePath,
			ScheduledCheckAt: time.Now().UTC().Add(time.Hour),
		},
	})
	require.Error(t, err, "one lost generation must reject the whole publication")

	currentA, getErr := repoA.GetFileHealth(context.Background(), ownerA.FilePath)
	require.NoError(t, getErr)
	assert.Equal(t, ownerA, currentA, "publication before the lost member must roll back")
	currentB, getErr := repoB.GetFileHealth(context.Background(), ownerB.FilePath)
	require.NoError(t, getErr)
	assert.Equal(t, ownerB, currentB, "the newer owner must remain unchanged")
}

func TestFACORECHG014ZeroGenerationCannotFinalizeClaim(t *testing.T) {
	repoA, _ := newCHG014HealthRepositories(t)
	insertCHG014PendingHealth(t, repoA, "movies/zero-generation.mkv", 0, nil)
	owner := claimCHG014Health(t, repoA, "movies/zero-generation.mkv")
	forged := *owner
	forged.ClaimGeneration = 0

	err := repoA.PublishClaimedHealthStatusBulk(context.Background(), []*FileHealth{&forged}, []HealthStatusUpdate{{
		Type:             UpdateTypeHealthy,
		Status:           HealthStatusHealthy,
		FilePath:         forged.FilePath,
		ScheduledCheckAt: time.Now().UTC().Add(time.Hour),
	}})
	require.Error(t, err, "the migration sentinel cannot authorize publication")

	_ = repoA.ReleaseClaimedHealthRows(context.Background(), []*FileHealth{&forged})

	current, getErr := repoA.GetFileHealth(context.Background(), owner.FilePath)
	require.NoError(t, getErr)
	assert.Equal(t, owner, current, "invalid ownership evidence cannot mutate the claim")
}

func TestFACORECHG014ReleaseClaimedHealthRowsHonorsOwnership(t *testing.T) {
	repoA, repoB := newCHG014HealthRepositories(t)

	t.Run("owned checking row becomes due without erasing evidence", func(t *testing.T) {
		priorError := "prior inconclusive evidence"
		insertCHG014PendingHealth(t, repoA, "movies/owned-release.mkv", 2, &priorError)
		insertCHG014PendingHealth(t, repoA, "movies/unrelated-owner.mkv", 1, nil)
		owned := claimCHG014Health(t, repoA, "movies/owned-release.mkv")
		unrelated := claimCHG014Health(t, repoB, "movies/unrelated-owner.mkv")

		require.NoError(t, repoA.ReleaseClaimedHealthRows(context.Background(), []*FileHealth{owned}))

		current, err := repoA.GetFileHealth(context.Background(), owned.FilePath)
		require.NoError(t, err)
		require.NotNil(t, current)
		assert.Equal(t, owned.ID, current.ID)
		assert.Equal(t, HealthStatusPending, current.Status)
		assert.Equal(t, 2, current.RetryCount, "release cannot consume retry budget")
		assert.Equal(t, &priorError, current.LastError, "release cannot erase prior evidence")
		require.NotNil(t, current.ScheduledCheckAt)
		assert.False(t, current.ScheduledCheckAt.After(time.Now().UTC().Add(time.Second)),
			"released work must be immediately selectable")

		stillUnrelated, err := repoB.GetFileHealth(context.Background(), unrelated.FilePath)
		require.NoError(t, err)
		assert.Equal(t, unrelated, stillUnrelated, "release cannot reset another owner's checking row")
	})

	t.Run("stale owner cannot release same row after reset and reclaim", func(t *testing.T) {
		insertCHG014PendingHealth(t, repoA, "movies/stale-release.mkv", 0, nil)
		ownerA := claimCHG014Health(t, repoA, "movies/stale-release.mkv")
		ownerB := rearmAndReclaimCHG014Health(t, repoB, ownerA)

		require.NoError(t, repoA.ReleaseClaimedHealthRows(context.Background(), []*FileHealth{ownerA}))

		current, err := repoB.GetFileHealth(context.Background(), ownerB.FilePath)
		require.NoError(t, err)
		assert.Equal(t, ownerB, current, "stale cleanup cannot release the new owner")
	})

	t.Run("same path replacement remains owned by its new row", func(t *testing.T) {
		insertCHG014PendingHealth(t, repoA, "movies/replacement-release.mkv", 0, nil)
		oldOwner := claimCHG014Health(t, repoA, "movies/replacement-release.mkv")
		_, err := repoB.db.ExecContext(context.Background(), `
			DELETE FROM file_health WHERE id = ?;
			INSERT INTO file_health (file_path, status, scheduled_check_at)
			VALUES (?, 'pending', datetime('now'));
		`, oldOwner.ID, oldOwner.FilePath)
		require.NoError(t, err)
		newOwner := claimCHG014Health(t, repoB, oldOwner.FilePath)
		require.NotEqual(t, oldOwner.ID, newOwner.ID)

		require.NoError(t, repoA.ReleaseClaimedHealthRows(context.Background(), []*FileHealth{oldOwner}))

		current, err := repoA.GetFileHealth(context.Background(), newOwner.FilePath)
		require.NoError(t, err)
		assert.Equal(t, newOwner, current)
	})

	t.Run("successful publication cannot be released afterward", func(t *testing.T) {
		insertCHG014PendingHealth(t, repoA, "movies/published-release.mkv", 0, nil)
		owner := claimCHG014Health(t, repoA, "movies/published-release.mkv")
		require.NoError(t, repoA.PublishClaimedHealthStatusBulk(context.Background(), []*FileHealth{owner}, []HealthStatusUpdate{{
			Type:             UpdateTypeHealthy,
			Status:           HealthStatusHealthy,
			FilePath:         owner.FilePath,
			ScheduledCheckAt: time.Now().UTC().Add(time.Hour),
		}}))
		published, err := repoA.GetFileHealth(context.Background(), owner.FilePath)
		require.NoError(t, err)
		require.Equal(t, HealthStatusHealthy, published.Status)

		require.NoError(t, repoA.ReleaseClaimedHealthRows(context.Background(), []*FileHealth{owner}))

		current, err := repoB.GetFileHealth(context.Background(), owner.FilePath)
		require.NoError(t, err)
		assert.Equal(t, published, current, "cleanup after success must be a fenced no-op")
	})

	t.Run("write failure rolls back the whole release", func(t *testing.T) {
		paths := []string{"movies/release-atomic-a.mkv", "movies/release-atomic-b.mkv"}
		owners := make([]*FileHealth, 0, len(paths))
		for _, path := range paths {
			insertCHG014PendingHealth(t, repoA, path, 0, nil)
			owners = append(owners, claimCHG014Health(t, repoA, path))
		}
		_, err := repoA.db.ExecContext(context.Background(), `
			CREATE TRIGGER reject_chg014_second_release
			BEFORE UPDATE OF status ON file_health
			WHEN OLD.file_path = 'movies/release-atomic-b.mkv'
			 AND OLD.status = 'checking' AND NEW.status = 'pending'
			BEGIN SELECT RAISE(ABORT, 'synthetic claim release failure'); END;
		`)
		require.NoError(t, err)

		err = repoA.ReleaseClaimedHealthRows(context.Background(), owners)
		require.Error(t, err)
		for _, owner := range owners {
			current, getErr := repoA.GetFileHealth(context.Background(), owner.FilePath)
			require.NoError(t, getErr)
			assert.Equal(t, owner, current, "failed release must not partially commit")
		}

		_, err = repoA.db.ExecContext(context.Background(), `DROP TRIGGER reject_chg014_second_release`)
		require.NoError(t, err)
	})
}
