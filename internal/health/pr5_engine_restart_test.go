package health

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pr5StateFixture struct {
	db         *database.DB
	repo       *database.HealthStateRepository
	run        *database.HealthRun
	revision   *database.HealthFileRevision
	providerID string
}

func newPR5StateFixture(t *testing.T) pr5StateFixture {
	t.Helper()
	db, err := database.NewDB(database.Config{
		Type:         "sqlite",
		DatabasePath: filepath.Join(t.TempDir(), "pr5-health-state.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	repo := database.NewHealthStateRepository(db.Connection(), database.DialectSQLite)
	ctx := context.Background()
	revision, err := repo.EnsureFileRevision(ctx, database.FileRevisionSpec{
		FilePath:          "synthetic/restart.mkv",
		LayoutFingerprint: "sha256:synthetic-layout-a",
		VirtualSize:       600,
		SegmentCount:      6,
	})
	require.NoError(t, err)
	providers, err := repo.ReconcileProviders(ctx, []database.ProviderSpec{{
		StableID:    "provider-a",
		DisplayName: "Synthetic A",
		Endpoint:    "provider-a.invalid",
		Port:        119,
		Account:     "synthetic-account",
		Role:        database.ProviderRolePrimary,
		Order:       0,
	}})
	require.NoError(t, err)
	snapshot, err := repo.CaptureActiveProviderSnapshot(ctx, time.Now().UTC())
	require.NoError(t, err)
	run, err := repo.CreateHealthRun(ctx, database.HealthRunSpec{
		ID:                 "run-restart",
		FileRevisionID:     revision.ID,
		ProviderSnapshotID: snapshot.ID,
		Trigger:            "scheduled",
		Mode:               "observation",
		TotalSegments:      6,
		CreatedAt:          time.Now().UTC(),
	})
	require.NoError(t, err)
	return pr5StateFixture{db: db, repo: repo, run: run, revision: revision, providerID: providers[0].ID}
}

func TestPR5RestartUsesCommittedCursorAndRejectsExpiredWorker(t *testing.T) {
	f := newPR5StateFixture(t)
	ctx := context.Background()
	oldLease, err := f.repo.AcquireRunLease(ctx, f.run.ID, "worker-before-restart", 10*time.Minute)
	require.NoError(t, err)

	first := presentChunkCommit(f, oldLease, "chunk-0", 0, 3)
	committed, err := f.repo.CommitHealthChunk(ctx, first)
	require.NoError(t, err)
	assert.Equal(t, int64(3), committed.CursorSegment)
	assert.Equal(t, int64(3), committed.ResolvedSegments)

	// Simulate process death: the durable lease expires without an in-memory
	// release, then another worker resumes the same compatible run.
	_, err = f.db.Connection().ExecContext(ctx,
		`UPDATE health_runs SET lease_expires_at = ? WHERE id = ?`,
		time.Now().UTC().Add(-time.Second), f.run.ID)
	require.NoError(t, err)
	newLease, err := f.repo.AcquireRunLease(ctx, f.run.ID, "worker-after-restart", 10*time.Minute)
	require.NoError(t, err)
	assert.Greater(t, newLease.FencingToken, oldLease.FencingToken)

	decision := decideObservationRunResume(newLease, f.revision)
	assert.True(t, decision.Compatible)
	assert.Equal(t, int64(3), decision.CursorSegment,
		"restart progress must come from atomically committed chunks")

	stale := presentChunkCommit(f, oldLease, "chunk-stale", 3, 3)
	stale.CommittedAt = time.Now().UTC()
	_, err = f.repo.CommitHealthChunk(ctx, stale)
	require.ErrorIs(t, err, database.ErrStaleHealthLease)

	fresh := presentChunkCommit(f, newLease, "chunk-1", 3, 3)
	fresh.CommittedAt = time.Now().UTC()
	finished, err := f.repo.CommitHealthChunk(ctx, fresh)
	require.NoError(t, err)
	assert.Equal(t, int64(6), finished.CursorSegment)
	assert.Equal(t, int64(6), finished.ResolvedSegments)
}

func TestPR5RestartAbandonsOnlyFingerprintIncompatibleRun(t *testing.T) {
	f := newPR5StateFixture(t)

	compatible := decideObservationRunResume(f.run, f.revision)
	assert.True(t, compatible.Compatible)
	assert.False(t, compatible.Abandon)

	replacement, err := f.repo.EnsureFileRevision(context.Background(), database.FileRevisionSpec{
		FilePath:          "synthetic/restart.mkv",
		LayoutFingerprint: "sha256:synthetic-layout-b",
		VirtualSize:       600,
		SegmentCount:      6,
	})
	require.NoError(t, err)
	incompatible := decideObservationRunResume(f.run, replacement)
	assert.False(t, incompatible.Compatible)
	assert.True(t, incompatible.Abandon,
		"a changed canonical layout must not inherit positional evidence")
}

func presentChunkCommit(f pr5StateFixture, lease *database.HealthRun, chunkID string, start, count int64) database.HealthChunkCommit {
	bitmap := byte((1 << count) - 1)
	return database.HealthChunkCommit{
		ChunkID:             chunkID,
		RunID:               f.run.ID,
		LeaseOwner:          *lease.LeaseOwner,
		FencingToken:        lease.FencingToken,
		ProviderID:          f.providerID,
		ProviderGeneration:  1,
		Stage:               "primary_stat",
		ObservationKind:     database.HealthObservationSTAT,
		SegmentStart:        start,
		SegmentCount:        count,
		TestedBitmap:        []byte{bitmap},
		PresentBitmap:       []byte{bitmap},
		AbsentBitmap:        []byte{0},
		CorruptBitmap:       []byte{0},
		TemporaryBitmap:     []byte{0},
		InconclusiveBitmap:  []byte{0},
		ResolvedBitmap:      []byte{bitmap},
		CursorSegment:       start + count,
		ResolvedDelta:       count,
		ProviderChecksDelta: count,
		CommittedAt:         time.Now().UTC(),
	}
}
