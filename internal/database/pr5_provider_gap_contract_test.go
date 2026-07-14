package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPR5ProviderReactivationAdvancesDurableActivationEpoch(t *testing.T) {
	db, repo := newPR4Repository(t)
	ctx := context.Background()
	clock := &pr4TestClock{now: time.Unix(1_710_000_000, 0).UTC()}
	repo.now = clock.Now
	spec := ProviderSpec{
		StableID: "provider-reactivation", DisplayName: "Primary",
		Endpoint: "reactivation.example.invalid", Port: 119, Account: "account",
		Role: ProviderRolePrimary, Order: 0,
	}

	initial, err := repo.ReconcileProviders(ctx, []ProviderSpec{spec})
	require.NoError(t, err)
	require.Len(t, initial, 1)
	assert.Equal(t, int64(1), initial[0].CurrentGeneration)
	assert.Equal(t, int64(1), initial[0].ActivationEpoch)
	assert.Equal(t, clock.now, initial[0].ActivatedAt)
	firstActivatedAt := initial[0].ActivatedAt

	firstSnapshot, err := repo.CaptureActiveProviderSnapshot(ctx, clock.now)
	require.NoError(t, err)
	require.Len(t, firstSnapshot.Entries, 1)
	assert.Equal(t, int64(1), firstSnapshot.Entries[0].ProviderActivationEpoch)

	clock.now = clock.now.Add(time.Minute)
	reordered, err := repo.ReconcileProviders(ctx, []ProviderSpec{spec})
	require.NoError(t, err)
	require.Len(t, reordered, 1)
	assert.Equal(t, int64(1), reordered[0].ActivationEpoch,
		"reconciling an already-active provider must not manufacture an activation boundary")
	assert.Equal(t, firstActivatedAt, reordered[0].ActivatedAt)

	clock.now = clock.now.Add(time.Minute)
	_, err = repo.ReconcileProviders(ctx, nil)
	require.NoError(t, err)

	// Recreate the repository to prove that activation detection does not rely on
	// an in-memory before/after map held by one process.
	restarted := NewHealthStateRepository(db.Connection(), DialectSQLite)
	clock.now = clock.now.Add(time.Minute)
	restarted.now = clock.Now
	reactivated, err := restarted.ReconcileProviders(ctx, []ProviderSpec{spec})
	require.NoError(t, err)
	require.Len(t, reactivated, 1)
	assert.Equal(t, int64(1), reactivated[0].CurrentGeneration,
		"same endpoint/account identity remains the same provider generation")
	assert.Equal(t, int64(2), reactivated[0].ActivationEpoch,
		"same-generation reactivation needs a new evidence boundary")
	assert.Equal(t, clock.now, reactivated[0].ActivatedAt)

	secondSnapshot, err := restarted.CaptureActiveProviderSnapshot(ctx, clock.now)
	require.NoError(t, err)
	require.Len(t, secondSnapshot.Entries, 1)
	assert.Equal(t, int64(2), secondSnapshot.Entries[0].ProviderActivationEpoch)

	retained, err := restarted.GetProviderSnapshot(ctx, firstSnapshot.ID)
	require.NoError(t, err)
	require.Len(t, retained.Entries, 1)
	assert.Equal(t, int64(1), retained.Entries[0].ProviderActivationEpoch,
		"an in-flight or historical snapshot must retain its original activation identity")
}

func TestPR5GapRangeRetainsEpisodesAndRequiresValidatedBodyToClear(t *testing.T) {
	f := newPR4RunFixture(t)
	ctx := context.Background()
	gap, err := f.repo.UpsertGapRange(ctx, GapRangeWrite{
		ID: "gap-episode-1", FileRevisionID: f.run.FileRevisionID,
		Kind: GapKindConfirmedAbsent, StartSegment: 2, SegmentCount: 1,
		Status: GapStatusActive, CreatedAt: f.now,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), gap.Episode)

	_, err = f.repo.UpsertGapRange(ctx, GapRangeWrite{
		ID: "gap-duplicate-active", FileRevisionID: f.run.FileRevisionID,
		Kind: GapKindConfirmedAbsent, StartSegment: 2, SegmentCount: 1,
		Status: GapStatusActive, CreatedAt: f.now.Add(time.Second),
	})
	require.Error(t, err, "one exact range may have only one active lifecycle episode")

	lease, err := f.repo.AcquireRunLease(ctx, f.run.ID, "gap-worker", 10*time.Minute)
	require.NoError(t, err)
	commitPresence := func(id, stage string, kind HealthObservationKind, at time.Time) {
		_, commitErr := f.repo.CommitHealthChunk(ctx, HealthChunkCommit{
			ChunkID: id, RunID: f.run.ID, LeaseOwner: "gap-worker",
			FencingToken: lease.FencingToken, ProviderID: f.providerID,
			ProviderGeneration: 1, Stage: stage, ObservationKind: kind,
			SegmentStart: 2, SegmentCount: 1, TestedBitmap: []byte{1},
			PresentBitmap: []byte{1}, AbsentBitmap: []byte{0}, CorruptBitmap: []byte{0},
			TemporaryBitmap: []byte{0}, InconclusiveBitmap: []byte{0},
			CursorSegment: 3, ResolvedDelta: 1, ProviderChecksDelta: 1,
			CommittedAt: at,
		})
		require.NoError(t, commitErr)
	}

	statAt := f.now.Add(time.Minute)
	commitPresence("gap-stat-presence", "gap_revalidate_stat", HealthObservationSTAT, statAt)
	_, err = f.repo.ClearGapRangeFromChunk(ctx, gap.ID, "gap-stat-presence", statAt)
	require.Error(t, err, "STAT presence cannot clear durable absence/corruption gap history")

	bodyAt := f.now.Add(2 * time.Minute)
	commitPresence("gap-body-presence", "gap_revalidate_body", HealthObservationValidatedBody, bodyAt)
	cleared, err := f.repo.ClearGapRangeFromChunk(ctx, gap.ID, "gap-body-presence", bodyAt)
	require.NoError(t, err)
	assert.Equal(t, GapStatusCleared, cleared.Status)
	assert.Equal(t, int64(1), cleared.Episode)
	assert.Equal(t, bodyAt, *cleared.ClearedAt)

	recurrence, err := f.repo.UpsertGapRange(ctx, GapRangeWrite{
		ID: "gap-episode-2", FileRevisionID: f.run.FileRevisionID,
		Kind: GapKindConfirmedAbsent, StartSegment: 2, SegmentCount: 1,
		Status: GapStatusActive, CreatedAt: f.now.Add(3 * time.Minute),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), recurrence.Episode,
		"a recurrence is a new episode, not resurrection of the cleared row")

	var total, active int
	require.NoError(t, f.db.Connection().QueryRow(`
		SELECT COUNT(*), SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END)
		FROM health_gap_ranges
		WHERE file_revision_id = ? AND kind = ? AND start_segment = ? AND segment_count = ?
	`, f.run.FileRevisionID, GapKindConfirmedAbsent, 2, 1).Scan(&total, &active))
	assert.Equal(t, 2, total)
	assert.Equal(t, 1, active)
}
