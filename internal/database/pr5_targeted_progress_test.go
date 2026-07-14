package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPR5HealthPendingUsesSparseProgressAndFullRevisionBounds(t *testing.T) {
	f := newPR5AuditImportFixture(t, "import")
	ctx := context.Background()
	importLease, err := f.repo.AcquireRunLease(ctx, f.run.ID, "sparse-health-pending-import", time.Minute)
	require.NoError(t, err)
	provider := f.snapshot.Entries[0]
	commitPR5ImportSTATCoverage(
		t, f.repo, f.run, importLease, provider, "sparse-health-pending-import-stat",
		HealthRunStageImportInitialSTAT, 0b00000011, 0b00000001, f.now,
	)
	require.NoError(t, f.repo.CompleteHealthRun(
		ctx, f.run.ID, *importLease.LeaseOwner, importLease.FencingToken, f.now,
	))
	_, err = f.db.Connection().ExecContext(ctx, `
		INSERT INTO health_import_validations
			(id, queue_item_id, file_revision_id, run_id, phase, damage_policy,
			 unresolved_segments, unresolved_bitmap, initial_pass_complete,
			 second_pass_complete, created_at, updated_at)
		VALUES (?, ?, ?, ?, 'health_pending', 'tolerant', 1, ?, TRUE, TRUE, ?, ?)
	`, "sparse-health-pending-validation", f.queueA.ID, f.revision.ID,
		f.run.ID, []byte{0b00000010}, f.now, f.now)
	require.NoError(t, err)

	run, created, err := f.repo.EnsureScheduledHealthRun(ctx, ScheduledHealthRunSpec{
		Run: HealthRunSpec{
			ID: "sparse-health-pending-run", FileRevisionID: f.revision.ID,
			ProviderSnapshotID: f.snapshot.ID, Trigger: "health_pending",
			Mode: "observation", TotalSegments: 1, CreatedAt: f.now,
		},
		DedupeKey: "sparse-health-pending", Priority: HealthRunPriorityHigh,
		NotBefore: f.now,
	})
	require.NoError(t, err)
	require.True(t, created)
	lease, err := f.repo.ClaimDueObservationHealthRun(ctx, "sparse-health-pending-worker", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.Equal(t, run.ID, lease.ID)
	committed, err := f.repo.CommitHealthChunk(ctx, HealthChunkCommit{
		ChunkID: "sparse-health-pending-present", RunID: run.ID,
		LeaseOwner: *lease.LeaseOwner, FencingToken: lease.FencingToken,
		ProviderID: provider.ProviderID, ProviderGeneration: provider.ProviderGeneration,
		ProviderActivationEpoch: provider.ProviderActivationEpoch,
		Stage:                   "health_pending_stat", ObservationKind: HealthObservationSTAT,
		SegmentStart: 1, SegmentCount: 1, TestedBitmap: []byte{1}, PresentBitmap: []byte{1},
		AbsentBitmap: []byte{0}, CorruptBitmap: []byte{0}, TemporaryBitmap: []byte{0},
		InconclusiveBitmap: []byte{0}, ResolvedBitmap: []byte{1}, CursorSegment: 2,
		ResolvedDelta: 1, ProviderChecksDelta: 1, CommittedAt: f.now,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), committed.TotalSegments)
	assert.Equal(t, committed.TotalSegments, committed.ResolvedSegments)
	assert.Equal(t, int64(2), committed.CursorSegment,
		"absolute cursor bounds continue to use the full two-segment revision")

	finalized, err := f.repo.FinalizeHealthPendingObservation(
		ctx, run.ID, *lease.LeaseOwner, lease.FencingToken, f.now,
	)
	require.NoError(t, err)
	require.NotNil(t, finalized)
	assert.True(t, finalized.Settled)
	assert.True(t, finalized.Recovered)
	completed, err := f.repo.GetHealthRun(ctx, run.ID)
	require.NoError(t, err)
	require.NotNil(t, completed)
	assert.Equal(t, HealthRunCompleted, completed.Status)
	assert.Equal(t, completed.TotalSegments, completed.ResolvedSegments)
}

func TestPR5PartialTargetedGapRecoveryCompletesItsOriginalWorkTotal(t *testing.T) {
	f := newPR5ScheduleFixture(t)
	ctx := context.Background()
	gap, err := f.repo.UpsertGapRange(ctx, GapRangeWrite{
		ID: "partial-targeted-recovery-gap", FileRevisionID: f.revision.ID,
		Kind: GapKindProvisional, StartSegment: 4, SegmentCount: 2,
		Status: GapStatusActive, CreatedAt: f.now,
	})
	require.NoError(t, err)
	run, _, err := f.repo.EnsureScheduledHealthRun(ctx, ScheduledHealthRunSpec{
		Run: HealthRunSpec{
			ID: "partial-targeted-recovery-run", FileRevisionID: f.revision.ID,
			ProviderSnapshotID: f.snapshot.ID, Trigger: "manual", Mode: "observation",
			TotalSegments: gap.SegmentCount, CreatedAt: f.now,
		},
		DedupeKey: "partial-targeted-recovery", Priority: HealthRunPriorityHigh,
		NotBefore: f.now, TargetGapID: gap.ID,
	})
	require.NoError(t, err)
	lease, err := f.repo.ClaimDueObservationHealthRun(ctx, "partial-targeted-recovery-worker", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.Equal(t, run.ID, lease.ID)
	compat := pr4Fixture{
		repo: f.repo, db: f.db, run: run, providerID: f.provider.ID,
		now: f.now, clock: f.clock,
	}
	body := pr5AuditPresentCommit(
		compat, lease, "partial-targeted-recovery-body", "validated_body",
		gap.StartSegment, HealthObservationValidatedBody, f.now,
	)
	committed, err := f.repo.CommitHealthChunk(ctx, body)
	require.NoError(t, err)
	assert.Equal(t, int64(1), committed.ResolvedSegments)
	assert.Equal(t, int64(2), committed.TotalSegments)

	_, err = f.repo.ClearGapRangeFromRunChunk(
		ctx, run.ID, *lease.LeaseOwner, lease.FencingToken, gap.ID, body.ChunkID, f.now,
	)
	require.NoError(t, err)
	completed, err := f.repo.GetHealthRun(ctx, run.ID)
	require.NoError(t, err)
	require.NotNil(t, completed)
	assert.Equal(t, HealthRunCompleted, completed.Status)
	assert.Equal(t, completed.TotalSegments, completed.ResolvedSegments,
		"a split recovery completes the original targeted work even though its remainder is a new gap")
	var remainderCount int
	require.NoError(t, f.db.Connection().QueryRowContext(ctx, `
		SELECT COUNT(*) FROM health_gap_ranges
		WHERE file_revision_id = ? AND status = 'active'
		  AND start_segment = ? AND segment_count = 1
	`, f.revision.ID, gap.StartSegment+1).Scan(&remainderCount))
	assert.Equal(t, 1, remainderCount)
}

func TestPR5AlreadyDurableHealthPendingGapSettlesFullProgress(t *testing.T) {
	f := newPR5AuditImportFixture(t, "import")
	ctx := context.Background()
	importLease, err := f.repo.AcquireRunLease(ctx, f.run.ID, "durable-pending-import", time.Minute)
	require.NoError(t, err)
	provider := f.snapshot.Entries[0]
	commitPR5ImportSTATCoverage(
		t, f.repo, f.run, importLease, provider, "durable-pending-import-stat",
		HealthRunStageImportInitialSTAT, 0b00000011, 0b00000001, f.now,
	)
	require.NoError(t, f.repo.CompleteHealthRun(
		ctx, f.run.ID, *importLease.LeaseOwner, importLease.FencingToken, f.now,
	))
	_, err = f.db.Connection().ExecContext(ctx, `
		INSERT INTO health_import_validations
			(id, queue_item_id, file_revision_id, run_id, phase, damage_policy,
			 unresolved_segments, unresolved_bitmap, initial_pass_complete,
			 second_pass_complete, created_at, updated_at)
		VALUES (?, ?, ?, ?, 'health_pending', 'tolerant', 1, ?, TRUE, TRUE, ?, ?)
	`, "durable-pending-validation", f.queueA.ID, f.revision.ID,
		f.run.ID, []byte{0b00000010}, f.now, f.now)
	require.NoError(t, err)
	gap, err := f.repo.UpsertGapRange(ctx, GapRangeWrite{
		ID: "durable-pending-gap", FileRevisionID: f.revision.ID,
		Kind: GapKindProvisional, StartSegment: 1, SegmentCount: 1,
		Status: GapStatusActive, CreatedAt: f.now,
	})
	require.NoError(t, err)
	_, err = f.db.Connection().ExecContext(ctx, `
		UPDATE health_gap_ranges
		SET kind = 'confirmed_absent', confirmed_at = ?, next_revalidation_at = ?
		WHERE id = ?
	`, f.now, f.now.Add(24*time.Hour), gap.ID)
	require.NoError(t, err)
	_, err = f.db.Connection().ExecContext(ctx, `
		INSERT INTO health_gap_provider_causes
			(gap_id, provider_id, provider_generation, provider_activation_epoch,
			 cause, confirmation_count, confirmed_at)
		VALUES (?, ?, ?, ?, 'absent', 2, ?)
	`, gap.ID, provider.ProviderID, provider.ProviderGeneration,
		provider.ProviderActivationEpoch, f.now)
	require.NoError(t, err)

	run, _, err := f.repo.EnsureScheduledHealthRun(ctx, ScheduledHealthRunSpec{
		Run: HealthRunSpec{
			ID: "durable-pending-run", FileRevisionID: f.revision.ID,
			ProviderSnapshotID: f.snapshot.ID, Trigger: "health_pending",
			Mode: "observation", TotalSegments: 1, CreatedAt: f.now,
		},
		DedupeKey: "durable-pending-run", Priority: HealthRunPriorityHigh, NotBefore: f.now,
	})
	require.NoError(t, err)
	lease, err := f.repo.ClaimDueObservationHealthRun(ctx, "durable-pending-worker", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, lease)
	finalized, err := f.repo.FinalizeHealthPendingObservation(
		ctx, run.ID, *lease.LeaseOwner, lease.FencingToken, f.now,
	)
	require.NoError(t, err)
	require.NotNil(t, finalized)
	assert.True(t, finalized.Settled)
	assert.False(t, finalized.Recovered)
	completed, err := f.repo.GetHealthRun(ctx, run.ID)
	require.NoError(t, err)
	require.NotNil(t, completed)
	assert.Equal(t, HealthRunCompleted, completed.Status)
	assert.Equal(t, completed.TotalSegments, completed.ResolvedSegments,
		"a persistent gap already durable before this run settles its complete work total")
}

func TestPR5UnnecessaryProviderTargetCompletesFullSparseProgress(t *testing.T) {
	f := newPR5ScheduleFixture(t)
	ctx := context.Background()
	positions, err := f.repo.ListUnresolvedSegmentPositions(
		ctx, f.revision.ID, f.provider.ID,
		f.provider.CurrentGeneration, f.provider.ActivationEpoch,
	)
	require.NoError(t, err)
	require.Empty(t, positions, "the targeted activation has no remaining negative work")
	run, _, err := f.repo.EnsureScheduledHealthRun(ctx, ScheduledHealthRunSpec{
		Run: HealthRunSpec{
			ID: "unnecessary-provider-target-run", FileRevisionID: f.revision.ID,
			ProviderSnapshotID: f.snapshot.ID, Trigger: "provider_activation",
			Mode: "observation", TotalSegments: 1, CreatedAt: f.now,
		},
		DedupeKey: "unnecessary-provider-target", Priority: HealthRunPriorityHigh,
		NotBefore: f.now, TargetProviderID: f.provider.ID,
		TargetProviderGeneration:      f.provider.CurrentGeneration,
		TargetProviderActivationEpoch: f.provider.ActivationEpoch,
	})
	require.NoError(t, err)
	lease, err := f.repo.ClaimDueObservationHealthRun(ctx, "unnecessary-provider-worker", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.NoError(t, f.repo.CompleteObservationHealthRun(
		ctx, run.ID, *lease.LeaseOwner, lease.FencingToken, f.now,
	))
	completed, err := f.repo.GetHealthRun(ctx, run.ID)
	require.NoError(t, err)
	require.NotNil(t, completed)
	assert.Equal(t, HealthRunCompleted, completed.Status)
	assert.Equal(t, completed.TotalSegments, completed.ResolvedSegments)
}
