package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPR5OrdinaryGapPublicationAndRunCompletionAreAtomic(t *testing.T) {
	f := newPR5ScheduleFixture(t)
	ctx := context.Background()
	run, _, err := f.repo.EnsureScheduledHealthRun(ctx, ScheduledHealthRunSpec{
		Run: HealthRunSpec{
			ID: "ordinary-atomic-run", FileRevisionID: f.revision.ID,
			ProviderSnapshotID: f.snapshot.ID, Trigger: "scheduled",
			Mode: "observation", TotalSegments: f.total, CreatedAt: f.now,
		},
		DedupeKey: "ordinary-atomic-run", Priority: HealthRunPriorityNormal,
		NotBefore: f.now,
	})
	require.NoError(t, err)
	lease, err := f.repo.ClaimDueObservationHealthRun(ctx, "ordinary-atomic-worker", 30*time.Minute)
	require.NoError(t, err)
	require.Equal(t, run.ID, lease.ID)
	compat := pr4Fixture{
		repo: f.repo, db: f.db, run: run, providerID: f.provider.ID,
		now: f.now, clock: f.clock,
	}
	_, err = f.repo.CommitHealthChunk(ctx, pr5AuditAbsentCommit(
		compat, run, lease, "ordinary-atomic-first", "observe_initial", 4,
		f.provider.ActivationEpoch, f.now,
	))
	require.NoError(t, err)
	secondAt := f.now.Add(DefaultGapConfirmationMinimumDelay)
	f.clock.now = secondAt
	_, err = f.repo.CommitHealthChunk(ctx, pr5AuditAbsentCommit(
		compat, run, lease, "ordinary-atomic-second", "observe_confirmation_2", 4,
		f.provider.ActivationEpoch, secondAt,
	))
	require.NoError(t, err)

	valid := GapRangeWrite{
		ID: "ordinary-atomic-gap", FileRevisionID: f.revision.ID,
		Kind: GapKindConfirmedAbsent, StartSegment: 4, SegmentCount: 1,
		Status: GapStatusActive, CreatedAt: f.now,
		Causes: []GapProviderCause{{
			ProviderID: f.provider.ID, ProviderGeneration: f.provider.CurrentGeneration,
			ProviderActivationEpoch: f.provider.ActivationEpoch, Cause: GapCauseAbsent,
		}},
	}
	invalid := valid
	invalid.ID = "ordinary-out-of-bounds-gap"
	invalid.StartSegment = f.total
	err = f.repo.FinalizeObservationHealthRun(
		ctx, run.ID, *lease.LeaseOwner, lease.FencingToken,
		[]GapRangeWrite{valid, invalid}, secondAt,
	)
	require.ErrorIs(t, err, ErrStaleHealthSchedule)
	gap, err := f.repo.GetHealthGapRange(ctx, valid.ID)
	require.NoError(t, err)
	assert.Nil(t, gap, "a later invalid gap must roll back earlier gap publication")
	retained, err := f.repo.GetHealthRun(ctx, run.ID)
	require.NoError(t, err)
	require.NotNil(t, retained)
	assert.Equal(t, HealthRunRunning, retained.Status)
	schedule, err := f.repo.GetHealthRunSchedule(ctx, run.ID)
	require.NoError(t, err)
	require.NotNil(t, schedule)
	assert.True(t, schedule.Active)

	require.NoError(t, f.repo.FinalizeObservationHealthRun(
		ctx, run.ID, *lease.LeaseOwner, lease.FencingToken,
		[]GapRangeWrite{valid}, secondAt,
	))
	gap, err = f.repo.GetHealthGapRange(ctx, valid.ID)
	require.NoError(t, err)
	require.NotNil(t, gap)
	assert.Equal(t, GapKindConfirmedAbsent, gap.Kind)
	require.Len(t, gap.Causes, 1)
	assert.Equal(t, 2, gap.Causes[0].ConfirmationCount)
	retained, err = f.repo.GetHealthRun(ctx, run.ID)
	require.NoError(t, err)
	assert.Equal(t, HealthRunCompleted, retained.Status)
	assert.Equal(t, retained.TotalSegments, retained.ResolvedSegments)
	schedule, err = f.repo.GetHealthRunSchedule(ctx, run.ID)
	require.NoError(t, err)
	assert.False(t, schedule.Active)
}

func TestPR5TargetedClearWinsBeforeOrdinaryGapFinalization(t *testing.T) {
	f := newPR5ScheduleFixture(t)
	ctx := context.Background()
	run, _, err := f.repo.EnsureScheduledHealthRun(ctx, ScheduledHealthRunSpec{
		Run: HealthRunSpec{
			ID: "ordinary-clear-race-run", FileRevisionID: f.revision.ID,
			ProviderSnapshotID: f.snapshot.ID, Trigger: "ordinary",
			Mode: "observation", TotalSegments: f.total, CreatedAt: f.now,
		},
		DedupeKey: "ordinary-clear-race", Priority: HealthRunPriorityNormal,
		NotBefore: f.now,
	})
	require.NoError(t, err)
	lease, err := f.repo.ClaimDueObservationHealthRun(ctx, "ordinary-clear-race-worker", 30*time.Minute)
	require.NoError(t, err)
	require.Equal(t, run.ID, lease.ID)
	compat := pr4Fixture{
		repo: f.repo, db: f.db, run: run, providerID: f.provider.ID,
		now: f.now, clock: f.clock,
	}
	_, err = f.repo.CommitHealthChunk(ctx, pr5AuditAbsentCommit(
		compat, run, lease, "ordinary-clear-race-first", "observe_initial", 4,
		f.provider.ActivationEpoch, f.now,
	))
	require.NoError(t, err)
	secondAt := f.now.Add(DefaultGapConfirmationMinimumDelay)
	f.clock.now = secondAt
	_, err = f.repo.CommitHealthChunk(ctx, pr5AuditAbsentCommit(
		compat, run, lease, "ordinary-clear-race-second", "observe_confirmation_2", 4,
		f.provider.ActivationEpoch, secondAt,
	))
	require.NoError(t, err)

	write := GapRangeWrite{
		ID: "ordinary-clear-race-gap", FileRevisionID: f.revision.ID,
		Kind: GapKindConfirmedAbsent, StartSegment: 4, SegmentCount: 1,
		Status: GapStatusActive, CreatedAt: f.now,
		Causes: []GapProviderCause{{
			ProviderID: f.provider.ID, ProviderGeneration: f.provider.CurrentGeneration,
			ProviderActivationEpoch: f.provider.ActivationEpoch, Cause: GapCauseAbsent,
		}},
	}
	_, err = f.repo.UpsertGapRange(ctx, write)
	require.NoError(t, err)

	clearRun, err := f.repo.CreateHealthRun(ctx, HealthRunSpec{
		ID: "ordinary-clear-race-targeted", FileRevisionID: f.revision.ID,
		ProviderSnapshotID: f.snapshot.ID, Trigger: "manual", Mode: "observation",
		TotalSegments: f.total, CreatedAt: secondAt,
	})
	require.NoError(t, err)
	// Inject the formerly possible overlapping lease directly. New claim paths
	// serialize by revision; this fixture preserves coverage for a pre-upgrade
	// or already-in-flight overlap that still has to converge safely.
	clearOwner := "ordinary-clear-race-target-worker"
	clearExpiry := secondAt.Add(10 * time.Minute)
	_, err = f.db.Connection().ExecContext(ctx, `
		UPDATE health_runs
		SET status = 'running', lease_owner = ?, lease_expires_at = ?,
		    fencing_token = fencing_token + 1, started_at = ?, updated_at = ?
		WHERE id = ?
	`, clearOwner, clearExpiry, secondAt, secondAt, clearRun.ID)
	require.NoError(t, err)
	clearLease, err := f.repo.GetHealthRun(ctx, clearRun.ID)
	require.NoError(t, err)
	require.NotNil(t, clearLease)
	clearAt := secondAt.Add(time.Minute)
	f.clock.now = clearAt
	clearCompat := pr4Fixture{
		repo: f.repo, db: f.db, run: clearRun, providerID: f.provider.ID,
		now: secondAt, clock: f.clock,
	}
	body := pr5AuditPresentCommit(
		clearCompat, clearLease, "ordinary-clear-race-body", "validated_body", 4,
		HealthObservationValidatedBody, clearAt,
	)
	_, err = f.repo.CommitHealthChunk(ctx, body)
	require.NoError(t, err)
	_, err = f.repo.ClearGapRangeFromChunk(ctx, write.ID, body.ChunkID, clearAt)
	require.NoError(t, err)

	finalAt := clearAt.Add(time.Minute)
	f.clock.now = finalAt
	require.NoError(t, f.repo.FinalizeObservationHealthRun(
		ctx, run.ID, *lease.LeaseOwner, lease.FencingToken, []GapRangeWrite{write}, finalAt,
	))
	gap, err := f.repo.GetHealthGapRange(ctx, write.ID)
	require.NoError(t, err)
	require.NotNil(t, gap)
	assert.Equal(t, GapStatusCleared, gap.Status,
		"ordinary finalization must not resurrect a newer validated-BODY clear")
	retained, err := f.repo.GetHealthRun(ctx, run.ID)
	require.NoError(t, err)
	assert.Equal(t, HealthRunCompleted, retained.Status)
	schedule, err := f.repo.GetHealthRunSchedule(ctx, run.ID)
	require.NoError(t, err)
	require.NotNil(t, schedule)
	assert.False(t, schedule.Active, "the converged stale ordinary run must retire deterministically")
}
