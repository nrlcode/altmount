package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pr5ScheduleFixture struct {
	db       *DB
	repo     *HealthStateRepository
	revision *HealthFileRevision
	snapshot *ProviderSnapshot
	provider HealthProvider
	gap      *HealthGapRange
	clock    *pr4TestClock
	now      time.Time
	total    int64
}

func newPR5ScheduleFixture(t *testing.T) pr5ScheduleFixture {
	t.Helper()
	db, repo := newPR4Repository(t)
	ctx := context.Background()
	now := time.Unix(1_711_000_000, 0).UTC()
	clock := &pr4TestClock{now: now}
	repo.now = clock.Now
	const total = int64(8)
	revision, err := repo.EnsureFileRevision(ctx, FileRevisionSpec{
		FilePath: "library/pr5-schedule.mkv", LayoutFingerprint: "sha256:pr5-schedule",
		VirtualSize: 800, SegmentCount: total,
	})
	require.NoError(t, err)
	providers, err := repo.ReconcileProviders(ctx, []ProviderSpec{{
		StableID: "pr5-schedule-provider", DisplayName: "Primary",
		Endpoint: "schedule.example.invalid", Port: 119, Account: "account",
		Role: ProviderRolePrimary, Order: 0,
	}})
	require.NoError(t, err)
	require.Len(t, providers, 1)
	snapshot, err := repo.CaptureActiveProviderSnapshot(ctx, now)
	require.NoError(t, err)
	gap, err := repo.UpsertGapRange(ctx, GapRangeWrite{
		ID: "pr5-schedule-gap", FileRevisionID: revision.ID,
		Kind: GapKindProvisional, StartSegment: 3, SegmentCount: 1,
		Status: GapStatusActive, CreatedAt: now,
	})
	require.NoError(t, err)
	return pr5ScheduleFixture{
		db: db, repo: repo, revision: revision, snapshot: snapshot,
		provider: providers[0], gap: gap, clock: clock, now: now, total: total,
	}
}

func (f pr5ScheduleFixture) scheduleSpec(id, dedupe string, priority HealthRunPriority, due time.Time) ScheduledHealthRunSpec {
	return ScheduledHealthRunSpec{
		Run: HealthRunSpec{
			ID: id, FileRevisionID: f.revision.ID, ProviderSnapshotID: f.snapshot.ID,
			Trigger: "provider_activation", Mode: "observation",
			TotalSegments: f.gap.SegmentCount, CreatedAt: f.now,
		},
		DedupeKey:                     dedupe,
		Priority:                      priority,
		NotBefore:                     due,
		TargetProviderID:              f.provider.ID,
		TargetProviderGeneration:      f.provider.CurrentGeneration,
		TargetProviderActivationEpoch: f.provider.ActivationEpoch,
		TargetGapID:                   f.gap.ID,
	}
}

func TestPR5ScheduledRunPersistsTargetAndPromotesOneActiveDedupe(t *testing.T) {
	f := newPR5ScheduleFixture(t)
	ctx := context.Background()
	const dedupe = "revision:provider-activation:gap"

	createdRun, created, err := f.repo.EnsureScheduledHealthRun(ctx,
		f.scheduleSpec("scheduled-low", dedupe, HealthRunPriorityLow, f.now.Add(time.Hour)))
	require.NoError(t, err)
	assert.True(t, created)
	assert.Equal(t, "scheduled-low", createdRun.ID)

	restarted := NewHealthStateRepository(f.db.Connection(), DialectSQLite)
	restarted.now = f.clock.Now
	schedule, err := restarted.GetHealthRunSchedule(ctx, createdRun.ID)
	require.NoError(t, err)
	require.NotNil(t, schedule)
	assert.Equal(t, dedupe, schedule.DedupeKey)
	assert.Equal(t, HealthRunPriorityLow, schedule.Priority)
	assert.Equal(t, f.now.Add(time.Hour), schedule.NotBefore)
	assert.Equal(t, f.provider.ID, schedule.TargetProviderID)
	assert.Equal(t, f.provider.CurrentGeneration, schedule.TargetProviderGeneration)
	assert.Equal(t, f.provider.ActivationEpoch, schedule.TargetProviderActivationEpoch)
	assert.Equal(t, f.gap.ID, schedule.TargetGapID)
	activeRun, err := restarted.GetActiveScheduledHealthRun(ctx, dedupe)
	require.NoError(t, err)
	require.NotNil(t, activeRun)
	assert.Equal(t, createdRun.ID, activeRun.ID)

	promotedRun, promotedCreated, err := restarted.EnsureScheduledHealthRun(ctx,
		f.scheduleSpec("must-not-create-a-duplicate", dedupe, HealthRunPriorityHigh, f.now.Add(-time.Minute)))
	require.NoError(t, err)
	assert.False(t, promotedCreated)
	assert.Equal(t, createdRun.ID, promotedRun.ID)
	promoted, err := restarted.GetHealthRunSchedule(ctx, createdRun.ID)
	require.NoError(t, err)
	assert.Equal(t, HealthRunPriorityHigh, promoted.Priority)
	assert.Equal(t, f.now.Add(-time.Minute), promoted.NotBefore,
		"promotion may make work earlier but must never postpone already-queued work")

	var active int
	require.NoError(t, f.db.Connection().QueryRow(`
		SELECT COUNT(*)
		FROM health_run_schedule s
		JOIN health_runs r ON r.id = s.run_id
		WHERE s.dedupe_key = ? AND r.status IN ('pending', 'running', 'paused')
	`, dedupe).Scan(&active))
	assert.Equal(t, 1, active)

	lease, err := restarted.ClaimDueHealthRun(ctx, "finisher", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.NoError(t, restarted.CompleteHealthRun(
		ctx, lease.ID, "finisher", lease.FencingToken, f.now.Add(time.Second)))

	next, nextCreated, err := restarted.EnsureScheduledHealthRun(ctx,
		f.scheduleSpec("scheduled-next-episode", dedupe, HealthRunPriorityNormal, f.now))
	require.NoError(t, err)
	assert.True(t, nextCreated, "terminal history must not block a later run with the same logical target")
	assert.Equal(t, "scheduled-next-episode", next.ID)
}

func TestPR5DueClaimLeaseLifecycleAndPauseAreFenced(t *testing.T) {
	f := newPR5ScheduleFixture(t)
	ctx := context.Background()
	due, _, err := f.repo.EnsureScheduledHealthRun(ctx,
		f.scheduleSpec("due-run", "due-run-key", HealthRunPriorityLow, f.now))
	require.NoError(t, err)
	_, _, err = f.repo.EnsureScheduledHealthRun(ctx,
		f.scheduleSpec("future-run", "future-run-key", HealthRunPriorityHigh, f.now.Add(time.Hour)))
	require.NoError(t, err)

	first, err := f.repo.ClaimDueHealthRun(ctx, "worker-one", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, first)
	assert.Equal(t, due.ID, first.ID, "a future high-priority run is not claimable before not_before")
	assert.Equal(t, int64(1), first.FencingToken)

	// At the half-open expiry boundary the old lease is expired, but a pause
	// request must prevent another worker from reacquiring it.
	f.clock.now = *first.LeaseExpiresAt
	require.NoError(t, f.repo.RequestRunPause(ctx, first.ID, true, f.clock.now))
	paused, err := f.repo.ClaimDueHealthRun(ctx, "worker-two", time.Minute)
	require.NoError(t, err)
	assert.Nil(t, paused)

	require.NoError(t, f.repo.RequestRunPause(ctx, first.ID, false, f.clock.now))
	second, err := f.repo.ClaimDueHealthRun(ctx, "worker-two", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, second)
	assert.Equal(t, first.ID, second.ID)
	assert.Equal(t, int64(2), second.FencingToken)

	f.clock.now = f.clock.now.Add(30 * time.Second)
	renewed, err := f.repo.RenewHealthRunLease(
		ctx, second.ID, "worker-two", second.FencingToken, 2*time.Minute)
	require.NoError(t, err)
	require.NotNil(t, renewed.LeaseExpiresAt)
	assert.Equal(t, f.clock.now.Add(2*time.Minute), *renewed.LeaseExpiresAt)
	assert.Equal(t, second.FencingToken, renewed.FencingToken,
		"renewal extends ownership without creating a new fence")

	_, err = f.repo.RenewHealthRunLease(ctx, second.ID, "worker-one", first.FencingToken, time.Minute)
	require.ErrorIs(t, err, ErrStaleHealthLease)
	err = f.repo.ParkHealthRun(ctx, second.ID, "worker-one", first.FencingToken,
		f.clock.now.Add(5*time.Minute), f.clock.now)
	require.ErrorIs(t, err, ErrStaleHealthLease)

	nextDue := f.clock.now.Add(5 * time.Minute)
	require.NoError(t, f.repo.ParkHealthRun(
		ctx, second.ID, "worker-two", second.FencingToken, nextDue, f.clock.now))
	parked, err := f.repo.GetHealthRun(ctx, second.ID)
	require.NoError(t, err)
	assert.Equal(t, HealthRunPending, parked.Status)
	assert.Nil(t, parked.LeaseOwner)
	assert.Nil(t, parked.LeaseExpiresAt)

	f.clock.now = nextDue.Add(-time.Nanosecond)
	notYet, err := f.repo.ClaimDueHealthRun(ctx, "worker-three", time.Minute)
	require.NoError(t, err)
	assert.Nil(t, notYet)
	f.clock.now = nextDue
	third, err := f.repo.ClaimDueHealthRun(ctx, "worker-three", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, third)
	assert.Equal(t, second.ID, third.ID)
	assert.Equal(t, int64(3), third.FencingToken)

	require.NoError(t, f.repo.CompleteHealthRun(
		ctx, third.ID, "worker-three", third.FencingToken, f.clock.now))
	completed, err := f.repo.GetHealthRun(ctx, third.ID)
	require.NoError(t, err)
	assert.Equal(t, HealthRunCompleted, completed.Status)
	assert.Nil(t, completed.LeaseOwner)

	err = f.repo.FailHealthRun(ctx, third.ID, "worker-three", third.FencingToken,
		"synthetic terminal reason", f.clock.now)
	require.ErrorIs(t, err, ErrStaleHealthLease,
		"a terminal transition cannot be overwritten by a late lifecycle call")
}

func TestPR5ConcurrentWorkersSerializeClaimsPerActiveRevision(t *testing.T) {
	runPR5ConcurrentSameRevisionClaims(t, DialectSQLite)
}

func TestPR5DirectLeaseAcquisitionCannotBypassActiveRevisionOwner(t *testing.T) {
	runPR5DirectSameRevisionLeaseFence(t, DialectSQLite)
}

func TestPR5FileObservationControlSelectsCurrentRunAndNeverImport(t *testing.T) {
	f := newPR5ScheduleFixture(t)
	ctx := context.Background()
	ensure := func(id, trigger string, priority HealthRunPriority) *HealthRun {
		t.Helper()
		run, _, err := f.repo.EnsureScheduledHealthRun(ctx, ScheduledHealthRunSpec{
			Run: HealthRunSpec{
				ID: id, FileRevisionID: f.revision.ID, ProviderSnapshotID: f.snapshot.ID,
				Trigger: trigger, Mode: "observation", TotalSegments: f.total,
				CreatedAt: f.now,
			},
			DedupeKey: id, Priority: priority, NotBefore: f.now,
		})
		require.NoError(t, err)
		return run
	}
	importRun := ensure("file-control-import", "import", HealthRunPriorityHigh)
	ordinary := ensure("file-control-ordinary", "ordinary", HealthRunPriorityNormal)
	manual := ensure("file-control-manual", "manual", HealthRunPriorityHigh)
	ordinaryLease, err := f.repo.AcquireRunLease(ctx, ordinary.ID, "file-control-worker", time.Minute)
	require.NoError(t, err)

	selected, err := f.repo.GetActiveObservationHealthRunForFile(ctx, f.revision.FileHealthID)
	require.NoError(t, err)
	require.NotNil(t, selected)
	assert.Equal(t, ordinaryLease.ID, selected.ID,
		"the current running observation owns control ahead of queued manual work")
	require.NoError(t, f.repo.RequestRunCancel(ctx, selected.ID, f.now.Add(time.Second)))

	selected, err = f.repo.GetActiveObservationHealthRunForFile(ctx, f.revision.FileHealthID)
	require.NoError(t, err)
	require.NotNil(t, selected)
	assert.Equal(t, manual.ID, selected.ID)
	require.NoError(t, f.repo.RequestRunCancel(ctx, selected.ID, f.now.Add(2*time.Second)))

	selected, err = f.repo.GetActiveObservationHealthRunForFile(ctx, f.revision.FileHealthID)
	require.NoError(t, err)
	assert.Nil(t, selected, "an active import run is never exposed to file observation cancellation")
	retainedImport, err := f.repo.GetHealthRun(ctx, importRun.ID)
	require.NoError(t, err)
	require.NotNil(t, retainedImport)
	assert.Equal(t, HealthRunPending, retainedImport.Status)
	importSchedule, err := f.repo.GetHealthRunSchedule(ctx, importRun.ID)
	require.NoError(t, err)
	require.NotNil(t, importSchedule)
	assert.True(t, importSchedule.Active)
}

func TestPR5FailedRunIsTerminalAndReleasesItsLease(t *testing.T) {
	f := newPR5ScheduleFixture(t)
	ctx := context.Background()
	_, _, err := f.repo.EnsureScheduledHealthRun(ctx,
		f.scheduleSpec("failed-run", "failed-run-key", HealthRunPriorityNormal, f.now))
	require.NoError(t, err)
	claimed, err := f.repo.ClaimDueHealthRun(ctx, "failure-worker", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	require.NoError(t, f.repo.FailHealthRun(ctx, claimed.ID, "failure-worker",
		claimed.FencingToken, "synthetic failure", f.now.Add(time.Second)))
	failed, err := f.repo.GetHealthRun(ctx, claimed.ID)
	require.NoError(t, err)
	assert.Equal(t, HealthRunFailed, failed.Status)
	assert.Nil(t, failed.LeaseOwner)
	assert.Nil(t, failed.LeaseExpiresAt)
	assert.Equal(t, "synthetic failure", failed.LastError)

	none, err := f.repo.ClaimDueHealthRun(ctx, "other-worker", time.Minute)
	require.NoError(t, err)
	assert.Nil(t, none, "failed work remains history and is not silently reacquired")
}

func TestPR5ResumeStateReconstructsCommittedProgressCoverageAndRetry(t *testing.T) {
	f := newPR4RunFixture(t)
	ctx := context.Background()
	lease, err := f.repo.AcquireRunLease(ctx, f.run.ID, "resume-worker", 10*time.Minute)
	require.NoError(t, err)
	commit := pr4Commit(f, "resume-chunk", lease.FencingToken, "resume-worker", 0)
	committed, err := f.repo.CommitHealthChunk(ctx, commit)
	require.NoError(t, err)
	assert.Equal(t, int64(3), committed.ResolvedSegments)

	restarted := NewHealthStateRepository(f.db.Connection(), DialectSQLite)
	resume, err := restarted.GetHealthRunResumeState(ctx, f.run.ID)
	require.NoError(t, err)
	require.NotNil(t, resume)
	assert.Equal(t, f.run.ID, resume.Run.ID)
	assert.Equal(t, int64(3), resume.Run.ResolvedSegments)
	assert.Equal(t, int64(4), resume.Run.ProviderChecks)
	assert.Equal(t, int64(4), resume.Run.CursorSegment)

	require.Len(t, resume.Chunks, 1)
	assert.Equal(t, commit.ChunkID, resume.Chunks[0].ID)
	assert.Equal(t, commit.TestedBitmap, resume.Chunks[0].TestedBitmap)
	assert.Equal(t, commit.PresentBitmap, resume.Chunks[0].PresentBitmap)
	assert.Equal(t, commit.AbsentBitmap, resume.Chunks[0].AbsentBitmap)
	assert.Equal(t, commit.TemporaryBitmap, resume.Chunks[0].TemporaryBitmap)

	require.Len(t, resume.Coverage, 1)
	assert.Equal(t, commit.ChunkID, resume.Coverage[0].SourceChunkID)
	assert.Equal(t, commit.ProviderID, resume.Coverage[0].ProviderID)
	assert.Equal(t, commit.ProviderGeneration, resume.Coverage[0].ProviderGeneration)
	assert.Equal(t, commit.TestedBitmap, resume.Coverage[0].TestedBitmap)
	assert.Equal(t, commit.PresentBitmap, resume.Coverage[0].PresentBitmap)

	require.Len(t, resume.Retries, 1)
	assert.Equal(t, commit.Retry.RetryKey, resume.Retries[0].RetryKey)
	assert.Equal(t, commit.Retry.SegmentStart, resume.Retries[0].SegmentStart)
	assert.Equal(t, commit.Retry.SegmentCount, resume.Retries[0].SegmentCount)
	assert.Equal(t, commit.Retry.Attempt, resume.Retries[0].Attempt)
	assert.Equal(t, commit.Retry.NextAttemptAt, resume.Retries[0].NextAttemptAt)
}
