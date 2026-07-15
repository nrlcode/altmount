package database

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPR5DueGapRevalidationIsAbsoluteAndDurable(t *testing.T) {
	f := newPR5ScheduleFixture(t)
	ctx := context.Background()
	confirmedAt := f.now.Add(20 * time.Minute)
	_, err := f.db.Connection().ExecContext(ctx, `
		UPDATE health_gap_ranges
		SET kind = 'confirmed_absent', confirmed_at = ?,
		    revalidation_step = 0, next_revalidation_at = ?
		WHERE id = ?
	`, confirmedAt, confirmedAt.Add(24*time.Hour), f.gap.ID)
	require.NoError(t, err)
	_, err = f.db.Connection().ExecContext(ctx, `
		INSERT INTO health_gap_provider_causes
			(gap_id, provider_id, provider_generation, provider_activation_epoch,
			 cause, confirmation_count, confirmed_at)
		VALUES (?, ?, ?, ?, 'absent', 2, ?)
	`, f.gap.ID, f.provider.ID, f.provider.CurrentGeneration,
		f.provider.ActivationEpoch, confirmedAt)
	require.NoError(t, err)

	f.clock.now = confirmedAt.Add(24*time.Hour - time.Nanosecond)
	due, err := f.repo.ListDueGapRevalidations(ctx, f.clock.now, 8)
	require.NoError(t, err)
	assert.Empty(t, due)

	f.clock.now = confirmedAt.Add(24 * time.Hour)
	due, err = f.repo.ListDueGapRevalidations(ctx, f.clock.now, 8)
	require.NoError(t, err)
	require.Len(t, due, 1)
	assert.Equal(t, f.gap.ID, due[0].Gap.ID)
	assert.Equal(t, 0, due[0].Step)
	assert.Equal(t, confirmedAt.Add(24*time.Hour), due[0].NotBefore)

	restarted := NewHealthStateRepository(f.db.Connection(), DialectSQLite)
	restarted.now = f.clock.Now
	due, err = restarted.ListDueGapRevalidations(ctx, f.clock.now, 8)
	require.NoError(t, err)
	require.Len(t, due, 1)
	assert.Equal(t, 0, due[0].Step)
}

func TestPR5TargetedChunkCommitRequiresLiveScheduleTarget(t *testing.T) {
	f := newPR5ScheduleFixture(t)
	ctx := context.Background()
	run, _, err := f.repo.EnsureScheduledHealthRun(ctx,
		f.scheduleSpec("target-fence-run", "target-fence", HealthRunPriorityHigh, f.now))
	require.NoError(t, err)
	lease, err := f.repo.ClaimDueHealthRun(ctx, "target-fence-worker", time.Minute)
	require.NoError(t, err)
	require.Equal(t, run.ID, lease.ID)

	_, err = f.db.Connection().ExecContext(ctx,
		`UPDATE health_run_schedule SET active = FALSE WHERE run_id = ?`, run.ID)
	require.NoError(t, err)
	compat := pr4Fixture{
		repo: f.repo, db: f.db, run: run, providerID: f.provider.ID,
		now: f.now, clock: f.clock,
	}
	commit := pr5AuditAbsentCommit(
		compat, run, lease, "inactive-target-chunk", "targeted", f.gap.StartSegment, 1, f.now,
	)
	_, err = f.repo.CommitHealthChunk(ctx, commit)
	require.ErrorIs(t, err, ErrStaleHealthSchedule)
}

func TestPR5ProviderActivationWorkIsLevelTriggeredAndSparse(t *testing.T) {
	f := newPR5ScheduleFixture(t)
	ctx := context.Background()
	ordinary, err := f.repo.CreateHealthRun(ctx, HealthRunSpec{
		ID: "activation-source-run", FileRevisionID: f.revision.ID,
		ProviderSnapshotID: f.snapshot.ID, Trigger: "ordinary", Mode: "observation",
		TotalSegments: f.total, CreatedAt: f.now,
	})
	require.NoError(t, err)
	lease, err := f.repo.AcquireRunLease(ctx, ordinary.ID, "activation-source", time.Minute)
	require.NoError(t, err)
	compat := pr4Fixture{
		repo: f.repo, db: f.db, run: ordinary, providerID: f.provider.ID,
		now: f.now, clock: f.clock,
	}
	_, err = f.repo.CommitHealthChunk(ctx, pr5AuditAbsentCommit(
		compat, ordinary, lease, "activation-source-chunk", "observe_initial", 2, 1, f.now,
	))
	require.NoError(t, err)

	providers, err := f.repo.ReconcileProviders(ctx, []ProviderSpec{
		{
			StableID: f.provider.ID, DisplayName: "Primary",
			Endpoint: "schedule.example.invalid", Port: 119, Account: "account",
			Role: ProviderRolePrimary, Order: 0,
		},
		{
			StableID: "new-provider", DisplayName: "New primary",
			Endpoint: "new-provider.example.invalid", Port: 119, Account: "account",
			Role: ProviderRolePrimary, Order: 1,
		},
	})
	require.NoError(t, err)
	require.Len(t, providers, 2)

	work, err := f.repo.ListProviderActivationWork(ctx, 32)
	require.NoError(t, err)
	var unresolved *ProviderActivationWork
	for i := range work {
		if work[i].Provider.ProviderID == "new-provider" && work[i].GapID == "" {
			unresolved = &work[i]
		}
	}
	require.NotNil(t, unresolved)
	assert.Equal(t, f.revision.ID, unresolved.RevisionID)
	assert.Equal(t, int64(1), unresolved.TotalSegments)
	positions, err := f.repo.ListUnresolvedSegmentPositions(
		ctx, unresolved.RevisionID, unresolved.Provider.ProviderID,
		unresolved.Provider.ProviderGeneration, unresolved.Provider.ProviderActivationEpoch,
	)
	require.NoError(t, err)
	assert.Equal(t, []int64{2}, positions)

	// The existing activation already supplied the negative observation, and a
	// pure reorder does not create a new generation/activation or new work.
	for _, item := range work {
		assert.False(t, item.Provider.ProviderID == f.provider.ID && item.GapID == "")
	}
	_, err = f.repo.ReconcileProviders(ctx, []ProviderSpec{
		{
			StableID: "new-provider", DisplayName: "New primary",
			Endpoint: "new-provider.example.invalid", Port: 119, Account: "account",
			Role: ProviderRolePrimary, Order: 0,
		},
		{
			StableID: f.provider.ID, DisplayName: "Primary",
			Endpoint: "schedule.example.invalid", Port: 119, Account: "account",
			Role: ProviderRolePrimary, Order: 1,
		},
	})
	require.NoError(t, err)
	reordered, err := f.repo.ListProviderActivationWork(ctx, 32)
	require.NoError(t, err)
	workKeys := make(map[string]struct{}, len(work))
	for _, item := range work {
		workKeys[item.RevisionID+":"+item.Provider.ProviderID+":"+item.GapID] = struct{}{}
	}
	reorderedKeys := make(map[string]struct{}, len(reordered))
	for _, item := range reordered {
		reorderedKeys[item.RevisionID+":"+item.Provider.ProviderID+":"+item.GapID] = struct{}{}
	}
	assert.Equal(t, workKeys, reorderedKeys)
}

func TestPR5ProviderActivationWorkDoesNotStarveBehindCoveredPairs(t *testing.T) {
	db, repo := newPR4Repository(t)
	ctx := context.Background()
	now := time.Unix(1_711_100_000, 0).UTC()
	clock := &pr4TestClock{now: now}
	repo.now = clock.Now
	revision, err := repo.EnsureFileRevision(ctx, FileRevisionSpec{
		FilePath:          "library/provider-activation-bounded.mkv",
		LayoutFingerprint: "sha256:provider-activation-bounded",
		VirtualSize:       100, SegmentCount: 1,
	})
	require.NoError(t, err)

	const coveredProviders = 70
	specs := make([]ProviderSpec, 0, coveredProviders)
	for i := 0; i < coveredProviders; i++ {
		specs = append(specs, ProviderSpec{
			StableID: fmt.Sprintf("covered-%03d", i), DisplayName: fmt.Sprintf("Covered %03d", i),
			Endpoint: fmt.Sprintf("covered-%03d.example.invalid", i), Port: 119, Account: "account",
			Role: ProviderRolePrimary, Order: i,
		})
	}
	providers, err := repo.ReconcileProviders(ctx, specs)
	require.NoError(t, err)
	snapshot, err := repo.CaptureActiveProviderSnapshot(ctx, now)
	require.NoError(t, err)
	run, err := repo.CreateHealthRun(ctx, HealthRunSpec{
		ID: "provider-activation-covered-source", FileRevisionID: revision.ID,
		ProviderSnapshotID: snapshot.ID, Trigger: "ordinary", Mode: "observation",
		TotalSegments: 1, CreatedAt: now,
	})
	require.NoError(t, err)
	lease, err := repo.AcquireRunLease(ctx, run.ID, "covered-source-worker", time.Hour)
	require.NoError(t, err)
	for i, provider := range providers {
		_, err = repo.CommitHealthChunk(ctx, HealthChunkCommit{
			ChunkID: fmt.Sprintf("covered-source-%03d", i), RunID: run.ID,
			LeaseOwner: *lease.LeaseOwner, FencingToken: lease.FencingToken,
			ProviderID: provider.ID, ProviderGeneration: provider.CurrentGeneration,
			ProviderActivationEpoch: provider.ActivationEpoch,
			Stage:                   "covered-source", ObservationKind: HealthObservationSTAT,
			SegmentStart: 0, SegmentCount: 1, TestedBitmap: []byte{1},
			PresentBitmap: []byte{0}, AbsentBitmap: []byte{1}, CorruptBitmap: []byte{0},
			TemporaryBitmap: []byte{0}, InconclusiveBitmap: []byte{0}, ResolvedBitmap: []byte{0},
			CursorSegment: 1, ProviderChecksDelta: 1, MissingCandidatesDelta: 1,
			CommittedAt: now,
		})
		require.NoError(t, err)
	}

	specs = append(specs, ProviderSpec{
		StableID: "eligible-after-covered", DisplayName: "Eligible after covered",
		Endpoint: "eligible-after-covered.example.invalid", Port: 119, Account: "account",
		Role: ProviderRoleBackup, Order: len(specs),
	})
	_, err = repo.ReconcileProviders(ctx, specs)
	require.NoError(t, err)

	work, err := repo.ListProviderActivationWork(ctx, 1)
	require.NoError(t, err)
	require.Len(t, work, 1,
		"SQL eligibility must apply before LIMIT even when more than one batch of covered pairs sorts first")
	assert.Equal(t, revision.ID, work[0].RevisionID)
	assert.Equal(t, "eligible-after-covered", work[0].Provider.ProviderID)
	assert.Empty(t, work[0].GapID)

	var coverageRows int
	require.NoError(t, db.Connection().QueryRowContext(ctx, `
		SELECT COUNT(*) FROM health_provider_coverage WHERE file_revision_id = ?
	`, revision.ID).Scan(&coverageRows))
	assert.Equal(t, coveredProviders, coverageRows)
}

func TestPR5HistoricalExceptionSeedsNewActivationAfterOriginRemoval(t *testing.T) {
	db, repo := newPR4Repository(t)
	ctx := context.Background()
	now := time.Unix(1_711_200_000, 0).UTC()
	clock := &pr4TestClock{now: now}
	repo.now = clock.Now
	revision, err := repo.EnsureFileRevision(ctx, FileRevisionSpec{
		FilePath:          "library/historical-exception.mkv",
		LayoutFingerprint: "sha256:historical-exception",
		VirtualSize:       100, SegmentCount: 1,
	})
	require.NoError(t, err)
	oldProviders, err := repo.ReconcileProviders(ctx, []ProviderSpec{{
		StableID: "removed-origin", DisplayName: "Removed origin",
		Endpoint: "removed-origin.example.invalid", Port: 119, Account: "account",
		Role: ProviderRolePrimary, Order: 0,
	}})
	require.NoError(t, err)
	snapshot, err := repo.CaptureActiveProviderSnapshot(ctx, now)
	require.NoError(t, err)
	run, err := repo.CreateHealthRun(ctx, HealthRunSpec{
		ID: "historical-exception-source", FileRevisionID: revision.ID,
		ProviderSnapshotID: snapshot.ID, Trigger: "ordinary", Mode: "observation",
		TotalSegments: 1, CreatedAt: now,
	})
	require.NoError(t, err)
	lease, err := repo.AcquireRunLease(ctx, run.ID, "historical-source-worker", time.Hour)
	require.NoError(t, err)
	_, err = repo.CommitHealthChunk(ctx, HealthChunkCommit{
		ChunkID: "historical-exception-chunk", RunID: run.ID,
		LeaseOwner: *lease.LeaseOwner, FencingToken: lease.FencingToken,
		ProviderID: oldProviders[0].ID, ProviderGeneration: oldProviders[0].CurrentGeneration,
		ProviderActivationEpoch: oldProviders[0].ActivationEpoch,
		Stage:                   "historical-source", ObservationKind: HealthObservationSTAT,
		SegmentStart: 0, SegmentCount: 1, TestedBitmap: []byte{1},
		PresentBitmap: []byte{0}, AbsentBitmap: []byte{0}, CorruptBitmap: []byte{0},
		TemporaryBitmap: []byte{1}, InconclusiveBitmap: []byte{0}, ResolvedBitmap: []byte{0},
		CursorSegment: 1, ProviderChecksDelta: 1, InconclusiveDelta: 1,
		CommittedAt: now,
	})
	require.NoError(t, err)

	newProviders, err := repo.ReconcileProviders(ctx, []ProviderSpec{{
		StableID: "new-activation", DisplayName: "New activation",
		Endpoint: "new-activation.example.invalid", Port: 119, Account: "account",
		Role: ProviderRolePrimary, Order: 0,
	}})
	require.NoError(t, err)
	positions, err := repo.ListUnresolvedSegmentPositions(
		ctx, revision.ID, newProviders[0].ID, newProviders[0].CurrentGeneration,
		newProviders[0].ActivationEpoch,
	)
	require.NoError(t, err)
	assert.Equal(t, []int64{0}, positions,
		"negative history remains a seed after its origin provider is removed")
	work, err := repo.ListProviderActivationWork(ctx, 1)
	require.NoError(t, err)
	require.Len(t, work, 1)
	assert.Equal(t, "new-activation", work[0].Provider.ProviderID)

	var activeOrigin int
	require.NoError(t, db.Connection().QueryRowContext(ctx, `
		SELECT COUNT(*) FROM health_providers WHERE id = 'removed-origin' AND active = TRUE
	`).Scan(&activeOrigin))
	assert.Zero(t, activeOrigin)
}

func TestPR5GapRevalidationFinalizationDormantsOnlyAfterConclusiveDayFourteen(t *testing.T) {
	f := newPR5ScheduleFixture(t)
	ctx := context.Background()
	confirmedAt := f.now.Add(-14 * 24 * time.Hour)
	_, err := f.db.Connection().ExecContext(ctx, `
		UPDATE health_gap_ranges
		SET kind = 'confirmed_absent', confirmed_at = ?, revalidation_step = 3,
		    next_revalidation_at = ?
		WHERE id = ?
	`, confirmedAt, f.now, f.gap.ID)
	require.NoError(t, err)
	_, err = f.db.Connection().ExecContext(ctx, `
		INSERT INTO health_gap_provider_causes
			(gap_id, provider_id, provider_generation, provider_activation_epoch,
			 cause, confirmation_count, confirmed_at)
		VALUES (?, ?, ?, ?, 'absent', 2, ?)
	`, f.gap.ID, f.provider.ID, f.provider.CurrentGeneration,
		f.provider.ActivationEpoch, confirmedAt)
	require.NoError(t, err)
	run, _, err := f.repo.EnsureScheduledHealthRun(ctx, ScheduledHealthRunSpec{
		Run: HealthRunSpec{
			ID: "day-fourteen-run", FileRevisionID: f.revision.ID,
			ProviderSnapshotID: f.snapshot.ID, Trigger: "gap_revalidation_3",
			Mode: "observation", TotalSegments: f.gap.SegmentCount, CreatedAt: f.now,
		},
		DedupeKey: "gap-revalidation-day-fourteen", Priority: HealthRunPriorityLow,
		NotBefore: f.now, TargetGapID: f.gap.ID,
	})
	require.NoError(t, err)
	lease, err := f.repo.ClaimDueHealthRun(ctx, "day-fourteen-worker", time.Minute)
	require.NoError(t, err)
	require.Equal(t, run.ID, lease.ID)
	compat := pr4Fixture{
		repo: f.repo, db: f.db, run: run, providerID: f.provider.ID,
		now: f.now, clock: f.clock,
	}
	commit := pr5AuditAbsentCommit(
		compat, run, lease, "day-fourteen-absence", "gap_revalidation_stat",
		f.gap.StartSegment, f.provider.ActivationEpoch, f.now,
	)
	commit.ResolvedBitmap = []byte{0}
	commit.ResolvedDelta = 0
	_, err = f.repo.CommitHealthChunk(ctx, commit)
	require.NoError(t, err)

	finalized, err := f.repo.FinalizeGapRevalidation(
		ctx, run.ID, *lease.LeaseOwner, lease.FencingToken, f.now,
	)
	require.NoError(t, err)
	require.NotNil(t, finalized)
	assert.True(t, finalized.Advanced)
	assert.True(t, finalized.Dormant)
	assert.Equal(t, 4, finalized.Gap.RevalidationStep)
	assert.Equal(t, GapStatusDormant, finalized.Gap.Status)
	assert.Nil(t, finalized.Gap.NextRevalidationAt)
	completed, err := f.repo.GetHealthRun(ctx, run.ID)
	require.NoError(t, err)
	require.NotNil(t, completed)
	assert.Equal(t, completed.TotalSegments, completed.ResolvedSegments,
		"a conclusive finalizer owns terminal progress even when evidence did not increment it")
}

func TestPR5TemporaryGapRevalidationDoesNotAdvanceMilestone(t *testing.T) {
	f := newPR5ScheduleFixture(t)
	ctx := context.Background()
	confirmedAt := f.now.Add(-3 * 24 * time.Hour)
	_, err := f.db.Connection().ExecContext(ctx, `
		UPDATE health_gap_ranges
		SET kind = 'confirmed_absent', confirmed_at = ?, revalidation_step = 1,
		    next_revalidation_at = ?
		WHERE id = ?
	`, confirmedAt, f.now, f.gap.ID)
	require.NoError(t, err)
	_, err = f.db.Connection().ExecContext(ctx, `
		INSERT INTO health_gap_provider_causes
			(gap_id, provider_id, provider_generation, provider_activation_epoch,
			 cause, confirmation_count, confirmed_at)
		VALUES (?, ?, ?, ?, 'absent', 2, ?)
	`, f.gap.ID, f.provider.ID, f.provider.CurrentGeneration,
		f.provider.ActivationEpoch, confirmedAt)
	require.NoError(t, err)
	run, _, err := f.repo.EnsureScheduledHealthRun(ctx, ScheduledHealthRunSpec{
		Run: HealthRunSpec{
			ID: "temporary-revalidation-run", FileRevisionID: f.revision.ID,
			ProviderSnapshotID: f.snapshot.ID, Trigger: "gap_revalidation_1",
			Mode: "observation", TotalSegments: f.gap.SegmentCount, CreatedAt: f.now,
		},
		DedupeKey: "temporary-revalidation", Priority: HealthRunPriorityLow,
		NotBefore: f.now, TargetGapID: f.gap.ID,
	})
	require.NoError(t, err)
	lease, err := f.repo.ClaimDueHealthRun(ctx, "temporary-revalidation-worker", time.Minute)
	require.NoError(t, err)
	require.Equal(t, run.ID, lease.ID)
	_, err = f.repo.CommitHealthChunk(ctx, HealthChunkCommit{
		ChunkID: "temporary-revalidation-chunk", RunID: run.ID,
		LeaseOwner: *lease.LeaseOwner, FencingToken: lease.FencingToken,
		ProviderID: f.provider.ID, ProviderGeneration: f.provider.CurrentGeneration,
		ProviderActivationEpoch: f.provider.ActivationEpoch,
		Stage:                   "gap_revalidation_stat_retry_4", ObservationKind: HealthObservationSTAT,
		SegmentStart: f.gap.StartSegment, SegmentCount: 1,
		TestedBitmap: []byte{1}, PresentBitmap: []byte{0}, AbsentBitmap: []byte{0},
		CorruptBitmap: []byte{0}, TemporaryBitmap: []byte{1}, InconclusiveBitmap: []byte{0},
		ResolvedBitmap: []byte{0}, CursorSegment: f.gap.StartSegment + 1,
		ProviderChecksDelta: 1, InconclusiveDelta: 1, CommittedAt: f.now,
	})
	require.NoError(t, err)

	finalized, err := f.repo.FinalizeGapRevalidation(
		ctx, run.ID, *lease.LeaseOwner, lease.FencingToken, f.now,
	)
	require.NoError(t, err)
	assert.False(t, finalized.Advanced)
	assert.False(t, finalized.Dormant)
	assert.Equal(t, 1, finalized.Gap.RevalidationStep)
	require.NotNil(t, finalized.Gap.NextRevalidationAt)
	assert.Equal(t, f.now.Add(time.Hour), finalized.Gap.NextRevalidationAt.UTC())
	failed, err := f.repo.GetHealthRun(ctx, run.ID)
	require.NoError(t, err)
	require.NotNil(t, failed)
	assert.Equal(t, HealthRunFailed, failed.Status)
	assert.Zero(t, failed.ResolvedSegments,
		"an inconclusive finalizer must not forge completed progress")
}
