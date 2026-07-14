package database

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pr5ActivationFinalizationFixture struct {
	pr5ScheduleFixture
	snapshot *ProviderSnapshot
	newA     HealthProvider
	newB     HealthProvider
}

func newPR5ActivationFinalizationFixture(t *testing.T) pr5ActivationFinalizationFixture {
	t.Helper()
	f := newPR5ScheduleFixture(t)
	ctx := context.Background()
	confirmedAt := f.now
	_, err := f.db.Connection().ExecContext(ctx, `
		UPDATE health_gap_ranges
		SET kind = 'confirmed_absent', confirmed_at = ?,
		    next_revalidation_at = ?
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

	providers, err := f.repo.ReconcileProviders(ctx, []ProviderSpec{
		{
			StableID: f.provider.ID, DisplayName: "Primary",
			Endpoint: "schedule.example.invalid", Port: 119, Account: "account",
			Role: ProviderRolePrimary, Order: 0,
		},
		{
			StableID: "activation-new-a", DisplayName: "New A",
			Endpoint: "activation-a.example.invalid", Port: 119, Account: "account",
			Role: ProviderRolePrimary, Order: 1,
		},
		{
			StableID: "activation-new-b", DisplayName: "New B",
			Endpoint: "activation-b.example.invalid", Port: 119, Account: "account",
			Role: ProviderRoleBackup, Order: 2,
		},
	})
	require.NoError(t, err)
	require.Len(t, providers, 3)
	byID := make(map[string]HealthProvider, len(providers))
	for _, provider := range providers {
		byID[provider.ID] = provider
	}
	gap, err := f.repo.GetHealthGapRange(ctx, f.gap.ID)
	require.NoError(t, err)
	require.NotNil(t, gap)
	assert.Equal(t, GapKindProvisional, gap.Kind,
		"adding providers must invalidate the old active-set conclusion immediately")
	snapshot, err := f.repo.CaptureActiveProviderSnapshot(ctx, f.now)
	require.NoError(t, err)
	return pr5ActivationFinalizationFixture{
		pr5ScheduleFixture: f, snapshot: snapshot,
		newA: byID["activation-new-a"], newB: byID["activation-new-b"],
	}
}

func (f pr5ActivationFinalizationFixture) createTargetRun(
	t *testing.T,
	provider HealthProvider,
	id string,
) (*HealthRun, *HealthRun) {
	t.Helper()
	run, _, err := f.repo.EnsureScheduledHealthRun(context.Background(), ScheduledHealthRunSpec{
		Run: HealthRunSpec{
			ID: id, FileRevisionID: f.revision.ID, ProviderSnapshotID: f.snapshot.ID,
			Trigger: "provider_activation_gap", Mode: "observation",
			TotalSegments: f.gap.SegmentCount, CreatedAt: f.clock.now,
		},
		DedupeKey: id, Priority: HealthRunPriorityHigh, NotBefore: f.clock.now,
		TargetProviderID: provider.ID, TargetProviderGeneration: provider.CurrentGeneration,
		TargetProviderActivationEpoch: provider.ActivationEpoch, TargetGapID: f.gap.ID,
	})
	require.NoError(t, err)
	lease, err := f.repo.ClaimDueObservationHealthRun(
		context.Background(), id+"-worker", 30*time.Minute,
	)
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.Equal(t, run.ID, lease.ID)
	return run, lease
}

func (f pr5ActivationFinalizationFixture) commitTargetAbsence(
	t *testing.T,
	run, lease *HealthRun,
	provider HealthProvider,
	chunkID, stage string,
	at time.Time,
) {
	t.Helper()
	f.clock.now = at
	_, err := f.repo.CommitHealthChunk(context.Background(), HealthChunkCommit{
		ChunkID: chunkID, RunID: run.ID, LeaseOwner: *lease.LeaseOwner,
		FencingToken: lease.FencingToken, ProviderID: provider.ID,
		ProviderGeneration:      provider.CurrentGeneration,
		ProviderActivationEpoch: provider.ActivationEpoch,
		Stage:                   stage, ObservationKind: HealthObservationSTAT,
		SegmentStart: f.gap.StartSegment, SegmentCount: f.gap.SegmentCount,
		TestedBitmap: []byte{1}, PresentBitmap: []byte{0}, AbsentBitmap: []byte{1},
		CorruptBitmap: []byte{0}, TemporaryBitmap: []byte{0}, InconclusiveBitmap: []byte{0},
		ResolvedBitmap: []byte{0}, CursorSegment: f.gap.StartSegment + 1,
		ResolvedDelta: 0, ProviderChecksDelta: 1, MissingCandidatesDelta: 1,
		CommittedAt: at,
		Confirmations: []HealthConfirmationEvent{{
			IdempotencyKey: chunkID + ":absence", SegmentIndex: f.gap.StartSegment,
			Cause: GapCauseAbsent, ObservedAt: at,
		}},
	})
	require.NoError(t, err)
}

func TestPR5ProviderActivationGapWaitsForEveryNewActivation(t *testing.T) {
	f := newPR5ActivationFinalizationFixture(t)
	ctx := context.Background()
	firstRun, firstLease := f.createTargetRun(t, f.newA, "activation-new-a-run")
	firstAt := f.now.Add(time.Minute)
	f.commitTargetAbsence(t, firstRun, firstLease, f.newA,
		"activation-new-a-first", "provider_activation_initial", firstAt)
	secondAt := firstAt.Add(DefaultGapConfirmationMinimumDelay)
	f.commitTargetAbsence(t, firstRun, firstLease, f.newA,
		"activation-new-a-second", "provider_activation_confirmation_2", secondAt)
	conclusive, err := f.repo.FinalizeProviderActivationGap(
		ctx, firstRun.ID, *firstLease.LeaseOwner, firstLease.FencingToken, secondAt,
	)
	require.NoError(t, err)
	assert.True(t, conclusive, "the target provider's two-wave run is conclusive")
	completedFirst, err := f.repo.GetHealthRun(ctx, firstRun.ID)
	require.NoError(t, err)
	require.NotNil(t, completedFirst)
	assert.Equal(t, f.gap.SegmentCount, completedFirst.TotalSegments)
	assert.Equal(t, completedFirst.TotalSegments, completedFirst.ResolvedSegments,
		"completed targeted progress must represent its sparse work as 100 percent")
	gap, err := f.repo.GetHealthGapRange(ctx, f.gap.ID)
	require.NoError(t, err)
	require.NotNil(t, gap)
	assert.Equal(t, GapKindProvisional, gap.Kind,
		"one completed target cannot confirm a gap while another active provider is untested")
	require.Len(t, gap.Causes, 2)

	work, err := f.repo.ListProviderActivationWork(ctx, 16)
	require.NoError(t, err)
	foundNewB := false
	for _, item := range work {
		if item.GapID == f.gap.ID && item.Provider.ProviderID == f.newB.ID {
			foundNewB = true
		}
	}
	assert.True(t, foundNewB, "the missing activation remains level-triggered")

	f.clock.now = secondAt.Add(time.Minute)
	secondRun, secondLease := f.createTargetRun(t, f.newB, "activation-new-b-run")
	thirdAt := f.clock.now
	f.commitTargetAbsence(t, secondRun, secondLease, f.newB,
		"activation-new-b-first", "provider_activation_initial", thirdAt)
	fourthAt := thirdAt.Add(DefaultGapConfirmationMinimumDelay)
	f.commitTargetAbsence(t, secondRun, secondLease, f.newB,
		"activation-new-b-second", "provider_activation_confirmation_2", fourthAt)
	conclusive, err = f.repo.FinalizeProviderActivationGap(
		ctx, secondRun.ID, *secondLease.LeaseOwner, secondLease.FencingToken, fourthAt,
	)
	require.NoError(t, err)
	assert.True(t, conclusive)
	gap, err = f.repo.GetHealthGapRange(ctx, f.gap.ID)
	require.NoError(t, err)
	require.NotNil(t, gap)
	assert.Equal(t, GapKindConfirmedAbsent, gap.Kind)
	require.Len(t, gap.Causes, 3)
}

func TestPR5ProviderActivationRetainsRemovedCauseWithoutCountingItAfterReactivation(t *testing.T) {
	f := newPR5ActivationFinalizationFixture(t)
	ctx := context.Background()
	_, err := f.repo.ReconcileProviders(ctx, []ProviderSpec{
		{
			StableID: f.newA.ID, DisplayName: "New A",
			Endpoint: "activation-a.example.invalid", Port: 119, Account: "account",
			Role: ProviderRolePrimary, Order: 0,
		},
		{
			StableID: f.newB.ID, DisplayName: "New B",
			Endpoint: "activation-b.example.invalid", Port: 119, Account: "account",
			Role: ProviderRoleBackup, Order: 1,
		},
	})
	require.NoError(t, err)
	f.snapshot, err = f.repo.CaptureActiveProviderSnapshot(ctx, f.now)
	require.NoError(t, err)

	run, lease := f.createTargetRun(t, f.newA, "activation-history-retention-run")
	firstAt := f.now.Add(time.Minute)
	f.commitTargetAbsence(t, run, lease, f.newA,
		"activation-history-retention-first", "provider_activation_initial", firstAt)
	secondAt := firstAt.Add(DefaultGapConfirmationMinimumDelay)
	f.commitTargetAbsence(t, run, lease, f.newA,
		"activation-history-retention-second", "provider_activation_confirmation_2", secondAt)
	conclusive, err := f.repo.FinalizeProviderActivationGap(
		ctx, run.ID, *lease.LeaseOwner, lease.FencingToken, secondAt,
	)
	require.NoError(t, err)
	require.True(t, conclusive)

	gap, err := f.repo.GetHealthGapRange(ctx, f.gap.ID)
	require.NoError(t, err)
	require.NotNil(t, gap)
	assert.Equal(t, GapKindProvisional, gap.Kind,
		"the removed provider's historical cause cannot satisfy the current active set")
	retainedOldEpoch := false
	for _, cause := range gap.Causes {
		if cause.ProviderID == f.provider.ID &&
			cause.ProviderGeneration == f.provider.CurrentGeneration &&
			cause.ProviderActivationEpoch == f.provider.ActivationEpoch {
			retainedOldEpoch = true
		}
	}
	assert.True(t, retainedOldEpoch,
		"provider removal must not erase immutable gap-cause history")

	providers, err := f.repo.ReconcileProviders(ctx, []ProviderSpec{
		{
			StableID: f.provider.ID, DisplayName: "Primary",
			Endpoint: "schedule.example.invalid", Port: 119, Account: "account",
			Role: ProviderRolePrimary, Order: 0,
		},
		{
			StableID: f.newA.ID, DisplayName: "New A",
			Endpoint: "activation-a.example.invalid", Port: 119, Account: "account",
			Role: ProviderRolePrimary, Order: 1,
		},
		{
			StableID: f.newB.ID, DisplayName: "New B",
			Endpoint: "activation-b.example.invalid", Port: 119, Account: "account",
			Role: ProviderRoleBackup, Order: 2,
		},
	})
	require.NoError(t, err)
	var reactivated HealthProvider
	for _, provider := range providers {
		if provider.ID == f.provider.ID {
			reactivated = provider
		}
	}
	require.Equal(t, int64(2), reactivated.ActivationEpoch)
	work, err := f.repo.ListProviderActivationWork(ctx, 16)
	require.NoError(t, err)
	foundReactivatedWork := false
	for _, item := range work {
		if item.GapID == f.gap.ID && item.Provider.ProviderID == reactivated.ID &&
			item.Provider.ProviderActivationEpoch == reactivated.ActivationEpoch {
			foundReactivatedWork = true
		}
	}
	assert.True(t, foundReactivatedWork,
		"an epoch-1 historical cause must not count for the reactivated epoch-2 provider")
	var oldCauseRows int
	require.NoError(t, f.db.Connection().QueryRowContext(ctx, `
		SELECT COUNT(*) FROM health_gap_provider_causes
		WHERE gap_id = ? AND provider_id = ? AND provider_generation = ?
		  AND provider_activation_epoch = ?
	`, f.gap.ID, f.provider.ID, f.provider.CurrentGeneration,
		f.provider.ActivationEpoch).Scan(&oldCauseRows))
	assert.Equal(t, 1, oldCauseRows)
}

func TestPR5ProviderActivationGapPropagatesStructuralEvidenceErrors(t *testing.T) {
	f := newPR5ActivationFinalizationFixture(t)
	run, lease := f.createTargetRun(t, f.newA, "activation-structural-error-run")
	at := f.now.Add(time.Minute)
	f.commitTargetAbsence(t, run, lease, f.newA,
		"activation-structural-error-chunk", "provider_activation_initial", at)
	_, err := f.db.Connection().ExecContext(context.Background(), `
		UPDATE health_confirmation_events
		SET observed_at = X'0102'
		WHERE source_chunk_id = ?
	`, "activation-structural-error-chunk")
	require.NoError(t, err)

	conclusive, err := f.repo.FinalizeProviderActivationGap(
		context.Background(), run.ID, *lease.LeaseOwner, lease.FencingToken, at,
	)
	require.Error(t, err)
	assert.False(t, conclusive)
	assert.True(t, strings.Contains(err.Error(), "scan range-wide gap confirmation evidence"))
	retained, getErr := f.repo.GetHealthRun(context.Background(), run.ID)
	require.NoError(t, getErr)
	require.NotNil(t, retained)
	assert.Equal(t, HealthRunRunning, retained.Status,
		"a repository error must roll back instead of consuming level-triggered work")
	schedule, getErr := f.repo.GetHealthRunSchedule(context.Background(), run.ID)
	require.NoError(t, getErr)
	require.NotNil(t, schedule)
	assert.True(t, schedule.Active)
}

func TestPR5ProviderActivationSingleWaveIsInconclusiveAndReseedable(t *testing.T) {
	f := newPR5ActivationFinalizationFixture(t)
	run, lease := f.createTargetRun(t, f.newA, "activation-single-wave-run")
	at := f.now.Add(time.Minute)
	f.commitTargetAbsence(t, run, lease, f.newA,
		"activation-single-wave-chunk", "provider_activation_initial", at)
	conclusive, err := f.repo.FinalizeProviderActivationGap(
		context.Background(), run.ID, *lease.LeaseOwner, lease.FencingToken, at,
	)
	require.NoError(t, err)
	assert.False(t, conclusive)
	retained, err := f.repo.GetHealthRun(context.Background(), run.ID)
	require.NoError(t, err)
	assert.Equal(t, HealthRunFailed, retained.Status)
	assert.Zero(t, retained.ResolvedSegments,
		"an inconclusive provider target remains partial")
	schedule, err := f.repo.GetHealthRunSchedule(context.Background(), run.ID)
	require.NoError(t, err)
	assert.False(t, schedule.Active)
	work, err := f.repo.ListProviderActivationWork(context.Background(), 16)
	require.NoError(t, err)
	found := false
	for _, item := range work {
		if item.GapID == f.gap.ID && item.Provider.ProviderID == f.newA.ID {
			found = true
		}
	}
	assert.True(t, found)
}

func TestPR5ProviderActivationExhaustedTemporaryGapIsNotRecreated(t *testing.T) {
	f := newPR5ActivationFinalizationFixture(t)
	ctx := context.Background()
	run, lease := f.createTargetRun(t, f.newA, "activation-exhausted-temporary-run")
	at := f.now.Add(time.Minute)
	f.clock.now = at
	_, err := f.repo.CommitHealthChunk(ctx, HealthChunkCommit{
		ChunkID: "activation-exhausted-temporary-chunk", RunID: run.ID,
		LeaseOwner: *lease.LeaseOwner, FencingToken: lease.FencingToken,
		ProviderID: f.newA.ID, ProviderGeneration: f.newA.CurrentGeneration,
		ProviderActivationEpoch: f.newA.ActivationEpoch,
		Stage:                   "provider_activation_stat_retry_4", ObservationKind: HealthObservationSTAT,
		SegmentStart: f.gap.StartSegment, SegmentCount: f.gap.SegmentCount,
		TestedBitmap: []byte{1}, PresentBitmap: []byte{0}, AbsentBitmap: []byte{0},
		CorruptBitmap: []byte{0}, TemporaryBitmap: []byte{1}, InconclusiveBitmap: []byte{0},
		ResolvedBitmap: []byte{0}, CursorSegment: f.gap.StartSegment + f.gap.SegmentCount,
		ProviderChecksDelta: 1, InconclusiveDelta: 1, CommittedAt: at,
		Retry: &HealthRetryState{
			RetryKey:     "activation-exhausted-temporary-retry",
			SegmentStart: f.gap.StartSegment, SegmentCount: f.gap.SegmentCount,
			Outcome: "temporary", Attempt: 4, Exhausted: true,
		},
	})
	require.NoError(t, err)
	conclusive, err := f.repo.FinalizeProviderActivationGap(
		ctx, run.ID, *lease.LeaseOwner, lease.FencingToken, at,
	)
	require.NoError(t, err)
	assert.False(t, conclusive)

	for cycle := 0; cycle < 2; cycle++ {
		work, err := f.repo.ListProviderActivationWork(ctx, 16)
		require.NoError(t, err)
		for _, item := range work {
			assert.False(t, item.GapID == f.gap.ID && item.Provider.ProviderID == f.newA.ID,
				"exhausted temporary activation evidence is a durable gate across scheduler cycles")
		}
	}
}

func runPR5DormantProviderActivationFinalizer(t *testing.T, dialect Dialect) {
	t.Helper()
	fixture := newPR5SchedulerConcurrencyFixture(t, dialect)
	ctx := context.Background()
	current := fixture.now
	fixture.repo.now = func() time.Time { return current }
	oldID := "dormant-old-" + fixture.token
	newID := "dormant-new-" + fixture.token
	oldSpec := ProviderSpec{
		StableID: oldID, DisplayName: "Dormant old provider",
		Endpoint: "dormant-old.example.invalid", Port: 119, Account: fixture.token,
		Role: ProviderRolePrimary, Order: 0,
	}
	providers, err := fixture.repo.ReconcileProviders(ctx, []ProviderSpec{oldSpec})
	require.NoError(t, err)
	require.Len(t, providers, 1)
	oldProvider := providers[0]
	gap, err := fixture.repo.UpsertGapRange(ctx, GapRangeWrite{
		ID: "dormant-gap-" + fixture.token, FileRevisionID: fixture.revision.ID,
		Kind: GapKindProvisional, StartSegment: 3, SegmentCount: 1,
		Status: GapStatusActive, CreatedAt: current,
	})
	require.NoError(t, err)
	_, err = newDialectAwareDB(fixture.db.Connection(), dialect).ExecContext(ctx, `
		UPDATE health_gap_ranges
		SET status = 'dormant', kind = 'confirmed_absent', confirmed_at = ?,
		    revalidation_step = 4, next_revalidation_at = NULL
		WHERE id = ?
	`, current, gap.ID)
	require.NoError(t, err)
	_, err = newDialectAwareDB(fixture.db.Connection(), dialect).ExecContext(ctx, `
		INSERT INTO health_gap_provider_causes
			(gap_id, provider_id, provider_generation, provider_activation_epoch,
			 cause, confirmation_count, confirmed_at)
		VALUES (?, ?, ?, ?, 'absent', 2, ?)
	`, gap.ID, oldProvider.ID, oldProvider.CurrentGeneration,
		oldProvider.ActivationEpoch, current)
	require.NoError(t, err)
	newSpec := ProviderSpec{
		StableID: newID, DisplayName: "Dormant new provider",
		Endpoint: "dormant-new.example.invalid", Port: 119, Account: fixture.token,
		Role: ProviderRoleBackup, Order: 1,
	}
	providers, err = fixture.repo.ReconcileProviders(ctx, []ProviderSpec{oldSpec, newSpec})
	require.NoError(t, err)
	require.Len(t, providers, 2)
	var newProvider HealthProvider
	for _, provider := range providers {
		if provider.ID == newID {
			newProvider = provider
		}
	}
	require.Equal(t, newID, newProvider.ID)
	snapshot, err := fixture.repo.CaptureActiveProviderSnapshot(ctx, current)
	require.NoError(t, err)
	t.Cleanup(func() {
		q := newDialectAwareDB(fixture.db.Connection(), dialect)
		_, cleanupErr := q.ExecContext(context.Background(),
			`DELETE FROM file_health WHERE file_path = ?`, fixture.filePath)
		assert.NoError(t, cleanupErr)
		_, cleanupErr = q.ExecContext(context.Background(),
			`DELETE FROM health_provider_snapshots WHERE id = ?`, snapshot.ID)
		assert.NoError(t, cleanupErr)
		_, cleanupErr = q.ExecContext(context.Background(),
			`DELETE FROM health_provider_generations WHERE provider_id IN (?, ?)`, oldID, newID)
		assert.NoError(t, cleanupErr)
		_, cleanupErr = q.ExecContext(context.Background(),
			`DELETE FROM health_providers WHERE id IN (?, ?)`, oldID, newID)
		assert.NoError(t, cleanupErr)
	})
	runID := "dormant-finalizer-run-" + fixture.token
	run, _, err := fixture.repo.EnsureScheduledHealthRun(ctx, ScheduledHealthRunSpec{
		Run: HealthRunSpec{
			ID: runID, FileRevisionID: fixture.revision.ID,
			ProviderSnapshotID: snapshot.ID, Trigger: "provider_activation_gap",
			Mode: "observation", TotalSegments: gap.SegmentCount, CreatedAt: current,
		},
		DedupeKey: runID, Priority: HealthRunPriorityHigh, NotBefore: current,
		TargetProviderID:              newProvider.ID,
		TargetProviderGeneration:      newProvider.CurrentGeneration,
		TargetProviderActivationEpoch: newProvider.ActivationEpoch,
		TargetGapID:                   gap.ID,
	})
	require.NoError(t, err)
	owner := "dormant-finalizer-worker-" + fixture.token
	lease, err := fixture.repo.ClaimDueObservationHealthRun(ctx, owner, time.Hour)
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.Equal(t, run.ID, lease.ID)
	commitWave := func(chunkID, stage string, at time.Time) {
		t.Helper()
		current = at
		_, commitErr := fixture.repo.CommitHealthChunk(ctx, HealthChunkCommit{
			ChunkID: chunkID, RunID: run.ID, LeaseOwner: owner,
			FencingToken: lease.FencingToken, ProviderID: newProvider.ID,
			ProviderGeneration:      newProvider.CurrentGeneration,
			ProviderActivationEpoch: newProvider.ActivationEpoch,
			Stage:                   stage, ObservationKind: HealthObservationSTAT,
			SegmentStart: gap.StartSegment, SegmentCount: gap.SegmentCount,
			TestedBitmap: []byte{1}, PresentBitmap: []byte{0}, AbsentBitmap: []byte{1},
			CorruptBitmap: []byte{0}, TemporaryBitmap: []byte{0}, InconclusiveBitmap: []byte{0},
			ResolvedBitmap: []byte{0}, CursorSegment: gap.StartSegment + gap.SegmentCount,
			ProviderChecksDelta: 1, MissingCandidatesDelta: 1, CommittedAt: at,
			Confirmations: []HealthConfirmationEvent{{
				IdempotencyKey: chunkID + ":confirmation", SegmentIndex: gap.StartSegment,
				Cause: GapCauseAbsent, ObservedAt: at,
			}},
		})
		require.NoError(t, commitErr)
	}
	firstAt := fixture.now.Add(time.Minute)
	commitWave("dormant-first-"+fixture.token, "provider_activation_initial", firstAt)
	secondAt := firstAt.Add(DefaultGapConfirmationMinimumDelay)
	commitWave("dormant-second-"+fixture.token, "provider_activation_confirmation_2", secondAt)
	conclusive, err := fixture.repo.FinalizeProviderActivationGap(
		ctx, run.ID, owner, lease.FencingToken, secondAt,
	)
	require.NoError(t, err)
	assert.True(t, conclusive)
	completed, err := fixture.repo.GetHealthRun(ctx, run.ID)
	require.NoError(t, err)
	require.NotNil(t, completed)
	assert.Equal(t, completed.TotalSegments, completed.ResolvedSegments)
	finalGap, err := fixture.repo.GetHealthGapRange(ctx, gap.ID)
	require.NoError(t, err)
	require.NotNil(t, finalGap)
	assert.Equal(t, GapStatusDormant, finalGap.Status)
	assert.Equal(t, GapKindConfirmedAbsent, finalGap.Kind)
	assert.Nil(t, finalGap.NextRevalidationAt,
		"dormant finalization must preserve a NULL next revalidation time")
}

func TestPR5SQLiteDormantProviderActivationFinalizer(t *testing.T) {
	runPR5DormantProviderActivationFinalizer(t, DialectSQLite)
}

func TestPR5PostgresDormantProviderActivationFinalizer(t *testing.T) {
	runPR5DormantProviderActivationFinalizer(t, DialectPostgres)
}
