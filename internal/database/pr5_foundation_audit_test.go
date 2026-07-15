package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func pr5AuditPresentCommit(
	f pr4Fixture,
	lease *HealthRun,
	chunkID, stage string,
	position int64,
	kind HealthObservationKind,
	at time.Time,
) HealthChunkCommit {
	return HealthChunkCommit{
		ChunkID: chunkID, RunID: lease.ID, LeaseOwner: *lease.LeaseOwner,
		FencingToken: lease.FencingToken, ProviderID: f.providerID,
		ProviderGeneration: 1, ProviderActivationEpoch: 1,
		Stage: stage, ObservationKind: kind,
		FreshTransport: kind == HealthObservationValidatedBody,
		SegmentStart:   position, SegmentCount: 1,
		TestedBitmap: []byte{1}, PresentBitmap: []byte{1},
		AbsentBitmap: []byte{0}, CorruptBitmap: []byte{0},
		TemporaryBitmap: []byte{0}, InconclusiveBitmap: []byte{0},
		ResolvedBitmap: []byte{1}, CursorSegment: position + 1,
		ResolvedDelta: 1, ProviderChecksDelta: 1, CommittedAt: at,
	}
}

func pr5AuditAbsentCommit(
	f pr4Fixture,
	run, lease *HealthRun,
	chunkID, stage string,
	position, activationEpoch int64,
	at time.Time,
) HealthChunkCommit {
	return HealthChunkCommit{
		ChunkID: chunkID, RunID: run.ID, LeaseOwner: *lease.LeaseOwner,
		FencingToken: lease.FencingToken, ProviderID: f.providerID,
		ProviderGeneration: 1, ProviderActivationEpoch: activationEpoch,
		Stage: stage, ObservationKind: HealthObservationSTAT,
		SegmentStart: position, SegmentCount: 1,
		TestedBitmap: []byte{1}, PresentBitmap: []byte{0},
		AbsentBitmap: []byte{1}, CorruptBitmap: []byte{0},
		TemporaryBitmap: []byte{0}, InconclusiveBitmap: []byte{0},
		ResolvedBitmap: []byte{1}, CursorSegment: position + 1,
		ResolvedDelta: 1, ProviderChecksDelta: 1, MissingCandidatesDelta: 1,
		CommittedAt: at,
		Confirmations: []HealthConfirmationEvent{{
			IdempotencyKey: chunkID + ":confirmation", SegmentIndex: position,
			Cause: GapCauseAbsent, ObservedAt: at,
		}},
	}
}

func requirePR5StaleTargetRunIsDurablyTerminal(
	t *testing.T,
	f pr5ScheduleFixture,
	runID string,
	at time.Time,
) {
	t.Helper()
	restarted := NewHealthStateRepository(f.db.Connection(), DialectSQLite)
	restarted.now = f.clock.Now
	run, err := restarted.GetHealthRun(context.Background(), runID)
	require.NoError(t, err)
	require.NotNil(t, run)
	assert.Equal(t, HealthRunCanceled, run.Status)
	assert.Nil(t, run.LeaseOwner)
	assert.Nil(t, run.LeaseExpiresAt)
	assert.True(t, run.CancelRequested)
	assert.Equal(t, "stale health target", run.LastError)
	require.NotNil(t, run.CompletedAt)
	assert.Equal(t, at.UTC(), run.CompletedAt.UTC())

	runs, err := restarted.ListHealthRuns(context.Background(), 500)
	require.NoError(t, err)
	found := false
	for _, listed := range runs {
		if listed.ID == runID {
			found = true
			assert.Equal(t, HealthRunCanceled, listed.Status)
			assert.Equal(t, "stale health target", listed.LastError)
			require.NotNil(t, listed.CompletedAt)
		}
	}
	assert.True(t, found, "terminal stale-target history remains visible after restart")
}

func TestPR5ResolvedProgressUnionsPositionsAcrossProviderStages(t *testing.T) {
	f := newPR4RunFixture(t)
	ctx := context.Background()
	lease, err := f.repo.AcquireRunLease(ctx, f.run.ID, "resolved-union-worker", 10*time.Minute)
	require.NoError(t, err)

	first := pr5AuditPresentCommit(
		f, lease, "resolved-union-first", "primary_stat", 0, HealthObservationSTAT, f.now)
	afterFirst, err := f.repo.CommitHealthChunk(ctx, first)
	require.NoError(t, err)
	require.Equal(t, int64(1), afterFirst.ResolvedSegments)

	second := pr5AuditPresentCommit(
		f, lease, "resolved-union-fallback", "fallback_stat", 0, HealthObservationSTAT, f.now)
	afterFallback, err := f.repo.CommitHealthChunk(ctx, second)
	require.NoError(t, err)
	assert.Equal(t, int64(1), afterFallback.ResolvedSegments,
		"the committed resolution bitmap is a positional union, not a sum of worker deltas")
	assert.Equal(t, int64(2), afterFallback.ProviderChecks,
		"provider work remains an attempt count even when positional resolution is already known")
}

func TestPR5ChunkRequiresExplicitResolvedPositionIdentity(t *testing.T) {
	f := newPR4RunFixture(t)
	ctx := context.Background()
	lease, err := f.repo.AcquireRunLease(ctx, f.run.ID, "explicit-resolution-worker", 10*time.Minute)
	require.NoError(t, err)

	ambiguous := pr5AuditPresentCommit(
		f, lease, "ambiguous-resolution", "primary_stat", 0, HealthObservationSTAT, f.now)
	ambiguous.ResolvedBitmap = nil
	ambiguous.ResolvedDelta = 1
	_, err = f.repo.CommitHealthChunk(ctx, ambiguous)
	require.Error(t, err,
		"a count cannot identify which canonical position resolved and must not be projected onto arbitrary outcomes")
}

type pr5AuditImportFixture struct {
	db       *DB
	repo     *HealthStateRepository
	clock    *pr4TestClock
	now      time.Time
	queueA   *ImportQueueItem
	queueB   *ImportQueueItem
	revision *HealthFileRevision
	snapshot *ProviderSnapshot
	run      *HealthRun
}

func newPR5AuditImportFixture(t *testing.T, trigger string) pr5AuditImportFixture {
	t.Helper()
	db, repo := newPR4Repository(t)
	ctx := context.Background()
	now := time.Unix(1_713_000_000, 0).UTC()
	clock := &pr4TestClock{now: now}
	repo.now = clock.Now
	queueA := &ImportQueueItem{
		NzbPath: "queue/audit-import-a.nzb", Priority: QueuePriorityNormal,
		Status: QueueStatusProcessing, MaxRetries: 3,
	}
	queueB := &ImportQueueItem{
		NzbPath: "queue/audit-import-b.nzb", Priority: QueuePriorityNormal,
		Status: QueueStatusProcessing, MaxRetries: 3,
	}
	require.NoError(t, db.Repository.AddToQueue(ctx, queueA))
	require.NoError(t, db.Repository.AddToQueue(ctx, queueB))
	revision, err := repo.EnsureFileRevision(ctx, FileRevisionSpec{
		FilePath: "library/audit-import.mkv", LayoutFingerprint: "sha256:audit-import-layout",
		VirtualSize: 200, SegmentCount: 2,
	})
	require.NoError(t, err)
	_, err = repo.ReconcileProviders(ctx, []ProviderSpec{{
		StableID: "audit-import-provider", DisplayName: "Audit provider",
		Endpoint: "audit-import.invalid", Port: 119, Account: "synthetic-account",
		Role: ProviderRolePrimary, Order: 0,
	}})
	require.NoError(t, err)
	snapshot, err := repo.CaptureActiveProviderSnapshot(ctx, now)
	require.NoError(t, err)
	run, err := repo.CreateHealthRun(ctx, HealthRunSpec{
		ID: "audit-import-run", FileRevisionID: revision.ID, ProviderSnapshotID: snapshot.ID,
		Trigger: trigger, Mode: "observation", TotalSegments: 2, CreatedAt: now,
	})
	require.NoError(t, err)
	return pr5AuditImportFixture{
		db: db, repo: repo, clock: clock, now: now, queueA: queueA, queueB: queueB,
		revision: revision, snapshot: snapshot, run: run,
	}
}

func TestPR5ImportValidationRejectsNonImportRunDirectTerminalAndRunReuse(t *testing.T) {
	t.Run("non-import run", func(t *testing.T) {
		f := newPR5AuditImportFixture(t, "manual")
		lease, err := f.repo.AcquireRunLease(context.Background(), f.run.ID, "manual-worker", time.Minute)
		require.NoError(t, err)
		_, err = f.repo.UpsertImportValidation(context.Background(), ImportValidationWrite{
			ID: "manual-validation", QueueItemID: f.queueA.ID,
			FileRevisionID: f.revision.ID, RunID: f.run.ID,
			Phase: ImportValidationPhaseInitialPass, DamagePolicy: ImportDamagePolicyStrict,
			CreatedAt: f.now, UpdatedAt: f.now,
			LeaseOwner: *lease.LeaseOwner, FencingToken: lease.FencingToken,
		})
		require.Error(t, err, "only an observation-mode import run may authorize import admission")
	})

	t.Run("direct terminal", func(t *testing.T) {
		f := newPR5AuditImportFixture(t, "import")
		lease, err := f.repo.AcquireRunLease(context.Background(), f.run.ID, "terminal-worker", time.Minute)
		require.NoError(t, err)
		_, err = f.repo.UpsertImportValidation(context.Background(), ImportValidationWrite{
			ID: "direct-accepted-validation", QueueItemID: f.queueA.ID,
			FileRevisionID: f.revision.ID, RunID: f.run.ID,
			Phase: ImportValidationPhaseAccepted, DamagePolicy: ImportDamagePolicyStrict,
			CreatedAt: f.now, UpdatedAt: f.now,
			LeaseOwner: *lease.LeaseOwner, FencingToken: lease.FencingToken,
		})
		require.Error(t, err,
			"a terminal phase cannot be created before an authoritative pass-complete transition")
	})

	t.Run("one run one validation", func(t *testing.T) {
		f := newPR5AuditImportFixture(t, "import")
		lease, err := f.repo.AcquireRunLease(context.Background(), f.run.ID, "unique-run-worker", time.Minute)
		require.NoError(t, err)
		first := ImportValidationWrite{
			ID: "unique-run-first", QueueItemID: f.queueA.ID,
			FileRevisionID: f.revision.ID, RunID: f.run.ID,
			Phase: ImportValidationPhaseInitialPass, DamagePolicy: ImportDamagePolicyStrict,
			CreatedAt: f.now, UpdatedAt: f.now,
			LeaseOwner: *lease.LeaseOwner, FencingToken: lease.FencingToken,
		}
		_, err = f.repo.UpsertImportValidation(context.Background(), first)
		require.NoError(t, err)
		second := first
		second.ID = "unique-run-second"
		second.QueueItemID = f.queueB.ID
		_, err = f.repo.UpsertImportValidation(context.Background(), second)
		require.Error(t, err, "one authoritative import run cannot be rebound to another queue item")
	})
}

func TestPR5ImportConfirmationPassIsFencedAndRestartDiscoverable(t *testing.T) {
	f := newPR5AuditImportFixture(t, "import")
	ctx := context.Background()
	due := f.now.Add(30 * time.Second)
	run, _, err := f.repo.EnsureScheduledHealthRun(ctx, ScheduledHealthRunSpec{
		Run: HealthRunSpec{
			ID: "scheduled-audit-import", FileRevisionID: f.revision.ID,
			ProviderSnapshotID: f.snapshot.ID, Trigger: "import", Mode: "observation",
			TotalSegments: f.revision.SegmentCount, CreatedAt: f.now,
		},
		DedupeKey: "audit-import-confirmation", Priority: HealthRunPriorityHigh,
		NotBefore: f.now,
	})
	require.NoError(t, err)
	initialLease, err := f.repo.ClaimDueHealthRun(ctx, "initial-import-worker", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, initialLease)
	require.Equal(t, run.ID, initialLease.ID)
	initial := ImportValidationWrite{
		ID: "restartable-import-validation", QueueItemID: f.queueA.ID,
		FileRevisionID: f.revision.ID, RunID: run.ID,
		Phase: ImportValidationPhaseInitialPass, DamagePolicy: ImportDamagePolicyStrict,
		UnresolvedSegments: 1, UnresolvedBitmap: []byte{0b00000010},
		CreatedAt: f.now, UpdatedAt: f.now,
		LeaseOwner: *initialLease.LeaseOwner, FencingToken: initialLease.FencingToken,
	}
	_, err = f.repo.UpsertImportValidation(ctx, initial)
	require.NoError(t, err)
	waiting := initial
	waiting.Phase = ImportValidationPhaseConfirmationWait
	waiting.ConfirmationDueAt = &due
	waiting.InitialPassComplete = true
	waiting.UpdatedAt = f.now.Add(time.Second)
	f.clock.now = waiting.UpdatedAt
	_, err = f.repo.UpsertImportValidation(ctx, waiting)
	require.Error(t, err,
		"the import lifecycle cannot trust a caller completion flag without full durable initial STAT coverage")

	provider := f.snapshot.Entries[0]
	initialCoverage := HealthChunkCommit{
		ChunkID: "audit-import-initial-coverage", RunID: run.ID,
		LeaseOwner: *initialLease.LeaseOwner, FencingToken: initialLease.FencingToken,
		ProviderID: provider.ProviderID, ProviderGeneration: provider.ProviderGeneration,
		ProviderActivationEpoch: provider.ProviderActivationEpoch,
		Stage:                   HealthRunStageImportInitialSTAT, ObservationKind: HealthObservationSTAT,
		SegmentStart: 0, SegmentCount: 2,
		TestedBitmap: []byte{0b00000011}, PresentBitmap: []byte{0b00000001},
		AbsentBitmap: []byte{0b00000010}, CorruptBitmap: []byte{0},
		TemporaryBitmap: []byte{0}, InconclusiveBitmap: []byte{0},
		ResolvedBitmap: []byte{0b00000011}, CursorSegment: 2,
		ResolvedDelta: 2, ProviderChecksDelta: 2, MissingCandidatesDelta: 1,
		CommittedAt: f.clock.now,
	}
	_, err = f.repo.CommitHealthChunk(ctx, initialCoverage)
	require.NoError(t, err)
	_, err = f.repo.UpsertImportValidation(ctx, waiting)
	require.NoError(t, err)
	f.clock.now = f.now.Add(2 * time.Second)
	require.NoError(t, f.repo.ParkHealthRun(
		ctx, run.ID, "initial-import-worker", initialLease.FencingToken, due, f.now.Add(2*time.Second)))

	f.clock.now = due
	confirmationLease, err := f.repo.ClaimDueHealthRun(ctx, "confirmation-worker", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, confirmationLease)
	confirmation := waiting
	confirmation.Phase = ImportValidationPhaseConfirmationPass
	confirmation.ConfirmationDueAt = nil
	confirmation.UpdatedAt = due
	confirmation.LeaseOwner = *confirmationLease.LeaseOwner
	confirmation.FencingToken = confirmationLease.FencingToken
	_, err = f.repo.UpsertImportValidation(ctx, confirmation)
	require.NoError(t, err)
	prematureTerminal := confirmation
	prematureTerminal.Phase = ImportValidationPhaseRejected
	prematureTerminal.SecondPassComplete = true
	prematureTerminal.UpdatedAt = due.Add(time.Second)
	f.clock.now = prematureTerminal.UpdatedAt
	_, err = f.repo.UpsertImportValidation(ctx, prematureTerminal)
	require.Error(t, err,
		"a terminal decision requires complete targeted confirmation STAT coverage for every unresolved provider tuple")

	f.clock.now = *confirmationLease.LeaseExpiresAt
	freshLease, err := f.repo.ClaimDueHealthRun(ctx, "restart-worker", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, freshLease)
	restarted := NewHealthStateRepository(f.db.Connection(), DialectSQLite)
	restarted.now = f.clock.Now
	dueAfterRestart, err := restarted.ListDueImportValidations(ctx, f.clock.now, 10)
	require.NoError(t, err)
	require.Len(t, dueAfterRestart, 1,
		"an interrupted confirmation pass remains discoverable instead of being stranded")
	assert.Equal(t, confirmation.ID, dueAfterRestart[0].ID)

	staleTerminal := confirmation
	staleTerminal.Phase = ImportValidationPhaseRejected
	staleTerminal.SecondPassComplete = true
	staleTerminal.UpdatedAt = f.clock.now
	_, err = restarted.UpsertImportValidation(ctx, staleTerminal)
	require.ErrorIs(t, err, ErrStaleHealthLease,
		"the expired confirmation worker cannot publish a terminal import decision")

	confirmationCoverage := HealthChunkCommit{
		ChunkID: "audit-import-confirmation-coverage", RunID: run.ID,
		LeaseOwner: *freshLease.LeaseOwner, FencingToken: freshLease.FencingToken,
		ProviderID: provider.ProviderID, ProviderGeneration: provider.ProviderGeneration,
		ProviderActivationEpoch: provider.ProviderActivationEpoch,
		Stage:                   HealthRunStageImportConfirmationSTAT, ObservationKind: HealthObservationSTAT,
		SegmentStart: 1, SegmentCount: 1,
		TestedBitmap: []byte{1}, PresentBitmap: []byte{0}, AbsentBitmap: []byte{1},
		CorruptBitmap: []byte{0}, TemporaryBitmap: []byte{0}, InconclusiveBitmap: []byte{0},
		ResolvedBitmap: []byte{1}, CursorSegment: 2,
		ResolvedDelta: 1, ProviderChecksDelta: 1, MissingCandidatesDelta: 1,
		CommittedAt: f.clock.now,
	}
	_, err = restarted.CommitHealthChunk(ctx, confirmationCoverage)
	require.NoError(t, err)

	terminal := confirmation
	terminal.Phase = ImportValidationPhaseRejected
	terminal.SecondPassComplete = true
	terminal.LeaseOwner = *freshLease.LeaseOwner
	terminal.FencingToken = freshLease.FencingToken
	terminal.UpdatedAt = f.clock.now.Add(time.Second)
	f.clock.now = terminal.UpdatedAt
	completedValidation, err := restarted.UpsertImportValidation(ctx, terminal)
	require.NoError(t, err)
	assert.Equal(t, ImportValidationPhaseRejected, completedValidation.Phase)
	completedRun, err := restarted.GetHealthRun(ctx, run.ID)
	require.NoError(t, err)
	require.NotNil(t, completedRun)
	assert.Equal(t, HealthRunCompleted, completedRun.Status,
		"the fenced terminal import transition and run completion must commit atomically")
	assert.Nil(t, completedRun.LeaseOwner)
}

func TestPR5GapRejectsBodyPresencePredatingItsEpisode(t *testing.T) {
	f := newPR4RunFixture(t)
	ctx := context.Background()
	lease, err := f.repo.AcquireRunLease(ctx, f.run.ID, "predating-body-worker", 10*time.Minute)
	require.NoError(t, err)
	commit := pr5AuditPresentCommit(
		f, lease, "predating-body", "validated_body", 2, HealthObservationValidatedBody, f.now)
	_, err = f.repo.CommitHealthChunk(ctx, commit)
	require.NoError(t, err)
	f.clock.now = f.now.Add(time.Minute)
	gap, err := f.repo.UpsertGapRange(ctx, GapRangeWrite{
		ID: "newer-gap-episode", FileRevisionID: f.run.FileRevisionID,
		Kind: GapKindProvisional, StartSegment: 2, SegmentCount: 1,
		Status: GapStatusActive, CreatedAt: f.now.Add(time.Minute),
	})
	require.NoError(t, err)
	f.clock.now = f.now.Add(2 * time.Minute)
	_, err = f.repo.ClearGapRangeFromChunk(
		ctx, gap.ID, commit.ChunkID, f.now.Add(2*time.Minute))
	require.Error(t, err, "historical BODY presence cannot clear a later absence episode")
}

func TestPR5GapPartialRecoverySplitsThenClearsAcrossChunks(t *testing.T) {
	f := newPR4RunFixture(t)
	ctx := context.Background()
	gap, err := f.repo.UpsertGapRange(ctx, GapRangeWrite{
		ID: "multi-chunk-gap", FileRevisionID: f.run.FileRevisionID,
		Kind: GapKindProvisional, StartSegment: 2, SegmentCount: 2,
		Status: GapStatusActive, CreatedAt: f.now,
	})
	require.NoError(t, err)
	lease, err := f.repo.AcquireRunLease(ctx, f.run.ID, "partial-recovery-worker", 10*time.Minute)
	require.NoError(t, err)
	first := pr5AuditPresentCommit(
		f, lease, "partial-recovery-first", "validated_body_first", 2,
		HealthObservationValidatedBody, f.now.Add(time.Minute))
	f.clock.now = first.CommittedAt
	_, err = f.repo.CommitHealthChunk(ctx, first)
	require.NoError(t, err)
	_, err = f.repo.ClearGapRangeFromChunk(ctx, gap.ID, first.ChunkID, f.now.Add(time.Minute))
	require.NoError(t, err,
		"validated recovery of one position must invalidate that position without waiting for the whole range")

	var remainingID string
	var remainingStart, remainingCount int64
	err = f.db.Connection().QueryRowContext(ctx, `
		SELECT id, start_segment, segment_count
		FROM health_gap_ranges
		WHERE file_revision_id = ? AND status = 'active'
		ORDER BY start_segment, id
	`, f.run.FileRevisionID).Scan(&remainingID, &remainingStart, &remainingCount)
	require.NoError(t, err)
	assert.Equal(t, int64(3), remainingStart)
	assert.Equal(t, int64(1), remainingCount)

	second := pr5AuditPresentCommit(
		f, lease, "partial-recovery-second", "validated_body_second", 3,
		HealthObservationValidatedBody, f.now.Add(2*time.Minute))
	f.clock.now = second.CommittedAt
	_, err = f.repo.CommitHealthChunk(ctx, second)
	require.NoError(t, err)
	_, err = f.repo.ClearGapRangeFromChunk(ctx, remainingID, second.ChunkID, f.now.Add(2*time.Minute))
	require.NoError(t, err)
	var active int
	require.NoError(t, f.db.Connection().QueryRowContext(ctx, `
		SELECT COUNT(*) FROM health_gap_ranges
		WHERE file_revision_id = ? AND status = 'active'
	`, f.run.FileRevisionID).Scan(&active))
	assert.Zero(t, active)
}

func TestPR5ActivationEpochSurvivesResumeAndKeysGapCauses(t *testing.T) {
	f := newPR4RunFixture(t)
	ctx := context.Background()
	lease, err := f.repo.AcquireRunLease(ctx, f.run.ID, "activation-one-worker", 10*time.Minute)
	require.NoError(t, err)
	epochOne := pr5AuditAbsentCommit(
		f, f.run, lease, "activation-one-absence", "activation_one", 1, 1, f.now)
	_, err = f.repo.CommitHealthChunk(ctx, epochOne)
	require.NoError(t, err)
	resume, err := f.repo.GetHealthRunResumeState(ctx, f.run.ID)
	require.NoError(t, err)
	require.Len(t, resume.Chunks, 1)
	require.Len(t, resume.Coverage, 1)
	assert.Equal(t, int64(1), resume.Chunks[0].ProviderActivationEpoch)
	assert.Equal(t, int64(1), resume.Coverage[0].ProviderActivationEpoch)
	require.NoError(t, f.repo.CompleteHealthRun(
		ctx, f.run.ID, "activation-one-worker", lease.FencingToken, f.now,
	))

	gapWrite := GapRangeWrite{
		ID: "activation-keyed-gap", FileRevisionID: f.run.FileRevisionID,
		Kind: GapKindProvisional, StartSegment: 1, SegmentCount: 1,
		Status: GapStatusActive, CreatedAt: f.now,
		Causes: []GapProviderCause{{
			ProviderID: f.providerID, ProviderGeneration: 1, ProviderActivationEpoch: 1,
			Cause: GapCauseAbsent, ConfirmationCount: 1, ConfirmedAt: f.now,
		}},
	}
	_, err = f.repo.UpsertGapRange(ctx, gapWrite)
	require.NoError(t, err)

	f.clock.now = f.now.Add(2 * time.Minute)
	_, err = f.repo.ReconcileProviders(ctx, nil)
	require.NoError(t, err)
	providers, err := f.repo.ReconcileProviders(ctx, []ProviderSpec{{
		StableID: f.providerID, DisplayName: "A", Endpoint: "provider-a.invalid",
		Port: 119, Account: "a", Role: ProviderRolePrimary, Order: 0,
	}})
	require.NoError(t, err)
	require.Equal(t, int64(2), providers[0].ActivationEpoch)
	snapshot, err := f.repo.CaptureActiveProviderSnapshot(ctx, f.clock.now)
	require.NoError(t, err)
	epochTwoRun, err := f.repo.CreateHealthRun(ctx, HealthRunSpec{
		ID: "activation-two-run", FileRevisionID: f.run.FileRevisionID,
		ProviderSnapshotID: snapshot.ID, Trigger: "provider_activation", Mode: "observation",
		TotalSegments: f.run.TotalSegments, CreatedAt: f.clock.now,
	})
	require.NoError(t, err)
	epochTwoLease, err := f.repo.AcquireRunLease(ctx, epochTwoRun.ID, "activation-two-worker", 10*time.Minute)
	require.NoError(t, err)
	epochTwo := pr5AuditAbsentCommit(
		f, epochTwoRun, epochTwoLease, "activation-two-absence", "activation_two", 1, 2, f.clock.now)
	_, err = f.repo.CommitHealthChunk(ctx, epochTwo)
	require.NoError(t, err)
	gapWrite.Causes = []GapProviderCause{{
		ProviderID: f.providerID, ProviderGeneration: 1, ProviderActivationEpoch: 2,
		Cause: GapCauseAbsent, ConfirmationCount: 1, ConfirmedAt: f.clock.now,
	}}
	gap, err := f.repo.UpsertGapRange(ctx, gapWrite)
	require.NoError(t, err)
	require.Len(t, gap.Causes, 2,
		"same-generation reactivation is a distinct evidence key and retains prior history")
	assert.ElementsMatch(t, []int64{1, 2}, []int64{
		gap.Causes[0].ProviderActivationEpoch, gap.Causes[1].ProviderActivationEpoch,
	})
}

func TestPR5GenericGapUpsertCannotClearOrForgeConfirmations(t *testing.T) {
	f := newPR4RunFixture(t)
	ctx := context.Background()
	active := GapRangeWrite{
		ID: "generic-clear-gap", FileRevisionID: f.run.FileRevisionID,
		Kind: GapKindProvisional, StartSegment: 0, SegmentCount: 1,
		Status: GapStatusActive, CreatedAt: f.now,
	}
	_, err := f.repo.UpsertGapRange(ctx, active)
	require.NoError(t, err)
	bypass := active
	bypass.Status = GapStatusCleared
	clearedAt := f.now.Add(time.Minute)
	bypass.ClearedAt = &clearedAt
	_, err = f.repo.UpsertGapRange(ctx, bypass)
	require.Error(t, err, "generic persistence cannot bypass validated-BODY gap recovery")

	_, err = f.repo.UpsertGapRange(ctx, GapRangeWrite{
		ID: "forged-confirmation-gap", FileRevisionID: f.run.FileRevisionID,
		Kind: GapKindConfirmedAbsent, StartSegment: 4, SegmentCount: 1,
		Status: GapStatusActive, CreatedAt: f.now,
		Causes: []GapProviderCause{{
			ProviderID: f.providerID, ProviderGeneration: 1, ProviderActivationEpoch: 1,
			Cause: GapCauseAbsent, ConfirmationCount: 2, ConfirmedAt: f.now,
		}},
	})
	require.Error(t, err,
		"a caller-supplied count cannot manufacture time-separated confirmation evidence")

	_, err = f.repo.UpsertGapRange(ctx, GapRangeWrite{
		ID: "forged-legacy-epoch-gap", FileRevisionID: f.run.FileRevisionID,
		Kind: GapKindProvisional, StartSegment: 5, SegmentCount: 1,
		Status: GapStatusActive, CreatedAt: f.now,
		Causes: []GapProviderCause{{
			ProviderID: f.providerID, ProviderGeneration: 1,
			ProviderActivationEpoch: 0, Cause: GapCauseAbsent,
			ConfirmationCount: 2, ConfirmedAt: f.now,
		}},
	})
	require.Error(t, err,
		"an epoch-zero compatibility value cannot bypass activation-scoped durable evidence")

	_, err = f.repo.UpsertGapRange(ctx, GapRangeWrite{
		ID: "forged-confirmed-kind-gap", FileRevisionID: f.run.FileRevisionID,
		Kind: GapKindConfirmedAbsent, StartSegment: 6, SegmentCount: 1,
		Status: GapStatusActive, CreatedAt: f.now,
	})
	require.Error(t, err,
		"a confirmed gap kind requires evidence-derived time-separated causes for every active provider")
}

func TestPR5GapConfirmationCountUsesTimeSeparatedDurableEvents(t *testing.T) {
	f := newPR4RunFixture(t)
	ctx := context.Background()
	gapWrite := GapRangeWrite{
		ID: "time-separated-gap", FileRevisionID: f.run.FileRevisionID,
		Kind: GapKindProvisional, StartSegment: 1, SegmentCount: 1,
		Status: GapStatusActive, CreatedAt: f.now,
	}
	_, err := f.repo.UpsertGapRange(ctx, gapWrite)
	require.NoError(t, err)
	commitAt := func(id, stage string, at time.Time) {
		t.Helper()
		f.clock.now = at
		run, runErr := f.repo.CreateHealthRun(ctx, HealthRunSpec{
			ID: id + "-run", FileRevisionID: f.run.FileRevisionID,
			ProviderSnapshotID: f.run.ProviderSnapshotID, Trigger: "confirmation",
			Mode: "observation", TotalSegments: f.run.TotalSegments, CreatedAt: at,
		})
		require.NoError(t, runErr)
		lease, leaseErr := f.repo.AcquireRunLease(ctx, run.ID, id+"-worker", 30*time.Minute)
		require.NoError(t, leaseErr)
		commit := pr5AuditAbsentCommit(f, run, lease, id, stage, 1, 1, at)
		_, commitErr := f.repo.CommitHealthChunk(ctx, commit)
		require.NoError(t, commitErr)
		require.NoError(t, f.repo.CompleteHealthRun(
			ctx, run.ID, *lease.LeaseOwner, lease.FencingToken, at,
		))
	}
	firstAt := f.now.Add(time.Minute)
	commitAt("time-separated-first", "time_separated_first", firstAt)
	commitAt("time-separated-too-soon", "time_separated_too_soon", firstAt.Add(9*time.Minute))

	gapWrite.Causes = []GapProviderCause{{
		ProviderID: f.providerID, ProviderGeneration: 1, ProviderActivationEpoch: 1,
		Cause: GapCauseAbsent,
	}}
	gap, err := f.repo.UpsertGapRange(ctx, gapWrite)
	require.NoError(t, err)
	require.Len(t, gap.Causes, 1)
	assert.Equal(t, 1, gap.Causes[0].ConfirmationCount,
		"evidence inside the ten-minute minimum cannot increment persistent confirmation")
	assert.Equal(t, firstAt, gap.Causes[0].ConfirmedAt)

	thirdAt := firstAt.Add(10 * time.Minute)
	commitAt("time-separated-boundary", "time_separated_boundary", thirdAt)
	gap, err = f.repo.UpsertGapRange(ctx, gapWrite)
	require.NoError(t, err)
	require.Len(t, gap.Causes, 1)
	assert.Equal(t, 2, gap.Causes[0].ConfirmationCount)
	assert.Equal(t, thirdAt, gap.Causes[0].ConfirmedAt,
		"the exact ten-minute boundary supplies the second independent confirmation")
}

func TestPR5ClearedGapScheduleIsRetiredBeforeClaim(t *testing.T) {
	f := newPR5ScheduleFixture(t)
	ctx := context.Background()
	due := f.now.Add(time.Minute)
	run, _, err := f.repo.EnsureScheduledHealthRun(ctx,
		f.scheduleSpec("stale-gap-schedule", "stale-gap-schedule-key", HealthRunPriorityHigh, due))
	require.NoError(t, err)
	clearRun, err := f.repo.CreateHealthRun(ctx, HealthRunSpec{
		ID: "gap-schedule-clear-run", FileRevisionID: f.revision.ID,
		ProviderSnapshotID: f.snapshot.ID, Trigger: "manual", Mode: "observation",
		TotalSegments: f.total, CreatedAt: f.now,
	})
	require.NoError(t, err)
	lease, err := f.repo.AcquireRunLease(ctx, clearRun.ID, "gap-schedule-clear-worker", time.Minute)
	require.NoError(t, err)
	compat := pr4Fixture{
		repo: f.repo, db: f.db, run: clearRun, providerID: f.provider.ID, now: f.now, clock: f.clock,
	}
	body := pr5AuditPresentCommit(
		compat, lease, "gap-schedule-clear-body", "validated_body", f.gap.StartSegment,
		HealthObservationValidatedBody, f.now.Add(time.Second))
	f.clock.now = body.CommittedAt
	_, err = f.repo.CommitHealthChunk(ctx, body)
	require.NoError(t, err)
	f.clock.now = f.now.Add(2 * time.Second)
	_, err = f.repo.ClearGapRangeFromChunk(ctx, f.gap.ID, body.ChunkID, f.now.Add(2*time.Second))
	require.NoError(t, err)

	f.clock.now = due
	claimed, err := f.repo.ClaimDueHealthRun(ctx, "stale-gap-claim-worker", time.Minute)
	require.NoError(t, err)
	assert.Nil(t, claimed, "a cleared target must not dispatch stale scheduled work")
	schedule, err := f.repo.GetHealthRunSchedule(ctx, run.ID)
	require.NoError(t, err)
	require.NotNil(t, schedule)
	assert.False(t, schedule.Active)
	requirePR5StaleTargetRunIsDurablyTerminal(t, f, run.ID, f.now.Add(2*time.Second))
}

func TestPR5SupersededProviderActivationScheduleIsRetiredBeforeClaim(t *testing.T) {
	f := newPR5ScheduleFixture(t)
	ctx := context.Background()
	due := f.now.Add(time.Minute)
	run, _, err := f.repo.EnsureScheduledHealthRun(ctx,
		f.scheduleSpec("stale-provider-schedule", "stale-provider-schedule-key", HealthRunPriorityHigh, due))
	require.NoError(t, err)
	_, err = f.repo.ReconcileProviders(ctx, nil)
	require.NoError(t, err)
	f.clock.now = f.now.Add(30 * time.Second)
	providers, err := f.repo.ReconcileProviders(ctx, []ProviderSpec{{
		StableID: "pr5-schedule-provider", DisplayName: "Primary",
		Endpoint: "schedule.example.invalid", Port: 119, Account: "account",
		Role: ProviderRolePrimary, Order: 0,
	}})
	require.NoError(t, err)
	require.Equal(t, int64(2), providers[0].ActivationEpoch)

	f.clock.now = due
	claimed, err := f.repo.ClaimDueHealthRun(ctx, "stale-provider-claim-worker", time.Minute)
	require.NoError(t, err)
	assert.Nil(t, claimed, "an epoch-1 target cannot run against the epoch-2 provider activation")
	schedule, err := f.repo.GetHealthRunSchedule(ctx, run.ID)
	require.NoError(t, err)
	require.NotNil(t, schedule)
	assert.False(t, schedule.Active)
	requirePR5StaleTargetRunIsDurablyTerminal(t, f, run.ID, due)
}

func TestPR5FutureEvidenceAndFreeFormFailureDetailsAreSafe(t *testing.T) {
	t.Run("future evidence", func(t *testing.T) {
		f := newPR4RunFixture(t)
		lease, err := f.repo.AcquireRunLease(
			context.Background(), f.run.ID, "future-evidence-worker", 48*time.Hour)
		require.NoError(t, err)
		future := pr5AuditPresentCommit(
			f, lease, "future-evidence", "future_stat", 0,
			HealthObservationSTAT, f.now.Add(24*time.Hour))
		_, err = f.repo.CommitHealthChunk(context.Background(), future)
		require.Error(t, err, "worker-controlled future timestamps cannot establish durable freshness")
	})

	t.Run("sanitized failure", func(t *testing.T) {
		f := newPR5ScheduleFixture(t)
		ctx := context.Background()
		_, _, err := f.repo.EnsureScheduledHealthRun(ctx,
			f.scheduleSpec("sanitized-failure-run", "sanitized-failure-key", HealthRunPriorityHigh, f.now))
		require.NoError(t, err)
		lease, err := f.repo.ClaimDueHealthRun(ctx, "sanitized-failure-worker", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, lease)
		const sensitiveSentinel = "synthetic-raw-article-token-must-not-persist"
		_ = f.repo.FailHealthRun(
			ctx, lease.ID, "sanitized-failure-worker", lease.FencingToken,
			"transport failure: "+sensitiveSentinel, f.now.Add(time.Second))
		// Rejecting free-form details is acceptable; accepting them requires
		// sanitization before they become durable or API-visible.
		failed, err := f.repo.GetHealthRun(ctx, lease.ID)
		require.NoError(t, err)
		require.NotNil(t, failed)
		assert.NotContains(t, failed.LastError, sensitiveSentinel,
			"durable/API-visible failure details must retain only a typed sanitized cause")
	})
}

func TestPR5PauseBlocksAllLeaseAcquisitionPaths(t *testing.T) {
	f := newPR4RunFixture(t)
	ctx := context.Background()
	require.NoError(t, f.repo.RequestRunPause(ctx, f.run.ID, true, f.now))
	_, err := f.repo.AcquireRunLease(ctx, f.run.ID, "pause-bypass-worker", time.Minute)
	require.Error(t, err, "the direct lease API cannot bypass a durable pause request")
}
