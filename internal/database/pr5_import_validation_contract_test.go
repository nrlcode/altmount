package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func commitPR5ImportSTATCoverage(
	t *testing.T,
	repo *HealthStateRepository,
	run, lease *HealthRun,
	provider ProviderSnapshotEntry,
	chunkID, stage string,
	tested, present byte,
	at time.Time,
) {
	t.Helper()
	absent := tested &^ present
	_, err := repo.CommitHealthChunk(context.Background(), HealthChunkCommit{
		ChunkID: chunkID, RunID: run.ID, LeaseOwner: *lease.LeaseOwner,
		FencingToken: lease.FencingToken, ProviderID: provider.ProviderID,
		ProviderGeneration:      provider.ProviderGeneration,
		ProviderActivationEpoch: provider.ProviderActivationEpoch,
		Stage:                   stage, ObservationKind: HealthObservationSTAT,
		SegmentStart: 0, SegmentCount: run.TotalSegments,
		TestedBitmap: []byte{tested}, PresentBitmap: []byte{present},
		AbsentBitmap: []byte{absent}, CorruptBitmap: []byte{0},
		TemporaryBitmap: []byte{0}, InconclusiveBitmap: []byte{0},
		ResolvedBitmap: []byte{tested}, CursorSegment: run.TotalSegments,
		ResolvedDelta:          bitmapPopulation([]byte{tested}),
		ProviderChecksDelta:    bitmapPopulation([]byte{tested}),
		MissingCandidatesDelta: bitmapPopulation([]byte{absent}),
		CommittedAt:            at,
	})
	require.NoError(t, err)
}

func TestPR5ImportValidationIsDurablePerFinalFileAndFreezesDamagePolicy(t *testing.T) {
	db, repo := newPR4Repository(t)
	ctx := context.Background()
	now := time.Unix(1_712_000_000, 0).UTC()
	clock := &pr4TestClock{now: now}
	repo.now = clock.Now

	queueItem := &ImportQueueItem{
		NzbPath: "queue/synthetic-import.nzb", Priority: QueuePriorityNormal,
		Status: QueueStatusProcessing, MaxRetries: 3,
	}
	require.NoError(t, db.Repository.AddToQueue(ctx, queueItem))
	require.Positive(t, queueItem.ID)
	tolerantQueueItem := &ImportQueueItem{
		NzbPath: "queue/synthetic-tolerant-import.nzb", Priority: QueuePriorityNormal,
		Status: QueueStatusProcessing, MaxRetries: 3,
	}
	require.NoError(t, db.Repository.AddToQueue(ctx, tolerantQueueItem))
	require.Positive(t, tolerantQueueItem.ID)

	providers, err := repo.ReconcileProviders(ctx, []ProviderSpec{{
		StableID: "import-validation-provider", DisplayName: "Primary",
		Endpoint: "import-validation.example.invalid", Port: 119, Account: "account",
		Role: ProviderRolePrimary, Order: 0,
	}})
	require.NoError(t, err)
	require.Len(t, providers, 1)
	snapshot, err := repo.CaptureActiveProviderSnapshot(ctx, now)
	require.NoError(t, err)

	newFinalFile := func(id, path, fingerprint string) (*HealthFileRevision, *HealthRun) {
		revision, revisionErr := repo.EnsureFileRevision(ctx, FileRevisionSpec{
			FilePath: path, LayoutFingerprint: fingerprint, VirtualSize: 800, SegmentCount: 8,
		})
		require.NoError(t, revisionErr)
		run, runErr := repo.CreateHealthRun(ctx, HealthRunSpec{
			ID: id, FileRevisionID: revision.ID, ProviderSnapshotID: snapshot.ID,
			Trigger: "import", Mode: "observation", TotalSegments: 8, CreatedAt: now,
		})
		require.NoError(t, runErr)
		return revision, run
	}
	firstRevision, firstRun := newFinalFile(
		"import-run-a", "library/import-final-a.mkv", "sha256:import-final-a")
	secondRevision, secondRun := newFinalFile(
		"import-run-b", "library/import-final-b.mkv", "sha256:import-final-b")
	thirdRevision, _ := newFinalFile(
		"import-run-c", "library/import-final-c.mkv", "sha256:import-final-c")

	due := now.Add(30 * time.Second)
	firstLease, err := repo.AcquireRunLease(ctx, firstRun.ID, "strict-durability-worker", 5*time.Minute)
	require.NoError(t, err)
	strictInitial := ImportValidationWrite{
		ID: "import-validation-a", QueueItemID: queueItem.ID,
		FileRevisionID: firstRevision.ID, RunID: firstRun.ID,
		Phase: ImportValidationPhaseInitialPass, DamagePolicy: ImportDamagePolicyStrict,
		CreatedAt: now, UpdatedAt: now,
		LeaseOwner: *firstLease.LeaseOwner, FencingToken: firstLease.FencingToken,
	}
	_, err = repo.UpsertImportValidation(ctx, strictInitial)
	require.NoError(t, err)
	commitPR5ImportSTATCoverage(
		t, repo, firstRun, firstLease, snapshot.Entries[0], "strict-initial-coverage",
		HealthRunStageImportInitialSTAT, 0xff, 0xfc, now,
	)
	strictWrite := strictInitial
	strictWrite.Phase = ImportValidationPhaseConfirmationWait
	strictWrite.ConfirmationDueAt = &due
	strictWrite.UnresolvedSegments = 2
	strictWrite.UnresolvedBitmap = []byte{0x03}
	strictWrite.InitialPassComplete = true
	strictWrite.UpdatedAt = now.Add(time.Second)
	clock.now = strictWrite.UpdatedAt
	strictState, err := repo.UpsertImportValidation(ctx, strictWrite)
	require.NoError(t, err)
	assert.Equal(t, ImportDamagePolicyStrict, strictState.DamagePolicy)
	assert.Equal(t, ImportValidationPhaseConfirmationWait, strictState.Phase)
	require.NotNil(t, strictState.ConfirmationDueAt)
	assert.Equal(t, due, *strictState.ConfirmationDueAt)
	assert.Equal(t, firstRun.ID, strictState.RunID)

	secondLease, err := repo.AcquireRunLease(ctx, secondRun.ID, "tolerant-durability-worker", 5*time.Minute)
	require.NoError(t, err)
	tolerantInitial := ImportValidationWrite{
		ID: "import-validation-b", QueueItemID: tolerantQueueItem.ID,
		FileRevisionID: secondRevision.ID, RunID: secondRun.ID,
		Phase: ImportValidationPhaseInitialPass, DamagePolicy: ImportDamagePolicyTolerant,
		CreatedAt: now, UpdatedAt: now,
		LeaseOwner: *secondLease.LeaseOwner, FencingToken: secondLease.FencingToken,
	}
	_, err = repo.UpsertImportValidation(ctx, tolerantInitial)
	require.NoError(t, err)
	commitPR5ImportSTATCoverage(
		t, repo, secondRun, secondLease, snapshot.Entries[0], "tolerant-initial-coverage",
		HealthRunStageImportInitialSTAT, 0xff, 0xfe, clock.now,
	)
	tolerantWaiting := tolerantInitial
	tolerantWaiting.Phase = ImportValidationPhaseConfirmationWait
	tolerantWaiting.ConfirmationDueAt = &due
	tolerantWaiting.UnresolvedSegments = 1
	tolerantWaiting.UnresolvedBitmap = []byte{0x01}
	tolerantWaiting.InitialPassComplete = true
	tolerantWaiting.UpdatedAt = now.Add(2 * time.Second)
	clock.now = tolerantWaiting.UpdatedAt
	_, err = repo.UpsertImportValidation(ctx, tolerantWaiting)
	require.NoError(t, err)
	clock.now = due
	tolerantConfirmation := tolerantWaiting
	tolerantConfirmation.Phase = ImportValidationPhaseConfirmationPass
	tolerantConfirmation.ConfirmationDueAt = nil
	tolerantConfirmation.UpdatedAt = due
	_, err = repo.UpsertImportValidation(ctx, tolerantConfirmation)
	require.NoError(t, err)
	commitPR5ImportSTATCoverage(
		t, repo, secondRun, secondLease, snapshot.Entries[0], "tolerant-confirmation-coverage",
		HealthRunStageImportConfirmationSTAT, 0x01, 0x00, due,
	)
	tolerantWrite := tolerantConfirmation
	tolerantWrite.Phase = ImportValidationPhaseHealthPending
	tolerantWrite.SecondPassComplete = true
	tolerantWrite.UpdatedAt = due.Add(time.Second)
	clock.now = tolerantWrite.UpdatedAt
	tolerantState, err := repo.UpsertImportValidation(ctx, tolerantWrite)
	require.NoError(t, err)
	assert.Equal(t, ImportValidationPhaseHealthPending, tolerantState.Phase)
	assert.Equal(t, ImportDamagePolicyTolerant, tolerantState.DamagePolicy,
		"degraded admission is an explicit per-import policy, not an inferred evidence class")

	var validationCount int
	require.NoError(t, db.Connection().QueryRow(`
		SELECT COUNT(*) FROM health_import_validations WHERE queue_item_id = ?
	`, queueItem.ID).Scan(&validationCount))
	assert.Equal(t, 1, validationCount)
	require.NoError(t, db.Connection().QueryRow(`
		SELECT COUNT(*) FROM health_import_validations WHERE queue_item_id = ?
	`, tolerantQueueItem.ID).Scan(&validationCount))
	assert.Equal(t, 1, validationCount,
		"each import freezes one damage policy while each final file keeps independent durable state")

	restarted := NewHealthStateRepository(db.Connection(), DialectSQLite)
	restarted.now = clock.Now
	restored, err := restarted.GetImportValidation(ctx, queueItem.ID, firstRevision.ID)
	require.NoError(t, err)
	require.NotNil(t, restored)
	assert.Equal(t, strictWrite.ID, restored.ID)
	assert.Equal(t, firstRun.ID, restored.RunID)
	assert.Equal(t, ImportDamagePolicyStrict, restored.DamagePolicy)
	require.NotNil(t, restored.ConfirmationDueAt)
	assert.Equal(t, due, *restored.ConfirmationDueAt,
		"restart resumes the due confirmation pass instead of restarting initial coverage")

	rebound := strictWrite
	rebound.ID = "different-validation-same-final-file"
	rebound.UpdatedAt = due.Add(2 * time.Second)
	clock.now = rebound.UpdatedAt
	_, err = restarted.UpsertImportValidation(ctx, rebound)
	require.Error(t, err, "queue item and final revision identify one durable validation lifecycle")

	policyChange := strictWrite
	policyChange.DamagePolicy = ImportDamagePolicyTolerant
	policyChange.UpdatedAt = due.Add(2 * time.Second)
	_, err = restarted.UpsertImportValidation(ctx, policyChange)
	require.Error(t, err, "an in-flight validation keeps the policy selected when it began")

	wrongRun := tolerantWrite
	wrongRun.ID = "wrong-revision-run-link"
	wrongRun.RunID = firstRun.ID
	wrongRun.FileRevisionID = thirdRevision.ID
	wrongRun.LeaseOwner = *firstLease.LeaseOwner
	wrongRun.FencingToken = firstLease.FencingToken
	wrongRun.UpdatedAt = due.Add(2 * time.Second)
	_, err = restarted.UpsertImportValidation(ctx, wrongRun)
	require.Error(t, err, "raw or unrelated run coverage cannot be attached to another final layout")
}

func TestPR5ImportValidationEnforcesStrictRejectVersusTolerantHealthPending(t *testing.T) {
	f := newPR4RunFixture(t)
	ctx := context.Background()
	queueItem := &ImportQueueItem{
		NzbPath: "queue/policy-import.nzb", Priority: QueuePriorityNormal,
		Status: QueueStatusProcessing, MaxRetries: 3,
	}
	require.NoError(t, f.db.Repository.AddToQueue(ctx, queueItem))
	tolerantQueueItem := &ImportQueueItem{
		NzbPath: "queue/policy-tolerant-import.nzb", Priority: QueuePriorityNormal,
		Status: QueueStatusProcessing, MaxRetries: 3,
	}
	require.NoError(t, f.db.Repository.AddToQueue(ctx, tolerantQueueItem))
	due := f.now.Add(30 * time.Second)
	strictRun, err := f.repo.CreateHealthRun(ctx, HealthRunSpec{
		ID: "strict-import-run", FileRevisionID: f.run.FileRevisionID,
		ProviderSnapshotID: f.run.ProviderSnapshotID, Trigger: "import",
		Mode: "observation", TotalSegments: 8, CreatedAt: f.now,
	})
	require.NoError(t, err)
	strictLease, err := f.repo.AcquireRunLease(ctx, strictRun.ID, "strict-policy-worker", 5*time.Minute)
	require.NoError(t, err)
	snapshot, err := f.repo.GetProviderSnapshot(ctx, strictRun.ProviderSnapshotID)
	require.NoError(t, err)
	require.Len(t, snapshot.Entries, 1)

	strictInitial := ImportValidationWrite{
		ID: "strict-validation", QueueItemID: queueItem.ID,
		FileRevisionID: f.run.FileRevisionID, RunID: strictRun.ID,
		Phase: ImportValidationPhaseInitialPass, DamagePolicy: ImportDamagePolicyStrict,
		CreatedAt: f.now, UpdatedAt: f.now,
		LeaseOwner: *strictLease.LeaseOwner, FencingToken: strictLease.FencingToken,
	}
	_, err = f.repo.UpsertImportValidation(ctx, strictInitial)
	require.NoError(t, err)
	commitPR5ImportSTATCoverage(
		t, f.repo, strictRun, strictLease, snapshot.Entries[0], "strict-policy-initial",
		HealthRunStageImportInitialSTAT, 0xff, 0xfe, f.now,
	)
	strict := strictInitial
	strict.Phase = ImportValidationPhaseConfirmationWait
	strict.ConfirmationDueAt = &due
	strict.UnresolvedSegments = 1
	strict.UnresolvedBitmap = []byte{0x01}
	strict.InitialPassComplete = true
	strict.UpdatedAt = f.now.Add(time.Second)
	f.clock.now = strict.UpdatedAt
	_, err = f.repo.UpsertImportValidation(ctx, strict)
	require.NoError(t, err)
	f.clock.now = due
	strictConfirmation := strict
	strictConfirmation.Phase = ImportValidationPhaseConfirmationPass
	strictConfirmation.ConfirmationDueAt = nil
	strictConfirmation.UpdatedAt = due
	_, err = f.repo.UpsertImportValidation(ctx, strictConfirmation)
	require.NoError(t, err)
	commitPR5ImportSTATCoverage(
		t, f.repo, strictRun, strictLease, snapshot.Entries[0], "strict-policy-confirmation",
		HealthRunStageImportConfirmationSTAT, 0x01, 0x00, due,
	)

	strictPending := strictConfirmation
	strictPending.Phase = ImportValidationPhaseHealthPending
	strictPending.SecondPassComplete = true
	strictPending.UpdatedAt = due.Add(time.Second)
	f.clock.now = strictPending.UpdatedAt
	_, err = f.repo.UpsertImportValidation(ctx, strictPending)
	require.Error(t, err, "strict mode rejects unresolved coverage rather than admitting it degraded")

	strictRejected := strictPending
	strictRejected.Phase = ImportValidationPhaseRejected
	rejected, err := f.repo.UpsertImportValidation(ctx, strictRejected)
	require.NoError(t, err)
	assert.Equal(t, ImportValidationPhaseRejected, rejected.Phase)
	assert.Equal(t, int64(1), rejected.UnresolvedSegments)
	assert.Equal(t, ImportDamagePolicyStrict, rejected.DamagePolicy)

	secondRevision, err := f.repo.EnsureFileRevision(ctx, FileRevisionSpec{
		FilePath: "library/policy-tolerant.mkv", LayoutFingerprint: "sha256:policy-tolerant",
		VirtualSize: 800, SegmentCount: 8,
	})
	require.NoError(t, err)
	secondRun, err := f.repo.CreateHealthRun(ctx, HealthRunSpec{
		ID: "tolerant-run", FileRevisionID: secondRevision.ID,
		ProviderSnapshotID: f.run.ProviderSnapshotID, Trigger: "import",
		Mode: "observation", TotalSegments: 8, CreatedAt: f.now,
	})
	require.NoError(t, err)
	tolerantLease, err := f.repo.AcquireRunLease(ctx, secondRun.ID, "tolerant-policy-worker", 5*time.Minute)
	require.NoError(t, err)
	tolerantInitial := ImportValidationWrite{
		ID: "tolerant-validation", QueueItemID: queueItem.ID,
		FileRevisionID: secondRevision.ID, RunID: secondRun.ID,
		Phase: ImportValidationPhaseInitialPass, DamagePolicy: ImportDamagePolicyTolerant,
		CreatedAt: f.now, UpdatedAt: f.clock.now,
		LeaseOwner: *tolerantLease.LeaseOwner, FencingToken: tolerantLease.FencingToken,
	}
	_, err = f.repo.UpsertImportValidation(ctx, tolerantInitial)
	require.ErrorIs(t, err, ErrImportDamagePolicy,
		"all final files in one import must use the damage policy frozen by its first validation")
	tolerantInitial.QueueItemID = tolerantQueueItem.ID
	_, err = f.repo.UpsertImportValidation(ctx, tolerantInitial)
	require.NoError(t, err)
	commitPR5ImportSTATCoverage(
		t, f.repo, secondRun, tolerantLease, snapshot.Entries[0], "tolerant-policy-initial",
		HealthRunStageImportInitialSTAT, 0xff, 0xfe, f.clock.now,
	)
	tolerantWaiting := tolerantInitial
	tolerantWaiting.Phase = ImportValidationPhaseConfirmationWait
	tolerantDue := f.clock.now.Add(30 * time.Second)
	tolerantWaiting.ConfirmationDueAt = &tolerantDue
	tolerantWaiting.UnresolvedSegments = 1
	tolerantWaiting.UnresolvedBitmap = []byte{0x01}
	tolerantWaiting.InitialPassComplete = true
	tolerantWaiting.UpdatedAt = f.clock.now.Add(time.Second)
	f.clock.now = tolerantWaiting.UpdatedAt
	_, err = f.repo.UpsertImportValidation(ctx, tolerantWaiting)
	require.NoError(t, err)
	f.clock.now = tolerantDue
	tolerantConfirmation := tolerantWaiting
	tolerantConfirmation.Phase = ImportValidationPhaseConfirmationPass
	tolerantConfirmation.ConfirmationDueAt = nil
	tolerantConfirmation.UpdatedAt = tolerantDue
	_, err = f.repo.UpsertImportValidation(ctx, tolerantConfirmation)
	require.NoError(t, err)
	commitPR5ImportSTATCoverage(
		t, f.repo, secondRun, tolerantLease, snapshot.Entries[0], "tolerant-policy-confirmation",
		HealthRunStageImportConfirmationSTAT, 0x01, 0x00, tolerantDue,
	)
	tolerant := tolerantConfirmation
	tolerant.Phase = ImportValidationPhaseHealthPending
	tolerant.SecondPassComplete = true
	tolerant.UpdatedAt = tolerantDue.Add(time.Second)
	f.clock.now = tolerant.UpdatedAt
	pending, err := f.repo.UpsertImportValidation(ctx, tolerant)
	require.NoError(t, err)
	assert.Equal(t, ImportValidationPhaseHealthPending, pending.Phase)
	assert.Equal(t, ImportDamagePolicyTolerant, pending.DamagePolicy)

	thirdRevision, err := f.repo.EnsureFileRevision(ctx, FileRevisionSpec{
		FilePath: "library/policy-deadline.mkv", LayoutFingerprint: "sha256:policy-deadline",
		VirtualSize: 800, SegmentCount: 8,
	})
	require.NoError(t, err)
	thirdRun, err := f.repo.CreateHealthRun(ctx, HealthRunSpec{
		ID: "deadline-run", FileRevisionID: thirdRevision.ID,
		ProviderSnapshotID: f.run.ProviderSnapshotID, Trigger: "import",
		Mode: "observation", TotalSegments: 8, CreatedAt: f.now,
	})
	require.NoError(t, err)
	deadlineLease, err := f.repo.AcquireRunLease(ctx, thirdRun.ID, "deadline-policy-worker", 5*time.Minute)
	require.NoError(t, err)
	deadlineInitial := ImportValidationWrite{
		ID: "missing-deadline", QueueItemID: tolerantQueueItem.ID,
		FileRevisionID: thirdRevision.ID, RunID: thirdRun.ID,
		Phase: ImportValidationPhaseInitialPass, DamagePolicy: ImportDamagePolicyTolerant,
		CreatedAt: f.now, UpdatedAt: f.clock.now,
		LeaseOwner: *deadlineLease.LeaseOwner, FencingToken: deadlineLease.FencingToken,
	}
	_, err = f.repo.UpsertImportValidation(ctx, deadlineInitial)
	require.NoError(t, err)
	missingDeadline := tolerant
	missingDeadline.ID = "missing-deadline"
	missingDeadline.FileRevisionID = thirdRevision.ID
	missingDeadline.RunID = thirdRun.ID
	missingDeadline.Phase = ImportValidationPhaseConfirmationWait
	missingDeadline.ConfirmationDueAt = nil
	missingDeadline.LeaseOwner = *deadlineLease.LeaseOwner
	missingDeadline.FencingToken = deadlineLease.FencingToken
	missingDeadline.CreatedAt = f.now
	missingDeadline.UpdatedAt = f.clock.now.Add(time.Second)
	_, err = f.repo.UpsertImportValidation(ctx, missingDeadline)
	require.Error(t, err, "a persisted confirmation wait needs an exact restart-safe deadline")
}
