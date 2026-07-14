package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	strictWrite := ImportValidationWrite{
		ID: "import-validation-a", QueueItemID: queueItem.ID,
		FileRevisionID: firstRevision.ID, RunID: firstRun.ID,
		Phase:             ImportValidationPhaseConfirmationWait,
		DamagePolicy:      ImportDamagePolicyStrict,
		ConfirmationDueAt: &due, UnresolvedSegments: 2,
		CreatedAt: now, UpdatedAt: now,
	}
	strictState, err := repo.UpsertImportValidation(ctx, strictWrite)
	require.NoError(t, err)
	assert.Equal(t, ImportDamagePolicyStrict, strictState.DamagePolicy)
	assert.Equal(t, ImportValidationPhaseConfirmationWait, strictState.Phase)
	require.NotNil(t, strictState.ConfirmationDueAt)
	assert.Equal(t, due, *strictState.ConfirmationDueAt)
	assert.Equal(t, firstRun.ID, strictState.RunID)

	tolerantWrite := ImportValidationWrite{
		ID: "import-validation-b", QueueItemID: queueItem.ID,
		FileRevisionID: secondRevision.ID, RunID: secondRun.ID,
		Phase:              ImportValidationPhaseHealthPending,
		DamagePolicy:       ImportDamagePolicyTolerant,
		UnresolvedSegments: 1, CreatedAt: now, UpdatedAt: now,
	}
	tolerantState, err := repo.UpsertImportValidation(ctx, tolerantWrite)
	require.NoError(t, err)
	assert.Equal(t, ImportValidationPhaseHealthPending, tolerantState.Phase)
	assert.Equal(t, ImportDamagePolicyTolerant, tolerantState.DamagePolicy,
		"degraded admission is an explicit per-import policy, not an inferred evidence class")

	var validationCount int
	require.NoError(t, db.Connection().QueryRow(`
		SELECT COUNT(*) FROM health_import_validations WHERE queue_item_id = ?
	`, queueItem.ID).Scan(&validationCount))
	assert.Equal(t, 2, validationCount,
		"one archive may produce multiple final files with independent canonical layouts")

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
	rebound.UpdatedAt = now.Add(time.Second)
	_, err = restarted.UpsertImportValidation(ctx, rebound)
	require.Error(t, err, "queue item and final revision identify one durable validation lifecycle")

	policyChange := strictWrite
	policyChange.DamagePolicy = ImportDamagePolicyTolerant
	policyChange.UpdatedAt = now.Add(time.Second)
	_, err = restarted.UpsertImportValidation(ctx, policyChange)
	require.Error(t, err, "an in-flight validation keeps the policy selected when it began")

	wrongRun := tolerantWrite
	wrongRun.ID = "wrong-revision-run-link"
	wrongRun.RunID = firstRun.ID
	wrongRun.FileRevisionID = thirdRevision.ID
	wrongRun.UpdatedAt = now.Add(time.Second)
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
	due := f.now.Add(30 * time.Second)

	strict := ImportValidationWrite{
		ID: "strict-validation", QueueItemID: queueItem.ID,
		FileRevisionID: f.run.FileRevisionID, RunID: f.run.ID,
		Phase:             ImportValidationPhaseConfirmationWait,
		DamagePolicy:      ImportDamagePolicyStrict,
		ConfirmationDueAt: &due, UnresolvedSegments: 1,
		CreatedAt: f.now, UpdatedAt: f.now,
	}
	_, err := f.repo.UpsertImportValidation(ctx, strict)
	require.NoError(t, err)

	strictPending := strict
	strictPending.Phase = ImportValidationPhaseHealthPending
	strictPending.ConfirmationDueAt = nil
	strictPending.UpdatedAt = due
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
	tolerant := ImportValidationWrite{
		ID: "tolerant-validation", QueueItemID: queueItem.ID,
		FileRevisionID: secondRevision.ID, RunID: secondRun.ID,
		Phase:              ImportValidationPhaseHealthPending,
		DamagePolicy:       ImportDamagePolicyTolerant,
		UnresolvedSegments: 1, CreatedAt: f.now, UpdatedAt: due,
	}
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
	missingDeadline := tolerant
	missingDeadline.ID = "missing-deadline"
	missingDeadline.FileRevisionID = thirdRevision.ID
	missingDeadline.RunID = thirdRun.ID
	missingDeadline.Phase = ImportValidationPhaseConfirmationWait
	missingDeadline.ConfirmationDueAt = nil
	_, err = f.repo.UpsertImportValidation(ctx, missingDeadline)
	require.Error(t, err, "a persisted confirmation wait needs an exact restart-safe deadline")
}
