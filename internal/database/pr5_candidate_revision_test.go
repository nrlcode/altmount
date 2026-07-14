package database

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func insertPR5AdmittedCandidate(
	t *testing.T,
	f pr5AuditImportFixture,
	revision *HealthFileRevision,
	queue *ImportQueueItem,
	id string,
	phase ImportValidationPhase,
	at time.Time,
) {
	t.Helper()
	run, err := f.repo.CreateHealthRun(context.Background(), HealthRunSpec{
		ID: id + "-run", FileRevisionID: revision.ID,
		ProviderSnapshotID: f.snapshot.ID, Trigger: "import", Mode: "observation",
		TotalSegments: revision.SegmentCount, CreatedAt: at.Add(-time.Second),
	})
	require.NoError(t, err)
	_, err = f.db.Connection().ExecContext(context.Background(), `
		UPDATE health_runs SET status = 'completed', completed_at = ?, updated_at = ?
		WHERE id = ?
	`, at, at, run.ID)
	require.NoError(t, err)
	unresolved := int64(0)
	bitmap := []byte{0}
	policy := ImportDamagePolicyStrict
	if phase == ImportValidationPhaseHealthPending {
		unresolved = 1
		bitmap = []byte{1}
		policy = ImportDamagePolicyTolerant
	}
	_, err = f.db.Connection().ExecContext(context.Background(), `
		INSERT INTO health_import_validations
			(id, queue_item_id, file_revision_id, run_id, phase, damage_policy,
			 confirmation_due_at, unresolved_segments, unresolved_bitmap,
			 initial_pass_complete, second_pass_complete, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, TRUE, TRUE, ?, ?)
	`, id, queue.ID, revision.ID, run.ID, phase, policy, unresolved, bitmap,
		at.Add(-time.Second), at)
	require.NoError(t, err)
}

func readPR5CandidateSchedule(
	t *testing.T,
	f pr5AuditImportFixture,
) (HealthStatus, time.Time, HealthPriority) {
	t.Helper()
	var status HealthStatus
	var scheduled time.Time
	var priority HealthPriority
	require.NoError(t, f.db.Connection().QueryRowContext(context.Background(), `
		SELECT status, scheduled_check_at, priority
		FROM file_health WHERE id = ?
	`, f.revision.FileHealthID).Scan(&status, &scheduled, &priority))
	return status, scheduled, priority
}

func TestPR5CandidateRevisionStaysInvisibleUntilAdmittedActivation(t *testing.T) {
	f := newPR5AuditImportFixture(t, "import")
	ctx := context.Background()
	candidate, err := f.repo.EnsureCandidateFileRevision(ctx, FileRevisionSpec{
		FilePath: "library/audit-import.mkv", LayoutFingerprint: "sha256:candidate-layout",
		VirtualSize: 300, SegmentCount: 2,
	})
	require.NoError(t, err)
	assert.False(t, candidate.Active)

	revisions, err := f.repo.ListFileRevisions(ctx, "library/audit-import.mkv")
	require.NoError(t, err)
	require.Len(t, revisions, 2)
	activeByID := make(map[string]bool, len(revisions))
	for _, revision := range revisions {
		activeByID[revision.ID] = revision.Active
	}
	assert.True(t, activeByID[f.revision.ID])
	assert.False(t, activeByID[candidate.ID])
	same, err := f.repo.EnsureCandidateFileRevision(ctx, FileRevisionSpec{
		FilePath: "library/audit-import.mkv", LayoutFingerprint: "sha256:candidate-layout",
		VirtualSize: 300, SegmentCount: 2,
	})
	require.NoError(t, err)
	assert.Equal(t, candidate.ID, same.ID)
	assert.False(t, same.Active)

	_, err = f.repo.ActivateFileRevision(ctx, candidate.ID)
	require.ErrorIs(t, err, ErrFileRevisionNotAdmitted)
	revisions, err = f.repo.ListFileRevisions(ctx, "library/audit-import.mkv")
	require.NoError(t, err)
	activeByID = make(map[string]bool, len(revisions))
	for _, revision := range revisions {
		activeByID[revision.ID] = revision.Active
	}
	assert.True(t, activeByID[f.revision.ID])
	assert.False(t, activeByID[candidate.ID])
}

func TestPR5AcceptedCandidateActivationIsAtomicAndIdempotentlyScheduledLater(t *testing.T) {
	f := newPR5AuditImportFixture(t, "import")
	ctx := context.Background()
	candidate, err := f.repo.EnsureCandidateFileRevision(ctx, FileRevisionSpec{
		FilePath: "library/audit-import.mkv", LayoutFingerprint: "sha256:accepted-candidate",
		VirtualSize: 300, SegmentCount: 2,
	})
	require.NoError(t, err)
	admittedAt := f.now.Add(time.Minute)
	insertPR5AdmittedCandidate(t, f, candidate, f.queueA, "accepted-candidate", ImportValidationPhaseAccepted, admittedAt)
	f.clock.now = admittedAt.Add(time.Second)

	activated, err := f.repo.ActivateFileRevision(ctx, candidate.ID)
	require.NoError(t, err)
	assert.True(t, activated.Active)
	status, scheduled, priority := readPR5CandidateSchedule(t, f)
	assert.Equal(t, HealthStatusPending, status)
	assert.Equal(t, f.clock.now.Add(24*time.Hour), scheduled)
	assert.Equal(t, HealthPriorityNormal, priority)

	f.clock.now = f.clock.now.Add(time.Hour)
	again, err := f.repo.ActivateFileRevision(ctx, candidate.ID)
	require.NoError(t, err)
	assert.True(t, again.Active)
	_, unchanged, _ := readPR5CandidateSchedule(t, f)
	assert.Equal(t, scheduled, unchanged, "idempotent publication must not postpone ordinary health work")

	revisions, err := f.repo.ListFileRevisions(ctx, "library/audit-import.mkv")
	require.NoError(t, err)
	active := 0
	for _, revision := range revisions {
		if revision.Active {
			active++
			assert.Equal(t, candidate.ID, revision.ID)
		}
	}
	assert.Equal(t, 1, active)
}

func TestPR5ImportActivationIsExactQueueBoundAndFencesForeignUnresolvedOwner(t *testing.T) {
	f := newPR5AuditImportFixture(t, "import")
	ctx := context.Background()
	candidate, err := f.repo.EnsureCandidateFileRevision(ctx, FileRevisionSpec{
		FilePath: "library/audit-import.mkv", LayoutFingerprint: "sha256:shared-candidate",
		VirtualSize: 300, SegmentCount: 2,
	})
	require.NoError(t, err)
	insertPR5AdmittedCandidate(t, f, candidate, f.queueA, "shared-candidate-a",
		ImportValidationPhaseAccepted, f.now.Add(time.Minute))

	_, err = f.repo.ActivateImportFileRevision(ctx, f.queueB.ID, candidate.ID)
	require.ErrorIs(t, err, ErrFileRevisionNotAdmitted,
		"revision-only activation must not borrow another queue's admission")

	insertPR5AdmittedCandidate(t, f, candidate, f.queueB, "shared-candidate-b",
		ImportValidationPhaseAccepted, f.now.Add(2*time.Minute))
	f.clock.now = f.now.Add(3 * time.Minute)
	_, err = f.repo.ActivateImportFileRevision(ctx, f.queueA.ID, candidate.ID)
	require.NoError(t, err)
	_, err = f.repo.ActivateImportFileRevision(ctx, f.queueB.ID, candidate.ID)
	require.ErrorIs(t, err, ErrStaleRevisionActivation,
		"a second queue cannot share rollback ownership while the first is unresolved")

	require.NoError(t, f.repo.CommitImportQueueActivations(ctx, f.queueA.ID, f.clock.now))
	_, err = f.repo.ActivateImportFileRevision(ctx, f.queueB.ID, candidate.ID)
	require.NoError(t, err)
	records, err := f.repo.BeginImportQueueActivationRollback(ctx, f.queueA.ID, f.clock.now)
	require.NoError(t, err)
	assert.Empty(t, records, "a committed earlier queue cannot later undo the new owner")
	records, err = f.repo.BeginImportQueueActivationRollback(ctx, f.queueB.ID, f.clock.now)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, candidate.ID, records[0].PriorRevisionID,
		"same-layout ownership rollback keeps the already-stable shared revision active")
}

func TestPR5InactiveCandidateCleanupClaimSerializesSharedRevisionOwnership(t *testing.T) {
	t.Run("cleanup claim fences later activation", func(t *testing.T) {
		f := newPR5AuditImportFixture(t, "import")
		ctx := context.Background()
		candidate, err := f.repo.EnsureCandidateFileRevision(ctx, FileRevisionSpec{
			FilePath: "library/audit-import.mkv", LayoutFingerprint: "sha256:cleanup-claim",
			VirtualSize: 300, SegmentCount: 2,
		})
		require.NoError(t, err)
		insertPR5AdmittedCandidate(t, f, candidate, f.queueA, "cleanup-claim-a",
			ImportValidationPhaseAccepted, f.now.Add(time.Minute))
		insertPR5AdmittedCandidate(t, f, candidate, f.queueB, "cleanup-claim-b",
			ImportValidationPhaseAccepted, f.now.Add(2*time.Minute))

		claimed, err := f.repo.ClaimInactiveImportCandidateCleanup(
			ctx, f.queueA.ID, candidate.ID, f.revision.LayoutFingerprint, true,
			f.now.Add(3*time.Minute),
		)
		require.NoError(t, err)
		assert.True(t, claimed)
		_, err = f.repo.ActivateImportFileRevision(ctx, f.queueB.ID, candidate.ID)
		require.ErrorIs(t, err, ErrStaleRevisionActivation)

		records, err := f.repo.BeginImportQueueActivationRollback(
			ctx, f.queueA.ID, f.now.Add(4*time.Minute),
		)
		require.NoError(t, err)
		require.Len(t, records, 1)
		assert.Equal(t, f.revision.ID, records[0].PriorRevisionID)
		require.NoError(t, f.repo.CompleteImportQueueActivationRollback(
			ctx, f.queueA.ID, []string{candidate.ID}, f.now.Add(5*time.Minute),
		))
		_, err = f.repo.ActivateImportFileRevision(ctx, f.queueB.ID, candidate.ID)
		require.NoError(t, err)
	})

	t.Run("foreign active shared candidate is preserved", func(t *testing.T) {
		f := newPR5AuditImportFixture(t, "import")
		ctx := context.Background()
		candidate, err := f.repo.EnsureCandidateFileRevision(ctx, FileRevisionSpec{
			FilePath: "library/audit-import.mkv", LayoutFingerprint: "sha256:foreign-shared",
			VirtualSize: 300, SegmentCount: 2,
		})
		require.NoError(t, err)
		insertPR5AdmittedCandidate(t, f, candidate, f.queueA, "foreign-shared-a",
			ImportValidationPhaseAccepted, f.now.Add(time.Minute))
		insertPR5AdmittedCandidate(t, f, candidate, f.queueB, "foreign-shared-b",
			ImportValidationPhaseAccepted, f.now.Add(2*time.Minute))
		_, err = f.repo.ActivateImportFileRevision(ctx, f.queueB.ID, candidate.ID)
		require.NoError(t, err)

		claimed, err := f.repo.ClaimInactiveImportCandidateCleanup(
			ctx, f.queueA.ID, candidate.ID, f.revision.LayoutFingerprint, true,
			f.now.Add(3*time.Minute),
		)
		require.NoError(t, err)
		assert.False(t, claimed,
			"the crashed queue must not gain authority to restore over the foreign owner")
		var activeID string
		require.NoError(t, f.db.Connection().QueryRowContext(ctx, `
			SELECT id FROM health_file_revisions
			WHERE file_health_id = ? AND active = TRUE
		`, candidate.FileHealthID).Scan(&activeID))
		assert.Equal(t, candidate.ID, activeID)
	})

	t.Run("foreign unresolved owner of active prior is preserved", func(t *testing.T) {
		f := newPR5AuditImportFixture(t, "import")
		ctx := context.Background()
		insertPR5AdmittedCandidate(t, f, f.revision, f.queueB, "foreign-prior-owner",
			ImportValidationPhaseAccepted, f.now.Add(time.Minute))
		_, err := f.repo.ActivateImportFileRevision(ctx, f.queueB.ID, f.revision.ID)
		require.NoError(t, err)

		candidate, err := f.repo.EnsureCandidateFileRevision(ctx, FileRevisionSpec{
			FilePath: "library/audit-import.mkv", LayoutFingerprint: "sha256:after-foreign-prior",
			VirtualSize: 300, SegmentCount: 2,
		})
		require.NoError(t, err)
		insertPR5AdmittedCandidate(t, f, candidate, f.queueA, "after-foreign-prior",
			ImportValidationPhaseAccepted, f.now.Add(2*time.Minute))

		claimed, err := f.repo.ClaimInactiveImportCandidateCleanup(
			ctx, f.queueA.ID, candidate.ID, f.revision.LayoutFingerprint, true,
			f.now.Add(3*time.Minute),
		)
		require.ErrorIs(t, err, ErrStaleRevisionActivation)
		assert.False(t, claimed)
	})

	t.Run("historical committed owner does not block cleanup claim", func(t *testing.T) {
		f := newPR5AuditImportFixture(t, "import")
		ctx := context.Background()
		insertPR5AdmittedCandidate(t, f, f.revision, f.queueB, "committed-prior-owner",
			ImportValidationPhaseAccepted, f.now.Add(time.Minute))
		_, err := f.repo.ActivateImportFileRevision(ctx, f.queueB.ID, f.revision.ID)
		require.NoError(t, err)
		require.NoError(t, f.repo.CommitImportQueueActivations(
			ctx, f.queueB.ID, f.now.Add(2*time.Minute),
		))

		candidate, err := f.repo.EnsureCandidateFileRevision(ctx, FileRevisionSpec{
			FilePath: "library/audit-import.mkv", LayoutFingerprint: "sha256:after-commit",
			VirtualSize: 300, SegmentCount: 2,
		})
		require.NoError(t, err)
		insertPR5AdmittedCandidate(t, f, candidate, f.queueA, "after-committed-owner",
			ImportValidationPhaseAccepted, f.now.Add(3*time.Minute))
		claimed, err := f.repo.ClaimInactiveImportCandidateCleanup(
			ctx, f.queueA.ID, candidate.ID, f.revision.LayoutFingerprint, true,
			f.now.Add(4*time.Minute),
		)
		require.NoError(t, err)
		assert.True(t, claimed,
			"resolved historical journal rows must not retain cleanup ownership")
	})
}

func TestPR5HealthPendingActivationIsImmediateAndNewerAdmissionFencesStaleCandidate(t *testing.T) {
	f := newPR5AuditImportFixture(t, "import")
	ctx := context.Background()
	older, err := f.repo.EnsureCandidateFileRevision(ctx, FileRevisionSpec{
		FilePath: "library/audit-import.mkv", LayoutFingerprint: "sha256:older-candidate",
		VirtualSize: 300, SegmentCount: 2,
	})
	require.NoError(t, err)
	insertPR5AdmittedCandidate(t, f, older, f.queueA, "older-admission", ImportValidationPhaseAccepted, f.now.Add(time.Minute))

	f.clock.now = f.now.Add(2 * time.Minute)
	newer, err := f.repo.EnsureCandidateFileRevision(ctx, FileRevisionSpec{
		FilePath: "library/audit-import.mkv", LayoutFingerprint: "sha256:newer-candidate",
		VirtualSize: 300, SegmentCount: 2,
	})
	require.NoError(t, err)
	insertPR5AdmittedCandidate(t, f, newer, f.queueB, "newer-admission", ImportValidationPhaseHealthPending, f.now.Add(3*time.Minute))
	f.clock.now = f.now.Add(4 * time.Minute)

	_, err = f.repo.ActivateFileRevision(ctx, older.ID)
	require.ErrorIs(t, err, ErrStaleRevisionActivation)
	activated, err := f.repo.ActivateFileRevision(ctx, newer.ID)
	require.NoError(t, err)
	assert.True(t, activated.Active)
	status, scheduled, priority := readPR5CandidateSchedule(t, f)
	assert.Equal(t, HealthStatusPending, status)
	assert.Equal(t, f.clock.now, scheduled)
	assert.Equal(t, HealthPriorityHigh, priority)
}

func TestPR5RejectedCandidateCannotDisplacePublishedRevision(t *testing.T) {
	f := newPR5AuditImportFixture(t, "import")
	ctx := context.Background()
	candidate, err := f.repo.EnsureCandidateFileRevision(ctx, FileRevisionSpec{
		FilePath: "library/audit-import.mkv", LayoutFingerprint: "sha256:rejected-candidate",
		VirtualSize: 300, SegmentCount: 2,
	})
	require.NoError(t, err)
	insertPR5AdmittedCandidate(t, f, candidate, f.queueA, "rejected-candidate", ImportValidationPhaseRejected, f.now.Add(time.Minute))
	_, err = f.repo.ActivateFileRevision(ctx, candidate.ID)
	require.ErrorIs(t, err, ErrFileRevisionNotAdmitted)

	var activeID string
	require.NoError(t, f.db.Connection().QueryRowContext(ctx, `
		SELECT id FROM health_file_revisions
		WHERE file_health_id = ? AND active = TRUE
	`, f.revision.FileHealthID).Scan(&activeID))
	assert.Equal(t, f.revision.ID, activeID)
}

func TestPR5AbandonImportValidationPreservesRunHistoryAndIsIdempotent(t *testing.T) {
	f := newPR5AuditImportFixture(t, "import")
	ctx := context.Background()
	lease, err := f.repo.AcquireRunLease(ctx, f.run.ID, "abandon-worker", time.Minute)
	require.NoError(t, err)
	write := ImportValidationWrite{
		ID: "abandoned-validation", QueueItemID: f.queueA.ID,
		FileRevisionID: f.revision.ID, RunID: f.run.ID,
		Phase: ImportValidationPhaseInitialPass, DamagePolicy: ImportDamagePolicyStrict,
		LeaseOwner: *lease.LeaseOwner, FencingToken: lease.FencingToken,
		CreatedAt: f.now, UpdatedAt: f.now,
	}
	_, err = f.repo.UpsertImportValidation(ctx, write)
	require.NoError(t, err)
	_, err = f.db.Connection().ExecContext(ctx, `
		INSERT INTO health_run_schedule
			(run_id, dedupe_key, active, priority, not_before, created_at, updated_at)
		VALUES (?, ?, TRUE, 2, ?, ?, ?)
	`, f.run.ID, "abandoned-import", f.now, f.now, f.now)
	require.NoError(t, err)

	at := f.now.Add(time.Second)
	require.NoError(t, f.repo.AbandonImportValidation(
		ctx, f.queueA.ID, f.revision.ID, f.run.ID, at,
	))
	validation, err := f.repo.GetImportValidation(ctx, f.queueA.ID, f.revision.ID)
	require.NoError(t, err)
	assert.Nil(t, validation)
	run, err := f.repo.GetHealthRun(ctx, f.run.ID)
	require.NoError(t, err)
	require.NotNil(t, run)
	assert.Equal(t, HealthRunCanceled, run.Status)
	assert.Nil(t, run.LeaseOwner)
	var active bool
	require.NoError(t, f.db.Connection().QueryRowContext(ctx, `
		SELECT active FROM health_run_schedule WHERE run_id = ?
	`, f.run.ID).Scan(&active))
	assert.False(t, active)
	require.NoError(t, f.repo.AbandonImportValidation(
		ctx, f.queueA.ID, f.revision.ID, f.run.ID, at,
	))

	var retained int
	require.NoError(t, f.db.Connection().QueryRowContext(ctx, `
		SELECT COUNT(*) FROM health_runs WHERE id = ?
	`, f.run.ID).Scan(&retained))
	assert.Equal(t, 1, retained)
}

func TestPR5CandidateActivationRejectsProviderMembershipChange(t *testing.T) {
	f := newPR5AuditImportFixture(t, "import")
	ctx := context.Background()
	candidate, err := f.repo.EnsureCandidateFileRevision(ctx, FileRevisionSpec{
		FilePath: "library/audit-import.mkv", LayoutFingerprint: "sha256:stale-provider-candidate",
		VirtualSize: 300, SegmentCount: 2,
	})
	require.NoError(t, err)
	insertPR5AdmittedCandidate(t, f, candidate, f.queueA, "stale-provider-candidate", ImportValidationPhaseAccepted, f.now.Add(time.Minute))
	_, err = f.repo.ReconcileProviders(ctx, []ProviderSpec{
		{
			StableID: "audit-import-provider", DisplayName: "Audit provider",
			Endpoint: "audit-import.invalid", Port: 119, Account: "synthetic-account",
			Role: ProviderRolePrimary, Order: 0,
		},
		{
			StableID: "new-provider", DisplayName: "New provider",
			Endpoint: "new.invalid", Port: 119, Account: "synthetic-account",
			Role: ProviderRoleBackup, Order: 1,
		},
	})
	require.NoError(t, err)
	_, err = f.repo.ActivateFileRevision(ctx, candidate.ID)
	require.ErrorIs(t, err, ErrProviderSnapshotMismatch)

	var activeID sql.NullString
	require.NoError(t, f.db.Connection().QueryRowContext(ctx, `
		SELECT id FROM health_file_revisions
		WHERE file_health_id = ? AND active = TRUE
	`, f.revision.FileHealthID).Scan(&activeID))
	assert.Equal(t, f.revision.ID, activeID.String)
}

func TestPR5ImportActivationJournalRollsReplacementBackAndCompletesIdempotently(t *testing.T) {
	f := newPR5AuditImportFixture(t, "import")
	ctx := context.Background()
	priorDue := f.now.Add(6 * time.Hour)
	_, err := f.db.Connection().ExecContext(ctx, `
		UPDATE file_health
		SET status = 'healthy', scheduled_check_at = ?, priority = 0,
		    retry_count = 2, repair_retry_count = 3
		WHERE id = ?
	`, priorDue, f.revision.FileHealthID)
	require.NoError(t, err)
	candidate, err := f.repo.EnsureCandidateFileRevision(ctx, FileRevisionSpec{
		FilePath: "library/audit-import.mkv", LayoutFingerprint: "sha256:journal-replacement",
		VirtualSize: 300, SegmentCount: 2,
	})
	require.NoError(t, err)
	insertPR5AdmittedCandidate(t, f, candidate, f.queueA, "journal-replacement",
		ImportValidationPhaseAccepted, f.now.Add(time.Minute))
	f.clock.now = f.now.Add(2 * time.Minute)
	_, err = f.repo.ActivateFileRevision(ctx, candidate.ID)
	require.NoError(t, err)

	rolledBack, err := f.repo.BeginImportQueueActivationRollback(ctx, f.queueA.ID, f.clock.now)
	require.NoError(t, err)
	require.Len(t, rolledBack, 1)
	assert.Equal(t, candidate.ID, rolledBack[0].CandidateRevisionID)
	assert.Equal(t, f.revision.ID, rolledBack[0].PriorRevisionID)
	assert.Equal(t, candidate.LayoutFingerprint, rolledBack[0].CandidateLayoutFingerprint)
	assert.Equal(t, f.revision.LayoutFingerprint, rolledBack[0].PriorLayoutFingerprint)
	assert.Equal(t, "library/audit-import.mkv", rolledBack[0].FilePath)

	var activeID string
	require.NoError(t, f.db.Connection().QueryRowContext(ctx, `
		SELECT id FROM health_file_revisions
		WHERE file_health_id = ? AND active = TRUE
	`, f.revision.FileHealthID).Scan(&activeID))
	assert.Equal(t, f.revision.ID, activeID)
	status, due, priority := readPR5CandidateSchedule(t, f)
	assert.Equal(t, HealthStatusHealthy, status)
	assert.Equal(t, priorDue, due)
	assert.Equal(t, HealthPriorityNormal, priority)

	again, err := f.repo.BeginImportQueueActivationRollback(ctx, f.queueA.ID, f.clock.now)
	require.NoError(t, err)
	assert.Equal(t, rolledBack, again)
	require.NoError(t, f.repo.CompleteImportQueueActivationRollback(
		ctx, f.queueA.ID, []string{candidate.ID}, f.clock.now,
	))
	require.NoError(t, f.repo.CompleteImportQueueActivationRollback(
		ctx, f.queueA.ID, []string{candidate.ID}, f.clock.now,
	))
	var state string
	require.NoError(t, f.db.Connection().QueryRowContext(ctx, `
		SELECT state FROM health_import_activation_journal
		WHERE queue_item_id = ? AND candidate_revision_id = ?
	`, f.queueA.ID, candidate.ID).Scan(&state))
	assert.Equal(t, "cleanup_completed", state)
	var validations, runs int
	require.NoError(t, f.db.Connection().QueryRowContext(ctx, `
		SELECT COUNT(*) FROM health_import_validations WHERE file_revision_id = ?
	`, candidate.ID).Scan(&validations))
	require.NoError(t, f.db.Connection().QueryRowContext(ctx, `
		SELECT COUNT(*) FROM health_runs WHERE file_revision_id = ?
	`, candidate.ID).Scan(&runs))
	assert.Equal(t, 1, validations)
	assert.Equal(t, 1, runs)
}

func TestPR5ImportActivationJournalCompensatesNewPathAndFencesReplacement(t *testing.T) {
	t.Run("new path compensation", func(t *testing.T) {
		f := newPR5AuditImportFixture(t, "import")
		ctx := context.Background()
		candidate, err := f.repo.EnsureCandidateFileRevision(ctx, FileRevisionSpec{
			FilePath: "library/journal-new-path.mkv", LayoutFingerprint: "sha256:journal-new-path",
			VirtualSize: 300, SegmentCount: 2,
		})
		require.NoError(t, err)
		insertPR5AdmittedCandidate(t, f, candidate, f.queueA, "journal-new-path",
			ImportValidationPhaseHealthPending, f.now.Add(time.Minute))
		f.clock.now = f.now.Add(2 * time.Minute)
		_, err = f.repo.ActivateFileRevision(ctx, candidate.ID)
		require.NoError(t, err)
		records, err := f.repo.BeginImportQueueActivationRollback(ctx, f.queueA.ID, f.clock.now)
		require.NoError(t, err)
		require.Len(t, records, 1)
		assert.Empty(t, records[0].PriorRevisionID)
		var active int
		require.NoError(t, f.db.Connection().QueryRowContext(ctx, `
			SELECT COUNT(*) FROM health_file_revisions
			WHERE file_health_id = ? AND active = TRUE
		`, candidate.FileHealthID).Scan(&active))
		assert.Zero(t, active)
		require.NoError(t, f.repo.CompensateImportQueueActivationRollback(
			ctx, f.queueA.ID, []string{candidate.ID}, f.clock.now.Add(time.Second),
		))
		require.NoError(t, f.repo.CompensateImportQueueActivationRollback(
			ctx, f.queueA.ID, []string{candidate.ID}, f.clock.now.Add(time.Second),
		))
		var activeID string
		require.NoError(t, f.db.Connection().QueryRowContext(ctx, `
			SELECT id FROM health_file_revisions
			WHERE file_health_id = ? AND active = TRUE
		`, candidate.FileHealthID).Scan(&activeID))
		assert.Equal(t, candidate.ID, activeID)
		var scheduled time.Time
		var priority HealthPriority
		require.NoError(t, f.db.Connection().QueryRowContext(ctx, `
			SELECT scheduled_check_at, priority FROM file_health WHERE id = ?
		`, candidate.FileHealthID).Scan(&scheduled, &priority))
		assert.Equal(t, f.clock.now, scheduled)
		assert.Equal(t, HealthPriorityHigh, priority)
	})

	t.Run("unrelated replacement fence", func(t *testing.T) {
		f := newPR5AuditImportFixture(t, "import")
		ctx := context.Background()
		candidate, err := f.repo.EnsureCandidateFileRevision(ctx, FileRevisionSpec{
			FilePath: "library/audit-import.mkv", LayoutFingerprint: "sha256:journal-fenced",
			VirtualSize: 300, SegmentCount: 2,
		})
		require.NoError(t, err)
		insertPR5AdmittedCandidate(t, f, candidate, f.queueA, "journal-fenced",
			ImportValidationPhaseAccepted, f.now.Add(time.Minute))
		f.clock.now = f.now.Add(2 * time.Minute)
		_, err = f.repo.ActivateFileRevision(ctx, candidate.ID)
		require.NoError(t, err)
		_, err = f.repo.BeginImportQueueActivationRollback(ctx, f.queueA.ID, f.clock.now)
		require.NoError(t, err)
		unrelated, err := f.repo.EnsureCandidateFileRevision(ctx, FileRevisionSpec{
			FilePath: "library/audit-import.mkv", LayoutFingerprint: "sha256:journal-unrelated",
			VirtualSize: 300, SegmentCount: 2,
		})
		require.NoError(t, err)
		_, err = f.db.Connection().ExecContext(ctx, `
			UPDATE health_file_revisions SET active = FALSE WHERE file_health_id = ?;
			UPDATE health_file_revisions SET active = TRUE WHERE id = ?
		`, candidate.FileHealthID, unrelated.ID)
		require.NoError(t, err)
		err = f.repo.CompensateImportQueueActivationRollback(
			ctx, f.queueA.ID, []string{candidate.ID}, f.clock.now.Add(time.Second),
		)
		require.ErrorIs(t, err, ErrStaleRevisionActivation)
		var activeID string
		require.NoError(t, f.db.Connection().QueryRowContext(ctx, `
			SELECT id FROM health_file_revisions
			WHERE file_health_id = ? AND active = TRUE
		`, candidate.FileHealthID).Scan(&activeID))
		assert.Equal(t, unrelated.ID, activeID)
	})
}

func TestPR5CommittedImportActivationsCannotEnterFailureRollback(t *testing.T) {
	f := newPR5AuditImportFixture(t, "import")
	ctx := context.Background()
	candidate, err := f.repo.EnsureCandidateFileRevision(ctx, FileRevisionSpec{
		FilePath: "library/audit-import.mkv", LayoutFingerprint: "sha256:journal-committed",
		VirtualSize: 300, SegmentCount: 2,
	})
	require.NoError(t, err)
	insertPR5AdmittedCandidate(t, f, candidate, f.queueA, "journal-committed",
		ImportValidationPhaseAccepted, f.now.Add(time.Minute))
	f.clock.now = f.now.Add(2 * time.Minute)
	_, err = f.repo.ActivateFileRevision(ctx, candidate.ID)
	require.NoError(t, err)
	require.NoError(t, f.repo.CommitImportQueueActivations(ctx, f.queueA.ID, f.clock.now))
	require.NoError(t, f.repo.CommitImportQueueActivations(ctx, f.queueA.ID, f.clock.now))
	records, err := f.repo.BeginImportQueueActivationRollback(ctx, f.queueA.ID, f.clock.now)
	require.NoError(t, err)
	assert.Empty(t, records)
}
