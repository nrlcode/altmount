package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func acceptPR5ReusableImportCoverage(
	t *testing.T,
	f pr5AuditImportFixture,
	validationID string,
) {
	t.Helper()
	ctx := context.Background()
	lease, err := f.repo.AcquireRunLease(ctx, f.run.ID, validationID+"-worker", time.Minute)
	require.NoError(t, err)
	initial := ImportValidationWrite{
		ID: validationID, QueueItemID: f.queueA.ID,
		FileRevisionID: f.revision.ID, RunID: f.run.ID,
		Phase: ImportValidationPhaseInitialPass, DamagePolicy: ImportDamagePolicyStrict,
		CreatedAt: f.now, UpdatedAt: f.now,
		LeaseOwner: *lease.LeaseOwner, FencingToken: lease.FencingToken,
	}
	_, err = f.repo.UpsertImportValidation(ctx, initial)
	require.NoError(t, err)
	commitPR5ImportSTATCoverage(
		t, f.repo, f.run, lease, f.snapshot.Entries[0], validationID+"-full-stat",
		HealthRunStageImportInitialSTAT, 0b00000011, 0b00000011, f.now,
	)
	accepted := initial
	accepted.Phase = ImportValidationPhaseAccepted
	accepted.UpdatedAt = f.now.Add(time.Second)
	f.clock.now = accepted.UpdatedAt
	_, err = f.repo.UpsertImportValidation(ctx, accepted)
	require.NoError(t, err)
}

func TestPR5ReusableImportCoverageRequiresAcceptedCurrentFullSTATRun(t *testing.T) {
	f := newPR5AuditImportFixture(t, "import")
	ctx := context.Background()
	lease, err := f.repo.AcquireRunLease(ctx, f.run.ID, "coverage-reuse-worker", time.Minute)
	require.NoError(t, err)

	initial := ImportValidationWrite{
		ID: "coverage-reuse-validation", QueueItemID: f.queueA.ID,
		FileRevisionID: f.revision.ID, RunID: f.run.ID,
		Phase: ImportValidationPhaseInitialPass, DamagePolicy: ImportDamagePolicyStrict,
		CreatedAt: f.now, UpdatedAt: f.now,
		LeaseOwner: *lease.LeaseOwner, FencingToken: lease.FencingToken,
	}
	_, err = f.repo.UpsertImportValidation(ctx, initial)
	require.NoError(t, err)

	reusable, err := f.repo.HasReusableCompletedImportSTATCoverage(ctx, f.revision.ID, 2)
	require.NoError(t, err)
	assert.False(t, reusable, "in-progress coverage must not suppress ordinary health")

	provider := f.snapshot.Entries[0]
	commitPR5ImportSTATCoverage(
		t, f.repo, f.run, lease, provider, "coverage-reuse-full-stat",
		HealthRunStageImportInitialSTAT, 0b00000011, 0b00000011, f.now,
	)
	accepted := initial
	accepted.Phase = ImportValidationPhaseAccepted
	accepted.InitialPassComplete = true
	accepted.UpdatedAt = f.now.Add(time.Second)
	f.clock.now = accepted.UpdatedAt
	_, err = f.repo.UpsertImportValidation(ctx, accepted)
	require.NoError(t, err)

	reusable, err = f.repo.HasReusableCompletedImportSTATCoverage(ctx, f.revision.ID, 2)
	require.NoError(t, err)
	assert.True(t, reusable)

	reusable, err = f.repo.HasReusableCompletedImportSTATCoverage(ctx, f.revision.ID, 3)
	require.NoError(t, err)
	assert.False(t, reusable, "coverage from a different canonical dimension must not be reused")

	_, err = f.repo.ReconcileProviders(ctx, []ProviderSpec{
		{
			StableID: provider.ProviderID, DisplayName: "Audit provider",
			Endpoint: "audit-import.invalid", Port: 119, Account: "synthetic-account",
			Role: ProviderRolePrimary, Order: 0,
		},
		{
			StableID: "new-active-provider", DisplayName: "New provider",
			Endpoint: "new-active.invalid", Port: 119, Account: "synthetic-account",
			Role: ProviderRoleBackup, Order: 1,
		},
	})
	require.NoError(t, err)
	reusable, err = f.repo.HasReusableCompletedImportSTATCoverage(ctx, f.revision.ID, 2)
	require.NoError(t, err)
	assert.False(t, reusable, "a stale provider snapshot must not suppress current health coverage")
}

func TestPR5CompletedImportCoverageKeepsHealthPendingUnresolvedWork(t *testing.T) {
	f := newPR5AuditImportFixture(t, "import")
	ctx := context.Background()
	lease, err := f.repo.AcquireRunLease(ctx, f.run.ID, "health-pending-coverage", time.Minute)
	require.NoError(t, err)
	provider := f.snapshot.Entries[0]
	commitPR5ImportSTATCoverage(
		t, f.repo, f.run, lease, provider, "health-pending-full-stat",
		HealthRunStageImportInitialSTAT, 0b00000011, 0b00000001, f.now,
	)
	require.NoError(t, f.repo.CompleteHealthRun(
		ctx, f.run.ID, *lease.LeaseOwner, lease.FencingToken, f.now,
	))
	_, err = f.db.Connection().ExecContext(ctx, `
		INSERT INTO health_import_validations
			(id, queue_item_id, file_revision_id, run_id, phase, damage_policy,
			 unresolved_segments, unresolved_bitmap, initial_pass_complete,
			 second_pass_complete, created_at, updated_at)
		VALUES (?, ?, ?, ?, 'health_pending', 'tolerant', 1, ?, TRUE, TRUE, ?, ?)
	`, "health-pending-coverage-validation", f.queueA.ID, f.revision.ID,
		f.run.ID, []byte{0b00000010}, f.now, f.now)
	require.NoError(t, err)

	coverage, err := f.repo.GetCompletedImportSTATCoverage(ctx, f.revision.ID, 2)
	require.NoError(t, err)
	require.NotNil(t, coverage)
	assert.False(t, coverage.Reusable)
	assert.True(t, coverage.HealthPending)
	assert.Equal(t, []int64{1}, coverage.UnresolvedPositions)

	reusable, err := f.repo.HasReusableCompletedImportSTATCoverage(ctx, f.revision.ID, 2)
	require.NoError(t, err)
	assert.False(t, reusable, "health_pending coverage must not defer unresolved health work")
}

func TestPR5ReusableImportCoverageTracksProviderMembershipNotRoleOrOrder(t *testing.T) {
	provider := func(endpoint, account string, role ProviderRole, order int) ProviderSpec {
		return ProviderSpec{
			StableID: "audit-import-provider", DisplayName: "Audit provider",
			Endpoint: endpoint, Port: 119, Account: account, Role: role, Order: order,
		}
	}
	t.Run("role and order only remain reusable", func(t *testing.T) {
		f := newPR5AuditImportFixture(t, "import")
		acceptPR5ReusableImportCoverage(t, f, "role-order-reuse")
		providers, err := f.repo.ReconcileProviders(context.Background(), []ProviderSpec{
			provider("audit-import.invalid", "synthetic-account", ProviderRoleBackup, 7),
		})
		require.NoError(t, err)
		require.Len(t, providers, 1)
		assert.Equal(t, ProviderRoleBackup, providers[0].Role)
		assert.Equal(t, 7, providers[0].Order)
		reusable, err := f.repo.HasReusableCompletedImportSTATCoverage(
			context.Background(), f.revision.ID, f.revision.SegmentCount,
		)
		require.NoError(t, err)
		assert.True(t, reusable)
	})

	for _, test := range []struct {
		name      string
		reconcile func(pr5AuditImportFixture) error
	}{
		{
			name: "endpoint generation change",
			reconcile: func(f pr5AuditImportFixture) error {
				_, err := f.repo.ReconcileProviders(context.Background(), []ProviderSpec{
					provider("replacement.invalid", "synthetic-account", ProviderRolePrimary, 0),
				})
				return err
			},
		},
		{
			name: "account generation change",
			reconcile: func(f pr5AuditImportFixture) error {
				_, err := f.repo.ReconcileProviders(context.Background(), []ProviderSpec{
					provider("audit-import.invalid", "synthetic-account-2", ProviderRolePrimary, 0),
				})
				return err
			},
		},
		{
			name: "provider added",
			reconcile: func(f pr5AuditImportFixture) error {
				_, err := f.repo.ReconcileProviders(context.Background(), []ProviderSpec{
					provider("audit-import.invalid", "synthetic-account", ProviderRolePrimary, 0),
					{
						StableID: "audit-import-provider-b", DisplayName: "Audit backup",
						Endpoint: "audit-import-b.invalid", Port: 119, Account: "synthetic-account",
						Role: ProviderRoleBackup, Order: 1,
					},
				})
				return err
			},
		},
		{
			name: "provider removed",
			reconcile: func(f pr5AuditImportFixture) error {
				_, err := f.repo.ReconcileProviders(context.Background(), nil)
				return err
			},
		},
		{
			name: "provider reactivated",
			reconcile: func(f pr5AuditImportFixture) error {
				if _, err := f.repo.ReconcileProviders(context.Background(), nil); err != nil {
					return err
				}
				_, err := f.repo.ReconcileProviders(context.Background(), []ProviderSpec{
					provider("audit-import.invalid", "synthetic-account", ProviderRolePrimary, 0),
				})
				return err
			},
		},
	} {
		t.Run(test.name+" is not reusable", func(t *testing.T) {
			f := newPR5AuditImportFixture(t, "import")
			acceptPR5ReusableImportCoverage(t, f, "membership-change-reuse")
			require.NoError(t, test.reconcile(f))
			reusable, err := f.repo.HasReusableCompletedImportSTATCoverage(
				context.Background(), f.revision.ID, f.revision.SegmentCount,
			)
			require.NoError(t, err)
			assert.False(t, reusable)
		})
	}
}

func TestPR5ExplicitManualRunInvalidatesImportCoverageSuppression(t *testing.T) {
	f := newPR5AuditImportFixture(t, "import")
	acceptPR5ReusableImportCoverage(t, f, "manual-bypass-reuse")
	ctx := context.Background()
	manualAt := f.clock.now.Add(time.Second)
	snapshot, err := f.repo.CaptureActiveProviderSnapshot(ctx, manualAt)
	require.NoError(t, err)
	manual, created, err := f.repo.EnsureScheduledHealthRun(ctx, ScheduledHealthRunSpec{
		Run: HealthRunSpec{
			ID: "manual-bypass-run", FileRevisionID: f.revision.ID,
			ProviderSnapshotID: snapshot.ID, Trigger: "manual", Mode: "observation",
			TotalSegments: f.revision.SegmentCount, CreatedAt: manualAt,
		},
		DedupeKey: "manual-bypass-reuse", Priority: HealthRunPriorityHigh,
		NotBefore: manualAt,
	})
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, "manual", manual.Trigger)

	reusable, err := f.repo.HasReusableCompletedImportSTATCoverage(
		ctx, f.revision.ID, f.revision.SegmentCount,
	)
	require.NoError(t, err)
	assert.False(t, reusable,
		"an explicit manual run must execute and permanently supersede import-coverage suppression")
}

func TestPR5ReusableImportCoverageConsumptionAndHealthDeferralAreAtomic(t *testing.T) {
	f := newPR5AuditImportFixture(t, "import")
	ctx := context.Background()
	lease, err := f.repo.AcquireRunLease(ctx, f.run.ID, "atomic-reuse-worker", time.Minute)
	require.NoError(t, err)

	initial := ImportValidationWrite{
		ID: "atomic-reuse-validation", QueueItemID: f.queueA.ID,
		FileRevisionID: f.revision.ID, RunID: f.run.ID,
		Phase: ImportValidationPhaseInitialPass, DamagePolicy: ImportDamagePolicyStrict,
		CreatedAt: f.now, UpdatedAt: f.now,
		LeaseOwner: *lease.LeaseOwner, FencingToken: lease.FencingToken,
	}
	_, err = f.repo.UpsertImportValidation(ctx, initial)
	require.NoError(t, err)
	commitPR5ImportSTATCoverage(
		t, f.repo, f.run, lease, f.snapshot.Entries[0], "atomic-reuse-full-stat",
		HealthRunStageImportInitialSTAT, 0b00000011, 0b00000011, f.now,
	)
	accepted := initial
	accepted.Phase = ImportValidationPhaseAccepted
	accepted.UpdatedAt = f.now.Add(time.Second)
	f.clock.now = accepted.UpdatedAt
	_, err = f.repo.UpsertImportValidation(ctx, accepted)
	require.NoError(t, err)

	consumedAt := f.now.Add(2 * time.Second)
	nextCheck := consumedAt.Add(24 * time.Hour)
	coverage, err := f.repo.ConsumeReusableCompletedImportSTATCoverageAndDeferHealth(
		ctx, f.revision.ID, f.revision.SegmentCount,
		"library/not-the-active-file.mkv", nextCheck, consumedAt,
	)
	require.Error(t, err)
	assert.Nil(t, coverage)

	var unconsumed, scheduleStillEmpty int
	require.NoError(t, f.db.Connection().QueryRowContext(ctx, `
		SELECT COUNT(*) FROM health_import_validations
		WHERE id = ? AND coverage_reused_at IS NULL
	`, initial.ID).Scan(&unconsumed))
	require.NoError(t, f.db.Connection().QueryRowContext(ctx, `
		SELECT COUNT(*) FROM file_health
		WHERE id = ? AND scheduled_check_at IS NULL
	`, f.revision.FileHealthID).Scan(&scheduleStillEmpty))
	assert.Equal(t, 1, unconsumed, "failed deferral must roll back the reuse marker")
	assert.Equal(t, 1, scheduleStillEmpty, "failed deferral must not change the schedule")

	coverage, err = f.repo.ConsumeReusableCompletedImportSTATCoverageAndDeferHealth(
		ctx, f.revision.ID, f.revision.SegmentCount,
		"/library/audit-import.mkv", nextCheck, consumedAt,
	)
	require.NoError(t, err)
	require.NotNil(t, coverage)
	assert.True(t, coverage.Reusable)

	var reusedAt, scheduledAt time.Time
	require.NoError(t, f.db.Connection().QueryRowContext(ctx, `
		SELECT validation.coverage_reused_at, health.scheduled_check_at
		FROM health_import_validations validation
		JOIN health_file_revisions revision ON revision.id = validation.file_revision_id
		JOIN file_health health ON health.id = revision.file_health_id
		WHERE validation.id = ?
	`, initial.ID).Scan(&reusedAt, &scheduledAt))
	assert.True(t, reusedAt.Equal(consumedAt), "%v != %v", reusedAt, consumedAt)
	assert.True(t, scheduledAt.Equal(nextCheck), "%v != %v", scheduledAt, nextCheck)

	coverage, err = f.repo.ConsumeReusableCompletedImportSTATCoverageAndDeferHealth(
		ctx, f.revision.ID, f.revision.SegmentCount,
		"library/audit-import.mkv", nextCheck.Add(time.Hour), consumedAt.Add(time.Hour),
	)
	require.NoError(t, err)
	assert.Nil(t, coverage, "accepted import coverage may be consumed only once")
	require.NoError(t, f.db.Connection().QueryRowContext(ctx, `
		SELECT scheduled_check_at FROM file_health WHERE id = ?
	`, f.revision.FileHealthID).Scan(&scheduledAt))
	assert.True(t, scheduledAt.Equal(nextCheck), "%v != %v", scheduledAt, nextCheck)
}

func TestPR5ListHealthRunsReturnsCommittedProgressNewestFirst(t *testing.T) {
	f := newPR4RunFixture(t)
	ctx := context.Background()
	newer, err := f.repo.CreateHealthRun(ctx, HealthRunSpec{
		ID: "newer-progress-run", FileRevisionID: f.run.FileRevisionID,
		ProviderSnapshotID: f.run.ProviderSnapshotID, Trigger: "manual",
		Mode: "observation", TotalSegments: f.run.TotalSegments,
		CreatedAt: f.now.Add(time.Minute),
	})
	require.NoError(t, err)

	runs, err := f.repo.ListHealthRuns(ctx, 1)
	require.NoError(t, err)
	require.Len(t, runs, 1)
	assert.Equal(t, newer.ID, runs[0].ID)
	assert.Equal(t, int64(8), runs[0].TotalSegments)
	assert.Empty(t, runs[0].LastError)

	_, err = f.repo.ListHealthRuns(ctx, 0)
	require.Error(t, err)
}
