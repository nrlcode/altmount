package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
