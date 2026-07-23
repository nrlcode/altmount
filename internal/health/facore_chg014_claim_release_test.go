package health

import (
	"context"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/testsupport/fakepool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func claimCHG014WorkerHealth(t *testing.T, env *repairTestEnv, filePath string) *database.FileHealth {
	t.Helper()
	ctx := context.Background()
	selected, err := env.healthRepo.GetFileHealth(ctx, filePath)
	require.NoError(t, err)
	require.NotNil(t, selected)
	claimed, err := env.healthRepo.ClaimFilesCheckingBulk(ctx, []*database.FileHealth{selected})
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	return claimed[0]
}

func receiveCHG014CheckError(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for canceled health check to return")
		return nil
	}
}

func requireCHG014PendingDue(t *testing.T, env *repairTestEnv, filePath string, retryCount int) *database.FileHealth {
	t.Helper()
	row, err := env.healthRepo.GetFileHealth(context.Background(), filePath)
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Equal(t, database.HealthStatusPending, row.Status,
		"an unsuccessful owner must release its durable checking claim")
	assert.Equal(t, retryCount, row.RetryCount, "claim release cannot consume retry budget")
	scheduled := queryScheduledAt(t, env.db, filePath)
	require.NotNil(t, scheduled, "released work must remain scheduled")
	assert.False(t, scheduled.After(time.Now().UTC().Add(time.Second)),
		"released work must be immediately selectable")
	return row
}

func TestFACORECHG014BulkPublicationFailureReleasesWholeClaimedBatch(t *testing.T) {
	client := fakepool.New()
	env := newBatchTestEnv(t, t.TempDir(), client)
	paths := []string{"movies/publish-failure-a.mkv", "movies/publish-failure-b.mkv"}
	for i, path := range paths {
		writeHealthyFile(t, env, path)
		insertFileHealth(t, env.db, path, "/library/"+path, i+1, 3)
	}
	_, err := env.db.Exec(`
		CREATE TRIGGER reject_chg014_health_publication
		BEFORE UPDATE OF status ON file_health
		WHEN OLD.status = 'checking' AND NEW.status = 'healthy'
		BEGIN SELECT RAISE(ABORT, 'synthetic claimed publication failure'); END;
	`)
	require.NoError(t, err)

	err = env.hw.runHealthCheckCycle(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to publish claimed health evidence")
	for i, path := range paths {
		requireCHG014PendingDue(t, env, path, i+1)
	}
	assert.Equal(t, int64(2), client.StatCalls(), "the red proof must fail after evidence gathering")

	_, err = env.db.Exec(`DROP TRIGGER reject_chg014_health_publication`)
	require.NoError(t, err)
	require.NoError(t, env.hw.runHealthCheckCycle(context.Background()))
	for _, path := range paths {
		row, getErr := env.healthRepo.GetFileHealth(context.Background(), path)
		require.NoError(t, getErr)
		require.Equal(t, database.HealthStatusHealthy, row.Status)
	}
	assert.Equal(t, int64(4), client.StatCalls(), "released claims must be selectable by the next cycle")
}

func TestFACORECHG014BulkCancellationUsesDetachedClaimCleanup(t *testing.T) {
	client := fakepool.New()
	client.SetDefaultBehavior(fakepool.SegmentBehavior{Latency: 30 * time.Second})
	env := newBatchTestEnv(t, t.TempDir(), client)
	const filePath = "movies/canceled-batch.mkv"
	writeHealthyFile(t, env, filePath)
	insertFileHealth(t, env.db, filePath, "/library/canceled-batch.mkv", 1, 3)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- env.hw.runHealthCheckCycle(ctx) }()
	waitForHealthContract(t, func() bool { return client.InFlight() == 1 },
		"timed out waiting for the cancellable batch STAT")
	cancel()

	err := receiveCHG014CheckError(t, result)
	require.ErrorIs(t, err, context.Canceled, "claim cleanup cannot mask the work-context error")
	requireCHG014PendingDue(t, env, filePath, 1)

	client.SetDefaultBehavior(fakepool.SegmentBehavior{})
	require.NoError(t, env.hw.runHealthCheckCycle(context.Background()))
	row, err := env.healthRepo.GetFileHealth(context.Background(), filePath)
	require.NoError(t, err)
	require.Equal(t, database.HealthStatusHealthy, row.Status)
}

func TestFACORECHG014DirectCancellationUsesDetachedClaimCleanup(t *testing.T) {
	client := fakepool.New()
	client.SetDefaultBehavior(fakepool.SegmentBehavior{Latency: 30 * time.Second})
	env := newBatchTestEnv(t, t.TempDir(), client)
	const filePath = "movies/canceled-direct.mkv"
	writeHealthyFile(t, env, filePath)
	insertFileHealth(t, env.db, filePath, "/library/canceled-direct.mkv", 2, 3)
	owner := claimCHG014WorkerHealth(t, env, filePath)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- env.hw.performClaimedDirectCheck(ctx, owner) }()
	waitForHealthContract(t, func() bool { return client.InFlight() == 1 },
		"timed out waiting for the cancellable direct STAT")
	cancel()

	err := receiveCHG014CheckError(t, result)
	require.ErrorIs(t, err, context.Canceled, "claim cleanup cannot mask the direct-check cancellation")
	assert.False(t, env.hw.IsCheckActive(filePath), "direct active ownership must be removed on exit")
	requireCHG014PendingDue(t, env, filePath, 2)

	client.SetDefaultBehavior(fakepool.SegmentBehavior{})
	newOwner := claimCHG014WorkerHealth(t, env, filePath)
	require.NoError(t, env.hw.performClaimedDirectCheck(context.Background(), newOwner))
	row, err := env.healthRepo.GetFileHealth(context.Background(), filePath)
	require.NoError(t, err)
	require.Equal(t, database.HealthStatusHealthy, row.Status)
}

func TestFACORECHG014DirectPublicationFailureReleasesClaim(t *testing.T) {
	client := fakepool.New()
	env := newBatchTestEnv(t, t.TempDir(), client)
	const filePath = "movies/direct-publication-failure.mkv"
	writeHealthyFile(t, env, filePath)
	insertFileHealth(t, env.db, filePath, "/library/direct-publication-failure.mkv", 1, 3)
	owner := claimCHG014WorkerHealth(t, env, filePath)
	_, err := env.db.Exec(`
		CREATE TRIGGER reject_chg014_direct_publication
		BEFORE UPDATE OF status ON file_health
		WHEN OLD.status = 'checking' AND NEW.status = 'healthy'
		BEGIN SELECT RAISE(ABORT, 'synthetic direct publication failure'); END;
	`)
	require.NoError(t, err)

	err = env.hw.performClaimedDirectCheck(context.Background(), owner)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to publish direct health evidence")
	assert.False(t, env.hw.IsCheckActive(filePath))
	requireCHG014PendingDue(t, env, filePath, 1)

	_, err = env.db.Exec(`DROP TRIGGER reject_chg014_direct_publication`)
	require.NoError(t, err)
	newOwner := claimCHG014WorkerHealth(t, env, filePath)
	require.NoError(t, env.hw.performClaimedDirectCheck(context.Background(), newOwner))
	row, err := env.healthRepo.GetFileHealth(context.Background(), filePath)
	require.NoError(t, err)
	require.Equal(t, database.HealthStatusHealthy, row.Status)
}

func TestFACORECHG014OldDirectExitCannotDeleteNewerActiveOwner(t *testing.T) {
	client := fakepool.New()
	client.SetDefaultBehavior(fakepool.SegmentBehavior{Latency: 30 * time.Second})
	env := newBatchTestEnv(t, t.TempDir(), client)
	const filePath = "movies/direct-active-generation.mkv"
	writeHealthyFile(t, env, filePath)
	insertFileHealth(t, env.db, filePath, "/library/direct-active-generation.mkv", 0, 3)

	ownerA := claimCHG014WorkerHealth(t, env, filePath)
	ctxA, cancelA := context.WithCancel(context.Background())
	resultA := make(chan error, 1)
	go func() { resultA <- env.hw.performClaimedDirectCheck(ctxA, ownerA) }()
	waitForHealthContract(t, func() bool { return client.InFlight() == 1 },
		"timed out waiting for the first direct owner")

	_, err := env.healthRepo.ResetHealthChecksBulk(context.Background(), []string{filePath})
	require.NoError(t, err)
	ownerB := claimCHG014WorkerHealth(t, env, filePath)
	ctxB, cancelB := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancelA()
		cancelB()
	})
	resultB := make(chan error, 1)
	go func() { resultB <- env.hw.performClaimedDirectCheck(ctxB, ownerB) }()
	waitForHealthContract(t, func() bool { return client.InFlight() == 2 },
		"timed out waiting for the reset-and-reclaimed direct owner")

	cancelA()
	require.ErrorIs(t, receiveCHG014CheckError(t, resultA), context.Canceled)
	require.True(t, env.hw.IsCheckActive(filePath),
		"an old direct goroutine cannot delete the newer owner's active entry")
	current, err := env.healthRepo.GetFileHealth(context.Background(), filePath)
	require.NoError(t, err)
	require.Equal(t, database.HealthStatusChecking, current.Status,
		"old detached claim cleanup cannot release the newer durable owner")

	cancelB()
	require.ErrorIs(t, receiveCHG014CheckError(t, resultB), context.Canceled)
	assert.False(t, env.hw.IsCheckActive(filePath))
	requireCHG014PendingDue(t, env, filePath, 0)
}
