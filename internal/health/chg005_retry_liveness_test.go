package health

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/testsupport/fakepool"
	"github.com/javi11/nntppool/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func forceHealthCheckDue(t *testing.T, env *repairTestEnv, filePath string) {
	t.Helper()
	_, err := env.db.ExecContext(context.Background(), `
		UPDATE file_health
		SET scheduled_check_at = datetime('now', '-1 second')
		WHERE file_path = ?
	`, filePath)
	require.NoError(t, err)
}

// installHealthClaimAudit observes each real checking transition without
// changing the repository API. It lets the omitted-file test distinguish a
// deferred/rescheduled claim from a row that was simply never selected.
func installHealthClaimAudit(t *testing.T, env *repairTestEnv) {
	t.Helper()
	_, err := env.db.ExecContext(context.Background(), `
		CREATE TABLE health_claim_audit (file_path TEXT NOT NULL);
		CREATE TRIGGER health_claim_audit_insert
		AFTER UPDATE OF status ON file_health
		WHEN NEW.status = 'checking' AND OLD.status <> 'checking'
		BEGIN
			INSERT INTO health_claim_audit (file_path) VALUES (NEW.file_path);
		END;
	`)
	require.NoError(t, err)
}

func healthClaimAuditCount(t *testing.T, env *repairTestEnv, filePath string) int {
	t.Helper()
	var count int
	err := env.db.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM health_claim_audit WHERE file_path = ?", filePath,
	).Scan(&count)
	require.NoError(t, err)
	return count
}

func requirePendingHealthReschedule(
	t *testing.T,
	env *repairTestEnv,
	filePath string,
	cycleStarted time.Time,
) *database.FileHealth {
	t.Helper()

	row, err := env.healthRepo.GetFileHealth(context.Background(), filePath)
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Equal(t, database.HealthStatusPending, row.Status)
	require.NotNil(t, row.ScheduledCheckAt,
		"inconclusive work must be given a durable next attempt")

	cycleFinished := time.Now().UTC()
	assert.True(t, row.ScheduledCheckAt.After(cycleFinished),
		"next attempt %s must remain in the future after the cycle finished at %s",
		row.ScheduledCheckAt, cycleFinished)
	assert.False(t, row.ScheduledCheckAt.After(cycleStarted.Add(16*time.Minute)),
		"zero-retry reschedule %s exceeded the bounded 15-minute backoff",
		row.ScheduledCheckAt)
	return row
}

func TestCHG005TemporaryChecksStayPendingWithoutConsumingRetryBudget(t *testing.T) {
	client := fakepool.New()
	client.SetDefaultBehavior(fakepool.SegmentBehavior{
		Err: &nntppool.TransportError{
			Kind:  nntppool.OutcomeTemporaryFailure,
			Cause: errors.New("provider temporarily unavailable"),
		},
	})
	env := newBatchTestEnv(t, t.TempDir(), client)
	const (
		filePath   = "complete/temporary.bin"
		maxRetries = 3
	)
	writeHealthyFile(t, env, filePath)
	insertFileHealth(t, env.db, filePath, "", 0, maxRetries)

	var row *database.FileHealth
	for cycle := 0; cycle < maxRetries+1; cycle++ {
		cycleStarted := time.Now().UTC()
		require.NoError(t, env.hw.runHealthCheckCycle(context.Background()), "cycle %d", cycle)
		row = requirePendingHealthReschedule(t, env, filePath, cycleStarted)
		assert.Zero(t, row.RetryCount,
			"temporary/inconclusive cycle %d consumed the conclusive retry budget", cycle)
		if cycle < maxRetries {
			forceHealthCheckDue(t, env, filePath)
		}
	}

	assert.Equal(t, database.HealthStatusPending, row.Status)
	assert.Zero(t, row.RetryCount,
		"temporary/inconclusive checks must not consume the conclusive retry budget")
	assert.Equal(t, int64(maxRetries+1), client.StatCalls(),
		"a pending inconclusive row must remain selectable on every due cycle")
}

func TestCHG005RemovedChecksRescheduleAndRemainRetryable(t *testing.T) {
	client := fakepool.New()
	env := newBatchTestEnv(t, t.TempDir(), client)
	const (
		filePath   = "complete/removed.bin"
		maxRetries = 3
	)
	insertFileHealth(t, env.db, filePath, "", 0, maxRetries)
	installHealthClaimAudit(t, env)

	var row *database.FileHealth
	for cycle := 0; cycle < maxRetries+1; cycle++ {
		cycleStarted := time.Now().UTC()
		require.NoError(t, env.hw.runHealthCheckCycle(context.Background()), "cycle %d", cycle)
		row = requirePendingHealthReschedule(t, env, filePath, cycleStarted)
		assert.Zero(t, row.RetryCount,
			"missing-metadata cycle %d consumed the conclusive retry budget", cycle)
		if cycle < maxRetries {
			forceHealthCheckDue(t, env, filePath)
		}
	}

	assert.Equal(t, database.HealthStatusPending, row.Status)
	assert.Zero(t, row.RetryCount,
		"missing metadata is inconclusive and must not consume a retry")
	assert.Equal(t, maxRetries+1, healthClaimAuditCount(t, env, filePath),
		"a removed row must be claimed and rescheduled on every due cycle")
	assert.Zero(t, client.StatCalls(), "missing metadata must not issue a network STAT")
}

func TestCHG005ConclusiveCorruptionStillReachesTerminalState(t *testing.T) {
	client := fakepool.New()
	client.SetDefaultBehavior(fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	env := newBatchTestEnv(t, t.TempDir(), client)
	const (
		filePath   = "complete/conclusive.bin"
		maxRetries = 3
	)
	writeHealthyFile(t, env, filePath)
	insertFileHealth(t, env.db, filePath, "", 0, maxRetries)

	for cycle := 0; cycle < maxRetries; cycle++ {
		if cycle > 0 {
			forceHealthCheckDue(t, env, filePath)
		}
		require.NoError(t, env.hw.runHealthCheckCycle(context.Background()), "cycle %d", cycle)
	}

	row, err := env.healthRepo.GetFileHealth(context.Background(), filePath)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, database.HealthStatusCorrupted, row.Status)
	assert.Equal(t, maxRetries-1, row.RetryCount,
		"terminal conclusive publication keeps the existing retry accounting")
	assert.Nil(t, row.ScheduledCheckAt, "terminal corruption must not remain scheduled")
	assert.Equal(t, int64(maxRetries), client.StatCalls())
}
