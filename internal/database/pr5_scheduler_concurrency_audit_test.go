package database

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pr5SchedulerConcurrencyFixture struct {
	db         *DB
	repo       *HealthStateRepository
	dialect    Dialect
	revision   *HealthFileRevision
	snapshotID string
	filePath   string
	token      string
	now        time.Time
}

func newPR5SchedulerConcurrencyFixture(
	t *testing.T,
	dialect Dialect,
) pr5SchedulerConcurrencyFixture {
	t.Helper()
	token := strings.ReplaceAll(uuid.NewString(), "-", "")
	var (
		db  *DB
		err error
	)
	if dialect == DialectPostgres {
		dsn := os.Getenv("ALTMOUNT_TEST_POSTGRES_DSN")
		if dsn == "" {
			t.Skip("ALTMOUNT_TEST_POSTGRES_DSN is not configured")
		}
		db, err = NewDB(Config{Type: "postgres", DSN: dsn})
	} else {
		db, err = NewDB(Config{
			Type:         "sqlite",
			DatabasePath: filepath.Join(t.TempDir(), "scheduler-concurrency.db"),
		})
	}
	require.NoError(t, err)

	repo := NewHealthStateRepository(db.Connection(), dialect)
	now := time.Unix(1_714_000_000, 0).UTC()
	repo.now = func() time.Time { return now }
	filePath := "library/pr5-scheduler-audit-" + token + ".mkv"
	revision, err := repo.EnsureFileRevision(context.Background(), FileRevisionSpec{
		FilePath:          filePath,
		LayoutFingerprint: "sha256:pr5-scheduler-audit-" + token,
		VirtualSize:       3200,
		SegmentCount:      32,
	})
	require.NoError(t, err)
	snapshotID := "pr5-scheduler-snapshot-" + token
	q := newDialectAwareDB(db.Connection(), dialect)
	_, err = q.ExecContext(context.Background(), `
		INSERT INTO health_provider_snapshots (id, created_at) VALUES (?, ?)
	`, snapshotID, now)
	require.NoError(t, err)

	fixture := pr5SchedulerConcurrencyFixture{
		db: db, repo: repo, dialect: dialect, revision: revision,
		snapshotID: snapshotID, filePath: filePath, token: token, now: now,
	}
	t.Cleanup(func() {
		q := newDialectAwareDB(db.Connection(), dialect)
		_, cleanupErr := q.ExecContext(context.Background(),
			`DELETE FROM file_health WHERE file_path = ?`, filePath)
		assert.NoError(t, cleanupErr)
		_, cleanupErr = q.ExecContext(context.Background(),
			`DELETE FROM health_provider_snapshots WHERE id = ?`, snapshotID)
		assert.NoError(t, cleanupErr)
		assert.NoError(t, db.Close())
	})
	return fixture
}

func (f pr5SchedulerConcurrencyFixture) scheduleSpec(
	id, dedupe, trigger string,
	priority HealthRunPriority,
) ScheduledHealthRunSpec {
	return ScheduledHealthRunSpec{
		Run: HealthRunSpec{
			ID:                 id,
			FileRevisionID:     f.revision.ID,
			ProviderSnapshotID: f.snapshotID,
			Trigger:            trigger,
			Mode:               "observation",
			TotalSegments:      f.revision.SegmentCount,
			CreatedAt:          f.now,
		},
		DedupeKey: dedupe,
		Priority:  priority,
		NotBefore: f.now,
	}
}

func installPR5ConcurrentCreateDelay(
	t *testing.T,
	fixture pr5SchedulerConcurrencyFixture,
) func() {
	t.Helper()
	if fixture.dialect == DialectPostgres {
		_, err := fixture.db.Connection().Exec(`
			CREATE OR REPLACE FUNCTION pr5_audit_delay_health_run_insert()
			RETURNS TRIGGER
			LANGUAGE plpgsql
			AS $$
			BEGIN
				IF NEW.trigger = 'concurrent_create_audit' THEN
					PERFORM pg_sleep(0.10);
				END IF;
				RETURN NEW;
			END;
			$$;
			DROP TRIGGER IF EXISTS pr5_audit_delay_health_run_insert ON health_runs;
			CREATE TRIGGER pr5_audit_delay_health_run_insert
			BEFORE INSERT ON health_runs
			FOR EACH ROW EXECUTE FUNCTION pr5_audit_delay_health_run_insert();
		`)
		require.NoError(t, err)
		return func() {
			_, cleanupErr := fixture.db.Connection().Exec(`
				DROP TRIGGER IF EXISTS pr5_audit_delay_health_run_insert ON health_runs;
				DROP FUNCTION IF EXISTS pr5_audit_delay_health_run_insert();
			`)
			assert.NoError(t, cleanupErr)
		}
	}
	_, err := fixture.db.Connection().Exec(`
		DROP TRIGGER IF EXISTS pr5_audit_delay_health_run_insert;
		CREATE TRIGGER pr5_audit_delay_health_run_insert
		BEFORE INSERT ON health_runs
		WHEN NEW.trigger = 'concurrent_create_audit'
		BEGIN
			SELECT randomblob(8000000);
		END;
	`)
	require.NoError(t, err)
	return func() {
		_, cleanupErr := fixture.db.Connection().Exec(
			`DROP TRIGGER IF EXISTS pr5_audit_delay_health_run_insert`)
		assert.NoError(t, cleanupErr)
	}
}

func runPR5ConcurrentEnsureConvergence(t *testing.T, dialect Dialect) {
	t.Helper()
	fixture := newPR5SchedulerConcurrencyFixture(t, dialect)
	removeDelay := installPR5ConcurrentCreateDelay(t, fixture)
	defer removeDelay()

	const workers = 8
	dedupe := "pr5-concurrent-create-" + fixture.token
	type result struct {
		run     *HealthRun
		created bool
		err     error
	}
	results := make(chan result, workers)
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(workers)
	for i := 0; i < workers; i++ {
		go func(worker int) {
			ready.Done()
			<-start
			run, created, err := fixture.repo.EnsureScheduledHealthRun(
				context.Background(),
				fixture.scheduleSpec(
					fmt.Sprintf("pr5-converge-%s-%02d", fixture.token, worker),
					dedupe,
					"concurrent_create_audit",
					HealthRunPriorityNormal,
				),
			)
			results <- result{run: run, created: created, err: err}
		}(i)
	}
	ready.Wait()
	close(start)

	createdCount := 0
	var retainedRunID string
	for i := 0; i < workers; i++ {
		result := <-results
		require.NoError(t, result.err,
			"concurrent ensure callers must converge instead of surfacing a uniqueness or busy error")
		require.NotNil(t, result.run)
		if retainedRunID == "" {
			retainedRunID = result.run.ID
		}
		assert.Equal(t, retainedRunID, result.run.ID)
		if result.created {
			createdCount++
		}
	}
	assert.Equal(t, 1, createdCount)

	q := newDialectAwareDB(fixture.db.Connection(), dialect)
	var activeSchedules, matchingRuns int
	require.NoError(t, q.QueryRowContext(context.Background(), `
		SELECT COUNT(*) FROM health_run_schedule
		WHERE dedupe_key = ? AND active = TRUE
	`, dedupe).Scan(&activeSchedules))
	require.NoError(t, q.QueryRowContext(context.Background(), `
		SELECT COUNT(*) FROM health_runs
		WHERE file_revision_id = ? AND trigger = 'concurrent_create_audit'
	`, fixture.revision.ID).Scan(&matchingRuns))
	assert.Equal(t, 1, activeSchedules)
	assert.Equal(t, 1, matchingRuns,
		"losing create attempts must roll back rather than leave orphan run history")
}

func TestPR5SQLiteConcurrentEnsureScheduledRunConverges(t *testing.T) {
	runPR5ConcurrentEnsureConvergence(t, DialectSQLite)
}

func TestPR5PostgresConcurrentEnsureScheduledRunConverges(t *testing.T) {
	runPR5ConcurrentEnsureConvergence(t, DialectPostgres)
}

func installPR5ConcurrentClaimDelay(
	t *testing.T,
	fixture pr5SchedulerConcurrencyFixture,
) func() {
	t.Helper()
	if fixture.dialect != DialectPostgres {
		return func() {}
	}
	_, err := fixture.db.Connection().Exec(`
		CREATE OR REPLACE FUNCTION pr5_audit_delay_health_run_claim()
		RETURNS TRIGGER
		LANGUAGE plpgsql
		AS $$
		BEGIN
			IF OLD.trigger = 'concurrent_claim_audit'
			   AND OLD.status = 'pending' AND NEW.status = 'running' THEN
				PERFORM pg_sleep(0.10);
			END IF;
			RETURN NEW;
		END;
		$$;
		DROP TRIGGER IF EXISTS pr5_audit_delay_health_run_claim ON health_runs;
		CREATE TRIGGER pr5_audit_delay_health_run_claim
		BEFORE UPDATE ON health_runs
		FOR EACH ROW EXECUTE FUNCTION pr5_audit_delay_health_run_claim();
	`)
	require.NoError(t, err)
	return func() {
		_, cleanupErr := fixture.db.Connection().Exec(`
			DROP TRIGGER IF EXISTS pr5_audit_delay_health_run_claim ON health_runs;
			DROP FUNCTION IF EXISTS pr5_audit_delay_health_run_claim();
		`)
		assert.NoError(t, cleanupErr)
	}
}

func runPR5ConcurrentDistinctDueClaims(t *testing.T, dialect Dialect) {
	t.Helper()
	fixture := newPR5SchedulerConcurrencyFixture(t, dialect)
	removeDelay := installPR5ConcurrentClaimDelay(t, fixture)
	defer removeDelay()

	const workers = 6
	ctx := context.Background()
	for i := 0; i < workers; i++ {
		_, created, err := fixture.repo.EnsureScheduledHealthRun(ctx,
			fixture.scheduleSpec(
				fmt.Sprintf("pr5-claim-%s-%02d", fixture.token, i),
				fmt.Sprintf("pr5-claim-dedupe-%s-%02d", fixture.token, i),
				"concurrent_claim_audit",
				HealthRunPriorityNormal,
			))
		require.NoError(t, err)
		require.True(t, created)
	}

	type claimResult struct {
		run *HealthRun
		err error
	}
	results := make(chan claimResult, workers)
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(workers)
	for i := 0; i < workers; i++ {
		go func(worker int) {
			ready.Done()
			<-start
			run, err := fixture.repo.ClaimDueHealthRun(
				context.Background(), fmt.Sprintf("claim-worker-%02d", worker), time.Minute)
			results <- claimResult{run: run, err: err}
		}(i)
	}
	ready.Wait()
	close(start)

	claimed := make(map[string]struct{}, workers)
	for i := 0; i < workers; i++ {
		result := <-results
		require.NoError(t, result.err)
		require.NotNil(t, result.run,
			"a worker must continue to another due row when its first candidate is claimed concurrently")
		if result.run != nil {
			claimed[result.run.ID] = struct{}{}
		}
	}
	assert.Len(t, claimed, workers,
		"simultaneous workers should claim distinct due runs without waiting for another scheduler tick")
}

func TestPR5SQLiteConcurrentWorkersClaimDistinctDueRuns(t *testing.T) {
	runPR5ConcurrentDistinctDueClaims(t, DialectSQLite)
}

func TestPR5PostgresConcurrentWorkersClaimDistinctDueRuns(t *testing.T) {
	runPR5ConcurrentDistinctDueClaims(t, DialectPostgres)
}
