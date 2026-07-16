package health

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/arrs"
	"github.com/javi11/altmount/internal/arrs/model"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/pool"
	"github.com/javi11/altmount/internal/testsupport/fakepool"
	nntppool "github.com/javi11/nntppool/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type discoveryMutationARRs struct {
	db       *sql.DB
	filePath string
	err      error
}

type sameStatusMutationARRs struct {
	db       *sql.DB
	filePath string
	err      error
}

func (m *sameStatusMutationARRs) TriggerFileRescan(context.Context, string, string, *string) error {
	return nil
}

func (m *sameStatusMutationARRs) DiscoverFileMetadata(context.Context, string, string, string, string) (*model.WebhookMetadata, error) {
	_, m.err = m.db.Exec(`
		UPDATE file_health
		SET metadata = '{"ownership":"changed"}'
		WHERE file_path = ? AND status = 'pending'
	`, m.filePath)
	return nil, m.err
}

func (m *discoveryMutationARRs) TriggerFileRescan(context.Context, string, string, *string) error {
	return nil
}

func (m *discoveryMutationARRs) DiscoverFileMetadata(context.Context, string, string, string, string) (*model.WebhookMetadata, error) {
	_, m.err = m.db.Exec(`
		UPDATE file_health
		SET status = 'healthy', scheduled_check_at = datetime('now', '+1 hour')
		WHERE file_path = ?
	`, m.filePath)
	return nil, m.err
}

type claimRevokingClient struct {
	*fakepool.Client
	db       *sql.DB
	filePath string
}

func (c *claimRevokingClient) StatMany(ctx context.Context, messageIDs []string, opts nntppool.StatManyOptions) <-chan nntppool.StatManyResult {
	// This ownership-relevant write lands after the worker's checking claim but
	// before hard-absence evidence returns. Migration 036 must revoke that claim.
	_, err := c.db.Exec(`UPDATE file_health SET metadata = '{}' WHERE file_path = ?`, c.filePath)
	if err != nil {
		out := make(chan nntppool.StatManyResult, 1)
		out <- nntppool.StatManyResult{Err: err}
		close(out)
		return out
	}
	return c.Client.StatMany(ctx, messageIDs, opts)
}

type destructiveClaimFixture struct {
	env          *repairTestEnv
	client       *fakepool.Client
	filePath     string
	libraryPath  string
	metadataPath string
}

func newDestructiveClaimFixture(t *testing.T, client pool.NntpClient) *destructiveClaimFixture {
	t.Helper()

	metadataRoot := t.TempDir()
	libraryRoot := t.TempDir()
	env := newRepairTestEnv(t, metadataRoot, nil)
	env.hw.configGetter().Health.CorruptionAction = "delete"
	env.hw.configGetter().Health.LibraryDir = &libraryRoot

	baseClient, ok := client.(*fakepool.Client)
	if !ok {
		if wrapped, wrappedOK := client.(*claimRevokingClient); wrappedOK {
			baseClient = wrapped.Client
		}
	}
	require.NotNil(t, baseClient)
	baseClient.SetDefaultBehavior(fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

	pm := &fakeClientPoolManager{client: client}
	env.healthChecker = NewHealthChecker(
		env.healthRepo,
		env.metadataService,
		pm,
		env.hw.configGetter,
		&MockRcloneClient{},
	)
	env.hw = NewHealthWorker(
		env.healthChecker,
		env.healthRepo,
		env.metadataService,
		env.mockARRs,
		&mockImportService{},
		env.hw.configGetter,
		nil,
	)

	filePath := "complete/fenced/show.s01e01.mkv"
	libraryPath := filepath.Join(libraryRoot, "fenced", "show.s01e01.mkv")
	require.NoError(t, os.MkdirAll(filepath.Dir(libraryPath), 0o755))
	require.NoError(t, os.WriteFile(libraryPath, []byte("current library content"), 0o644))
	writeHealthyFile(t, env, filePath)
	insertFileHealth(t, env.db, filePath, libraryPath, 0, 3)

	return &destructiveClaimFixture{
		env:          env,
		client:       baseClient,
		filePath:     filePath,
		libraryPath:  libraryPath,
		metadataPath: env.metadataService.GetMetadataFilePath(filePath),
	}
}

func (f *destructiveClaimFixture) assertPreserved(t *testing.T) {
	t.Helper()
	_, err := os.Stat(f.metadataPath)
	require.NoError(t, err, "metadata must remain when destructive eligibility is absent")
	_, err = os.Stat(f.libraryPath)
	require.NoError(t, err, "library content must remain when destructive eligibility is absent")
	record, err := f.env.healthRepo.GetFileHealth(context.Background(), f.filePath)
	require.NoError(t, err)
	require.NotNil(t, record, "health state must remain when destructive eligibility is absent")
}

func createFailCheckingTrigger(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`
		CREATE TRIGGER fail_checking_claim
		BEFORE UPDATE OF health_claim_token ON file_health
		WHEN NEW.health_claim_token IS NOT NULL
		BEGIN
			SELECT RAISE(FAIL, 'synthetic checking claim failure');
		END;
	`)
	require.NoError(t, err)
}

func createFailClaimConsumptionTrigger(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`
		CREATE TRIGGER fail_claim_consumption
		BEFORE UPDATE OF health_claim_token ON file_health
		WHEN OLD.health_claim_token IS NOT NULL
		 AND NEW.health_claim_token IS NULL
		BEGIN
			SELECT RAISE(FAIL, 'synthetic claim consumption failure');
		END;
	`)
	require.NoError(t, err)
}

func TestHealthCycleClaimFailureStopsBeforeSweepAndDeletion(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	createFailCheckingTrigger(t, fixture.env.db)

	err := fixture.env.hw.runHealthCheckCycle(context.Background())
	assert.Error(t, err, "claim failure must fail the cycle closed")
	assert.Equal(t, int64(0), client.StatCalls(), "failed claim must prevent the hard-absence sweep")
	fixture.assertPreserved(t)
}

func TestMissingMetadataDeletionRequiresConsumedClaim(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	require.NoError(t, os.Remove(fixture.metadataPath))
	createFailClaimConsumptionTrigger(t, fixture.env.db)

	err := fixture.env.hw.runHealthCheckCycle(context.Background())
	assert.Error(t, err)
	record, getErr := fixture.env.healthRepo.GetFileHealth(context.Background(), fixture.filePath)
	require.NoError(t, getErr)
	assert.NotNil(t, record, "missing metadata is evidence only until exact claim consumption succeeds")
	_, statErr := os.Stat(fixture.libraryPath)
	require.NoError(t, statErr)
}

func TestHealthCycleMixedBatchOmitsLostClaimAndPreservesSiblingAlignment(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)

	secondPath := "complete/fenced/show.s01e02.mkv"
	secondLibraryPath := filepath.Join(filepath.Dir(fixture.libraryPath), "show.s01e02.mkv")
	require.NoError(t, os.WriteFile(secondLibraryPath, []byte("second current library content"), 0o644))
	writeHealthyFile(t, fixture.env, secondPath)
	insertFileHealth(t, fixture.env.db, secondPath, secondLibraryPath, 0, 3)
	secondMetadataPath := fixture.env.metadataService.GetMetadataFilePath(secondPath)

	_, err := fixture.env.db.Exec(`
		CREATE TRIGGER fail_one_checking_claim
		BEFORE UPDATE OF health_claim_token ON file_health
		WHEN NEW.health_claim_token IS NOT NULL
		 AND NEW.file_path = 'complete/fenced/show.s01e02.mkv'
		BEGIN
			SELECT RAISE(FAIL, 'synthetic second-row claim failure');
		END;
	`)
	require.NoError(t, err)

	err = fixture.env.hw.runHealthCheckCycle(context.Background())
	assert.Error(t, err, "the lost candidate must be reported without discarding the owned sibling")
	assert.Equal(t, int64(1), client.StatCalls(), "StatMany must omit the lost claim without shifting sibling results")
	_, err = os.Stat(fixture.metadataPath)
	assert.True(t, os.IsNotExist(err), "the independently owned sibling may complete its configured deletion")
	_, err = os.Stat(secondMetadataPath)
	require.NoError(t, err, "the failed candidate metadata must remain")
	_, err = os.Stat(secondLibraryPath)
	require.NoError(t, err, "the failed candidate library content must remain")
	secondRecord, err := fixture.env.healthRepo.GetFileHealth(context.Background(), secondPath)
	require.NoError(t, err)
	require.NotNil(t, secondRecord)
	assert.NotEqual(t, database.HealthStatusChecking, secondRecord.Status, "the lost candidate must remain eligible for a later attempt")
}

func TestHealthCycleSameStatusOwnershipMutationCannotBeClaimed(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	mutator := &sameStatusMutationARRs{db: fixture.env.db, filePath: fixture.filePath}
	fixture.env.hw.arrsService = mutator

	err := fixture.env.hw.runHealthCheckCycle(context.Background())
	assert.Error(t, err, "same-status ownership mutation must invalidate the selected snapshot")
	require.NoError(t, mutator.err)
	assert.Equal(t, int64(0), client.StatCalls(), "a pre-claim ownership mutation must prevent STAT")
	fixture.assertPreserved(t)
}

func TestHealthCycleStaleSelectedRowRollsBackThenCanProgress(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	mutator := &discoveryMutationARRs{db: fixture.env.db, filePath: fixture.filePath}
	fixture.env.hw.arrsService = mutator

	err := fixture.env.hw.runHealthCheckCycle(context.Background())
	assert.Error(t, err, "selection made stale during discovery must not be claimed")
	require.NoError(t, mutator.err)
	assert.Equal(t, int64(0), client.StatCalls(), "stale claim must prevent the hard-absence sweep")
	fixture.assertPreserved(t)

	// Once a later actor deliberately re-arms the row, a new unique claim must
	// succeed and the next cycle must make progress rather than strand the batch.
	fixture.env.hw.arrsService = fixture.env.mockARRs
	_, err = fixture.env.db.Exec(`
		UPDATE file_health
		SET status = 'pending', scheduled_check_at = datetime('now', '-1 second'), metadata = NULL
		WHERE file_path = ?
	`, fixture.filePath)
	require.NoError(t, err)
	require.NoError(t, fixture.env.hw.runHealthCheckCycle(context.Background()))
	assert.Equal(t, int64(1), client.StatCalls(), "freshly re-armed row should be checked once")
	_, err = os.Stat(fixture.metadataPath)
	assert.True(t, os.IsNotExist(err), "a current claim may authorize configured deletion")
}

func TestHealthCycleRevokedClaimCannotAuthorizeDeletion(t *testing.T) {
	base := fakepool.New()
	wrapped := &claimRevokingClient{Client: base}
	fixture := newDestructiveClaimFixture(t, wrapped)
	wrapped.db = fixture.env.db
	wrapped.filePath = fixture.filePath

	err := fixture.env.hw.runHealthCheckCycle(context.Background())
	assert.Error(t, err, "ownership-relevant write after the sweep claim must revoke deletion eligibility")
	assert.Equal(t, int64(1), base.StatCalls(), "the already-admitted sweep may finish")
	fixture.assertPreserved(t)
}

func TestDirectCheckClaimFailureCannotDelete(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	_, err := fixture.env.db.Exec(`UPDATE file_health SET status = 'checking' WHERE file_path = ?`, fixture.filePath)
	require.NoError(t, err)
	createFailCheckingTrigger(t, fixture.env.db)

	err = fixture.env.hw.performDirectCheck(context.Background(), fixture.filePath)
	assert.Error(t, err, "direct check must acquire destructive ownership")
	assert.Equal(t, int64(0), client.StatCalls(), "failed direct claim must prevent its sweep")
	fixture.assertPreserved(t)
}

func TestRepairNotificationClaimFailureCannotDelete(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	_, err := fixture.env.db.Exec(`
		UPDATE file_health
		SET status = 'repair_triggered', scheduled_check_at = datetime('now', '-1 second')
		WHERE file_path = ?
	`, fixture.filePath)
	require.NoError(t, err)
	createFailCheckingTrigger(t, fixture.env.db)

	err = fixture.env.hw.runHealthCheckCycle(context.Background())
	assert.Error(t, err, "repair notification deletion must acquire destructive ownership")
	assert.Equal(t, int64(0), client.StatCalls(), "repair notification must not perform an ordinary sweep")
	fixture.assertPreserved(t)

	record, getErr := fixture.env.healthRepo.GetFileHealth(context.Background(), fixture.filePath)
	require.NoError(t, getErr)
	require.NotNil(t, record)
	assert.Equal(t, database.HealthStatusRepairTriggered, record.Status)
	assert.Empty(t, fixture.env.mockARRs.calls, "a lost repair claim must not reach ARR")
}

func TestRepairNotificationRequiresConsumedClaimBeforeARRAndMove(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	fixture.env.hw.configGetter().Health.CorruptionAction = "repair"
	_, err := fixture.env.db.Exec(`
		UPDATE file_health
		SET status = 'repair_triggered', scheduled_check_at = datetime('now', '-1 second')
		WHERE file_path = ?
	`, fixture.filePath)
	require.NoError(t, err)
	createFailClaimConsumptionTrigger(t, fixture.env.db)

	err = fixture.env.hw.runHealthCheckCycle(context.Background())
	assert.Error(t, err)
	assert.Empty(t, fixture.env.mockARRs.calls, "repair notification must consume its exact claim before ARR")
	fixture.assertPreserved(t)
}

func TestDirectCheckOnlyOneConcurrentClaimantCanReachSTAT(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	_, err := fixture.env.db.Exec(`UPDATE file_health SET status = 'checking' WHERE file_path = ?`, fixture.filePath)
	require.NoError(t, err)

	release := make(chan struct{})
	client.BlockUntil(release)
	firstDone := make(chan error, 1)
	go func() { firstDone <- fixture.env.hw.performDirectCheck(context.Background(), fixture.filePath) }()
	require.Eventually(t, func() bool { return client.InFlight() == 1 }, time.Second, time.Millisecond)

	secondDone := make(chan error, 1)
	go func() { secondDone <- fixture.env.hw.performDirectCheck(context.Background(), fixture.filePath) }()
	time.Sleep(25 * time.Millisecond)
	assert.Equal(t, int32(1), client.InFlight(), "the second claimant must fail before STAT")
	close(release)

	firstErr := <-firstDone
	secondErr := <-secondDone
	assert.Equal(t, int64(1), client.StatCalls(), "one durable claim permits one sweep")
	assert.True(t, (firstErr == nil) != (secondErr == nil), "exactly one claimant should own the check: first=%v second=%v", firstErr, secondErr)
}

func TestBackgroundClaimFailureDoesNotClobberForeignOwner(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	_, err := fixture.env.db.Exec(`
		UPDATE file_health
		SET status = 'checking', health_claim_token = 'foreign-owner', health_claim_version = health_claim_version + 1
		WHERE file_path = ?
	`, fixture.filePath)
	require.NoError(t, err)
	fixture.env.hw.mu.Lock()
	fixture.env.hw.running = true
	fixture.env.hw.mu.Unlock()

	require.NoError(t, fixture.env.hw.PerformBackgroundCheck(context.Background(), fixture.filePath))
	require.Eventually(t, func() bool {
		var status, token string
		queryErr := fixture.env.db.QueryRow(`SELECT status, health_claim_token FROM file_health WHERE file_path = ?`, fixture.filePath).Scan(&status, &token)
		return queryErr == nil && status == "checking" && token == "foreign-owner" && !fixture.env.hw.IsCheckActive(fixture.filePath)
	}, time.Second, 5*time.Millisecond, "a failed background admission must not reset another owner's row")
	assert.Equal(t, int64(0), client.StatCalls())
}

func TestBackgroundCheckClaimsSynchronouslyBeforeReturning(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	_, err := fixture.env.db.Exec(`UPDATE file_health SET status = 'checking' WHERE file_path = ?`, fixture.filePath)
	require.NoError(t, err)
	createFailCheckingTrigger(t, fixture.env.db)
	fixture.env.hw.mu.Lock()
	fixture.env.hw.running = true
	fixture.env.hw.mu.Unlock()

	err = fixture.env.hw.PerformBackgroundCheck(context.Background(), fixture.filePath)
	assert.Error(t, err, "manual admission failure must be returned before a background goroutine is accepted")
	assert.False(t, fixture.env.hw.IsCheckActive(fixture.filePath))
	assert.Equal(t, int64(0), client.StatCalls())
}

func TestHealthCycleFinalPublicationFailureReleasesOwnedClaimInProcess(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	client.SetDefaultBehavior(fakepool.SegmentBehavior{})
	_, err := fixture.env.db.Exec(`
		CREATE TRIGGER fail_healthy_publication
		BEFORE UPDATE OF status ON file_health
		WHEN OLD.health_claim_token IS NOT NULL AND NEW.status = 'healthy'
		BEGIN
			SELECT RAISE(FAIL, 'synthetic final publication failure');
		END;
	`)
	require.NoError(t, err)

	err = fixture.env.hw.runHealthCheckCycle(context.Background())
	assert.Error(t, err)
	var status string
	var token sql.NullString
	require.NoError(t, fixture.env.db.QueryRow(`SELECT status, health_claim_token FROM file_health WHERE file_path = ?`, fixture.filePath).Scan(&status, &token))
	assert.Equal(t, "pending", status, "failed publication must restore the owned phase without restart")
	assert.False(t, token.Valid, "failed publication must release only its own token")

	_, err = fixture.env.db.Exec(`DROP TRIGGER fail_healthy_publication`)
	require.NoError(t, err)
	require.NoError(t, fixture.env.hw.runHealthCheckCycle(context.Background()))
	assert.Equal(t, int64(2), client.StatCalls(), "the released row must make progress in the same process")
}

func TestRepairExhaustedMoveRequiresConsumedClaim(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	fixture.env.hw.configGetter().Health.CorruptionAction = "repair"
	_, err := fixture.env.db.Exec(`
		UPDATE file_health SET retry_count = 2, repair_retry_count = 3 WHERE file_path = ?
	`, fixture.filePath)
	require.NoError(t, err)
	createFailClaimConsumptionTrigger(t, fixture.env.db)

	err = fixture.env.hw.runHealthCheckCycle(context.Background())
	assert.Error(t, err)
	fixture.assertPreserved(t)
}

func TestRepairTriggerMoveRequiresConsumedClaim(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	fixture.env.hw.configGetter().Health.CorruptionAction = "repair"
	_, err := fixture.env.db.Exec(`UPDATE file_health SET retry_count = 2 WHERE file_path = ?`, fixture.filePath)
	require.NoError(t, err)
	createFailClaimConsumptionTrigger(t, fixture.env.db)

	err = fixture.env.hw.runHealthCheckCycle(context.Background())
	assert.Error(t, err)
	fixture.assertPreserved(t)
}

func TestRepairReplacementCleanupRequiresConsumedClaim(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	fixture.env.hw.configGetter().Health.CorruptionAction = "repair"
	fixture.env.mockARRs.returnErr = arrs.ErrEpisodeAlreadySatisfied
	_, err := fixture.env.db.Exec(`UPDATE file_health SET retry_count = 2 WHERE file_path = ?`, fixture.filePath)
	require.NoError(t, err)
	createFailClaimConsumptionTrigger(t, fixture.env.db)

	err = fixture.env.hw.runHealthCheckCycle(context.Background())
	assert.Error(t, err)
	fixture.assertPreserved(t)
}

func TestMetadataRegenerationRequiresConsumedClaim(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	fixture.env.hw.configGetter().Health.CorruptionAction = "repair"
	importer := &mockImportService{}
	fixture.env.hw.importerService = importer
	require.NoError(t, os.WriteFile(fixture.metadataPath, []byte("not protobuf"), 0o644))
	_, err := fixture.env.db.Exec(`
		UPDATE file_health
		SET library_path = file_path, retry_count = 2
		WHERE file_path = ?
	`, fixture.filePath)
	require.NoError(t, err)
	createFailClaimConsumptionTrigger(t, fixture.env.db)

	err = fixture.env.hw.runHealthCheckCycle(context.Background())
	assert.Error(t, err)
	assert.Equal(t, int64(0), importer.calls.Load(), "metadata regeneration must follow checked claim consumption")
}

func TestHealthClaimReleaseDoesNotRaceAcrossConcurrentCleanup(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	client.SetDefaultBehavior(fakepool.SegmentBehavior{Latency: 20 * time.Millisecond})

	var wg sync.WaitGroup
	wg.Add(2)
	for range 2 {
		go func() {
			defer wg.Done()
			_ = fixture.env.hw.runHealthCheckCycle(context.Background())
		}()
	}
	wg.Wait()
	assert.LessOrEqual(t, client.StatCalls(), int64(1), "cleanup by a losing claimant must not release the winner")
}

func TestRestartClearsClaimsButPreservesDurableRepairPhase(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	const repairPath = "complete/fenced/repair-phase.mkv"
	_, err := fixture.env.db.Exec(`
		UPDATE file_health
		SET status = 'checking', health_claim_token = 'checking-owner', health_claim_version = 7
		WHERE file_path = ?
	`, fixture.filePath)
	require.NoError(t, err)
	_, err = fixture.env.db.Exec(`
		INSERT INTO file_health
			(file_path, status, scheduled_check_at, health_claim_token, health_claim_version)
		VALUES (?, 'repair_triggered', datetime('now', '-1 second'), 'repair-owner', 11)
	`, repairPath)
	require.NoError(t, err)

	require.NoError(t, fixture.env.healthRepo.ResetFileAllChecking(context.Background()))
	rows, err := fixture.env.db.Query(`
		SELECT file_path, status, health_claim_token FROM file_health
		WHERE file_path IN (?, ?) ORDER BY file_path
	`, fixture.filePath, repairPath)
	require.NoError(t, err)
	defer rows.Close()
	got := map[string]struct {
		status string
		token  sql.NullString
	}{}
	for rows.Next() {
		var path, status string
		var token sql.NullString
		require.NoError(t, rows.Scan(&path, &status, &token))
		got[path] = struct {
			status string
			token  sql.NullString
		}{status: status, token: token}
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, "pending", got[fixture.filePath].status)
	assert.False(t, got[fixture.filePath].token.Valid)
	assert.Equal(t, "repair_triggered", got[repairPath].status, "migration-035 repair phase must remain durable across restart")
	assert.False(t, got[repairPath].token.Valid, "restart may clear abandoned ownership without erasing durable phase")
}
