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
	db             *sql.DB
	filePath       string
	newLibraryPath string
	err            error
}

func (m *sameStatusMutationARRs) TriggerFileRescan(context.Context, string, string, *string) error {
	return nil
}

func (m *sameStatusMutationARRs) DiscoverFileMetadata(context.Context, string, string, string, string) (*model.WebhookMetadata, error) {
	_, m.err = m.db.Exec(`
		UPDATE file_health
		SET library_path = ?
		WHERE file_path = ? AND status = 'pending'
	`, m.newLibraryPath, m.filePath)
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

// finalizationReplacingClient waits for the underlying STAT sweep to finish,
// installs a replacement owner, and only then releases the results to the
// worker. This pins the ownership change between transport evidence and final
// publication without adding a production-only test hook.
type finalizationReplacingClient struct {
	*fakepool.Client
	db       *sql.DB
	filePath string
	err      error
}

func (c *finalizationReplacingClient) StatMany(ctx context.Context, messageIDs []string, opts nntppool.StatManyOptions) <-chan nntppool.StatManyResult {
	in := c.Client.StatMany(ctx, messageIDs, opts)
	out := make(chan nntppool.StatManyResult, len(messageIDs))
	go func() {
		defer close(out)
		results := make([]nntppool.StatManyResult, 0, len(messageIDs))
		for result := range in {
			results = append(results, result)
		}

		_, c.err = c.db.Exec(`
			UPDATE file_health
			SET status = 'checking',
			    metadata = '{"owner":"b"}',
			    health_claim_token = 'owner-b',
			    scheduled_check_at = '2042-03-04 05:06:07',
			    last_error = 'owner-b-error',
			    error_details = '{"owner":"b","evidence":"complete"}'
			WHERE file_path = ?
		`, c.filePath)
		if c.err != nil {
			return
		}
		for _, result := range results {
			select {
			case out <- result:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
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
		switch wrapped := client.(type) {
		case *claimRevokingClient:
			baseClient = wrapped.Client
		case *finalizationReplacingClient:
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

func createFailNextClaimRotationTrigger(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`
		CREATE TRIGGER fail_next_claim_rotation
		BEFORE UPDATE OF health_claim_token ON file_health
		WHEN OLD.health_claim_token IS NOT NULL
		 AND NEW.health_claim_token IS NOT NULL
		 AND NEW.health_claim_token != OLD.health_claim_token
		 AND (SELECT COUNT(*) FROM test_health_claim_audit) >= 1
		BEGIN
			SELECT RAISE(FAIL, 'synthetic claim rotation failure');
		END;
	`)
	require.NoError(t, err)
}

func createFailSecondClaimRotationTrigger(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`
		CREATE TRIGGER fail_second_claim_rotation
		BEFORE UPDATE OF health_claim_token ON file_health
		WHEN OLD.health_claim_token IS NOT NULL
		 AND NEW.health_claim_token IS NOT NULL
		 AND NEW.health_claim_token != OLD.health_claim_token
		 AND (SELECT COUNT(*) FROM test_health_claim_audit) >= 2
		BEGIN
			SELECT RAISE(FAIL, 'synthetic destructive rotation failure');
		END;
	`)
	require.NoError(t, err)
}

func createIgnoreSecondClaimRotationTrigger(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`
		CREATE TRIGGER ignore_second_claim_rotation
		BEFORE UPDATE OF health_claim_token ON file_health
		WHEN OLD.health_claim_token IS NOT NULL
		 AND NEW.health_claim_token IS NOT NULL
		 AND NEW.health_claim_token != OLD.health_claim_token
		 AND (SELECT COUNT(*) FROM test_health_claim_audit) >= 2
		BEGIN
			SELECT RAISE(IGNORE);
		END;
	`)
	require.NoError(t, err)
}

const postARRFreshRotationSentinel = "facore_post_arr_fresh_rotation_attempt"

func installRejectFreshClaimRotationWithSentinel(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TRIGGER reject_fresh_claim_rotation_with_sentinel
		BEFORE UPDATE OF health_claim_token ON file_health
		WHEN OLD.health_claim_token IS NOT NULL
		 AND NEW.health_claim_token IS NOT NULL
		 AND NEW.health_claim_token != OLD.health_claim_token
		BEGIN
			SELECT RAISE(FAIL, 'facore_post_arr_fresh_rotation_attempt');
		END;
	`)
	return err
}

func assertPostARRFreshRotationSentinel(t *testing.T, err error) {
	t.Helper()
	assert.Error(t, err, "post-ARR authority must attempt a checked distinct-token rotation")
	errText := ""
	if err != nil {
		errText = err.Error()
	}
	assert.Contains(t, errText, postARRFreshRotationSentinel,
		"only the distinct-token trigger sentinel proves the post-ARR rotation was attempted")
}

func createReplaceOwnerInsideClaimTrigger(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`
		CREATE TRIGGER replace_owner_inside_claim
		AFTER UPDATE OF health_claim_token ON file_health
		WHEN OLD.health_claim_token IS NULL
		 AND NEW.health_claim_token IS NOT NULL
		BEGIN
			UPDATE file_health
			SET library_path = '/owner-b/library.mkv',
			    metadata = '{"owner":"b"}',
			    health_claim_token = 'owner-b',
			    scheduled_check_at = '2042-03-04 05:06:07',
			    last_error = 'owner-b-error',
			    error_details = '{"owner":"b","evidence":"replacement"}'
			WHERE id = NEW.id;
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

func TestMissingMetadataDeletionRequiresFreshDestructiveRotation(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	require.NoError(t, os.Remove(fixture.metadataPath))
	createFailNextClaimRotationTrigger(t, fixture.env.db)

	err := fixture.env.hw.runHealthCheckCycle(context.Background())
	assert.Error(t, err)
	record, getErr := fixture.env.healthRepo.GetFileHealth(context.Background(), fixture.filePath)
	require.NoError(t, getErr)
	assert.NotNil(t, record, "missing metadata is evidence only until a fresh destructive rotation succeeds")
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

func TestHealthCycleClaimsAndUsesFreshRowAfterPreClaimMutation(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	newLibraryPath := filepath.Join(filepath.Dir(fixture.libraryPath), "current-show.s01e01.mkv")
	require.NoError(t, os.WriteFile(newLibraryPath, []byte("new current library content"), 0o644))
	mutator := &sameStatusMutationARRs{
		db:             fixture.env.db,
		filePath:       fixture.filePath,
		newLibraryPath: newLibraryPath,
	}
	fixture.env.hw.arrsService = mutator

	err := fixture.env.hw.runHealthCheckCycle(context.Background())
	require.NoError(t, err, "the worker may claim a current row after a pre-claim mutation")
	require.NoError(t, mutator.err)
	assert.Equal(t, int64(1), client.StatCalls())
	_, oldErr := os.Stat(fixture.libraryPath)
	require.NoError(t, oldErr, "the stale preselection library path must never be deleted")
	_, currentErr := os.Stat(newLibraryPath)
	assert.True(t, os.IsNotExist(currentErr), "the fresh row returned under the claim must drive the deletion")
}

func TestClaimTransactionRejectsReplacementBeforeTokenBoundReread(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	createReplaceOwnerInsideClaimTrigger(t, fixture.env.db)

	err := fixture.env.hw.runHealthCheckCycle(context.Background())
	assert.Error(t, err, "a claim whose token is replaced before its row is returned must roll back")
	assert.Equal(t, int64(0), client.StatCalls(), "an unlocked post-claim read must never authorize STAT")
	fixture.assertPreserved(t)

	var status, libraryPath string
	var token sql.NullString
	require.NoError(t, fixture.env.db.QueryRow(`
		SELECT status, library_path, health_claim_token
		FROM file_health WHERE file_path = ?
	`, fixture.filePath).Scan(&status, &libraryPath, &token))
	assert.Equal(t, "pending", status, "the failed claim transaction must restore the pre-claim phase")
	assert.Equal(t, fixture.libraryPath, libraryPath, "an unowned replacement row must not escape a failed claim transaction")
	assert.False(t, token.Valid, "the rejected claim and trigger replacement must both roll back")
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

func TestRepairNotificationRequiresFreshRotationBeforeARRAndMove(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	fixture.env.hw.configGetter().Health.CorruptionAction = "repair"
	_, err := fixture.env.db.Exec(`
		UPDATE file_health
		SET status = 'repair_triggered', scheduled_check_at = datetime('now', '-1 second')
		WHERE file_path = ?
	`, fixture.filePath)
	require.NoError(t, err)
	createFailNextClaimRotationTrigger(t, fixture.env.db)

	err = fixture.env.hw.runHealthCheckCycle(context.Background())
	assert.Error(t, err)
	assert.Empty(t, fixture.env.mockARRs.calls, "repair notification must freshly rotate its exact claim before ARR")
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

func TestBackgroundClaimFailureCannotReleaseOrClobberForeignOwner(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	_, err := fixture.env.db.Exec(`
		UPDATE file_health
		SET status = 'checking', health_claim_token = 'foreign-owner'
		WHERE file_path = ?
	`, fixture.filePath)
	require.NoError(t, err)
	fixture.env.hw.mu.Lock()
	fixture.env.hw.running = true
	fixture.env.hw.mu.Unlock()

	err = fixture.env.hw.PerformBackgroundCheck(context.Background(), fixture.filePath)
	assert.Error(t, err, "foreign ownership must reject background admission synchronously")
	var status, token string
	require.NoError(t, fixture.env.db.QueryRow(`SELECT status, health_claim_token FROM file_health WHERE file_path = ?`, fixture.filePath).Scan(&status, &token))
	assert.Equal(t, "checking", status)
	assert.Equal(t, "foreign-owner", token, "a losing worker must not clear or rotate the winner's token")
	assert.False(t, fixture.env.hw.IsCheckActive(fixture.filePath))
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

func TestBackgroundCheckStopCancelsTrackedClaimBeforeReturning(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	client.SetDefaultBehavior(fakepool.SegmentBehavior{Latency: time.Hour})

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	t.Cleanup(cancelWorker)
	require.NoError(t, fixture.env.hw.Start(workerCtx))
	require.NoError(t, fixture.env.hw.PerformBackgroundCheck(context.Background(), fixture.filePath))
	require.Eventually(t, func() bool {
		return client.InFlight() == 1 && fixture.env.hw.IsCheckActive(fixture.filePath)
	}, time.Second, time.Millisecond)

	stopDone := make(chan error, 1)
	go func() { stopDone <- fixture.env.hw.Stop(context.Background()) }()
	select {
	case err := <-stopDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return after canceling the admitted background check")
	}

	assert.Zero(t, client.InFlight(), "worker shutdown must cancel admitted background transport")
	assert.False(t, fixture.env.hw.IsCheckActive(fixture.filePath))
	var status string
	var token sql.NullString
	require.NoError(t, fixture.env.db.QueryRow(`
		SELECT status, health_claim_token FROM file_health WHERE file_path = ?
	`, fixture.filePath).Scan(&status, &token))
	assert.Equal(t, "pending", status, "shutdown cancellation must re-arm an owned checking row")
	assert.False(t, token.Valid, "Stop must not return while its background claim remains installed")
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

func TestRepairExhaustedMoveRequiresFreshDestructiveRotation(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	fixture.env.hw.configGetter().Health.CorruptionAction = "repair"
	_, err := fixture.env.db.Exec(`
		UPDATE file_health SET retry_count = 2, repair_retry_count = 3 WHERE file_path = ?
	`, fixture.filePath)
	require.NoError(t, err)
	createFailSecondClaimRotationTrigger(t, fixture.env.db)

	err = fixture.env.hw.runHealthCheckCycle(context.Background())
	assert.Error(t, err)
	fixture.assertPreserved(t)
}

func TestRepairTriggerRequiresFreshRotationBeforeARR(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	fixture.env.hw.configGetter().Health.CorruptionAction = "repair"
	_, err := fixture.env.db.Exec(`UPDATE file_health SET retry_count = 2 WHERE file_path = ?`, fixture.filePath)
	require.NoError(t, err)
	createFailSecondClaimRotationTrigger(t, fixture.env.db)

	err = fixture.env.hw.runHealthCheckCycle(context.Background())
	assert.Error(t, err)
	assert.Empty(t, fixture.env.mockARRs.calls, "failed pre-ARR rotation must prevent every ARR request")
	fixture.assertPreserved(t)
}

func TestPreSTATRotationFailureStopsBeforeSweepAndPreservesState(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	createFailNextClaimRotationTrigger(t, fixture.env.db)

	err := fixture.env.hw.runHealthCheckCycle(context.Background())
	assert.Error(t, err, "failed pre-STAT rotation must fail the worker cycle closed")
	assert.Equal(t, int64(0), client.StatCalls(), "transport evidence must not start without the fresh pre-STAT token")
	fixture.assertPreserved(t)
}

func TestHealthCycleLostClaimBeforeHealthyPublicationPreservesReplacementOwnerState(t *testing.T) {
	base := fakepool.New()
	wrapped := &finalizationReplacingClient{Client: base}
	fixture := newDestructiveClaimFixture(t, wrapped)
	base.SetDefaultBehavior(fakepool.SegmentBehavior{})
	wrapped.db = fixture.env.db
	wrapped.filePath = fixture.filePath

	err := fixture.env.hw.runHealthCheckCycle(context.Background())
	assert.Error(t, err, "a zero-row token-bound final publication must be reported")
	require.NoError(t, wrapped.err)
	assert.Equal(t, int64(1), base.StatCalls(), "owner replacement occurs only after the accepted STAT finishes")

	var status, metadata, scheduledAt string
	var token, lastError, errorDetails sql.NullString
	require.NoError(t, fixture.env.db.QueryRow(`
		SELECT status, metadata, health_claim_token, CAST(scheduled_check_at AS TEXT), last_error, error_details
		FROM file_health WHERE file_path = ?
	`, fixture.filePath).Scan(&status, &metadata, &token, &scheduledAt, &lastError, &errorDetails))
	assert.Equal(t, "checking", status)
	assert.JSONEq(t, `{"owner":"b"}`, metadata)
	assert.True(t, token.Valid, "replacement ownership token must remain present")
	assert.Equal(t, "owner-b", token.String)
	assert.Contains(t, scheduledAt, "2042-03-04 05:06:07")
	assert.True(t, lastError.Valid)
	assert.Equal(t, "owner-b-error", lastError.String)
	assert.True(t, errorDetails.Valid)
	assert.JSONEq(t, `{"owner":"b","evidence":"complete"}`, errorDetails.String)
}

func TestDestructiveRotationZeroRowsFailsClosed(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	createIgnoreSecondClaimRotationTrigger(t, fixture.env.db)

	err := fixture.env.hw.runHealthCheckCycle(context.Background())
	assert.Error(t, err, "a zero-row destructive rotation must be treated as lost ownership")
	assert.Equal(t, int64(1), client.StatCalls(), "the already-authorized sweep may finish")
	fixture.assertPreserved(t)
}

func TestSuccessfulARRThenLostClaimCannotMoveMetadata(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	fixture.env.hw.configGetter().Health.CorruptionAction = "repair"
	fixture.env.mockARRs.onTrigger = func() {
		_, err := fixture.env.db.Exec(`
			UPDATE file_health SET health_claim_token = 'post-arr-owner'
			WHERE file_path = ?
		`, fixture.filePath)
		require.NoError(t, err)
	}
	_, err := fixture.env.db.Exec(`UPDATE file_health SET retry_count = 2 WHERE file_path = ?`, fixture.filePath)
	require.NoError(t, err)

	err = fixture.env.hw.runHealthCheckCycle(context.Background())
	assert.Error(t, err)
	require.Len(t, fixture.env.mockARRs.calls, 1, "ARR may complete before ownership is stolen")
	fixture.assertPreserved(t)
	var token string
	require.NoError(t, fixture.env.db.QueryRow(`SELECT health_claim_token FROM file_health WHERE file_path = ?`, fixture.filePath).Scan(&token))
	assert.Equal(t, "post-arr-owner", token)
}

func TestSuccessfulARRRequiresFreshPostARRRotationBeforeMove(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	fixture.env.hw.configGetter().Health.CorruptionAction = "repair"
	var triggerErr error
	fixture.env.mockARRs.onTrigger = func() {
		triggerErr = installRejectFreshClaimRotationWithSentinel(fixture.env.db)
	}
	_, err := fixture.env.db.Exec(`UPDATE file_health SET retry_count = 2 WHERE file_path = ?`, fixture.filePath)
	require.NoError(t, err)

	err = fixture.env.hw.runHealthCheckCycle(context.Background())
	assertPostARRFreshRotationSentinel(t, err)
	require.NoError(t, triggerErr)
	require.Len(t, fixture.env.mockARRs.calls, 1, "ARR succeeds before the rejected post-ARR rotation")
	fixture.assertPreserved(t)
}

func TestRepairReplacementCleanupRequiresCurrentRotationAfterARR(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	fixture.env.hw.configGetter().Health.CorruptionAction = "repair"
	fixture.env.mockARRs.returnErr = arrs.ErrEpisodeAlreadySatisfied
	fixture.env.mockARRs.onTrigger = func() {
		_, err := fixture.env.db.Exec(`
			UPDATE file_health SET health_claim_token = 'replacement-owner'
			WHERE file_path = ?
		`, fixture.filePath)
		require.NoError(t, err)
	}
	_, err := fixture.env.db.Exec(`UPDATE file_health SET retry_count = 2 WHERE file_path = ?`, fixture.filePath)
	require.NoError(t, err)

	err = fixture.env.hw.runHealthCheckCycle(context.Background())
	assert.Error(t, err)
	fixture.assertPreserved(t)
	var token string
	require.NoError(t, fixture.env.db.QueryRow(`SELECT health_claim_token FROM file_health WHERE file_path = ?`, fixture.filePath).Scan(&token))
	assert.Equal(t, "replacement-owner", token)
}

func TestAlreadySatisfiedCleanupRequiresFreshPostARRRotation(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	fixture.env.hw.configGetter().Health.CorruptionAction = "repair"
	fixture.env.mockARRs.returnErr = arrs.ErrEpisodeAlreadySatisfied
	var triggerErr error
	fixture.env.mockARRs.onTrigger = func() {
		triggerErr = installRejectFreshClaimRotationWithSentinel(fixture.env.db)
	}
	_, err := fixture.env.db.Exec(`UPDATE file_health SET retry_count = 2 WHERE file_path = ?`, fixture.filePath)
	require.NoError(t, err)

	err = fixture.env.hw.runHealthCheckCycle(context.Background())
	assertPostARRFreshRotationSentinel(t, err)
	require.NoError(t, triggerErr)
	require.Len(t, fixture.env.mockARRs.calls, 1, "ARR response arrives before the rejected cleanup rotation")
	fixture.assertPreserved(t)
}

func TestRepairNotificationSuccessfulARRRequiresFreshPostARRRotationBeforeMove(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	fixture.env.hw.configGetter().Health.CorruptionAction = "repair"
	_, err := fixture.env.db.Exec(`
		UPDATE file_health
		SET status = 'repair_triggered', scheduled_check_at = datetime('now', '-1 second')
		WHERE file_path = ?
	`, fixture.filePath)
	require.NoError(t, err)
	var triggerErr error
	fixture.env.mockARRs.onTrigger = func() {
		triggerErr = installRejectFreshClaimRotationWithSentinel(fixture.env.db)
	}

	err = fixture.env.hw.runHealthCheckCycle(context.Background())
	assertPostARRFreshRotationSentinel(t, err)
	require.NoError(t, triggerErr)
	require.Len(t, fixture.env.mockARRs.calls, 1, "repair notification reaches ARR before the rejected post-ARR rotation")
	fixture.assertPreserved(t)
}

func TestMetadataRegenerationRequiresFreshRotation(t *testing.T) {
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
	createFailNextClaimRotationTrigger(t, fixture.env.db)

	err = fixture.env.hw.runHealthCheckCycle(context.Background())
	assert.Error(t, err)
	assert.Equal(t, int64(0), importer.calls.Load(), "metadata regeneration must follow a fresh checked rotation")
}

func TestHealthClaimAndSweepRotationUseDistinctFreshTokens(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	client.SetDefaultBehavior(fakepool.SegmentBehavior{})

	require.NoError(t, fixture.env.hw.runHealthCheckCycle(context.Background()))
	rows, err := fixture.env.db.Query(`
		SELECT token FROM test_health_claim_audit
		WHERE file_path = ? ORDER BY rowid
	`, fixture.filePath)
	require.NoError(t, err)
	defer rows.Close()
	var tokens []string
	for rows.Next() {
		var token string
		require.NoError(t, rows.Scan(&token))
		tokens = append(tokens, token)
	}
	require.NoError(t, rows.Err())
	require.GreaterOrEqual(t, len(tokens), 2, "admission and pre-STAT rotation must both be audited")
	seen := make(map[string]struct{}, len(tokens))
	for _, token := range tokens {
		assert.NotEmpty(t, token)
		_, reused := seen[token]
		assert.False(t, reused, "every ownership boundary must use a globally fresh token")
		seen[token] = struct{}{}
	}
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
		SET status = 'checking', health_claim_token = 'checking-owner'
		WHERE file_path = ?
	`, fixture.filePath)
	require.NoError(t, err)
	_, err = fixture.env.db.Exec(`
		INSERT INTO file_health
			(file_path, status, scheduled_check_at, health_claim_token)
		VALUES (?, 'repair_triggered', datetime('now', '-1 second'), 'repair-owner')
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
