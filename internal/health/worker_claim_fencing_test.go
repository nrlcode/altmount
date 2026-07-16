package health

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

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
		BEFORE UPDATE OF status ON file_health
		WHEN NEW.status = 'checking'
		BEGIN
			SELECT RAISE(FAIL, 'synthetic checking claim failure');
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
}
