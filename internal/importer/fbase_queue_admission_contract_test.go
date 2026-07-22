package importer

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fbaseAdmissionEnv struct {
	service   *Service
	database  *database.DB
	queueRoot string
}

func newFbaseAdmissionEnv(t *testing.T) *fbaseAdmissionEnv {
	t.Helper()
	tempRoot := t.TempDir()
	t.Setenv("TMPDIR", tempRoot)
	configDir := filepath.Join(tempRoot, "config")
	require.NoError(t, os.MkdirAll(configDir, 0o755))

	cfg := config.DefaultConfig(configDir)
	cfg.Database.Path = filepath.Join(configDir, "altmount.db")
	cfg.Metadata.RootPath = filepath.Join(configDir, "metadata")
	db, err := database.NewDB(database.Config{DatabasePath: cfg.Database.Path})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	queueRoot := filepath.Join(tempRoot, ".altmount-queue")
	storeRoot := filepath.Join(configDir, ".nzbs")
	metadataService := metadata.NewMetadataService(cfg.Metadata.RootPath)
	require.NoError(t, metadataService.ConfigureCleanupRoots(storeRoot, queueRoot, storeRoot))
	cfgGetter := config.ConfigGetter(func() *config.Config { return cfg })
	service, err := NewService(
		ServiceConfig{Workers: 1}, metadataService, db, nil, nil, cfgGetter,
		nil, nil, nil,
	)
	require.NoError(t, err)
	t.Cleanup(service.cancel)

	return &fbaseAdmissionEnv{service: service, database: db, queueRoot: queueRoot}
}

func waitForScan(t *testing.T, service *Service) ScanInfo {
	t.Helper()
	require.Eventually(t, func() bool {
		return service.GetScanStatus().Status == ScanStatusIdle
	}, 5*time.Second, 10*time.Millisecond)
	return service.GetScanStatus()
}

func waitForNzbdavImport(t *testing.T, service *Service) ImportInfo {
	t.Helper()
	require.Eventually(t, func() bool {
		return service.GetImportStatus().Status == scannerImportCompleted
	}, 5*time.Second, 10*time.Millisecond)
	return service.GetImportStatus()
}

// Keep the scanner package's completed value local to this contract so the
// public importer aliases do not need to grow just for a test assertion.
const scannerImportCompleted = "completed"

func queueRows(t *testing.T, db *sql.DB) map[int64]string {
	t.Helper()
	rows, err := db.Query(`SELECT id, nzb_path FROM import_queue ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()
	got := make(map[int64]string)
	for rows.Next() {
		var id int64
		var path string
		require.NoError(t, rows.Scan(&id, &path))
		got[id] = path
	}
	require.NoError(t, rows.Err())
	return got
}

func TestDirectoryScannerPublishesOnlyRootedQueuePaths(t *testing.T) {
	env := newFbaseAdmissionEnv(t)
	installOwnedQueuePathTrigger(t, env.database.Connection(), env.queueRoot)

	scanRoot := t.TempDir()
	source := filepath.Join(scanRoot, "scanner-release.nzb")
	require.NoError(t, os.WriteFile(source, []byte("<nzb/>"), 0o600))
	require.NoError(t, env.service.StartManualScan(scanRoot))
	info := waitForScan(t, env.service)

	require.Equal(t, 1, info.FilesFound)
	require.Equal(t, 1, info.FilesAdded)
	require.Nil(t, info.LastError)
	rows := queueRows(t, env.database.Connection())
	require.Len(t, rows, 1)
	for _, path := range rows {
		requireStrictChildPath(t, env.queueRoot, path)
		assert.FileExists(t, path)
	}
	assert.NoFileExists(t, source)
}

func TestDirectoryScannerReportsRootedAdmissionFailureTruthfully(t *testing.T) {
	env := newFbaseAdmissionEnv(t)
	require.NoError(t, os.Symlink(t.TempDir(), env.queueRoot))

	scanRoot := t.TempDir()
	source := filepath.Join(scanRoot, "rejected-release.nzb")
	require.NoError(t, os.WriteFile(source, []byte("<nzb/>"), 0o600))
	require.NoError(t, env.service.StartManualScan(scanRoot))
	info := waitForScan(t, env.service)

	require.Equal(t, 1, info.FilesFound)
	assert.Zero(t, info.FilesAdded)
	require.NotNil(t, info.LastError)
	assert.NotEmpty(t, *info.LastError)
	assert.Empty(t, queueRows(t, env.database.Connection()))
	assert.FileExists(t, source)
}

func createLegacyNzbdavFixture(t *testing.T, tempRoot string) string {
	t.Helper()
	dbPath := filepath.Join(tempRoot, "nzbdav.db")
	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	_, err = db.Exec(`
		CREATE TABLE DavItems (
			Id TEXT PRIMARY KEY,
			ParentId TEXT,
			Name TEXT,
			FileSize INTEGER,
			Type INTEGER,
			Path TEXT
		);
		CREATE TABLE DavNzbFiles (
			Id TEXT PRIMARY KEY,
			SegmentIds TEXT
		);
		INSERT INTO DavItems (Id, ParentId, Name, FileSize, Type, Path) VALUES
			('root', NULL, '/', NULL, 1, '/'),
			('movies', 'root', 'movies', NULL, 1, '/movies'),
			('first-release', 'movies', 'First.Release', NULL, 1, '/movies/First.Release'),
			('first-file', 'first-release', 'first.mkv', 100, 0, '/movies/First.Release/first.mkv'),
			('second-release', 'movies', 'Second.Release', NULL, 1, '/movies/Second.Release'),
			('second-file', 'second-release', 'second.mkv', 200, 0, '/movies/Second.Release/second.mkv');
		INSERT INTO DavNzbFiles (Id, SegmentIds) VALUES
			('first-file', '["first@test"]'),
			('second-file', '["second@test"]');
	`)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	return dbPath
}

func TestNzbdavBatchPublishesOnlyRootedQueuePathsAndLinksMigrations(t *testing.T) {
	env := newFbaseAdmissionEnv(t)
	installOwnedQueuePathTrigger(t, env.database.Connection(), env.queueRoot)
	dbPath := createLegacyNzbdavFixture(t, t.TempDir())

	require.NoError(t, env.service.StartNzbdavImport(dbPath, "", false))
	info := waitForNzbdavImport(t, env.service)
	require.Equal(t, 2, info.Total)
	require.Equal(t, 2, info.Added)
	require.Zero(t, info.Failed)

	rows := queueRows(t, env.database.Connection())
	require.Len(t, rows, 2)
	rowIDs := make(map[int64]struct{}, len(rows))
	for id, path := range rows {
		rowIDs[id] = struct{}{}
		requireStrictChildPath(t, env.queueRoot, path)
		assert.FileExists(t, path)
	}
	for _, externalID := range []string{"first-release", "second-release"} {
		migration, err := env.database.MigrationRepo.LookupByExternalID(context.Background(), "nzbdav", externalID)
		require.NoError(t, err)
		require.NotNil(t, migration)
		require.NotNil(t, migration.QueueItemID)
		_, ok := rowIDs[*migration.QueueItemID]
		assert.True(t, ok, "migration must reference a committed rooted queue row")
	}
}

func TestNzbdavBatchRejectionRollsBackRowsCopiesAndMigrationLinks(t *testing.T) {
	env := newFbaseAdmissionEnv(t)
	_, err := env.database.Connection().Exec(`
		CREATE TRIGGER reject_second_batch_item
		BEFORE INSERT ON import_queue
		WHEN EXISTS (SELECT 1 FROM import_queue)
		BEGIN
			SELECT RAISE(ABORT, 'injected second batch failure');
		END;
	`)
	require.NoError(t, err)
	dbPath := createLegacyNzbdavFixture(t, t.TempDir())

	require.NoError(t, env.service.StartNzbdavImport(dbPath, "", false))
	info := waitForNzbdavImport(t, env.service)
	require.Equal(t, 2, info.Total)
	require.Zero(t, info.Added)
	require.Equal(t, 2, info.Failed)
	assert.Empty(t, queueRows(t, env.database.Connection()))
	entries, readErr := os.ReadDir(env.queueRoot)
	if !os.IsNotExist(readErr) {
		require.NoError(t, readErr)
		assert.Empty(t, entries, "a rolled-back batch must not leak rooted copies")
	}
	for _, externalID := range []string{"first-release", "second-release"} {
		migration, lookupErr := env.database.MigrationRepo.LookupByExternalID(context.Background(), "nzbdav", externalID)
		require.NoError(t, lookupErr)
		require.NotNil(t, migration)
		assert.Nil(t, migration.QueueItemID)
	}
}
