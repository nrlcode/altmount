package importer

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/metadata"
	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fbaseAdmissionEnv struct {
	service   *Service
	database  *database.DB
	tempRoot  string
	queueRoot string
}

func newFbaseAdmissionEnv(t *testing.T) *fbaseAdmissionEnv {
	t.Helper()
	tempRoot := t.TempDir()
	setImporterTempRoot(t, tempRoot)
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

	return &fbaseAdmissionEnv{service: service, database: db, tempRoot: tempRoot, queueRoot: queueRoot}
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

func installQueuePublicationCommitGuard(t *testing.T, db *sql.DB, queueRoot string) *atomic.Bool {
	t.Helper()
	const connections = 8
	db.SetMaxOpenConns(connections)
	db.SetMaxIdleConns(connections)
	opened := make([]*sql.Conn, 0, connections)
	attempted := &atomic.Bool{}
	for range connections {
		conn, err := db.Conn(context.Background())
		require.NoError(t, err)
		opened = append(opened, conn)
		err = conn.Raw(func(driverConn any) error {
			sqliteConn, ok := driverConn.(*sqlite3.SQLiteConn)
			if !ok {
				return assert.AnError
			}
			var sawPath atomic.Bool
			var rootedPath atomic.Bool
			if err := sqliteConn.RegisterFunc("fbase_record_admission_path", func(path string) int {
				rel, err := filepath.Rel(queueRoot, path)
				sawPath.Store(true)
				rootedPath.Store(err == nil && rel != "." && filepath.IsLocal(rel))
				return 0
			}, true); err != nil {
				return err
			}
			sqliteConn.RegisterCommitHook(func() int {
				if sawPath.Load() && !rootedPath.Load() {
					attempted.Store(true)
					return 1
				}
				return 0
			})
			sqliteConn.RegisterRollbackHook(func() {
				sawPath.Store(false)
				rootedPath.Store(false)
			})
			return nil
		})
		require.NoError(t, err)
	}
	for _, conn := range opened {
		require.NoError(t, conn.Close())
	}
	_, err := db.Exec(`
		CREATE TRIGGER record_admission_path_insert
		AFTER INSERT ON import_queue
		BEGIN
			SELECT fbase_record_admission_path(NEW.nzb_path);
		END;
		CREATE TRIGGER record_admission_path_update
		AFTER UPDATE OF nzb_path ON import_queue
		BEGIN
			SELECT fbase_record_admission_path(NEW.nzb_path);
		END;
	`)
	require.NoError(t, err)
	return attempted
}

func TestDirectoryScannerPublishesOnlyRootedQueuePaths(t *testing.T) {
	env := newFbaseAdmissionEnv(t)
	unsafeCommit := installQueuePublicationCommitGuard(t, env.database.Connection(), env.queueRoot)

	scanRoot := t.TempDir()
	source := filepath.Join(scanRoot, "scanner-release.nzb")
	require.NoError(t, os.WriteFile(source, []byte("<nzb/>"), 0o600))
	require.NoError(t, env.service.StartManualScan(scanRoot))
	info := waitForScan(t, env.service)

	assert.False(t, unsafeCommit.Load(), "scanner admission must not commit its source path")
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
	if runtime.GOOS == "windows" {
		t.Skip("symlink authority test requires Unix symlink semantics")
	}
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
	unsafeCommit := installQueuePublicationCommitGuard(t, env.database.Connection(), env.queueRoot)
	dbPath := createLegacyNzbdavFixture(t, t.TempDir())

	require.NoError(t, env.service.StartNzbdavImport(dbPath, "", false))
	info := waitForNzbdavImport(t, env.service)
	assert.False(t, unsafeCommit.Load(), "NZBDav admission must not commit generated staging paths")
	require.Equal(t, 2, info.Total)
	require.Equal(t, 2, info.Added)
	require.Zero(t, info.Failed)

	rows := queueRows(t, env.database.Connection())
	require.Len(t, rows, 2)
	for _, path := range rows {
		requireStrictChildPath(t, env.queueRoot, path)
		assert.FileExists(t, path)
	}
	for externalID, articleID := range map[string]string{
		"first-release":  "first@test",
		"second-release": "second@test",
	} {
		migration, err := env.database.MigrationRepo.LookupByExternalID(context.Background(), "nzbdav", externalID)
		require.NoError(t, err)
		require.NotNil(t, migration)
		require.NotNil(t, migration.QueueItemID)
		path, ok := rows[*migration.QueueItemID]
		require.True(t, ok, "migration must reference a committed rooted queue row")
		contents, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		assert.Contains(t, string(contents), articleID,
			"each migration must reference its own generated NZB, not merely any batch row")
	}
	requireNoNzbdavStagingArtifacts(t, env.tempRoot)
}

func requireNoNzbdavStagingArtifacts(t *testing.T, tempRoot string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(tempRoot, "altmount-nzbdav-imports-*"))
	require.NoError(t, err)
	assert.Empty(t, matches, "NZBDav staging directories must not survive admission")
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
		if migration != nil {
			assert.Nil(t, migration.QueueItemID)
		}
	}
	requireNoNzbdavStagingArtifacts(t, env.tempRoot)
}
