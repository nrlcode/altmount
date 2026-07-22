package importer

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/metadata"
)

func setImporterTempRoot(t *testing.T, root string) {
	t.Helper()
	for _, name := range []string{"TMPDIR", "TMP", "TEMP"} {
		t.Setenv(name, root)
	}
}

// newMinimalServiceForPersistTest builds just enough of *Service to exercise
// ensurePersistentNzb.
func newMinimalServiceForPersistTest(t *testing.T) *Service {
	t.Helper()
	service, _ := newMinimalServiceForPersistTestWithDB(t)
	return service
}

func newMinimalServiceForPersistTestWithDB(t *testing.T) (*Service, *sql.DB) {
	t.Helper()

	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "queue.db")+"?_busy_timeout=5000")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	setupMinimalPersistSchema(t, db)
	return newMinimalServiceForPersistTestDB(t, db), db
}

func setupMinimalPersistSchema(t *testing.T, db *sql.DB) {
	t.Helper()

	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS import_queue (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			download_id TEXT DEFAULT NULL,
			nzb_path TEXT NOT NULL,
			relative_path TEXT DEFAULT NULL,
			storage_path TEXT DEFAULT NULL,
			priority INTEGER NOT NULL DEFAULT 1,
			status TEXT NOT NULL DEFAULT 'pending'
				CHECK(status IN ('pending','processing','completed','failed','fallback','paused')),
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			started_at DATETIME DEFAULT NULL,
			completed_at DATETIME DEFAULT NULL,
			retry_count INTEGER NOT NULL DEFAULT 0,
			max_retries INTEGER NOT NULL DEFAULT 3,
			error_message TEXT DEFAULT NULL,
			batch_id TEXT DEFAULT NULL,
			metadata TEXT DEFAULT NULL,
			category TEXT DEFAULT NULL,
			file_size BIGINT DEFAULT NULL,
			target_path TEXT DEFAULT NULL,
			skip_arr_notification BOOLEAN NOT NULL DEFAULT FALSE,
			skip_post_import_links BOOLEAN NOT NULL DEFAULT FALSE,
			indexer TEXT DEFAULT NULL,
			UNIQUE(nzb_path)
		);
		CREATE INDEX IF NOT EXISTS idx_queue_nzb_path ON import_queue(nzb_path);
	`)
	require.NoError(t, err)
}

func newMinimalServiceForPersistTestDB(t *testing.T, db *sql.DB) *Service {
	t.Helper()

	repo := database.NewQueueRepository(db, database.DialectSQLite)
	dbWrapper := &database.DB{}
	dbWrapper.Repository = repo

	tmpCfgDir := t.TempDir()
	cfg := config.DefaultConfig(tmpCfgDir)
	cfg.Database.Path = filepath.Join(tmpCfgDir, "test.db")
	cfgGetter := config.ConfigGetter(func() *config.Config { return cfg })
	metadataService := metadata.NewMetadataService(cfg.Metadata.RootPath)
	require.NoError(t, metadataService.ConfigureCleanupRoots(
		filepath.Join(tmpCfgDir, ".nzbs"),
		filepath.Join(os.TempDir(), ".altmount-queue"),
		filepath.Join(tmpCfgDir, ".nzbs"),
	))

	return &Service{
		database:        dbWrapper,
		configGetter:    cfgGetter,
		metadataService: metadataService,
		log:             slog.Default(),
		cancelFuncs:     make(map[int64]context.CancelFunc),
		mu:              sync.RWMutex{},
	}
}

func TestEnsurePersistentNzb_UsesOSTempQueueDir(t *testing.T) {
	// Arrange: write a real .nzb file in a temp dir (simulates stageDir).
	stageDir := t.TempDir()
	nzbPath := filepath.Join(stageDir, "movie.nzb")
	require.NoError(t, os.WriteFile(nzbPath, []byte("<nzb/>"), 0644))

	item := &database.ImportQueueItem{ID: 42, NzbPath: nzbPath}

	svc := newMinimalServiceForPersistTest(t)

	// Act
	err := svc.ensurePersistentNzb(context.Background(), item)
	require.NoError(t, err)

	// Cleanup: remove the file from the OS temp queue dir (registered before assertions so it
	// always runs even if an assertion fails).
	t.Cleanup(func() { os.Remove(item.NzbPath) })

	// Assert: item.NzbPath must be inside os.TempDir()/.altmount-queue/
	expected := filepath.Join(os.TempDir(), ".altmount-queue")
	assert.True(t, strings.HasPrefix(item.NzbPath, expected),
		"expected OS temp queue dir prefix %q, got %q", expected, item.NzbPath)
	assert.False(t, strings.Contains(item.NzbPath, ".nzbs"),
		"should not be in .nzbs/ directory, got %q", item.NzbPath)

	// Assert: the file actually exists at the new path
	_, statErr := os.Stat(item.NzbPath)
	assert.NoError(t, statErr, "moved file should exist at new path")
}

func TestEnsurePersistentNzb_AlreadyInTempQueueDir_IsNoop(t *testing.T) {
	// Arrange: NZB is already in the target queue dir — should be a no-op.
	queueDir := filepath.Join(os.TempDir(), ".altmount-queue")
	require.NoError(t, os.MkdirAll(queueDir, 0755))

	existingPath := filepath.Join(queueDir, "movie.nzb")
	require.NoError(t, os.WriteFile(existingPath, []byte("<nzb/>"), 0644))
	t.Cleanup(func() { os.Remove(existingPath) })

	item := &database.ImportQueueItem{ID: 99, NzbPath: existingPath}

	svc := newMinimalServiceForPersistTest(t)

	// Act
	err := svc.ensurePersistentNzb(context.Background(), item)
	require.NoError(t, err)

	// Assert: path unchanged
	assert.Equal(t, existingPath, item.NzbPath,
		"path should not change when already in OS temp queue dir")
}

func TestEnsurePersistentNzbRejectsSymlinkedTempQueueRoot(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink authority test requires Unix symlink semantics")
	}
	tempRoot := t.TempDir()
	setImporterTempRoot(t, tempRoot)

	svc := newMinimalServiceForPersistTest(t)
	outside := t.TempDir()
	queueRoot := filepath.Join(tempRoot, ".altmount-queue")
	require.NoError(t, os.Symlink(outside, queueRoot))

	victim := filepath.Join(outside, "victim.nzb")
	require.NoError(t, os.WriteFile(victim, []byte("<nzb/>"), 0o600))
	item := &database.ImportQueueItem{ID: 101, NzbPath: filepath.Join(queueRoot, "victim.nzb")}

	err := svc.ensurePersistentNzb(context.Background(), item)

	require.Error(t, err, "a symlinked persistent queue root must never be trusted")
	assert.FileExists(t, victim)
}

var fbaseQueueCommitDriverID atomic.Uint64

func TestAddToQueueNeverPublishesAnUnownedSourcePath(t *testing.T) {
	tempRoot := t.TempDir()
	setImporterTempRoot(t, tempRoot)
	queueRoot := filepath.Join(tempRoot, ".altmount-queue")
	var unsafePath atomic.Bool
	var unsafeCommitAttempted atomic.Bool
	driverName := fmt.Sprintf("fbase-queue-commit-%d", fbaseQueueCommitDriverID.Add(1))
	sql.Register(driverName, &sqlite3.SQLiteDriver{ConnectHook: func(conn *sqlite3.SQLiteConn) error {
		if err := conn.RegisterFunc("fbase_record_queue_path", func(path string) int {
			rel, err := filepath.Rel(queueRoot, path)
			unsafePath.Store(err != nil || rel == "." || !filepath.IsLocal(rel))
			return 0
		}, true); err != nil {
			return err
		}
		conn.RegisterCommitHook(func() int {
			if unsafePath.Load() {
				unsafeCommitAttempted.Store(true)
				return 1
			}
			return 0
		})
		conn.RegisterRollbackHook(func() { unsafePath.Store(false) })
		return nil
	}})
	db, err := sql.Open(driverName, filepath.Join(t.TempDir(), "queue.db")+"?_busy_timeout=5000")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	setupMinimalPersistSchema(t, db)
	_, err = db.Exec(`
		CREATE TRIGGER record_queue_path_insert
		AFTER INSERT ON import_queue
		BEGIN
			SELECT fbase_record_queue_path(NEW.nzb_path);
		END;
		CREATE TRIGGER record_queue_path_update
		AFTER UPDATE OF nzb_path ON import_queue
		BEGIN
			SELECT fbase_record_queue_path(NEW.nzb_path);
		END;
	`)
	require.NoError(t, err)
	svc := newMinimalServiceForPersistTestDB(t, db)

	stageDir := t.TempDir()
	source := filepath.Join(stageDir, "admission.nzb")
	require.NoError(t, os.WriteFile(source, []byte("<nzb/>"), 0o600))

	item, err := svc.AddToQueue(context.Background(), source, nil, nil, nil, nil, nil, nil)

	assert.False(t, unsafeCommitAttempted.Load(),
		"the caller path may exist provisionally in one transaction but must not reach commit")
	require.NoError(t, err)
	require.NotNil(t, item)
	require.NotZero(t, item.ID)
	requireStrictChildPath(t, queueRoot, item.NzbPath)
	assert.FileExists(t, item.NzbPath)
}

func requireQueueRowCount(t *testing.T, db *sql.DB, want int) {
	t.Helper()
	var got int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM import_queue`).Scan(&got))
	require.Equal(t, want, got)
}

func requireStrictChildPath(t *testing.T, root, path string) string {
	t.Helper()
	rel, err := filepath.Rel(root, path)
	require.NoError(t, err)
	require.NotEqual(t, ".", rel)
	require.True(t, filepath.IsLocal(rel), "%q must be a strict child of %q", path, root)
	rootInfo, err := os.Lstat(root)
	require.NoError(t, err)
	require.True(t, rootInfo.IsDir())
	require.Zero(t, rootInfo.Mode()&os.ModeSymlink, "queue root must not be a symlink")
	components := strings.Split(filepath.Clean(rel), string(os.PathSeparator))
	current := root
	for i, component := range components {
		current = filepath.Join(current, component)
		info, statErr := os.Lstat(current)
		require.NoError(t, statErr)
		require.Zero(t, info.Mode()&os.ModeSymlink, "%q must not be a symlink", current)
		if i < len(components)-1 {
			require.True(t, info.IsDir(), "%q must be an unambiguous directory", current)
		} else {
			require.True(t, info.Mode().IsRegular(), "%q must be a regular file", current)
		}
	}
	return rel
}

func TestAddToQueueRejectsSymlinkedOwnedAncestor(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink authority test requires Unix symlink semantics")
	}
	tempRoot := t.TempDir()
	setImporterTempRoot(t, tempRoot)
	svc, db := newMinimalServiceForPersistTestWithDB(t)
	queueRoot := filepath.Join(tempRoot, ".altmount-queue")
	require.NoError(t, os.MkdirAll(queueRoot, 0o755))

	outside := t.TempDir()
	victim := filepath.Join(outside, "victim.nzb")
	require.NoError(t, os.WriteFile(victim, []byte("<nzb/>"), 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(queueRoot, "escape")))

	item, err := svc.AddToQueue(
		context.Background(), filepath.Join(queueRoot, "escape", "victim.nzb"),
		nil, nil, nil, nil, nil, nil,
	)

	require.Error(t, err)
	require.Nil(t, item)
	assert.FileExists(t, victim)
	requireQueueRowCount(t, db, 0)
}

func TestAddToQueueAcceptsAlreadyOwnedRegularFile(t *testing.T) {
	tempRoot := t.TempDir()
	setImporterTempRoot(t, tempRoot)
	svc, db := newMinimalServiceForPersistTestWithDB(t)
	queueRoot := filepath.Join(tempRoot, ".altmount-queue")
	require.NoError(t, os.MkdirAll(queueRoot, 0o755))

	source := filepath.Join(queueRoot, "already-owned.nzb")
	require.NoError(t, os.WriteFile(source, []byte("<nzb/>"), 0o600))
	item, err := svc.AddToQueue(context.Background(), source, nil, nil, nil, nil, nil, nil)

	require.NoError(t, err)
	require.NotNil(t, item)
	assert.Equal(t, source, item.NzbPath)
	assert.FileExists(t, source)
	requireQueueRowCount(t, db, 1)
}

func TestAddToQueueRejectsNonRegularOwnedSources(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, string) (string, string)
	}{
		{
			name: "directory",
			setup: func(t *testing.T, queueRoot string) (string, string) {
				path := filepath.Join(queueRoot, "directory.nzb")
				require.NoError(t, os.Mkdir(path, 0o755))
				return path, ""
			},
		},
		{
			name: "leaf symlink",
			setup: func(t *testing.T, queueRoot string) (string, string) {
				if runtime.GOOS == "windows" {
					t.Skip("symlink authority test requires Unix symlink semantics")
				}
				victim := filepath.Join(t.TempDir(), "victim.nzb")
				require.NoError(t, os.WriteFile(victim, []byte("keep"), 0o600))
				path := filepath.Join(queueRoot, "leaf.nzb")
				require.NoError(t, os.Symlink(victim, path))
				return path, victim
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempRoot := t.TempDir()
			setImporterTempRoot(t, tempRoot)
			svc, db := newMinimalServiceForPersistTestWithDB(t)
			queueRoot := filepath.Join(tempRoot, ".altmount-queue")
			require.NoError(t, os.MkdirAll(queueRoot, 0o755))
			source, victim := tt.setup(t, queueRoot)

			item, err := svc.AddToQueue(context.Background(), source, nil, nil, nil, nil, nil, nil)

			require.Error(t, err)
			require.Nil(t, item)
			requireQueueRowCount(t, db, 0)
			if victim != "" {
				assert.FileExists(t, victim)
			}
		})
	}
}

func TestAddToQueueRollsBackSourceAndRootedCopyWhenFinalPublicationFails(t *testing.T) {
	tempRoot := t.TempDir()
	setImporterTempRoot(t, tempRoot)
	queueRoot := filepath.Join(tempRoot, ".altmount-queue")
	var sawQueuePath atomic.Bool
	var rootedPath atomic.Bool
	var rootedCommitAttempted atomic.Bool
	driverName := fmt.Sprintf("fbase-queue-rollback-%d", fbaseQueueCommitDriverID.Add(1))
	sql.Register(driverName, &sqlite3.SQLiteDriver{ConnectHook: func(conn *sqlite3.SQLiteConn) error {
		if err := conn.RegisterFunc("fbase_record_final_queue_path", func(path string) int {
			rel, err := filepath.Rel(queueRoot, path)
			sawQueuePath.Store(true)
			rootedPath.Store(err == nil && rel != "." && filepath.IsLocal(rel))
			return 0
		}, true); err != nil {
			return err
		}
		conn.RegisterCommitHook(func() int {
			if sawQueuePath.Load() && rootedPath.Load() {
				rootedCommitAttempted.Store(true)
				return 1
			}
			return 0
		})
		conn.RegisterRollbackHook(func() {
			sawQueuePath.Store(false)
			rootedPath.Store(false)
		})
		return nil
	}})
	db, err := sql.Open(driverName, filepath.Join(t.TempDir(), "queue.db")+"?_busy_timeout=5000")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	setupMinimalPersistSchema(t, db)
	_, err = db.Exec(`
		CREATE TRIGGER record_final_queue_path_insert
		AFTER INSERT ON import_queue
		BEGIN
			SELECT fbase_record_final_queue_path(NEW.nzb_path);
		END;
		CREATE TRIGGER record_final_queue_path_update
		AFTER UPDATE OF nzb_path ON import_queue
		BEGIN
			SELECT fbase_record_final_queue_path(NEW.nzb_path);
		END;
	`)
	require.NoError(t, err)
	svc := newMinimalServiceForPersistTestDB(t, db)

	source := filepath.Join(t.TempDir(), "rollback.nzb")
	require.NoError(t, os.WriteFile(source, []byte("<nzb/>"), 0o600))
	item, err := svc.AddToQueue(context.Background(), source, nil, nil, nil, nil, nil, nil)

	require.Error(t, err)
	require.Nil(t, item)
	assert.True(t, rootedCommitAttempted.Load(), "fixture must reject the final rooted publication")
	assert.FileExists(t, source, "a failed DB publication must retain the caller-owned source")
	requireQueueRowCount(t, db, 0)
	entries, readErr := os.ReadDir(filepath.Join(tempRoot, ".altmount-queue"))
	if !os.IsNotExist(readErr) {
		require.NoError(t, readErr)
		assert.Empty(t, entries, "a rolled-back admission must not leak a rooted copy")
	}
}
