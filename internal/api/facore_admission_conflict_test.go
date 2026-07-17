package api

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/health"
	"github.com/javi11/altmount/internal/metadata"
	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var facoreAdmissionDriverID atomic.Uint64

func TestDirectHealthCheckMapsStaleAdmissionToConflict(t *testing.T) {
	var (
		db          *sql.DB
		armed       atomic.Bool
		fired       atomic.Bool
		mutationErr atomic.Value
	)
	driverName := fmt.Sprintf("facore_admission_%d", facoreAdmissionDriverID.Add(1))
	sql.Register(driverName, &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			return conn.RegisterFunc("facore_stale_claim", func(id int64) (string, error) {
				if !armed.Load() || !fired.CompareAndSwap(false, true) {
					return "", nil
				}
				_, err := db.ExecContext(context.Background(),
					`UPDATE file_health SET status = 'checking' WHERE id = ?`, id)
				if err != nil {
					mutationErr.Store(err)
				}
				return "", err
			}, true)
		},
	})

	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL&_busy_timeout=5000",
		filepath.ToSlash(filepath.Join(t.TempDir(), "health.db")))
	var err error
	db, err = sql.Open(driverName, dsn)
	require.NoError(t, err)
	db.SetMaxOpenConns(4)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Exec(`
		CREATE TABLE file_health (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			file_path TEXT NOT NULL UNIQUE,
			library_path TEXT,
			status TEXT NOT NULL,
			last_checked DATETIME,
			last_error TEXT,
			retry_count INTEGER DEFAULT 0,
			max_retries INTEGER DEFAULT 3,
			repair_retry_count INTEGER DEFAULT 0,
			max_repair_retries INTEGER DEFAULT 3,
			source_nzb_path TEXT,
			error_details TEXT,
			metadata TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			release_date DATETIME,
			scheduled_check_at DATETIME,
			priority INTEGER DEFAULT 0,
			streaming_failure_count INTEGER DEFAULT 0,
			is_masked BOOLEAN DEFAULT FALSE,
			indexer TEXT,
			download_id TEXT GENERATED ALWAYS AS (facore_stale_claim(id)) VIRTUAL
		)
	`)
	require.NoError(t, err)

	cfg := config.DefaultConfig()
	enabled := true
	cfg.Health.Enabled = &enabled
	cfg.Health.CheckIntervalSeconds = 3600
	manager := config.NewManager(cfg, "")
	repo := database.NewHealthRepository(db, database.DialectSQLite)
	metadataService := metadata.NewMetadataService(t.TempDir())
	checker := health.NewHealthChecker(repo, metadataService, nil, manager.GetConfig, nil)
	worker := health.NewHealthWorker(checker, repo, metadataService, nil, nil, manager.GetConfig, nil)
	workerCtx, cancelWorker := context.WithCancel(context.Background())
	require.NoError(t, worker.Start(workerCtx))
	t.Cleanup(func() {
		cancelWorker()
		if worker.IsRunning() {
			require.NoError(t, worker.Stop(context.Background()))
		}
	})

	result, err := db.Exec(`INSERT INTO file_health (file_path, status) VALUES ('movies/stale.mkv', 'pending')`)
	require.NoError(t, err)
	id, err := result.LastInsertId()
	require.NoError(t, err)
	armed.Store(true)

	server := &Server{
		healthRepo:     repo,
		healthWorker:   worker,
		metadataReader: metadata.NewMetadataReader(metadataService),
	}
	app := fiber.New()
	app.Post("/health/:id/check-now", server.handleDirectHealthCheck)
	response, err := app.Test(httptest.NewRequest(http.MethodPost,
		fmt.Sprintf("/health/%d/check-now", id), nil), -1)
	require.NoError(t, err)
	defer response.Body.Close()

	assert.Equal(t, http.StatusConflict, response.StatusCode,
		"a claim lost after the handler precheck is the same conflict as an initially checking row")
	assert.True(t, fired.Load(), "the fixture must advance the row after the handler's observed snapshot")
	if value := mutationErr.Load(); value != nil {
		require.NoError(t, value.(error))
	}
	current, err := repo.GetFileHealthByID(context.Background(), id)
	require.NoError(t, err)
	require.NotNil(t, current)
	assert.Equal(t, database.HealthStatusChecking, current.Status)
}
