package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/javi11/altmount/internal/arrs"
	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/metadata"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newAPIHealthClaimFixture(t *testing.T) (*sql.DB, *database.HealthRepository, *metadata.MetadataService, *config.Config) {
	t.Helper()
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared&mode=memory")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
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
			download_id TEXT,
			health_claim_token TEXT,
			health_claim_version INTEGER NOT NULL DEFAULT 0
		)
	`)
	require.NoError(t, err)
	cfg := config.DefaultConfig()
	cfg.Metadata.RootPath = t.TempDir()
	return db, database.NewHealthRepository(db, database.DialectSQLite), metadata.NewMetadataService(cfg.Metadata.RootPath), cfg
}

func installRejectAPIHealthClaim(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`
		CREATE TRIGGER reject_api_health_claim
		BEFORE UPDATE OF health_claim_token ON file_health
		WHEN NEW.health_claim_token IS NOT NULL
		BEGIN
			SELECT RAISE(FAIL, 'synthetic API claim failure');
		END
	`)
	require.NoError(t, err)
}

func TestManualRepairHandlersRequireClaimBeforeARR(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
		body []byte
	}{
		{name: "single", path: "/repair/1", body: nil},
		{name: "bulk", path: "/repair/bulk", body: []byte(`{"file_paths":["movies/fenced.mkv"]}`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, repo, _, cfg := newAPIHealthClaimFixture(t)
			var requests atomic.Int64
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`[]`))
			}))
			t.Cleanup(upstream.Close)
			enabled := true
			cfg.Arrs.RadarrInstances = []config.ArrsInstanceConfig{{Name: "radarr-test", URL: upstream.URL, APIKey: "test", Enabled: &enabled}}
			meta := `{"instanceName":"radarr-test","movie":{"id":1}}`
			_, err := db.Exec(`
				INSERT INTO file_health (file_path, status, metadata, scheduled_check_at)
				VALUES ('movies/fenced.mkv', 'corrupted', ?, datetime('now'))
			`, meta)
			require.NoError(t, err)
			installRejectAPIHealthClaim(t, db)

			manager := &mockConfigManager{cfg: cfg}
			server := &Server{
				healthRepo:    repo,
				configManager: manager,
				arrsService:   arrs.NewService(manager.GetConfigGetter(), manager, nil, nil),
			}
			app := fiber.New()
			if tc.name == "single" {
				app.Post("/repair/:id", server.handleRepairHealth)
			} else {
				app.Post("/repair/bulk", server.handleRepairHealthBulk)
			}
			req := httptest.NewRequest(http.MethodPost, tc.path, bytes.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			_, err = app.Test(req, -1)
			require.NoError(t, err)
			assert.Equal(t, int64(0), requests.Load(), "ARR must not be called when durable repair admission fails")
		})
	}
}

func TestBulkDeleteDoesNotRemoveMetadataBeforeDatabaseOwnership(t *testing.T) {
	db, repo, ms, cfg := newAPIHealthClaimFixture(t)
	const filePath = "movies/bulk-delete.mkv"
	require.NoError(t, ms.WriteFileMetadata(filePath, ms.CreateFileMetadata(
		0, "", metapb.FileStatus_FILE_STATUS_HEALTHY, nil,
		metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
	)))
	_, err := db.Exec(`INSERT INTO file_health (file_path, status) VALUES (?, 'corrupted')`, filePath)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TRIGGER reject_bulk_health_delete BEFORE DELETE ON file_health BEGIN SELECT RAISE(FAIL, 'synthetic delete failure'); END`)
	require.NoError(t, err)

	server := &Server{healthRepo: repo, metadataService: ms, configManager: &mockConfigManager{cfg: cfg}}
	app := fiber.New()
	app.Post("/delete", server.handleDeleteHealthBulk)
	body, err := json.Marshal(map[string]any{"file_paths": []string{filePath}, "delete_meta": true})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/delete", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, -1)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, resp.StatusCode, 400)
	meta, err := ms.ReadFileMetadata(filePath)
	require.NoError(t, err)
	assert.NotNil(t, meta, "physical metadata must remain if DB ownership deletion is rejected")
}

func TestCleanupDoesNotRemoveLibraryFileBeforeDatabaseOwnership(t *testing.T) {
	db, repo, _, cfg := newAPIHealthClaimFixture(t)
	libraryRoot := t.TempDir()
	libraryPath := filepath.Join(libraryRoot, "old.mkv")
	require.NoError(t, os.WriteFile(libraryPath, []byte("current"), 0o644))
	cfg.Health.LibraryDir = &libraryRoot
	_, err := db.Exec(`
		INSERT INTO file_health (file_path, library_path, status, created_at)
		VALUES ('movies/old.mkv', ?, 'corrupted', datetime('now', '-30 days'))
	`, libraryPath)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TRIGGER reject_cleanup_health_delete BEFORE DELETE ON file_health BEGIN SELECT RAISE(FAIL, 'synthetic cleanup failure'); END`)
	require.NoError(t, err)

	server := &Server{healthRepo: repo, configManager: &mockConfigManager{cfg: cfg}}
	_, _, _, err = server.cleanupHealthRecords(context.Background(), time.Now().Add(-7*24*time.Hour), nil, true)
	require.Error(t, err)
	_, statErr := os.Stat(libraryPath)
	assert.NoError(t, statErr, "cleanup must delete DB ownership before physical library content")
}
