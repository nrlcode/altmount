package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
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

func newFailClosedHealthAPI(t *testing.T) (*sql.DB, *database.HealthRepository, *metadata.MetadataService, *config.Config) {
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
			download_id TEXT
		)
	`)
	require.NoError(t, err)
	cfg := config.DefaultConfig()
	cfg.Metadata.RootPath = t.TempDir()
	return db, database.NewHealthRepository(db, database.DialectSQLite), metadata.NewMetadataService(cfg.Metadata.RootPath), cfg
}

func stableNon2xx(t *testing.T, app *fiber.App, request func() *http.Request) {
	t.Helper()
	first, err := app.Test(request(), -1)
	require.NoError(t, err)
	defer first.Body.Close()
	firstBody, err := io.ReadAll(first.Body)
	require.NoError(t, err)
	second, err := app.Test(request(), -1)
	require.NoError(t, err)
	defer second.Body.Close()
	secondBody, err := io.ReadAll(second.Body)
	require.NoError(t, err)
	type errorEnvelope struct {
		Success bool `json:"success"`
		Error   struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	var firstEnvelope, secondEnvelope errorEnvelope
	require.NoError(t, json.Unmarshal(firstBody, &firstEnvelope))
	require.NoError(t, json.Unmarshal(secondBody, &secondEnvelope))
	assert.Equal(t, http.StatusConflict, first.StatusCode)
	assert.Equal(t, http.StatusConflict, second.StatusCode)
	assert.False(t, firstEnvelope.Success)
	assert.False(t, secondEnvelope.Success)
	assert.Equal(t, "health_effects_temporarily_disabled", firstEnvelope.Error.Code)
	assert.Equal(t, "health_effects_temporarily_disabled", secondEnvelope.Error.Code)
	assert.Equal(t, string(firstBody), string(secondBody), "the fail-closed response body must be stable")
}

func TestManualRepairHandlersFailClosedBeforeARR(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
		body []byte
	}{
		{name: "single", path: "/health/1/repair"},
		{name: "bulk", path: "/health/bulk/repair", body: []byte(`{"file_paths":["movies/manual.mkv"]}`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, repo, _, cfg := newFailClosedHealthAPI(t)
			var requests atomic.Int64
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/api/v3/movie/1":
					_, _ = w.Write([]byte(`{"id":1,"title":"Manual repair fixture","path":"/movies/manual","hasFile":false,"monitored":true}`))
				case r.Method == http.MethodGet && r.URL.Path == "/api/v3/history":
					_, _ = w.Write([]byte(`{"page":1,"pageSize":100,"totalRecords":0,"records":[]}`))
				case r.Method == http.MethodPost && r.URL.Path == "/api/v3/command":
					_, _ = w.Write([]byte(`{"id":7,"name":"MoviesSearch","commandName":"MoviesSearch","status":"queued"}`))
				default:
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(upstream.Close)
			enabled := true
			cfg.Arrs.RadarrInstances = []config.ArrsInstanceConfig{{Name: "radarr-test", URL: upstream.URL, APIKey: "test", Enabled: &enabled}}
			_, err := db.Exec(`
				INSERT INTO file_health (file_path, status, metadata)
				VALUES ('movies/manual.mkv', 'corrupted', '{"instanceName":"radarr-test","movie":{"id":1}}')
			`)
			require.NoError(t, err)
			manager := &mockConfigManager{cfg: cfg}
			server := &Server{
				healthRepo: repo, configManager: manager,
				arrsService: arrs.NewService(manager.GetConfigGetter(), manager, nil, nil),
			}
			app := fiber.New()
			if tc.name == "single" {
				app.Post("/health/:id/repair", server.handleRepairHealth)
			} else {
				app.Post("/health/bulk/repair", server.handleRepairHealthBulk)
			}
			stableNon2xx(t, app, func() *http.Request {
				req := httptest.NewRequest(http.MethodPost, tc.path, bytes.NewReader(tc.body))
				req.Header.Set("Content-Type", "application/json")
				return req
			})
			assert.Zero(t, requests.Load(), "manual repair must fail before ARR dispatch")
			row, err := repo.GetFileHealth(context.Background(), "movies/manual.mkv")
			require.NoError(t, err)
			require.NotNil(t, row)
			assert.Equal(t, database.HealthStatusCorrupted, row.Status)
		})
	}
}

func TestDestructiveHealthAPIHandlersFailClosedBeforeMutation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		method string
		path   string
		body   func(time.Time) []byte
	}{
		{name: "single", method: http.MethodDelete, path: "/health/1?delete_meta=true&delete_symlink=true"},
		{name: "bulk", method: http.MethodPost, path: "/health/bulk/delete", body: func(time.Time) []byte {
			return []byte(`{"file_paths":["movies/delete.mkv"],"delete_meta":true,"delete_symlink":true}`)
		}},
		{name: "date cleanup", method: http.MethodDelete, path: "/health/cleanup", body: func(older time.Time) []byte {
			payload, _ := json.Marshal(HealthCleanupRequest{OlderThan: &older, DeleteFiles: true})
			return payload
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, repo, ms, cfg := newFailClosedHealthAPI(t)
			const filePath = "movies/delete.mkv"
			libraryPath := filepath.Join(t.TempDir(), "delete.mkv")
			require.NoError(t, os.WriteFile(libraryPath, []byte("current"), 0o644))
			require.NoError(t, ms.WriteFileMetadata(filePath, ms.CreateFileMetadata(
				1, "current.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY, nil,
				metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
			)))
			_, err := db.Exec(`
				INSERT INTO file_health (file_path, library_path, status, created_at)
				VALUES (?, ?, 'corrupted', datetime('now', '-30 days'))
			`, filePath, libraryPath)
			require.NoError(t, err)
			server := &Server{healthRepo: repo, metadataService: ms, configManager: &mockConfigManager{cfg: cfg}}
			app := fiber.New()
			switch tc.name {
			case "single":
				app.Delete("/health/:id", server.handleDeleteHealth)
			case "bulk":
				app.Post("/health/bulk/delete", server.handleDeleteHealthBulk)
			case "date cleanup":
				app.Delete("/health/cleanup", server.handleCleanupHealth)
			}
			older := time.Now().Add(-7 * 24 * time.Hour)
			stableNon2xx(t, app, func() *http.Request {
				var body []byte
				if tc.body != nil {
					body = tc.body(older)
				}
				req := httptest.NewRequest(tc.method, tc.path, bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				return req
			})
			row, err := repo.GetFileHealth(context.Background(), filePath)
			require.NoError(t, err)
			assert.NotNil(t, row)
			meta, err := ms.ReadFileMetadata(filePath)
			require.NoError(t, err)
			assert.NotNil(t, meta)
			_, err = os.Stat(libraryPath)
			assert.NoError(t, err)
		})
	}
}

func TestARRDeletionWebhookFailsClosedBeforeMutation(t *testing.T) {
	db, repo, ms, cfg := newFailClosedHealthAPI(t)
	const filePath = "movies/webhook.mkv"
	libraryPath := filepath.Join(t.TempDir(), "webhook.mkv")
	cfg.API.KeyOverride = "12345678901234567890123456789012"
	require.NoError(t, ms.WriteFileMetadata(filePath, ms.CreateFileMetadata(
		1, "current.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY, nil,
		metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
	)))
	_, err := db.Exec(`INSERT INTO file_health (file_path, library_path, status) VALUES (?, ?, 'corrupted')`, filePath, libraryPath)
	require.NoError(t, err)
	server := &Server{
		healthRepo: repo, metadataService: ms,
		configManager: &mockConfigManager{cfg: cfg}, arrsService: &arrs.Service{},
	}
	app := fiber.New()
	app.Post("/arrs/webhook", server.handleArrsWebhook)
	body, err := json.Marshal(map[string]any{
		"eventType": "Upgrade", "deletedFiles": []map[string]string{{"path": libraryPath}},
	})
	require.NoError(t, err)
	stableNon2xx(t, app, func() *http.Request {
		req := httptest.NewRequest(http.MethodPost, "/arrs/webhook?apikey="+cfg.API.KeyOverride, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		return req
	})
	row, err := repo.GetFileHealth(context.Background(), filePath)
	require.NoError(t, err)
	assert.NotNil(t, row)
	meta, err := ms.ReadFileMetadata(filePath)
	require.NoError(t, err)
	assert.NotNil(t, meta)
}
