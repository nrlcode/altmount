package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/importer"
	"github.com/javi11/altmount/internal/metadata"
	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type queueCleanupTestEnv struct {
	server    *Server
	repo      *database.Repository
	db        *sql.DB
	ownedRoot string
}

func newQueueCleanupTestEnv(t *testing.T) *queueCleanupTestEnv {
	t.Helper()
	configDir := t.TempDir()
	dbPath := filepath.Join(configDir, "altmount.db")
	db, err := database.NewDB(database.Config{DatabasePath: dbPath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	cfg := config.DefaultConfig(configDir)
	repo := database.NewRepository(db.Connection(), database.DialectSQLite)
	ownedRoot := filepath.Join(configDir, ".nzbs")
	metadataService := metadata.NewMetadataService(cfg.Metadata.RootPath)
	require.NoError(t, metadataService.ConfigureCleanupRoots(ownedRoot, ownedRoot))
	return &queueCleanupTestEnv{
		server: &Server{
			queueRepo:       repo,
			configManager:   &mockConfigManager{cfg: cfg},
			importerService: &importer.Service{},
			metadataService: metadataService,
		},
		repo:      repo,
		db:        db.Connection(),
		ownedRoot: filepath.Join(ownedRoot, "failed"),
	}
}

func (e *queueCleanupTestEnv) addItem(t *testing.T, path string, status database.QueueStatus, downloadID ...string) *database.ImportQueueItem {
	t.Helper()
	item := &database.ImportQueueItem{
		NzbPath:    path,
		Priority:   database.QueuePriorityNormal,
		Status:     status,
		MaxRetries: 3,
		CreatedAt:  time.Now(),
	}
	if len(downloadID) > 0 {
		item.DownloadID = &downloadID[0]
	}
	require.NoError(t, e.repo.AddToQueue(context.Background(), item))
	return item
}

func requireQueueItemExists(t *testing.T, repo *database.Repository, id int64, want bool) {
	t.Helper()
	item, err := repo.GetQueueItem(context.Background(), id)
	require.NoError(t, err)
	if want {
		require.NotNil(t, item)
	} else {
		require.Nil(t, item)
	}
}

func requireQueueItemStatus(t *testing.T, repo *database.Repository, id int64, want database.QueueStatus) {
	t.Helper()
	item, err := repo.GetQueueItem(context.Background(), id)
	require.NoError(t, err)
	require.NotNil(t, item)
	assert.Equal(t, want, item.Status)
}

func (e *queueCleanupTestEnv) ageItem(t *testing.T, item *database.ImportQueueItem) {
	t.Helper()
	old := time.Now().Add(-8 * 24 * time.Hour)
	_, err := e.db.Exec(`UPDATE import_queue SET created_at = ?, updated_at = ? WHERE id = ?`, old, old, item.ID)
	require.NoError(t, err)
}

func (e *queueCleanupTestEnv) addSymlinkEscapedItem(
	t *testing.T,
	status database.QueueStatus,
	downloadID ...string,
) (*database.ImportQueueItem, string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(e.ownedRoot, 0o755))
	outside := t.TempDir()
	victim := filepath.Join(outside, "victim.nzb")
	require.NoError(t, os.WriteFile(victim, []byte("keep"), 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(e.ownedRoot, "escape")))
	item := e.addItem(t, filepath.Join(e.ownedRoot, "escape", "victim.nzb"), status, downloadID...)
	return item, victim
}

func (e *queueCleanupTestEnv) addNonEmptyDirectoryItem(
	t *testing.T,
	status database.QueueStatus,
	downloadID ...string,
) (*database.ImportQueueItem, string) {
	t.Helper()
	target := filepath.Join(e.ownedRoot, fmt.Sprintf("non-empty-%d.nzb", time.Now().UnixNano()))
	require.NoError(t, os.MkdirAll(target, 0o755))
	child := filepath.Join(target, "keep")
	require.NoError(t, os.WriteFile(child, []byte("keep"), 0o600))
	return e.addItem(t, target, status, downloadID...), child
}

func (e *queueCleanupTestEnv) addHistory(t *testing.T, item *database.ImportQueueItem, nzbName string) {
	t.Helper()
	nzbID := item.ID
	require.NoError(t, e.repo.AddImportHistory(context.Background(), &database.ImportHistory{
		DownloadID:  item.DownloadID,
		NzbID:       &nzbID,
		NzbName:     nzbName,
		FileName:    "payload.mkv",
		FileSize:    123,
		VirtualPath: "/library/payload.mkv",
		CompletedAt: time.Now(),
	}))
}

func requireSABDeleteResponse(t *testing.T, resp *http.Response, wantStatus bool) {
	t.Helper()
	require.Equal(t, fiber.StatusOK, resp.StatusCode)
	defer resp.Body.Close()
	var body SABnzbdDeleteResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, wantStatus, body.Status)
	if wantStatus {
		assert.Nil(t, body.Error)
		return
	}
	require.NotNil(t, body.Error)
	assert.NotEmpty(t, *body.Error)
}

func TestQueueDeleteRejectsUnownedPersistedPathBeforeRowRemoval(t *testing.T) {
	env := newQueueCleanupTestEnv(t)
	unowned := filepath.Join(t.TempDir(), "operator-source.nzb")
	require.NoError(t, os.WriteFile(unowned, []byte("<nzb/>"), 0o600))
	item := env.addItem(t, unowned, database.QueueStatusPending)

	app := fiber.New()
	app.Delete("/queue/:id", env.server.handleDeleteQueue)
	resp, err := app.Test(httptest.NewRequest("DELETE", fmt.Sprintf("/queue/%d", item.ID), nil))
	require.NoError(t, err)
	assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
	assert.FileExists(t, unowned, "an external source path is not queue-owned deletion authority")
	requireQueueItemExists(t, env.repo, item.ID, true)
}

func TestQueueDeleteRejectsSymlinkedOwnedParentBeforeRowRemoval(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink authority test requires Unix symlink semantics")
	}
	env := newQueueCleanupTestEnv(t)
	item, victim := env.addSymlinkEscapedItem(t, database.QueueStatusFailed)

	app := fiber.New()
	app.Delete("/queue/:id", env.server.handleDeleteQueue)
	resp, err := app.Test(httptest.NewRequest("DELETE", fmt.Sprintf("/queue/%d", item.ID), nil))
	require.NoError(t, err)
	assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
	assert.FileExists(t, victim)
	requireQueueItemStatus(t, env.repo, item.ID, database.QueueStatusFailed)
}

func TestQueueDeleteRetainsRowWhenContainedDirectoryCleanupFails(t *testing.T) {
	env := newQueueCleanupTestEnv(t)
	target := filepath.Join(env.ownedRoot, "non-empty.nzb")
	require.NoError(t, os.MkdirAll(target, 0o755))
	child := filepath.Join(target, "payload")
	require.NoError(t, os.WriteFile(child, []byte("keep"), 0o600))
	item := env.addItem(t, target, database.QueueStatusFailed)

	app := fiber.New()
	app.Delete("/queue/:id", env.server.handleDeleteQueue)
	resp, err := app.Test(httptest.NewRequest("DELETE", fmt.Sprintf("/queue/%d", item.ID), nil))
	require.NoError(t, err)
	assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
	assert.DirExists(t, target)
	assert.FileExists(t, child)
	requireQueueItemStatus(t, env.repo, item.ID, database.QueueStatusFailed)
}

func TestQueueAndSystemCleanupOwnersRejectSymlinkEscapes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink authority test requires Unix symlink semantics")
	}
	tests := []struct {
		name   string
		status database.QueueStatus
		method string
		path   string
		mount  func(*fiber.App, *Server)
		body   func(*database.ImportQueueItem) string
	}{
		{
			name: "bulk", status: database.QueueStatusPending, method: "DELETE", path: "/queue/bulk",
			mount: func(app *fiber.App, server *Server) { app.Delete("/queue/bulk", server.handleDeleteQueueBulk) },
			body:  func(item *database.ImportQueueItem) string { return fmt.Sprintf(`{"ids":[%d]}`, item.ID) },
		},
		{
			name: "completed", status: database.QueueStatusCompleted, method: "DELETE", path: "/queue/completed",
			mount: func(app *fiber.App, server *Server) { app.Delete("/queue/completed", server.handleClearCompletedQueue) },
		},
		{
			name: "failed", status: database.QueueStatusFailed, method: "DELETE", path: "/queue/failed",
			mount: func(app *fiber.App, server *Server) { app.Delete("/queue/failed", server.handleClearFailedQueue) },
		},
		{
			name: "pending", status: database.QueueStatusPending, method: "DELETE", path: "/queue/pending",
			mount: func(app *fiber.App, server *Server) { app.Delete("/queue/pending", server.handleClearPendingQueue) },
		},
		{
			name: "system cleanup", status: database.QueueStatusCompleted, method: "POST", path: "/system/cleanup",
			mount: func(app *fiber.App, server *Server) { app.Post("/system/cleanup", server.handleSystemCleanup) },
		},
		{
			name: "queue reset", status: database.QueueStatusFailed, method: "POST", path: "/system/reset?reset_queue=true",
			mount: func(app *fiber.App, server *Server) { app.Post("/system/reset", server.handleResetSystemStats) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newQueueCleanupTestEnv(t)
			item, victim := env.addSymlinkEscapedItem(t, tt.status)
			if tt.name == "system cleanup" {
				env.ageItem(t, item)
			}
			app := fiber.New()
			tt.mount(app, env.server)
			var body *bytes.Buffer
			if tt.body != nil {
				body = bytes.NewBufferString(tt.body(item))
			} else {
				body = bytes.NewBuffer(nil)
			}
			req := httptest.NewRequest(tt.method, tt.path, body)
			if tt.body != nil {
				req.Header.Set("Content-Type", "application/json")
			}
			resp, err := app.Test(req)
			require.NoError(t, err)
			assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
			assert.FileExists(t, victim)
			requireQueueItemStatus(t, env.repo, item.ID, tt.status)
		})
	}
}

func TestQueueAndSystemCleanupOwnersRemoveOwnedFilesAndRows(t *testing.T) {
	tests := []struct {
		name   string
		status database.QueueStatus
		method string
		path   string
		mount  func(*fiber.App, *Server)
	}{
		{
			name: "single", status: database.QueueStatusPending, method: "DELETE",
			mount: func(app *fiber.App, server *Server) { app.Delete("/queue/:id", server.handleDeleteQueue) },
		},
		{
			name: "completed", status: database.QueueStatusCompleted, method: "DELETE", path: "/queue/completed",
			mount: func(app *fiber.App, server *Server) { app.Delete("/queue/completed", server.handleClearCompletedQueue) },
		},
		{
			name: "failed", status: database.QueueStatusFailed, method: "DELETE", path: "/queue/failed",
			mount: func(app *fiber.App, server *Server) { app.Delete("/queue/failed", server.handleClearFailedQueue) },
		},
		{
			name: "pending", status: database.QueueStatusPending, method: "DELETE", path: "/queue/pending",
			mount: func(app *fiber.App, server *Server) { app.Delete("/queue/pending", server.handleClearPendingQueue) },
		},
		{
			name: "system cleanup", status: database.QueueStatusCompleted, method: "POST", path: "/system/cleanup",
			mount: func(app *fiber.App, server *Server) { app.Post("/system/cleanup", server.handleSystemCleanup) },
		},
		{
			name: "queue reset", status: database.QueueStatusFailed, method: "POST", path: "/system/reset?reset_queue=true",
			mount: func(app *fiber.App, server *Server) { app.Post("/system/reset", server.handleResetSystemStats) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newQueueCleanupTestEnv(t)
			require.NoError(t, os.MkdirAll(env.ownedRoot, 0o755))
			path := filepath.Join(env.ownedRoot, "owned.nzb")
			require.NoError(t, os.WriteFile(path, []byte("<nzb/>"), 0o600))
			item := env.addItem(t, path, tt.status)
			if tt.name == "system cleanup" {
				env.ageItem(t, item)
			}
			app := fiber.New()
			tt.mount(app, env.server)
			requestPath := tt.path
			if requestPath == "" {
				requestPath = fmt.Sprintf("/queue/%d", item.ID)
			}
			resp, err := app.Test(httptest.NewRequest(tt.method, requestPath, nil))
			require.NoError(t, err)
			if tt.name == "single" {
				assert.Equal(t, fiber.StatusNoContent, resp.StatusCode)
			} else {
				assert.Equal(t, fiber.StatusOK, resp.StatusCode)
			}
			assert.NoFileExists(t, path)
			requireQueueItemExists(t, env.repo, item.ID, false)
		})
	}
}

func TestQueueBulkDeleteCleansOwnedFiles(t *testing.T) {
	env := newQueueCleanupTestEnv(t)
	require.NoError(t, os.MkdirAll(env.ownedRoot, 0o755))
	paths := []string{
		filepath.Join(env.ownedRoot, "one.nzb"),
		filepath.Join(env.ownedRoot, "two.nzb"),
	}
	items := make([]*database.ImportQueueItem, 0, len(paths))
	for _, path := range paths {
		require.NoError(t, os.WriteFile(path, []byte("<nzb/>"), 0o600))
		items = append(items, env.addItem(t, path, database.QueueStatusPending))
	}

	app := fiber.New()
	app.Delete("/queue/bulk", env.server.handleDeleteQueueBulk)
	body := fmt.Sprintf(`{"ids":[%d,%d]}`, items[0].ID, items[1].ID)
	req := httptest.NewRequest("DELETE", "/queue/bulk", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, fiber.StatusOK, resp.StatusCode)
	for i, path := range paths {
		assert.NoFileExists(t, path)
		requireQueueItemExists(t, env.repo, items[i].ID, false)
	}
}

func TestQueueBulkDeleteStopsAfterOwnedCleanupFailure(t *testing.T) {
	env := newQueueCleanupTestEnv(t)
	require.NoError(t, os.MkdirAll(env.ownedRoot, 0o755))

	failing, child := env.addNonEmptyDirectoryItem(t, database.QueueStatusFailed)
	ownedPath := filepath.Join(env.ownedRoot, "first-owned.nzb")
	require.NoError(t, os.WriteFile(ownedPath, []byte("delete"), 0o600))
	owned := env.addItem(t, ownedPath, database.QueueStatusPending)
	sentinelPath := filepath.Join(env.ownedRoot, "last-owned.nzb")
	require.NoError(t, os.WriteFile(sentinelPath, []byte("retain"), 0o600))
	sentinel := env.addItem(t, sentinelPath, database.QueueStatusPending)
	require.Less(t, failing.ID, owned.ID, "fixture row order must differ from request order")

	app := fiber.New()
	app.Delete("/queue/bulk", env.server.handleDeleteQueueBulk)
	body := fmt.Sprintf(`{"ids":[%d,%d,%d]}`, owned.ID, failing.ID, sentinel.ID)
	req := httptest.NewRequest("DELETE", "/queue/bulk", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)

	assert.NoFileExists(t, ownedPath, "the earlier owned item completes before the later failure")
	requireQueueItemExists(t, env.repo, owned.ID, false)
	assert.FileExists(t, child)
	requireQueueItemStatus(t, env.repo, failing.ID, database.QueueStatusFailed)
	assert.FileExists(t, sentinelPath, "fail-stop must not process a later request item")
	requireQueueItemStatus(t, env.repo, sentinel.ID, database.QueueStatusPending)
}

func TestQueueStatusClearPreservesOwnershipWhenCleanupFails(t *testing.T) {
	tests := []struct {
		name   string
		status database.QueueStatus
		path   string
	}{
		{name: "completed", status: database.QueueStatusCompleted, path: "/queue/completed"},
		{name: "failed", status: database.QueueStatusFailed, path: "/queue/failed"},
		{name: "pending", status: database.QueueStatusPending, path: "/queue/pending"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newQueueCleanupTestEnv(t)
			item, child := env.addNonEmptyDirectoryItem(t, tt.status)

			app := fiber.New()
			switch tt.status {
			case database.QueueStatusCompleted:
				app.Delete(tt.path, env.server.handleClearCompletedQueue)
			case database.QueueStatusFailed:
				app.Delete(tt.path, env.server.handleClearFailedQueue)
			default:
				app.Delete(tt.path, env.server.handleClearPendingQueue)
			}
			resp, err := app.Test(httptest.NewRequest("DELETE", tt.path, nil))
			require.NoError(t, err)
			assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
			assert.FileExists(t, child)
			requireQueueItemStatus(t, env.repo, item.ID, tt.status)
		})
	}
}

func TestSystemCleanupRetainsCompletedRowWhenCleanupFails(t *testing.T) {
	env := newQueueCleanupTestEnv(t)
	item, child := env.addNonEmptyDirectoryItem(t, database.QueueStatusCompleted)
	env.ageItem(t, item)

	app := fiber.New()
	app.Post("/system/cleanup", env.server.handleSystemCleanup)
	resp, err := app.Test(httptest.NewRequest("POST", "/system/cleanup", nil))
	require.NoError(t, err)
	assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
	assert.FileExists(t, child)
	requireQueueItemStatus(t, env.repo, item.ID, database.QueueStatusCompleted)
}

func TestResetSystemStatsSurfacesQueueCleanupFailure(t *testing.T) {
	env := newQueueCleanupTestEnv(t)
	item, child := env.addNonEmptyDirectoryItem(t, database.QueueStatusFailed)

	app := fiber.New()
	app.Post("/system/stats/reset", env.server.handleResetSystemStats)
	resp, err := app.Test(httptest.NewRequest("POST", "/system/stats/reset?reset_queue=true", nil))
	require.NoError(t, err)
	assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
	assert.FileExists(t, child)
	requireQueueItemStatus(t, env.repo, item.ID, database.QueueStatusFailed)
}

func TestSABQueueDeletionCleansOwnedFileForEveryIdentifier(t *testing.T) {
	tests := []struct {
		name       string
		downloadID string
		value      func(*database.ImportQueueItem) string
	}{
		{name: "numeric id", value: func(item *database.ImportQueueItem) string { return fmt.Sprint(item.ID) }},
		{name: "download id", downloadID: "sab-download-id", value: func(item *database.ImportQueueItem) string { return *item.DownloadID }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newQueueCleanupTestEnv(t)
			require.NoError(t, os.MkdirAll(env.ownedRoot, 0o755))
			path := filepath.Join(env.ownedRoot, "sab-owned.nzb")
			require.NoError(t, os.WriteFile(path, []byte("<nzb/>"), 0o600))
			item := env.addItem(t, path, database.QueueStatusPending, tt.downloadID)

			app := fiber.New()
			app.Get("/sab", env.server.handleSABnzbdQueueDelete)
			resp, err := app.Test(httptest.NewRequest("GET", "/sab?value="+tt.value(item), nil))
			require.NoError(t, err)
			require.Equal(t, fiber.StatusOK, resp.StatusCode)
			assert.NoFileExists(t, path)
			requireQueueItemExists(t, env.repo, item.ID, false)
		})
	}
}

func TestSABQueueDeletionReportsCleanupFailureForEveryIdentifier(t *testing.T) {
	tests := []struct {
		name       string
		downloadID string
		value      func(*database.ImportQueueItem) string
	}{
		{name: "numeric id", value: func(item *database.ImportQueueItem) string { return fmt.Sprint(item.ID) }},
		{name: "download id", downloadID: "sab-queue-failure", value: func(item *database.ImportQueueItem) string { return *item.DownloadID }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newQueueCleanupTestEnv(t)
			item, child := env.addNonEmptyDirectoryItem(t, database.QueueStatusPending, tt.downloadID)

			app := fiber.New()
			app.Get("/sab", env.server.handleSABnzbdQueueDelete)
			resp, err := app.Test(httptest.NewRequest("GET", "/sab?value="+tt.value(item), nil))
			require.NoError(t, err)
			requireSABDeleteResponse(t, resp, false)
			assert.FileExists(t, child)
			requireQueueItemStatus(t, env.repo, item.ID, database.QueueStatusPending)
		})
	}
}

func TestSABQueueDeletionRejectsSymlinkEscapeForEveryIdentifier(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink authority test requires Unix symlink semantics")
	}
	tests := []struct {
		name       string
		downloadID string
		value      func(*database.ImportQueueItem) string
	}{
		{name: "numeric id", value: func(item *database.ImportQueueItem) string { return fmt.Sprint(item.ID) }},
		{name: "download id", downloadID: "sab-queue-symlink", value: func(item *database.ImportQueueItem) string { return *item.DownloadID }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newQueueCleanupTestEnv(t)
			item, victim := env.addSymlinkEscapedItem(t, database.QueueStatusPending, tt.downloadID)
			app := fiber.New()
			app.Get("/sab", env.server.handleSABnzbdQueueDelete)
			resp, err := app.Test(httptest.NewRequest("GET", "/sab?value="+tt.value(item), nil))
			require.NoError(t, err)
			requireSABDeleteResponse(t, resp, false)
			assert.FileExists(t, victim)
			requireQueueItemStatus(t, env.repo, item.ID, database.QueueStatusPending)
		})
	}
}

func TestSABHistoryDeletionReportsQueueCleanupFailureForEveryIdentifier(t *testing.T) {
	tests := []struct {
		name       string
		downloadID string
		value      func(*database.ImportQueueItem) string
	}{
		{name: "numeric queue id", downloadID: "sab-history-numeric", value: func(item *database.ImportQueueItem) string { return fmt.Sprint(item.ID) }},
		{name: "download id", downloadID: "sab-history-download", value: func(item *database.ImportQueueItem) string { return *item.DownloadID }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newQueueCleanupTestEnv(t)
			item, child := env.addNonEmptyDirectoryItem(t, database.QueueStatusCompleted, tt.downloadID)
			env.addHistory(t, item, "queue-backed.nzb")

			app := fiber.New()
			app.Get("/sab-history", env.server.handleSABnzbdHistoryDelete)
			resp, err := app.Test(httptest.NewRequest("GET", "/sab-history?value="+tt.value(item), nil))
			require.NoError(t, err)
			requireSABDeleteResponse(t, resp, false)
			assert.FileExists(t, child)
			requireQueueItemStatus(t, env.repo, item.ID, database.QueueStatusCompleted)
			history, historyErr := env.repo.GetImportHistoryByNzbID(context.Background(), item.ID)
			require.NoError(t, historyErr)
			require.NotNil(t, history, "cleanup failure must retain associated history")
		})
	}
}

func TestSABHistoryDeletionCleansOwnedQueueAndHistoryForEveryIdentifier(t *testing.T) {
	tests := []struct {
		name       string
		downloadID string
		value      func(*database.ImportQueueItem) string
	}{
		{name: "numeric queue id", downloadID: "sab-history-owned-numeric", value: func(item *database.ImportQueueItem) string { return fmt.Sprint(item.ID) }},
		{name: "download id", downloadID: "sab-history-owned-download", value: func(item *database.ImportQueueItem) string { return *item.DownloadID }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newQueueCleanupTestEnv(t)
			require.NoError(t, os.MkdirAll(env.ownedRoot, 0o755))
			path := filepath.Join(env.ownedRoot, "history-owned.nzb")
			require.NoError(t, os.WriteFile(path, []byte("<nzb/>"), 0o600))
			item := env.addItem(t, path, database.QueueStatusCompleted, tt.downloadID)
			env.addHistory(t, item, "history-owned.nzb")
			app := fiber.New()
			app.Get("/sab-history", env.server.handleSABnzbdHistoryDelete)
			resp, err := app.Test(httptest.NewRequest("GET", "/sab-history?value="+tt.value(item), nil))
			require.NoError(t, err)
			requireSABDeleteResponse(t, resp, true)
			assert.NoFileExists(t, path)
			requireQueueItemExists(t, env.repo, item.ID, false)
			history, historyErr := env.repo.GetImportHistoryByNzbID(context.Background(), item.ID)
			require.NoError(t, historyErr)
			assert.Nil(t, history)
		})
	}
}

func TestSABHistoryDeletionRejectsSymlinkEscapeForEveryIdentifier(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink authority test requires Unix symlink semantics")
	}
	tests := []struct {
		name       string
		downloadID string
		value      func(*database.ImportQueueItem) string
	}{
		{name: "numeric queue id", downloadID: "sab-history-link-numeric", value: func(item *database.ImportQueueItem) string { return fmt.Sprint(item.ID) }},
		{name: "download id", downloadID: "sab-history-link-download", value: func(item *database.ImportQueueItem) string { return *item.DownloadID }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newQueueCleanupTestEnv(t)
			item, victim := env.addSymlinkEscapedItem(t, database.QueueStatusCompleted, tt.downloadID)
			env.addHistory(t, item, "queue-backed.nzb")
			app := fiber.New()
			app.Get("/sab-history", env.server.handleSABnzbdHistoryDelete)
			resp, err := app.Test(httptest.NewRequest("GET", "/sab-history?value="+tt.value(item), nil))
			require.NoError(t, err)
			requireSABDeleteResponse(t, resp, false)
			assert.FileExists(t, victim)
			requireQueueItemStatus(t, env.repo, item.ID, database.QueueStatusCompleted)
			history, historyErr := env.repo.GetImportHistoryByNzbID(context.Background(), item.ID)
			require.NoError(t, historyErr)
			require.NotNil(t, history)
		})
	}
}

func TestSABHistoryOnlyDeletionDoesNotTreatNzbNameAsPathAuthority(t *testing.T) {
	env := newQueueCleanupTestEnv(t)
	victim := filepath.Join(t.TempDir(), "history-name-is-not-authority.nzb")
	require.NoError(t, os.WriteFile(victim, []byte("keep"), 0o600))
	downloadID := "history-only-download"
	require.NoError(t, env.repo.AddImportHistory(context.Background(), &database.ImportHistory{
		DownloadID:  &downloadID,
		NzbName:     victim,
		FileName:    "payload.mkv",
		FileSize:    123,
		VirtualPath: "/library/history-only.mkv",
		CompletedAt: time.Now(),
	}))

	app := fiber.New()
	app.Get("/sab-history", env.server.handleSABnzbdHistoryDelete)
	resp, err := app.Test(httptest.NewRequest("GET", "/sab-history?value="+downloadID, nil))
	require.NoError(t, err)
	requireSABDeleteResponse(t, resp, true)
	assert.FileExists(t, victim, "history nzb_name is display metadata, not deletion authority")
	history, historyErr := env.repo.GetImportHistoryByDownloadID(context.Background(), downloadID)
	require.NoError(t, historyErr)
	require.Nil(t, history)
}

func TestManualImportFilePublishesOnlyRootedQueueAuthority(t *testing.T) {
	tempRoot := t.TempDir()
	for _, name := range []string{"TMPDIR", "TMP", "TEMP"} {
		t.Setenv(name, tempRoot)
	}
	configDir := filepath.Join(tempRoot, "config")
	require.NoError(t, os.MkdirAll(configDir, 0o755))
	cfg := config.DefaultConfig(configDir)
	cfg.Database.Path = filepath.Join(configDir, "altmount.db")
	cfg.Metadata.RootPath = filepath.Join(configDir, "metadata")
	apiKey := strings.Repeat("k", 32)
	cfg.API.KeyOverride = apiKey

	db, err := database.NewDB(database.Config{DatabasePath: cfg.Database.Path})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	queueRoot := filepath.Join(tempRoot, ".altmount-queue")
	unsafeCommit := installAPIQueuePublicationCommitGuard(t, db.Connection(), queueRoot)
	storeRoot := filepath.Join(configDir, ".nzbs")
	metadataService := metadata.NewMetadataService(cfg.Metadata.RootPath)
	require.NoError(t, metadataService.ConfigureCleanupRoots(storeRoot, queueRoot, storeRoot))
	cfgGetter := config.ConfigGetter(func() *config.Config { return cfg })
	importService, err := importer.NewService(
		importer.ServiceConfig{Workers: 1}, metadataService, db, nil, nil, cfgGetter,
		nil, nil, nil,
	)
	require.NoError(t, err)

	server := &Server{
		queueRepo:       database.NewRepository(db.Connection(), db.Dialect()),
		configManager:   &mockConfigManager{cfg: cfg},
		importerService: importService,
		metadataService: metadataService,
	}
	source := filepath.Join(t.TempDir(), "manual-import.nzb")
	require.NoError(t, os.WriteFile(source, []byte("<nzb/>"), 0o600))
	body := fmt.Sprintf(`{"file_path":%q}`, source)
	app := fiber.New()
	app.Post("/import/file", server.handleManualImportFile)
	req := httptest.NewRequest("POST", "/import/file?apikey="+apiKey, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.False(t, unsafeCommit.Load(), "manual import must not commit its caller path")
	require.Equal(t, fiber.StatusOK, resp.StatusCode)
	defer resp.Body.Close()
	var envelope struct {
		Success bool                 `json:"success"`
		Data    ManualImportResponse `json:"data"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&envelope))
	require.True(t, envelope.Success)
	require.NotZero(t, envelope.Data.QueueID)

	item, err := db.Repository.GetQueueItem(context.Background(), envelope.Data.QueueID)
	require.NoError(t, err)
	require.NotNil(t, item)
	rel, err := filepath.Rel(queueRoot, item.NzbPath)
	require.NoError(t, err)
	require.NotEqual(t, ".", rel)
	require.True(t, filepath.IsLocal(rel), "persisted path must be a strict queue-root child")
	assert.FileExists(t, item.NzbPath)
}

func installAPIQueuePublicationCommitGuard(t *testing.T, db *sql.DB, queueRoot string) *atomic.Bool {
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
				return fmt.Errorf("unexpected SQLite driver connection %T", driverConn)
			}
			var sawPath atomic.Bool
			var rootedPath atomic.Bool
			if err := sqliteConn.RegisterFunc("fbase_record_api_admission_path", func(path string) int {
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
		CREATE TRIGGER record_api_admission_path_insert
		AFTER INSERT ON import_queue
		BEGIN
			SELECT fbase_record_api_admission_path(NEW.nzb_path);
		END;
		CREATE TRIGGER record_api_admission_path_update
		AFTER UPDATE OF nzb_path ON import_queue
		BEGIN
			SELECT fbase_record_api_admission_path(NEW.nzb_path);
		END;
	`)
	require.NoError(t, err)
	return attempted
}
