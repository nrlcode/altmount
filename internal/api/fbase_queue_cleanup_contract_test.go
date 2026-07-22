package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/importer"
	"github.com/javi11/altmount/internal/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type queueCleanupTestEnv struct {
	server    *Server
	repo      *database.Repository
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

	ownedPath := filepath.Join(env.ownedRoot, "first-owned.nzb")
	require.NoError(t, os.WriteFile(ownedPath, []byte("delete"), 0o600))
	owned := env.addItem(t, ownedPath, database.QueueStatusPending)
	failing, child := env.addNonEmptyDirectoryItem(t, database.QueueStatusFailed)
	require.Less(t, owned.ID, failing.ID, "fixture IDs must preserve request order")

	app := fiber.New()
	app.Delete("/queue/bulk", env.server.handleDeleteQueueBulk)
	body := fmt.Sprintf(`{"ids":[%d,%d]}`, owned.ID, failing.ID)
	req := httptest.NewRequest("DELETE", "/queue/bulk", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)

	assert.NoFileExists(t, ownedPath, "the earlier owned item completes before the later failure")
	requireQueueItemExists(t, env.repo, owned.ID, false)
	assert.FileExists(t, child)
	requireQueueItemStatus(t, env.repo, failing.ID, database.QueueStatusFailed)
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
	t.Setenv("TMPDIR", tempRoot)
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
	storeRoot := filepath.Join(configDir, ".nzbs")
	metadataService := metadata.NewMetadataService(cfg.Metadata.RootPath)
	require.NoError(t, metadataService.ConfigureCleanupRoots(storeRoot, queueRoot, storeRoot))
	cfgGetter := config.ConfigGetter(func() *config.Config { return cfg })
	importService, err := importer.NewService(
		importer.ServiceConfig{Workers: 1}, metadataService, db, nil, nil, cfgGetter,
		nil, nil, nil,
	)
	require.NoError(t, err)

	queuePrefix := queueRoot + string(os.PathSeparator)
	sqlPrefix := strings.ReplaceAll(queuePrefix, "'", "''")
	_, err = db.Connection().Exec(`
		CREATE TRIGGER reject_unowned_manual_import_path
		BEFORE INSERT ON import_queue
		WHEN substr(NEW.nzb_path, 1, ` + strconv.Itoa(len(queuePrefix)) + `) != '` + sqlPrefix + `'
		BEGIN
			SELECT RAISE(ABORT, 'unowned manual import path');
		END;
	`)
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
