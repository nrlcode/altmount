package api

import (
	"bytes"
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
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
	require.NoError(t, os.MkdirAll(env.ownedRoot, 0o755))
	outside := t.TempDir()
	victim := filepath.Join(outside, "victim.nzb")
	require.NoError(t, os.WriteFile(victim, []byte("keep"), 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(env.ownedRoot, "escape")))
	item := env.addItem(t, filepath.Join(env.ownedRoot, "escape", "victim.nzb"), database.QueueStatusFailed)

	app := fiber.New()
	app.Delete("/queue/:id", env.server.handleDeleteQueue)
	resp, err := app.Test(httptest.NewRequest("DELETE", fmt.Sprintf("/queue/%d", item.ID), nil))
	require.NoError(t, err)
	assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
	assert.FileExists(t, victim)
	requireQueueItemExists(t, env.repo, item.ID, true)
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
			if runtime.GOOS == "windows" {
				t.Skip("symlink authority test requires Unix symlink semantics")
			}
			env := newQueueCleanupTestEnv(t)
			require.NoError(t, os.MkdirAll(env.ownedRoot, 0o755))
			outside := t.TempDir()
			victim := filepath.Join(outside, "victim.nzb")
			require.NoError(t, os.WriteFile(victim, []byte("keep"), 0o600))
			require.NoError(t, os.Symlink(outside, filepath.Join(env.ownedRoot, "escape")))
			item := env.addItem(t, filepath.Join(env.ownedRoot, "escape", "victim.nzb"), tt.status)

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
			assert.FileExists(t, victim)
			requireQueueItemExists(t, env.repo, item.ID, true)
		})
	}
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
