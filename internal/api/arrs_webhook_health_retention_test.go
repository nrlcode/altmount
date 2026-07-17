package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/javi11/altmount/internal/arrs"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpgradeDeletionRetainsRecentHealthyCurrentRowAndFiles(t *testing.T) {
	db, repo, metadataService, cfg := newAPIHealthClaimFixture(t)
	const apiKey = "facore-arr-webhook-retention-key"
	cfg.API.KeyOverride = apiKey
	cfg.MountPath = t.TempDir()
	libraryRoot := t.TempDir()
	cfg.Health.LibraryDir = &libraryRoot

	const filePath = "movies/recent-current.mkv"
	libraryPath := filepath.Join(libraryRoot, "recent-current.mkv")
	localPath := filepath.Join(cfg.MountPath, filePath)
	require.NoError(t, os.MkdirAll(filepath.Dir(localPath), 0o755))
	require.NoError(t, os.WriteFile(localPath, []byte("current mount content"), 0o644))
	require.NoError(t, metadataService.WriteFileMetadata(filePath, metadataService.CreateFileMetadata(
		1024, "current.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY, nil,
		metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
	)))
	_, err := db.Exec(`
		INSERT INTO file_health
			(file_path, library_path, status, metadata, updated_at)
		VALUES (?, ?, 'healthy', '{"revision":"current"}', datetime('now'))
	`, filePath, libraryPath)
	require.NoError(t, err)

	server := &Server{
		healthRepo:      repo,
		metadataService: metadataService,
		configManager:   &mockConfigManager{cfg: cfg},
		arrsService:     &arrs.Service{},
	}
	app := fiber.New()
	app.Post("/arrs/webhook", server.handleArrsWebhook)
	payload, err := json.Marshal(map[string]any{
		"eventType":    "Upgrade",
		"deletedFiles": []map[string]string{{"path": libraryPath}},
	})
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPost, "/arrs/webhook?apikey="+apiKey, bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	response, err := app.Test(request, -1)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.StatusCode)

	current, err := repo.GetFileHealth(context.Background(), filePath)
	require.NoError(t, err)
	require.NotNil(t, current, "the recent healthy guard must retain database ownership atomically")
	meta, err := metadataService.ReadFileMetadata(filePath)
	require.NoError(t, err)
	require.NotNil(t, meta, "the retained current row must prevent metadata deletion")
	content, err := os.ReadFile(localPath)
	require.NoError(t, err, "the retained current row must prevent local mount deletion")
	assert.Equal(t, "current mount content", string(content))
}

func TestUpgradeDeletionStillConsumesOldCorruptedRowAndFiles(t *testing.T) {
	db, repo, metadataService, cfg := newAPIHealthClaimFixture(t)
	const apiKey = "facore-arr-webhook-old-deletekey"
	cfg.API.KeyOverride = apiKey
	cfg.MountPath = t.TempDir()
	libraryRoot := t.TempDir()
	cfg.Health.LibraryDir = &libraryRoot

	const filePath = "movies/old-corrupted.mkv"
	libraryPath := filepath.Join(libraryRoot, "old-corrupted.mkv")
	localPath := filepath.Join(cfg.MountPath, filePath)
	require.NoError(t, os.MkdirAll(filepath.Dir(localPath), 0o755))
	require.NoError(t, os.WriteFile(localPath, []byte("obsolete mount content"), 0o644))
	require.NoError(t, metadataService.WriteFileMetadata(filePath, metadataService.CreateFileMetadata(
		1024, "obsolete.nzb", metapb.FileStatus_FILE_STATUS_CORRUPTED, nil,
		metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
	)))
	_, err := db.Exec(`
		INSERT INTO file_health
			(file_path, library_path, status, metadata, updated_at)
		VALUES (?, ?, 'corrupted', '{"revision":"obsolete"}', datetime('now', '-10 minutes'))
	`, filePath, libraryPath)
	require.NoError(t, err)

	server := &Server{
		healthRepo:      repo,
		metadataService: metadataService,
		configManager:   &mockConfigManager{cfg: cfg},
		arrsService:     &arrs.Service{},
	}
	app := fiber.New()
	app.Post("/arrs/webhook", server.handleArrsWebhook)
	payload, err := json.Marshal(map[string]any{
		"eventType":    "Upgrade",
		"deletedFiles": []map[string]string{{"path": libraryPath}},
	})
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPost, "/arrs/webhook?apikey="+apiKey, bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	response, err := app.Test(request, -1)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.StatusCode)

	current, err := repo.GetFileHealth(context.Background(), filePath)
	require.NoError(t, err)
	assert.Nil(t, current)
	meta, err := metadataService.ReadFileMetadata(filePath)
	require.NoError(t, err)
	assert.Nil(t, meta)
	_, err = os.Stat(localPath)
	assert.True(t, os.IsNotExist(err))
}
