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
	"strconv"
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

type chg020QueueAPIEnv struct {
	server  *Server
	repo    *database.Repository
	rootDir string
}

func newCHG020QueueAPIEnv(t *testing.T) *chg020QueueAPIEnv {
	t.Helper()
	rootDir := t.TempDir()
	configDir := filepath.Join(rootDir, "config")
	require.NoError(t, os.MkdirAll(configDir, 0o755))
	cfg := config.DefaultConfig(configDir)
	cfg.Database.Path = filepath.Join(configDir, "altmount.db")
	cfg.Metadata.RootPath = filepath.Join(configDir, "metadata")

	db, err := database.NewDB(database.Config{DatabasePath: cfg.Database.Path})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	storeRoot := filepath.Join(configDir, ".nzbs")
	queueRoot := filepath.Join(rootDir, ".altmount-queue")
	metadataService := metadata.NewMetadataService(cfg.Metadata.RootPath)
	require.NoError(t, metadataService.ConfigureCleanupRoots(storeRoot, queueRoot, storeRoot))
	configGetter := config.ConfigGetter(func() *config.Config { return cfg })
	importService, err := importer.NewService(
		importer.ServiceConfig{Workers: 1}, metadataService, db, nil, nil, configGetter,
		nil, nil, nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, importService.Close()) })

	repo := database.NewRepository(db.Connection(), db.Dialect())
	return &chg020QueueAPIEnv{
		server: &Server{
			queueRepo:       repo,
			configManager:   &mockConfigManager{cfg: cfg},
			importerService: importService,
			metadataService: metadataService,
		},
		repo:    repo,
		rootDir: rootDir,
	}
}

func (e *chg020QueueAPIEnv) addItem(t *testing.T, name string, status database.QueueStatus) *database.ImportQueueItem {
	t.Helper()
	item := &database.ImportQueueItem{
		NzbPath:    filepath.Join(e.rootDir, name+".nzb"),
		Priority:   database.QueuePriorityNormal,
		Status:     status,
		MaxRetries: 3,
		CreatedAt:  time.Now(),
	}
	require.NoError(t, e.repo.AddToQueue(context.Background(), item))
	return item
}

func TestFACORECHG020CancelQueueRejectsProcessingRowWithoutRuntimeOwner(t *testing.T) {
	env := newCHG020QueueAPIEnv(t)
	item := env.addItem(t, "processing-without-owner", database.QueueStatusProcessing)

	app := fiber.New()
	app.Post("/queue/:id/cancel", env.server.handleCancelQueue)
	response, err := app.Test(httptest.NewRequest(http.MethodPost,
		fmt.Sprintf("/queue/%d/cancel", item.ID), nil), -1)
	require.NoError(t, err)
	defer response.Body.Close()

	assert.Equal(t, http.StatusConflict, response.StatusCode,
		"a persisted processing state without a live runtime owner is a state conflict, not an accepted cancellation")
	current, err := env.repo.GetQueueItem(context.Background(), item.ID)
	require.NoError(t, err)
	require.NotNil(t, current)
	assert.Equal(t, database.QueueStatusProcessing, current.Status)
}

func TestFACORECHG020CancelQueueBulkCountsMissingRuntimeOwnerAsNotProcessing(t *testing.T) {
	env := newCHG020QueueAPIEnv(t)
	withoutOwner := env.addItem(t, "processing-without-owner", database.QueueStatusProcessing)
	notProcessing := env.addItem(t, "pending", database.QueueStatusPending)
	missingID := int64(1 << 62)

	requestBody := fmt.Sprintf(`{"ids":[%d,%d,%d]}`, withoutOwner.ID, notProcessing.ID, missingID)
	app := fiber.New()
	app.Post("/queue/bulk/cancel", env.server.handleCancelQueueBulk)
	request := httptest.NewRequest(http.MethodPost, "/queue/bulk/cancel", bytes.NewBufferString(requestBody))
	request.Header.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
	response, err := app.Test(request, -1)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusAccepted, response.StatusCode)

	var envelope struct {
		Success bool `json:"success"`
		Data    struct {
			CancelledCount     int               `json:"cancelled_count"`
			NotProcessingCount int               `json:"not_processing_count"`
			NotFoundCount      int               `json:"not_found_count"`
			Results            map[string]string `json:"results"`
		} `json:"data"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&envelope))
	require.True(t, envelope.Success)
	assert.Zero(t, envelope.Data.CancelledCount,
		"a missing runtime owner must never be counted as a successful cancellation")
	assert.Equal(t, 2, envelope.Data.NotProcessingCount,
		"both a non-processing row and a processing row without an owner are not cancellable")
	assert.Equal(t, 1, envelope.Data.NotFoundCount)
	assert.NotEqual(t, "Cancellation requested", envelope.Data.Results[strconv.FormatInt(withoutOwner.ID, 10)])
}
