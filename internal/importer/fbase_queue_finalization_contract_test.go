package importer

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/importer/postprocessor"
	"github.com/javi11/altmount/internal/metadata"
)

type fbaseFinalizationEnv struct {
	service   *Service
	database  *database.DB
	config    *config.Config
	storeRoot string
}

func newFbaseFinalizationEnv(t *testing.T) *fbaseFinalizationEnv {
	t.Helper()

	configDir := t.TempDir()
	cfg := config.DefaultConfig(configDir)
	cfg.Database.Path = filepath.Join(configDir, "altmount.db")
	cfg.Metadata.RootPath = filepath.Join(configDir, "metadata")

	db, err := database.NewDB(database.Config{
		Type:         "sqlite",
		DatabasePath: cfg.Database.Path,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	storeRoot := filepath.Join(configDir, ".nzbs")
	metadataService := metadata.NewMetadataService(cfg.Metadata.RootPath)
	require.NoError(t, metadataService.ConfigureCleanupRoots(
		storeRoot,
		filepath.Join(os.TempDir(), ".altmount-queue"),
		storeRoot,
	))
	metadataService.SetStoreRefCounter(db.StoreRefRepo)
	configGetter := config.ConfigGetter(func() *config.Config { return cfg })

	service := &Service{
		database:        db,
		metadataService: metadataService,
		configGetter:    configGetter,
		postProcessor: postprocessor.NewCoordinator(postprocessor.Config{
			ConfigGetter:    configGetter,
			MetadataService: metadataService,
		}),
		log:         slog.Default(),
		cancelFuncs: make(map[int64]context.CancelFunc),
	}

	return &fbaseFinalizationEnv{
		service:   service,
		database:  db,
		config:    cfg,
		storeRoot: storeRoot,
	}
}

func (e *fbaseFinalizationEnv) addProcessingItem(t *testing.T, nzbPath string) *database.ImportQueueItem {
	t.Helper()
	item := &database.ImportQueueItem{
		NzbPath:             nzbPath,
		Priority:            database.QueuePriorityNormal,
		Status:              database.QueueStatusProcessing,
		MaxRetries:          3,
		SkipArrNotification: true,
		SkipPostImportLinks: true,
		CreatedAt:           time.Now(),
	}
	require.NoError(t, e.database.Repository.AddToQueue(context.Background(), item))
	require.NotZero(t, item.ID)
	return item
}

func TestSuccessfulImportRetainsProcessingOwnershipWhenSourceUnlinkFails(t *testing.T) {
	env := newFbaseFinalizationEnv(t)
	nzbPath := filepath.Join(env.storeRoot, "non-empty-success.nzb")
	require.NoError(t, os.MkdirAll(nzbPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(nzbPath, "keep"), []byte("keep"), 0o600))
	item := env.addProcessingItem(t, nzbPath)

	err := env.service.handleProcessingSuccess(
		context.Background(), item, "/complete/movie.mkv", nil,
	)

	require.Error(t, err, "source unlink failure must prevent completed publication")
	assert.DirExists(t, nzbPath)
	remaining, getErr := env.database.Repository.GetQueueItem(context.Background(), item.ID)
	require.NoError(t, getErr)
	require.NotNil(t, remaining)
	assert.Equal(t, database.QueueStatusProcessing, remaining.Status)
}

func TestSuccessfulImportRemovesOwnedSourceBeforePublishingCompleted(t *testing.T) {
	env := newFbaseFinalizationEnv(t)
	nzbPath := filepath.Join(env.storeRoot, "successful-import.nzb")
	require.NoError(t, os.MkdirAll(filepath.Dir(nzbPath), 0o755))
	require.NoError(t, os.WriteFile(nzbPath, []byte("<nzb/>"), 0o600))
	item := env.addProcessingItem(t, nzbPath)

	err := env.service.handleProcessingSuccess(
		context.Background(), item, "/complete/movie.mkv", nil,
	)

	require.NoError(t, err)
	assert.NoFileExists(t, nzbPath)
	completed, getErr := env.database.Repository.GetQueueItem(context.Background(), item.ID)
	require.NoError(t, getErr)
	require.NotNil(t, completed)
	assert.Equal(t, database.QueueStatusCompleted, completed.Status)
}

func TestSuccessfulImportRejectsUnownedSourceBeforePublishingCompleted(t *testing.T) {
	env := newFbaseFinalizationEnv(t)
	nzbPath := filepath.Join(t.TempDir(), "operator-source.nzb")
	require.NoError(t, os.WriteFile(nzbPath, []byte("keep"), 0o600))
	item := env.addProcessingItem(t, nzbPath)

	err := env.service.handleProcessingSuccess(
		context.Background(), item, "/complete/movie.mkv", nil,
	)

	require.Error(t, err)
	assert.FileExists(t, nzbPath)
	remaining, getErr := env.database.Repository.GetQueueItem(context.Background(), item.ID)
	require.NoError(t, getErr)
	require.NotNil(t, remaining)
	assert.Equal(t, database.QueueStatusProcessing, remaining.Status)
}

func TestSuccessfulFallbackRetainsQueueOwnershipWhenSourceUnlinkFails(t *testing.T) {
	env := newFbaseFinalizationEnv(t)
	nzbPath := filepath.Join(env.storeRoot, "fallback.nzb")
	require.NoError(t, os.MkdirAll(filepath.Dir(nzbPath), 0o755))
	require.NoError(t, os.WriteFile(nzbPath, []byte("<nzb></nzb>"), 0o600))
	item := env.addProcessingItem(t, nzbPath)

	received := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if err := os.Remove(nzbPath); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := os.Mkdir(nzbPath, 0o755); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := os.WriteFile(filepath.Join(nzbPath, "keep"), []byte("keep"), 0o600); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		received <- struct{}{}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":true,"nzo_ids":["SAB-1"]}`))
	}))
	t.Cleanup(server.Close)
	env.config.SABnzbd.FallbackHost = server.URL
	env.config.SABnzbd.FallbackAPIKey = "test-key"

	env.service.handleProcessingFailure(context.Background(), item, errors.New("force fallback"))

	select {
	case <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("fallback server did not receive the NZB")
	}
	assert.DirExists(t, nzbPath)
	remaining, err := env.database.Repository.GetQueueItem(context.Background(), item.ID)
	require.NoError(t, err)
	require.NotNil(t, remaining, "failed local cleanup must retain the transferred queue row")
	assert.Equal(t, database.QueueStatusFailed, remaining.Status)
}

func TestSuccessfulFallbackRemovesOwnedSourceBeforeQueueRow(t *testing.T) {
	env := newFbaseFinalizationEnv(t)
	nzbPath := filepath.Join(env.storeRoot, "fallback-success.nzb")
	require.NoError(t, os.MkdirAll(filepath.Dir(nzbPath), 0o755))
	require.NoError(t, os.WriteFile(nzbPath, []byte("<nzb></nzb>"), 0o600))
	item := env.addProcessingItem(t, nzbPath)

	received := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		received <- struct{}{}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":true,"nzo_ids":["SAB-1"]}`))
	}))
	t.Cleanup(server.Close)
	env.config.SABnzbd.FallbackHost = server.URL
	env.config.SABnzbd.FallbackAPIKey = "test-key"

	env.service.handleProcessingFailure(context.Background(), item, errors.New("force fallback"))

	select {
	case <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("fallback server did not receive the NZB")
	}
	assert.NoFileExists(t, nzbPath)
	remaining, err := env.database.Repository.GetQueueItem(context.Background(), item.ID)
	require.NoError(t, err)
	assert.Nil(t, remaining)
}

func TestSuccessfulFallbackRejectsUnownedSourceBeforeQueueRowRemoval(t *testing.T) {
	env := newFbaseFinalizationEnv(t)
	nzbPath := filepath.Join(t.TempDir(), "operator-fallback.nzb")
	require.NoError(t, os.WriteFile(nzbPath, []byte("keep"), 0o600))
	item := env.addProcessingItem(t, nzbPath)

	received := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		received <- struct{}{}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":true,"nzo_ids":["SAB-1"]}`))
	}))
	t.Cleanup(server.Close)
	env.config.SABnzbd.FallbackHost = server.URL
	env.config.SABnzbd.FallbackAPIKey = "test-key"

	env.service.handleProcessingFailure(context.Background(), item, errors.New("force fallback"))

	select {
	case <-received:
		t.Error("unowned source must be rejected before external fallback transfer")
	default:
	}
	assert.FileExists(t, nzbPath)
	remaining, err := env.database.Repository.GetQueueItem(context.Background(), item.ID)
	require.NoError(t, err)
	require.NotNil(t, remaining)
	assert.Equal(t, database.QueueStatusFailed, remaining.Status)
}
