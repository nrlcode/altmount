package stremio

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type cleanupContractFixture struct {
	cfg     *config.Config
	repo    *database.Repository
	service *StremioCleanupService
}

func newCleanupContractFixture(t *testing.T) cleanupContractFixture {
	t.Helper()
	configRoot := t.TempDir()
	cfg := config.DefaultConfig(configRoot)
	db, err := database.NewDB(database.Config{
		Type:         "sqlite",
		DatabasePath: cfg.Database.Path,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	repo := database.NewRepository(db.Connection(), database.DialectSQLite)
	require.NoError(t, os.MkdirAll(cfg.Metadata.RootPath, 0o755))
	metadataService := metadata.NewMetadataService(cfg.Metadata.RootPath)
	return cleanupContractFixture{
		cfg:  cfg,
		repo: repo,
		service: NewStremioCleanupService(repo, metadataService, func() *config.Config {
			return cfg
		}),
	}
}

func addCleanupContractItem(t *testing.T, repo *database.Repository, nzbPath string, storagePath *string) *database.ImportQueueItem {
	t.Helper()
	item := &database.ImportQueueItem{
		NzbPath:     nzbPath,
		StoragePath: storagePath,
		Priority:    database.QueuePriorityNormal,
		Status:      database.QueueStatusCompleted,
		MaxRetries:  1,
	}
	require.NoError(t, repo.AddToQueue(context.Background(), item))
	require.NotZero(t, item.ID)
	return item
}

func writeCleanupContractNZB(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte("nzb"), 0o600))
}

func TestStremioDeleteItemEnforcesNZBAuthority(t *testing.T) {
	tests := []struct {
		name       string
		path       func(cleanupContractFixture) string
		wantDelete bool
	}{
		{
			name: "contained store path",
			path: func(f cleanupContractFixture) string {
				return filepath.Join(filepath.Dir(f.cfg.Database.Path), ".nzbs", "stremio", "item.nzb")
			},
			wantDelete: true,
		},
		{
			name: "outside path",
			path: func(f cleanupContractFixture) string {
				return filepath.Join(filepath.Dir(f.cfg.Database.Path), "outside", "item.nzb")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newCleanupContractFixture(t)
			nzbPath := tt.path(f)
			writeCleanupContractNZB(t, nzbPath)
			item := addCleanupContractItem(t, f.repo, nzbPath, nil)
			persisted, err := f.repo.GetQueueItem(context.Background(), item.ID)
			require.NoError(t, err)
			require.NotNil(t, persisted)

			f.service.deleteItem(context.Background(), persisted)

			got, err := f.repo.GetQueueItem(context.Background(), item.ID)
			require.NoError(t, err)
			if tt.wantDelete {
				assert.NoFileExists(t, nzbPath)
				assert.Nil(t, got)
				return
			}
			assert.FileExists(t, nzbPath)
			assert.DirExists(t, filepath.Dir(nzbPath))
			assert.NotNil(t, got, "unsafe filesystem cleanup must preserve its retry record")
		})
	}
}

func TestStremioDeleteItemStopsAfterMetadataCleanupFailure(t *testing.T) {
	f := newCleanupContractFixture(t)
	nzbPath := filepath.Join(filepath.Dir(f.cfg.Database.Path), ".nzbs", "stremio", "item.nzb")
	writeCleanupContractNZB(t, nzbPath)
	rootEquivalent := "."
	item := addCleanupContractItem(t, f.repo, nzbPath, &rootEquivalent)

	f.service.deleteItem(context.Background(), item)

	got, err := f.repo.GetQueueItem(context.Background(), item.ID)
	require.NoError(t, err)
	assert.FileExists(t, nzbPath)
	assert.NotNil(t, got, "failed metadata cleanup must preserve its retry record")
}
