package stremio

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/metadata"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type cleanupContractFixture struct {
	cfg     *config.Config
	db      *database.DB
	repo    *database.Repository
	meta    *metadata.MetadataService
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
	metadataService.SetStoreRefCounter(db.StoreRefRepo)
	return cleanupContractFixture{
		cfg:  cfg,
		db:   db,
		repo: repo,
		meta: metadataService,
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

func TestStremioDeleteItemCleansStorage(t *testing.T) {
	tests := []struct {
		name              string
		storagePath       string
		existingDirectory bool
	}{
		{
			name:              "existing metadata directory",
			storagePath:       filepath.Join("movies", "release"),
			existingDirectory: true,
		},
		{
			name:        "absent directory single file fallback",
			storagePath: filepath.Join("movies", "release", "movie.mkv"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			f := newCleanupContractFixture(t)
			storeRoot := filepath.Join(filepath.Dir(f.cfg.Database.Path), ".nzbs")
			caseName := filepath.Base(t.Name())
			sourcePath := filepath.Join(storeRoot, "sources", caseName, "item.nzb")
			storePath := filepath.Join(storeRoot, "stores", caseName, "item.nzbz")
			virtualPath := filepath.Join("movies", "release", "movie.mkv")

			writeCleanupContractNZB(t, sourcePath)
			require.NoError(t, f.meta.Store().WriteStore(storePath, &metapb.NzbStore{}))
			require.NoError(t, f.meta.WriteFileMetadataV3(
				ctx,
				virtualPath,
				&metapb.FileMetadata{
					FileSize: 1,
					Status:   metapb.FileStatus_FILE_STATUS_HEALTHY,
				},
				nil,
				storePath,
			))

			metaPath := f.meta.GetMetadataFilePath(virtualPath)
			require.FileExists(t, metaPath)
			require.FileExists(t, sourcePath)
			require.FileExists(t, storePath)
			count, err := f.db.StoreRefRepo.GetStoreRefCount(ctx, storePath)
			require.NoError(t, err)
			require.Equal(t, int64(1), count)
			require.Equal(t, tt.existingDirectory, f.meta.DirectoryExists(tt.storagePath))

			item := addCleanupContractItem(t, f.repo, sourcePath, &tt.storagePath)
			require.NoError(t, f.db.Repository.AddStoragePath(ctx, item.ID, tt.storagePath))
			persisted, err := f.repo.GetQueueItem(ctx, item.ID)
			require.NoError(t, err)
			require.NotNil(t, persisted)
			require.NotNil(t, persisted.StoragePath)
			require.Equal(t, tt.storagePath, *persisted.StoragePath)

			f.service.deleteItem(ctx, persisted)

			assert.NoFileExists(t, metaPath)
			assert.NoFileExists(t, sourcePath)
			assert.NoFileExists(t, storePath)
			count, err = f.db.StoreRefRepo.GetStoreRefCount(ctx, storePath)
			require.NoError(t, err)
			assert.Zero(t, count)
			assert.NoDirExists(t, filepath.Dir(sourcePath))
			assert.NoDirExists(t, filepath.Dir(filepath.Dir(sourcePath)))
			assert.DirExists(t, storeRoot)
			assert.DirExists(t, f.cfg.Metadata.RootPath)

			metadataReleaseDir := filepath.Dir(metaPath)
			assert.NoDirExists(t, metadataReleaseDir)
			if tt.existingDirectory {
				assert.DirExists(t, filepath.Dir(metadataReleaseDir))
			} else {
				assert.NoDirExists(t, filepath.Dir(metadataReleaseDir))
			}

			got, err := f.repo.GetQueueItem(ctx, item.ID)
			require.NoError(t, err)
			assert.Nil(t, got)
		})
	}
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
			name: "contained queue path",
			path: func(f cleanupContractFixture) string {
				testRoot := filepath.Base(filepath.Dir(filepath.Dir(f.cfg.Database.Path)))
				return filepath.Join(os.TempDir(), ".altmount-queue", testRoot, "item.nzb")
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

func TestStremioDeleteItemPreflightsDBPathBeforeMetadata(t *testing.T) {
	f := newCleanupContractFixture(t)
	outsideNZB := filepath.Join(filepath.Dir(f.cfg.Database.Path), "outside", "item.nzb")
	writeCleanupContractNZB(t, outsideNZB)
	storagePath := filepath.Join("movies", "release")
	virtualPath := filepath.Join(storagePath, "movie.mkv")
	require.NoError(t, f.meta.WriteFileMetadata(virtualPath, &metapb.FileMetadata{
		FileSize: 1,
		Status:   metapb.FileStatus_FILE_STATUS_HEALTHY,
	}))
	metaPath := f.meta.GetMetadataFilePath(virtualPath)

	item := addCleanupContractItem(t, f.repo, outsideNZB, &storagePath)
	_, err := f.db.Connection().ExecContext(
		context.Background(), `UPDATE import_queue SET storage_path = ? WHERE id = ?`, storagePath, item.ID,
	)
	require.NoError(t, err)
	persisted, err := f.repo.GetQueueItem(context.Background(), item.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	require.NotNil(t, persisted.StoragePath)

	f.service.deleteItem(context.Background(), persisted)

	got, err := f.repo.GetQueueItem(context.Background(), item.ID)
	require.NoError(t, err)
	assert.FileExists(t, metaPath, "NZB authority must be preflighted before metadata mutation")
	assert.FileExists(t, outsideNZB)
	assert.DirExists(t, filepath.Dir(outsideNZB))
	assert.NotNil(t, got, "unsafe cleanup must preserve its retry record")
}
