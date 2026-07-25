package metadata

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/javi11/altmount/internal/database"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/require"
)

func TestWriteFileMetadataV3StoreRefRepositoryParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			config := database.Config{Type: backend}
			if backend == "postgres" {
				config.DSN = os.Getenv("ALTMOUNT_TEST_POSTGRES_DSN")
				if config.DSN == "" {
					t.Skip("ALTMOUNT_TEST_POSTGRES_DSN is not configured")
				}
			} else {
				config.DatabasePath = filepath.Join(t.TempDir(), "store-refs.db")
			}

			db, err := database.NewDB(config)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })

			ctx := context.Background()
			root := t.TempDir()
			paths := []string{
				filepath.Join(root, "success.nzbz"),
				filepath.Join(root, "publication-failure.nzbz"),
				filepath.Join(root, "missing.nzbz"),
			}
			t.Cleanup(func() {
				for _, path := range paths {
					for {
						count, cleanupErr := db.StoreRefRepo.GetStoreRefCount(ctx, path)
						if cleanupErr != nil {
							t.Errorf("read store ref during cleanup: %v", cleanupErr)
							break
						}
						if count == 0 {
							break
						}
						if _, cleanupErr = db.StoreRefRepo.DecStoreRef(ctx, path); cleanupErr != nil {
							t.Errorf("decrement store ref during cleanup: %v", cleanupErr)
							break
						}
					}
				}
			})

			t.Run("success", func(t *testing.T) {
				service := NewMetadataService(filepath.Join(root, "success-metadata"))
				service.SetStoreRefCounter(db.StoreRefRepo)
				require.NoError(t, service.Store().WriteStore(paths[0], &metapb.NzbStore{}))

				require.NoError(t, service.WriteFileMetadataV3(
					ctx,
					"movie.mkv",
					&metapb.FileMetadata{FileSize: 1, Status: metapb.FileStatus_FILE_STATUS_HEALTHY},
					nil,
					paths[0],
				))
				require.FileExists(t, service.GetMetadataFilePath("movie.mkv"))
				count, countErr := db.StoreRefRepo.GetStoreRefCount(ctx, paths[0])
				require.NoError(t, countErr)
				require.Equal(t, int64(1), count)
			})

			t.Run("publication failure restores prior owner", func(t *testing.T) {
				metadataRoot := filepath.Join(root, "metadata-obstruction")
				require.NoError(t, os.WriteFile(metadataRoot, []byte("obstruction"), 0o600))
				service := NewMetadataService(metadataRoot)
				service.SetStoreRefCounter(db.StoreRefRepo)
				require.NoError(t, service.Store().WriteStore(paths[1], &metapb.NzbStore{}))
				require.NoError(t, db.StoreRefRepo.IncStoreRef(ctx, paths[1]))

				err := service.WriteFileMetadataV3(
					ctx,
					"movie.mkv",
					&metapb.FileMetadata{FileSize: 1, Status: metapb.FileStatus_FILE_STATUS_HEALTHY},
					nil,
					paths[1],
				)
				require.Error(t, err)
				require.NoFileExists(t, service.GetMetadataFilePath("movie.mkv"))
				require.FileExists(t, paths[1])
				count, countErr := db.StoreRefRepo.GetStoreRefCount(ctx, paths[1])
				require.NoError(t, countErr)
				require.Equal(t, int64(1), count)
			})

			t.Run("fresh validation failure removes provisional row", func(t *testing.T) {
				service := NewMetadataService(filepath.Join(root, "missing-metadata"))
				service.SetStoreRefCounter(db.StoreRefRepo)

				err := service.WriteFileMetadataV3(
					ctx,
					"movie.mkv",
					&metapb.FileMetadata{FileSize: 1, Status: metapb.FileStatus_FILE_STATUS_HEALTHY},
					nil,
					paths[2],
				)
				require.Error(t, err)
				require.NoFileExists(t, service.GetMetadataFilePath("movie.mkv"))
				count, countErr := db.StoreRefRepo.GetStoreRefCount(ctx, paths[2])
				require.NoError(t, countErr)
				require.Zero(t, count)

				placeholder := "?"
				if backend == "postgres" {
					placeholder = "$1"
				}
				var rows int
				query := fmt.Sprintf("SELECT COUNT(*) FROM nzb_store_refs WHERE store_path = %s", placeholder)
				require.NoError(t, db.Connection().QueryRowContext(ctx, query, paths[2]).Scan(&rows))
				require.Zero(t, rows)
			})
		})
	}
}
