package cmd

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/javi11/altmount/internal/config"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitializeMetadataWiresCleanupAuthorities(t *testing.T) {
	configRoot := t.TempDir()
	cfg := config.DefaultConfig(configRoot)
	service, _ := initializeMetadata(cfg)

	queueRoot := filepath.Join(os.TempDir(), ".altmount-queue")
	require.NoError(t, os.MkdirAll(queueRoot, 0o755))
	queueCaseRoot, err := os.MkdirTemp(queueRoot, "facore-wiring-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(queueCaseRoot) })

	storeRoot := filepath.Join(filepath.Dir(cfg.Database.Path), ".nzbs")
	require.NoError(t, os.MkdirAll(storeRoot, 0o755))

	tests := []struct {
		name         string
		sourcePath   string
		shouldDelete bool
	}{
		{name: "queue source", sourcePath: filepath.Join(queueCaseRoot, "movie.nzb"), shouldDelete: true},
		{name: "legacy store source", sourcePath: filepath.Join(storeRoot, "movie.nzb"), shouldDelete: true},
		{name: "outside source", sourcePath: filepath.Join(configRoot, "outside", "movie.nzb")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, os.MkdirAll(filepath.Dir(tt.sourcePath), 0o755))
			require.NoError(t, os.WriteFile(tt.sourcePath, []byte("nzb"), 0o600))
			virtualPath := filepath.Join("movies", tt.name+".mkv")
			meta := &metapb.FileMetadata{
				FileSize:      1,
				SourceNzbPath: tt.sourcePath,
				Status:        metapb.FileStatus_FILE_STATUS_HEALTHY,
			}
			require.NoError(t, service.WriteFileMetadata(virtualPath, meta))
			metaPath := service.GetMetadataFilePath(virtualPath)

			err := service.DeleteFileMetadataWithSourceNzb(context.Background(), virtualPath, true)
			if tt.shouldDelete {
				require.NoError(t, err)
				require.NoFileExists(t, tt.sourcePath)
				require.NoFileExists(t, metaPath)
				return
			}
			assert.Error(t, err)
			assert.FileExists(t, tt.sourcePath)
			assert.FileExists(t, metaPath)
		})
	}
}
