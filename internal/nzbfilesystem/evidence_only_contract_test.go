package nzbfilesystem

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/javi11/altmount/internal/usenet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilesystemRemoveAndRemoveAllFailBeforeMutation(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(*NzbFilesystem, context.Context, string) error
	}{
		{name: "Remove", call: func(fs *NzbFilesystem, ctx context.Context, path string) error { return fs.Remove(ctx, path) }},
		{name: "RemoveAll", call: func(fs *NzbFilesystem, ctx context.Context, path string) error { return fs.RemoveAll(ctx, path) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo, db, ms := setupStreamHealthEnv(t)
			cfg := config.DefaultConfig()
			remote := NewMetadataRemoteFile(ms, repo, nil, nil, nil, func() *config.Config { return cfg }, nil, nil)
			t.Cleanup(remote.repairCoalescer.Close)
			fs := NewNzbFilesystem(remote)
			const path = "movies/current.mkv"
			writeStreamMeta(t, ms, path)
			_, err := db.Exec(`INSERT INTO file_health (file_path, status) VALUES (?, 'healthy')`, path)
			require.NoError(t, err)

			removeErr := tc.call(fs, context.Background(), path)
			require.Error(t, removeErr, "name-only removal must fail closed")
			assert.True(t, errors.Is(removeErr, os.ErrPermission), "removal must expose the stable read-only sentinel")
			row, err := repo.GetFileHealth(context.Background(), path)
			require.NoError(t, err)
			assert.NotNil(t, row, "failed removal must preserve database authority")
			meta, err := ms.ReadFileMetadata(path)
			require.NoError(t, err)
			assert.NotNil(t, meta, "failed removal must preserve metadata")
		})
	}
}

func TestStreamingFailureInsertsAbsentCorruptionEvidenceOnly(t *testing.T) {
	repo, _, ms := setupStreamHealthEnv(t)
	const path = "movies/first-stream-failure.mkv"
	segments := writeStreamMeta(t, ms, path)
	enabled := true
	cfg := config.DefaultConfig()
	cfg.Health.Enabled = &enabled
	mvf := newStreamFailureMVF(context.Background(), path, repo, ms, segments, cfg)

	mvf.updateFileHealthOnError(&usenet.DataCorruptionError{
		UnderlyingErr: errors.New("article not found"), NoRetry: true,
	}, true)

	row, err := repo.GetFileHealth(context.Background(), path)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, database.HealthStatusCorrupted, row.Status,
		"an absent path may receive corruption evidence but no repair authority")
	meta, err := ms.ReadFileMetadata(path)
	require.NoError(t, err)
	require.NotNil(t, meta)
	assert.Equal(t, metapb.FileStatus_FILE_STATUS_HEALTHY, meta.Status,
		"stream evidence must not mutate or move metadata")
}

func TestStreamingFailureDoesNotOverwriteOrDeleteExistingRow(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("filesystem fixture is Unix-only")
	}
	repo, db, ms := setupStreamHealthEnv(t)
	const path = "movies/current-stream.mkv"
	libraryRoot := t.TempDir()
	libraryPath := filepath.Join(libraryRoot, "current-stream.mkv")
	require.NoError(t, os.WriteFile(libraryPath, []byte("current"), 0o644))
	segments := writeStreamMeta(t, ms, path)
	_, err := db.Exec(`
		INSERT INTO file_health (file_path, library_path, status, metadata, last_error)
		VALUES (?, ?, 'healthy', '{"identity":"current"}', 'keep-me')
	`, path, libraryPath)
	require.NoError(t, err)
	before, err := repo.GetFileHealth(context.Background(), path)
	require.NoError(t, err)
	enabled := true
	cfg := config.DefaultConfig()
	cfg.Health.Enabled = &enabled
	cfg.Health.CorruptionAction = "delete"
	cfg.Health.LibraryDir = &libraryRoot
	mvf := newStreamFailureMVF(context.Background(), path, repo, ms, segments, cfg)

	mvf.updateFileHealthOnError(&usenet.DataCorruptionError{
		UnderlyingErr: errors.New("article not found"), NoRetry: true,
	}, true)

	after, err := repo.GetFileHealth(context.Background(), path)
	require.NoError(t, err)
	require.NotNil(t, after, "existing same-path authority must never be consumed")
	assert.Equal(t, before.ID, after.ID)
	assert.Equal(t, before.Status, after.Status)
	assert.Equal(t, before.LastError, after.LastError)
	assert.Equal(t, before.Metadata, after.Metadata)
	meta, err := ms.ReadFileMetadata(path)
	require.NoError(t, err)
	require.NotNil(t, meta)
	assert.Equal(t, metapb.FileStatus_FILE_STATUS_HEALTHY, meta.Status)
	_, err = os.Stat(libraryPath)
	assert.NoError(t, err, "streaming corruption evidence must not delete library content")
}
