package nzbfilesystem

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilesystemRemoveNotFoundPreservesSamePathReplacement(t *testing.T) {
	repo, db, _ := setupStreamHealthEnv(t)
	cfg := config.DefaultConfig()
	remote := NewMetadataRemoteFile(
		metadata.NewMetadataService(t.TempDir()), repo, nil, nil, nil,
		func() *config.Config { return cfg }, nil, nil,
	)
	t.Cleanup(remote.repairCoalescer.Close)
	nfs := NewNzbFilesystem(remote)
	const path = "movies/not-found-but-current.mkv"
	const currentLibraryPath = "/library/current/not-found.mkv"
	_, err := db.Exec(`
		INSERT INTO file_health (file_path, library_path, status, metadata)
		VALUES (?, '/library/stale/not-found.mkv', 'corrupted', '{"revision":"observed"}')
	`, path)
	require.NoError(t, err)
	observed, err := repo.GetFileHealth(context.Background(), path)
	require.NoError(t, err)
	require.NotNil(t, observed)
	_, err = db.Exec(`DELETE FROM file_health WHERE id = ?`, observed.ID)
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO file_health (file_path, library_path, status, metadata)
		VALUES (?, ?, 'healthy', '{"revision":"replacement"}')
	`, path, currentLibraryPath)
	require.NoError(t, err)
	replacement, err := repo.GetFileHealth(context.Background(), path)
	require.NoError(t, err)
	require.NotNil(t, replacement)
	require.NotEqual(t, observed.ID, replacement.ID)

	err = nfs.Remove(context.Background(), path)
	require.Error(t, err)
	assert.True(t, errors.Is(err, os.ErrNotExist))
	current, err := repo.GetFileHealth(context.Background(), path)
	require.NoError(t, err)
	require.NotNil(t, current, "not-found removal must not consume a same-path replacement")
	assert.Equal(t, replacement.ID, current.ID)
	require.NotNil(t, current.LibraryPath)
	assert.Equal(t, currentLibraryPath, *current.LibraryPath)
	require.NotNil(t, current.Metadata)
	assert.JSONEq(t, `{"revision":"replacement"}`, *current.Metadata)
}

func TestFilesystemRemoveMetadataFailurePreservesHealthRecord(t *testing.T) {
	repo, db, metadataService := setupStreamHealthEnv(t)
	cfg := config.DefaultConfig()
	remote := NewMetadataRemoteFile(
		metadataService, repo, nil, nil, nil,
		func() *config.Config { return cfg }, nil, nil,
	)
	t.Cleanup(remote.repairCoalescer.Close)
	nfs := NewNzbFilesystem(remote)
	const path = "movies/rejected-removal-owner.mkv"
	writeStreamMeta(t, metadataService, path)
	metadataPath := metadataService.GetMetadataFilePath(path)
	require.NoError(t, os.Remove(metadataPath))
	require.NoError(t, os.Mkdir(metadataPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(metadataPath, "deletion-blocker"), []byte("block"), 0o644))
	_, err := db.Exec(`
		INSERT INTO file_health (file_path, status, metadata)
		VALUES (?, 'healthy', '{"revision":"current"}')
	`, path)
	require.NoError(t, err)

	err = nfs.Remove(context.Background(), path)
	require.Error(t, err, "failed metadata removal must be reported")
	assert.True(t, metadataService.FileExists(path), "failed metadata removal must preserve the object")
	current, err := repo.GetFileHealth(context.Background(), path)
	require.NoError(t, err)
	require.NotNil(t, current, "failed metadata removal must preserve the current health mapping")
}
