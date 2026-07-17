package nzbfilesystem

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilesystemRemoveNotFoundPreservesHealthRecord(t *testing.T) {
	repo, db, _ := setupStreamHealthEnv(t)
	cfg := config.DefaultConfig()
	remote := NewMetadataRemoteFile(
		metadata.NewMetadataService(t.TempDir()), repo, nil, nil, nil,
		func() *config.Config { return cfg }, nil, nil,
	)
	t.Cleanup(remote.repairCoalescer.Close)
	nfs := NewNzbFilesystem(remote)
	const path = "movies/not-found-but-current.mkv"
	_, err := db.Exec(`
		INSERT INTO file_health (file_path, status, metadata)
		VALUES (?, 'healthy', '{"revision":"current"}')
	`, path)
	require.NoError(t, err)

	err = nfs.Remove(context.Background(), path)
	require.Error(t, err)
	assert.True(t, errors.Is(err, os.ErrNotExist))
	current, err := repo.GetFileHealth(context.Background(), path)
	require.NoError(t, err)
	require.NotNil(t, current, "a missing metadata object is not authority to consume a current health row")
	require.NotNil(t, current.Metadata)
	assert.JSONEq(t, `{"revision":"current"}`, *current.Metadata)
}

func TestFilesystemRemoveClaimFailurePreservesMetadataAndHealth(t *testing.T) {
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
	_, err := db.Exec(`
		INSERT INTO file_health (file_path, status, metadata)
		VALUES (?, 'healthy', '{"revision":"current"}')
	`, path)
	require.NoError(t, err)
	_, err = db.Exec(`
		CREATE TRIGGER reject_filesystem_removal_claim
		BEFORE UPDATE OF health_claim_token ON file_health
		WHEN NEW.health_claim_token IS NOT NULL
		BEGIN
			SELECT RAISE(FAIL, 'synthetic administrative claim failure');
		END;
	`)
	require.NoError(t, err)

	err = nfs.Remove(context.Background(), path)
	require.Error(t, err, "metadata removal must not start without durable administrative authority")
	assert.True(t, metadataService.FileExists(path), "failed admission must preserve metadata")
	current, err := repo.GetFileHealth(context.Background(), path)
	require.NoError(t, err)
	require.NotNil(t, current, "failed admission must preserve the health mapping")
}
