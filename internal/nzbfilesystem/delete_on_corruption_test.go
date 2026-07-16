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
	"github.com/javi11/nntppool/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUpdateFileHealthOnError_DeleteOnCorruption_DeletesFile verifies that when
// health.corruption_action is "delete", a mid-stream DataCorruptionError deletes the
// file's metadata, health record, and physical library file (plus any now-empty parent
// directory) instead of triggering an Arr repair.
func TestUpdateFileHealthOnError_DeleteOnCorruption_DeletesFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}
	repo, db, ms := setupStreamHealthEnv(t)
	ctx := context.Background()

	libraryRoot := t.TempDir()
	filePath := "series/stream.s01e03.mkv"
	libraryDir := filepath.Join(libraryRoot, "series", "stream")
	libraryPath := filepath.Join(libraryDir, "stream.s01e03.mkv")
	seg := writeStreamMeta(t, ms, filePath)

	require.NoError(t, os.MkdirAll(libraryDir, 0o755))
	require.NoError(t, os.WriteFile(libraryPath, []byte("dummy"), 0o644))

	_, err := db.Exec(
		`INSERT INTO file_health (file_path, library_path, status, scheduled_check_at) VALUES (?, ?, 'healthy', datetime('now'))`,
		filePath, libraryPath,
	)
	require.NoError(t, err)

	enabled := true
	cfg := config.DefaultConfig()
	cfg.Health.Enabled = &enabled
	cfg.Health.CorruptionAction = "delete"
	cfg.Health.LibraryDir = &libraryRoot
	cfg.MountPath = ""

	mvf := newStreamFailureMVF(ctx, filePath, repo, ms, seg, cfg)
	mvf.updateFileHealthOnError(&usenet.DataCorruptionError{UnderlyingErr: errors.New("article not found"), NoRetry: true}, true)

	fh, err := repo.GetFileHealth(ctx, filePath)
	require.NoError(t, err)
	assert.Nil(t, fh, "health record should be deleted")

	deletedMeta, err := ms.ReadFileMetadata(filePath)
	require.NoError(t, err)
	assert.Nil(t, deletedMeta, "metadata should be deleted")

	_, statErr := os.Stat(libraryPath)
	assert.True(t, os.IsNotExist(statErr), "physical library file should be deleted")

	_, dirErr := os.Stat(libraryDir)
	assert.True(t, os.IsNotExist(dirErr), "now-empty library directory should be removed")
}

func TestPR3UnconfirmedCorruptBodyDoesNotDeleteOrRepair(t *testing.T) {
	repo, db, ms := setupStreamHealthEnv(t)
	ctx := context.Background()
	filePath := "movies/unconfirmed-corrupt.mkv"
	seg := writeStreamMeta(t, ms, filePath)
	_, err := db.Exec(
		`INSERT INTO file_health (file_path, status, scheduled_check_at) VALUES (?, 'healthy', datetime('now'))`,
		filePath,
	)
	require.NoError(t, err)

	enabled := true
	cfg := config.DefaultConfig()
	cfg.Health.Enabled = &enabled
	cfg.Health.CorruptionAction = "delete"
	mvf := newStreamFailureMVF(ctx, filePath, repo, ms, seg, cfg)
	typed := &nntppool.TransportError{Kind: nntppool.OutcomeCorruptBody, Cause: nntppool.ErrBodyCorrupt}
	mvf.updateFileHealthOnError(&usenet.DataCorruptionError{
		UnderlyingErr: typed,
		Outcome:       nntppool.OutcomeCorruptBody,
	}, false)

	fh, err := repo.GetFileHealth(ctx, filePath)
	require.NoError(t, err)
	require.NotNil(t, fh, "unconfirmed corruption must not delete the health record")
	assert.Equal(t, database.HealthStatusPending, fh.Status)
	meta, err := ms.ReadFileMetadata(filePath)
	require.NoError(t, err)
	assert.NotNil(t, meta, "unconfirmed corruption must not delete or move metadata")
}

func TestStreamingDeleteOnCorruptionRequiresCurrentHealthOwnership(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}
	repo, db, ms := setupStreamHealthEnv(t)
	ctx := context.Background()
	libraryRoot := t.TempDir()
	filePath := "series/fenced-delete.mkv"
	libraryPath := filepath.Join(libraryRoot, "series", "fenced-delete.mkv")
	seg := writeStreamMeta(t, ms, filePath)
	require.NoError(t, os.MkdirAll(filepath.Dir(libraryPath), 0o755))
	require.NoError(t, os.WriteFile(libraryPath, []byte("current"), 0o644))
	_, err := db.Exec(`
		INSERT INTO file_health
			(file_path, library_path, status, scheduled_check_at, health_claim_token)
		VALUES (?, ?, 'checking', datetime('now'), 'foreign-owner')
	`, filePath, libraryPath)
	require.NoError(t, err)

	enabled := true
	cfg := config.DefaultConfig()
	cfg.Health.Enabled = &enabled
	cfg.Health.CorruptionAction = "delete"
	cfg.Health.LibraryDir = &libraryRoot
	mvf := newStreamFailureMVF(ctx, filePath, repo, ms, seg, cfg)
	mvf.updateFileHealthOnError(&usenet.DataCorruptionError{UnderlyingErr: errors.New("article not found"), NoRetry: true}, true)

	fh, err := repo.GetFileHealth(ctx, filePath)
	require.NoError(t, err)
	require.NotNil(t, fh, "an unowned streaming callback must not delete the health authority")
	assert.Equal(t, database.HealthStatusChecking, fh.Status)
	meta, err := ms.ReadFileMetadata(filePath)
	require.NoError(t, err)
	require.NotNil(t, meta, "an unowned callback must not delete or hide metadata")
	assert.Equal(t, metapb.FileStatus_FILE_STATUS_HEALTHY, meta.Status)
	_, err = os.Stat(libraryPath)
	require.NoError(t, err, "an unowned callback must not delete library content")
}

func TestStreamingRepairMoveRequiresCurrentHealthOwnership(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}
	repo, db, ms := setupStreamHealthEnv(t)
	ctx := context.Background()
	filePath := "series/fenced-repair.mkv"
	libraryPath := "/media/library/fenced-repair.mkv"
	seg := writeStreamMeta(t, ms, filePath)
	_, err := db.Exec(`
		INSERT INTO file_health
			(file_path, library_path, status, scheduled_check_at, health_claim_token)
		VALUES (?, ?, 'checking', datetime('now'), 'foreign-owner')
	`, filePath, libraryPath)
	require.NoError(t, err)

	enabled := true
	cfg := config.DefaultConfig()
	cfg.Health.Enabled = &enabled
	mvf := newStreamFailureMVF(ctx, filePath, repo, ms, seg, cfg)
	mvf.updateFileHealthOnError(&usenet.DataCorruptionError{UnderlyingErr: errors.New("article not found"), NoRetry: true}, true)

	fh, err := repo.GetFileHealth(ctx, filePath)
	require.NoError(t, err)
	require.NotNil(t, fh)
	assert.Equal(t, database.HealthStatusChecking, fh.Status, "unowned streaming evidence may not publish repair authority")
	meta, err := ms.ReadFileMetadata(filePath)
	require.NoError(t, err)
	require.NotNil(t, meta, "unowned streaming evidence must not move metadata")
	assert.Equal(t, metapb.FileStatus_FILE_STATUS_HEALTHY, meta.Status)
}
