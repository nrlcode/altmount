package health

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/javi11/altmount/internal/testsupport/fakepool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setEffectTestMetadataStatus(t *testing.T, env *repairTestEnv, path string, status metapb.FileStatus) {
	t.Helper()
	fileMetadata, err := env.metadataService.ReadFileMetadata(path)
	require.NoError(t, err)
	require.NotNil(t, fileMetadata)
	fileMetadata.Status = status
	require.NoError(t, env.metadataService.WriteFileMetadata(path, fileMetadata))
}

func TestHealthCycleStaleRandomTokenCannotAuthorizeMetadataEffect(t *testing.T) {
	base := fakepool.New()
	wrapper := &finalizationReplacingClient{Client: base}
	fixture := newDestructiveClaimFixture(t, wrapper)
	base.SetDefaultBehavior(fakepool.SegmentBehavior{})
	wrapper.db = fixture.env.db
	wrapper.filePath = fixture.filePath
	setEffectTestMetadataStatus(t, fixture.env, fixture.filePath, metapb.FileStatus_FILE_STATUS_CORRUPTED)

	err := fixture.env.hw.runHealthCheckCycle(context.Background())
	require.Error(t, err, "a stale in-memory token must not authorize an external metadata write")
	require.NoError(t, wrapper.err)

	currentMetadata, err := fixture.env.metadataService.ReadFileMetadata(fixture.filePath)
	require.NoError(t, err)
	require.NotNil(t, currentMetadata)
	assert.Equal(t, metapb.FileStatus_FILE_STATUS_CORRUPTED, currentMetadata.Status,
		"only the token that is current in the database may authorize the metadata transition")
	var status, token string
	require.NoError(t, fixture.env.db.QueryRow(`
		SELECT status, health_claim_token FROM file_health WHERE file_path = ?
	`, fixture.filePath).Scan(&status, &token))
	assert.Equal(t, "checking", status)
	assert.Equal(t, "owner-b", token)
}

func TestEffectfulCycleRowsPublishIndependentlyFromFailingSibling(t *testing.T) {
	client := fakepool.New()
	fixture := newDestructiveClaimFixture(t, client)
	client.SetDefaultBehavior(fakepool.SegmentBehavior{})

	const secondPath = "complete/fenced/show.s01e02.mkv"
	secondLibraryPath := filepath.Join(filepath.Dir(fixture.libraryPath), "show.s01e02.mkv")
	require.NoError(t, os.WriteFile(secondLibraryPath, []byte("second library content"), 0o644))
	writeHealthyFile(t, fixture.env, secondPath)
	insertFileHealth(t, fixture.env.db, secondPath, secondLibraryPath, 0, 3)
	setEffectTestMetadataStatus(t, fixture.env, fixture.filePath, metapb.FileStatus_FILE_STATUS_CORRUPTED)
	setEffectTestMetadataStatus(t, fixture.env, secondPath, metapb.FileStatus_FILE_STATUS_CORRUPTED)

	_, err := fixture.env.db.Exec(`
		CREATE TRIGGER fail_second_effect_publication
		BEFORE UPDATE OF status ON file_health
		WHEN OLD.file_path = 'complete/fenced/show.s01e02.mkv'
		 AND OLD.health_claim_token IS NOT NULL
		 AND NEW.status = 'healthy'
		BEGIN
			SELECT RAISE(FAIL, 'synthetic sibling publication failure');
		END;
	`)
	require.NoError(t, err)

	err = fixture.env.hw.runHealthCheckCycle(context.Background())
	require.Error(t, err)

	firstMetadata, err := fixture.env.metadataService.ReadFileMetadata(fixture.filePath)
	require.NoError(t, err)
	require.NotNil(t, firstMetadata)
	assert.Equal(t, metapb.FileStatus_FILE_STATUS_HEALTHY, firstMetadata.Status)
	secondMetadata, err := fixture.env.metadataService.ReadFileMetadata(secondPath)
	require.NoError(t, err)
	require.NotNil(t, secondMetadata)
	assert.Equal(t, metapb.FileStatus_FILE_STATUS_HEALTHY, secondMetadata.Status,
		"the second external effect completed before its publication failed")

	var firstStatus string
	var firstToken sql.NullString
	require.NoError(t, fixture.env.db.QueryRow(`
		SELECT status, health_claim_token FROM file_health WHERE file_path = ?
	`, fixture.filePath).Scan(&firstStatus, &firstToken))
	assert.Equal(t, "healthy", firstStatus,
		"a sibling publication failure must not roll back an independently completed effect")
	assert.False(t, firstToken.Valid)

	var secondStatus string
	var secondToken sql.NullString
	require.NoError(t, fixture.env.db.QueryRow(`
		SELECT status, health_claim_token FROM file_health WHERE file_path = ?
	`, secondPath).Scan(&secondStatus, &secondToken))
	assert.Equal(t, "checking", secondStatus,
		"once an external effect succeeds, failed publication must retain recovery ownership instead of rearming it")
	assert.True(t, secondToken.Valid)
}
