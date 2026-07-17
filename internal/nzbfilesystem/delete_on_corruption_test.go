package nzbfilesystem

import (
	"context"
	"testing"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/usenet"
	"github.com/javi11/nntppool/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	assert.Equal(t, database.HealthStatusHealthy, fh.Status,
		"streaming evidence must not mutate an existing health row")
	meta, err := ms.ReadFileMetadata(filePath)
	require.NoError(t, err)
	assert.NotNil(t, meta, "unconfirmed corruption must not delete or move metadata")
}
