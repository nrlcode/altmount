package database

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPR5ResumePausedImportQueueItemIsConditionalAndIdempotent(t *testing.T) {
	db, err := NewDB(Config{
		Type:         "sqlite",
		DatabasePath: filepath.Join(t.TempDir(), "import-resume.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	ctx := context.Background()
	repo := db.Repository
	item := &ImportQueueItem{
		NzbPath:    filepath.Join(t.TempDir(), "synthetic-import.nzb"),
		Priority:   QueuePriorityNormal,
		Status:     QueueStatusPaused,
		MaxRetries: 3,
	}
	require.NoError(t, repo.AddToQueue(ctx, item))

	resumed, err := repo.ResumePausedQueueItem(ctx, item.ID)
	require.NoError(t, err)
	assert.True(t, resumed)
	stored, err := repo.GetQueueItem(ctx, item.ID)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, QueueStatusPending, stored.Status)
	assert.Nil(t, stored.ErrorMessage)

	resumed, err = repo.ResumePausedQueueItem(ctx, item.ID)
	require.NoError(t, err)
	assert.False(t, resumed, "a second resumer must not manufacture another transition")
}

func TestPR5ResumePausedImportQueueItemNeverResurrectsTerminalWork(t *testing.T) {
	db, err := NewDB(Config{
		Type:         "sqlite",
		DatabasePath: filepath.Join(t.TempDir(), "import-terminal-resume.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	ctx := context.Background()
	repo := db.Repository
	for _, status := range []QueueStatus{QueueStatusCompleted, QueueStatusFailed} {
		item := &ImportQueueItem{
			NzbPath:    filepath.Join(t.TempDir(), string(status)+".nzb"),
			Priority:   QueuePriorityNormal,
			Status:     status,
			MaxRetries: 3,
		}
		require.NoError(t, repo.AddToQueue(ctx, item))

		resumed, err := repo.ResumePausedQueueItem(ctx, item.ID)
		require.NoError(t, err)
		assert.False(t, resumed, "terminal %s item was resurrected", status)
		stored, err := repo.GetQueueItem(ctx, item.ID)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, status, stored.Status)
	}
}
