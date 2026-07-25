package database

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type facoreCHG021FinalizationHolder interface {
	HoldQueueItemFinalizationFailure(context.Context, int64, string) error
}

func requireFACORECHG021FinalizationHolder(
	t *testing.T,
	repo *QueueRepository,
) facoreCHG021FinalizationHolder {
	t.Helper()
	holder, ok := any(repo).(facoreCHG021FinalizationHolder)
	require.True(t, ok,
		"QueueRepository must expose a guarded success-finalization failure hold")
	return holder
}

func TestFACORECHG021FinalizationHoldSQLite(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:"+filepath.Join(t.TempDir(), "queue.db")+
		"?_journal_mode=WAL&_busy_timeout=30000&_txlock=immediate")
	require.NoError(t, err)
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	setupQueueSchema(t, db)

	testFACORECHG021FinalizationHold(t, NewQueueRepository(db, DialectSQLite))
}

func TestFACORECHG021FinalizationHoldPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	backend := newFACORECHG009PostgresMigrationBackend(t, ctx)
	goose.SetBaseFS(embedMigrations)
	require.NoError(t, goose.SetDialect(backend.gooseDialect))
	require.NoError(t, goose.UpContext(ctx, backend.db, backend.migrationsDir))

	testFACORECHG021FinalizationHold(t, NewQueueRepository(backend.db, DialectPostgres))
}

func testFACORECHG021FinalizationHold(t *testing.T, repo *QueueRepository) {
	t.Helper()
	ctx := context.Background()
	holder := requireFACORECHG021FinalizationHolder(t, repo)

	t.Run("processing_enters_failed_hold", func(t *testing.T) {
		item := seedFACORECHG020QueueItem(t, ctx, repo, QueueStatusProcessing)
		message := "success finalization failed: rooted source release"

		require.NoError(t, holder.HoldQueueItemFinalizationFailure(ctx, item.ID, message))

		stored, err := repo.GetQueueItem(ctx, item.ID)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, QueueStatusFailed, stored.Status)
		require.NotNil(t, stored.ErrorMessage)
		assert.Equal(t, message, *stored.ErrorMessage)
		assert.Equal(t, item.RetryCount, stored.RetryCount)
	})

	for _, status := range []QueueStatus{
		QueueStatusPending,
		QueueStatusFailed,
		QueueStatusCompleted,
	} {
		status := status
		t.Run("does_not_overwrite_"+string(status), func(t *testing.T) {
			item := seedFACORECHG020QueueItem(t, ctx, repo, status)

			require.NoError(t, holder.HoldQueueItemFinalizationFailure(
				ctx, item.ID, "stale finalization error"))

			stored, err := repo.GetQueueItem(ctx, item.ID)
			require.NoError(t, err)
			require.NotNil(t, stored)
			assert.Equal(t, status, stored.Status)
			assert.Nil(t, stored.ErrorMessage,
				"a stale finalizer must not overwrite another durable state")
		})
	}

	t.Run("missing_row_is_safe", func(t *testing.T) {
		require.NoError(t, holder.HoldQueueItemFinalizationFailure(
			ctx, 1<<62, "stale finalization error"))
	})
}
