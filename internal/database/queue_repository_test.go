package database

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResetStaleItems_ResetsAllProcessingItems(t *testing.T) {
	// Setup
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err, "Failed to open in-memory database")
	defer db.Close()

	setupQueueSchema(t, db)

	// Insert items with different ages
	now := time.Now()

	// Item 1: Stuck for 15 minutes
	insertQueueItemWithTime(t, db, 1, "old.nzb", "processing", now.Add(-15*time.Minute))

	// Item 2: Stuck for 5 minutes
	insertQueueItemWithTime(t, db, 2, "recent.nzb", "processing", now.Add(-5*time.Minute))

	// Item 3: Already pending (should remain unchanged)
	insertQueueItemWithTime(t, db, 3, "pending.nzb", "pending", now)

	repo := NewQueueRepository(db, DialectSQLite)

	// Test: Reset stale items
	err = repo.ResetStaleItems(context.Background())
	require.NoError(t, err, "ResetStaleItems should not error")

	// Verify: Both processing items were reset
	status1 := getQueueItemStatus(t, db, 1)
	status2 := getQueueItemStatus(t, db, 2)
	status3 := getQueueItemStatus(t, db, 3)

	assert.Equal(t, "pending", status1, "Item 1 (15min old) should be reset to pending")
	assert.Equal(t, "pending", status2, "Item 2 (5min old) should be reset to pending")
	assert.Equal(t, "pending", status3, "Item 3 (already pending) should remain pending")

	// Verify: started_at was cleared for reset item
	var startedAt *time.Time
	err = db.QueryRow("SELECT started_at FROM import_queue WHERE id = 1").Scan(&startedAt)
	require.NoError(t, err)
	assert.Nil(t, startedAt, "started_at should be NULL after reset")
}

func TestResetStaleItems_NoItemsToReset(t *testing.T) {
	// Setup: Empty queue
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	setupQueueSchema(t, db)
	repo := NewQueueRepository(db, DialectSQLite)

	// Test: Reset with no items
	err = repo.ResetStaleItems(context.Background())
	require.NoError(t, err, "Should not error on empty queue")

	// Verify: No items in queue
	count := countQueueItemsByStatus(t, db, "pending")
	assert.Equal(t, 0, count, "Queue should still be empty")
}

func TestResetStaleItems_MixedStatuses(t *testing.T) {
	// Setup: Items with various statuses
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	setupQueueSchema(t, db)

	now := time.Now()

	// Insert items with different statuses
	insertQueueItemWithTime(t, db, 1, "old-processing.nzb", "processing", now.Add(-20*time.Minute))
	insertQueueItemWithTime(t, db, 2, "old-completed.nzb", "completed", now.Add(-20*time.Minute))
	insertQueueItemWithTime(t, db, 3, "old-failed.nzb", "failed", now.Add(-20*time.Minute))
	insertQueueItemWithTime(t, db, 4, "old-pending.nzb", "pending", now.Add(-20*time.Minute))

	repo := NewQueueRepository(db, DialectSQLite)

	// Test: Reset stale items
	err = repo.ResetStaleItems(context.Background())
	require.NoError(t, err)

	// Verify: Only processing items are affected
	status1 := getQueueItemStatus(t, db, 1)
	status2 := getQueueItemStatus(t, db, 2)
	status3 := getQueueItemStatus(t, db, 3)
	status4 := getQueueItemStatus(t, db, 4)

	assert.Equal(t, "pending", status1, "Old processing item should be reset")
	assert.Equal(t, "completed", status2, "Completed items should not be affected")
	assert.Equal(t, "failed", status3, "Failed items should not be affected")
	assert.Equal(t, "pending", status4, "Already pending items should remain pending")
}

func TestResetStaleItems_VeryOldItems(t *testing.T) {
	// Setup: Items stuck for hours/days
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	setupQueueSchema(t, db)

	now := time.Now()

	// Insert items with extreme ages
	insertQueueItemWithTime(t, db, 1, "1hour-old.nzb", "processing", now.Add(-1*time.Hour))
	insertQueueItemWithTime(t, db, 2, "1day-old.nzb", "processing", now.Add(-24*time.Hour))
	insertQueueItemWithTime(t, db, 3, "1week-old.nzb", "processing", now.Add(-7*24*time.Hour))

	repo := NewQueueRepository(db, DialectSQLite)

	// Test: Reset stale items
	err = repo.ResetStaleItems(context.Background())
	require.NoError(t, err)

	// Verify: All very old items were reset
	status1 := getQueueItemStatus(t, db, 1)
	status2 := getQueueItemStatus(t, db, 2)
	status3 := getQueueItemStatus(t, db, 3)

	assert.Equal(t, "pending", status1, "1 hour old item should be reset")
	assert.Equal(t, "pending", status2, "1 day old item should be reset")
	assert.Equal(t, "pending", status3, "1 week old item should be reset")

	// Verify: All reset items now pending
	pendingCount := countQueueItemsByStatus(t, db, "pending")
	assert.Equal(t, 3, pendingCount, "All old items should now be pending")

	processingCount := countQueueItemsByStatus(t, db, "processing")
	assert.Equal(t, 0, processingCount, "No items should remain in processing")
}

func TestGetQueueItemByNzbPath(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	setupQueueSchema(t, db)
	repo := NewQueueRepository(db, DialectSQLite)
	ctx := context.Background()

	insertQueueItemWithTime(t, db, 1, "/some/path/test.nzb.gz", "pending", time.Now())

	found, err := repo.GetQueueItemByNzbPath(ctx, "/some/path/test.nzb.gz")
	require.NoError(t, err)
	require.NotNil(t, found)
	assert.Equal(t, "/some/path/test.nzb.gz", found.NzbPath)

	notFound, err := repo.GetQueueItemByNzbPath(ctx, "/no/such/file.nzb")
	require.NoError(t, err)
	assert.Nil(t, notFound)
}

func TestAddToQueueReturnsExistingRowIDForNoopConflict(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	setupQueueSchema(t, db)
	insertQueueItemWithTime(t, db, 1, "active.nzb", "processing", time.Now())
	insertQueueItemWithTime(t, db, 2, "last-insert.nzb", "failed", time.Now())
	_, err = db.Exec(`UPDATE import_queue SET category = 'protected', metadata = 'protected' WHERE id = 1`)
	require.NoError(t, err)

	repo := NewQueueRepository(db, DialectSQLite)
	incomingCategory := "incoming"
	incomingMetadata := "incoming"
	item := &ImportQueueItem{
		NzbPath:    "active.nzb",
		Status:     QueueStatusPending,
		Priority:   QueuePriorityNormal,
		MaxRetries: 3,
		Category:   &incomingCategory,
		Metadata:   &incomingMetadata,
	}

	require.NoError(t, repo.AddToQueue(context.Background(), item))
	require.EqualValues(t, 1, item.ID,
		"a no-op conflict must return the authoritative existing row ID, not last_insert_rowid")
	requireNoopConflictRowUnchanged(t, db, "active.nzb", QueueStatusProcessing)
}

func TestAddToQueueResolvesIDBeforeCommit(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "queue.db"))
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })
	setupQueueSchema(t, db)

	var committed atomic.Bool
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	require.NoError(t, conn.Raw(func(driverConn any) error {
		sqliteConn := driverConn.(*sqlite3.SQLiteConn)
		sqliteConn.RegisterCommitHook(func() int {
			committed.Store(true)
			return 0
		})
		sqliteConn.RegisterAuthorizer(func(operation int, table, _, _ string) int {
			if operation == sqlite3.SQLITE_READ && table == "import_queue" && committed.Load() {
				return sqlite3.SQLITE_DENY
			}
			return sqlite3.SQLITE_OK
		})
		return nil
	}))
	require.NoError(t, conn.Close())

	repo := NewQueueRepository(db, DialectSQLite)
	item := &ImportQueueItem{
		NzbPath: "atomic/id.nzb", Status: QueueStatusPending,
		Priority: QueuePriorityNormal, MaxRetries: 3,
	}
	require.NoError(t, repo.AddToQueue(context.Background(), item),
		"the authoritative ID lookup must complete in the insert transaction")
	require.NotZero(t, item.ID)
}

func TestAddBatchToQueueReturnsExistingRowIDForNoopConflict(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	setupQueueSchema(t, db)
	insertQueueItemWithTime(t, db, 1, "completed.nzb", "completed", time.Now())
	insertQueueItemWithTime(t, db, 2, "last-insert.nzb", "failed", time.Now())
	_, err = db.Exec(`UPDATE import_queue SET category = 'protected', metadata = 'protected' WHERE id = 1`)
	require.NoError(t, err)

	repo := NewQueueRepository(db, DialectSQLite)
	incomingCategory := "incoming"
	incomingMetadata := "incoming"
	item := &ImportQueueItem{
		NzbPath:    "completed.nzb",
		Status:     QueueStatusPending,
		Priority:   QueuePriorityNormal,
		MaxRetries: 3,
		Category:   &incomingCategory,
		Metadata:   &incomingMetadata,
	}

	require.NoError(t, repo.AddBatchToQueue(context.Background(), []*ImportQueueItem{item}))
	require.EqualValues(t, 1, item.ID,
		"a no-op batch conflict must return the authoritative existing row ID, not last_insert_rowid")
	requireNoopConflictRowUnchanged(t, db, "completed.nzb", QueueStatusCompleted)
}

func requireNoopConflictRowUnchanged(t *testing.T, db *sql.DB, path string, status QueueStatus) {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM import_queue WHERE nzb_path = ?`, path).Scan(&count))
	require.Equal(t, 1, count)
	var gotStatus QueueStatus
	var category, metadata string
	require.NoError(t, db.QueryRow(
		`SELECT status, category, metadata FROM import_queue WHERE nzb_path = ?`, path,
	).Scan(&gotStatus, &category, &metadata))
	assert.Equal(t, status, gotStatus)
	assert.Equal(t, "protected", category)
	assert.Equal(t, "protected", metadata)
}

func TestPostgresNoopQueueConflictsReturnAuthoritativeIDs(t *testing.T) {
	dsn := os.Getenv("ALTMOUNT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("ALTMOUNT_TEST_POSTGRES_DSN is not configured")
	}
	db, err := NewDB(Config{Type: "postgres", DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	repo := NewQueueRepository(db.Connection(), DialectPostgres)

	tests := []struct {
		name   string
		status QueueStatus
		add    func(context.Context, *ImportQueueItem) error
	}{
		{
			name:   "single processing conflict",
			status: QueueStatusProcessing,
			add: func(ctx context.Context, item *ImportQueueItem) error {
				return repo.AddToQueue(ctx, item)
			},
		},
		{
			name:   "batch completed conflict",
			status: QueueStatusCompleted,
			add: func(ctx context.Context, item *ImportQueueItem) error {
				return repo.AddBatchToQueue(ctx, []*ImportQueueItem{item})
			},
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			path := fmt.Sprintf("fbase/noop-%d-%d.nzb", time.Now().UnixNano(), i)
			protectedCategory := "protected"
			protectedMetadata := `{"value":"protected"}`
			seed := &ImportQueueItem{
				NzbPath: path, Status: QueueStatusPending, Priority: QueuePriorityNormal,
				MaxRetries: 3, Category: &protectedCategory, Metadata: &protectedMetadata,
			}
			require.NoError(t, repo.AddToQueue(ctx, seed))
			require.NotZero(t, seed.ID)
			require.NoError(t, repo.UpdateQueueItemStatus(ctx, seed.ID, tt.status, nil))
			t.Cleanup(func() { _ = repo.RemoveFromQueue(context.Background(), seed.ID) })

			incomingCategory := "incoming"
			incomingMetadata := `{"value":"incoming"}`
			incoming := &ImportQueueItem{
				NzbPath: path, Status: QueueStatusPending, Priority: QueuePriorityNormal,
				MaxRetries: 3, Category: &incomingCategory, Metadata: &incomingMetadata,
			}
			require.NoError(t, tt.add(ctx, incoming))
			require.Equal(t, seed.ID, incoming.ID)

			got, getErr := repo.GetQueueItem(ctx, seed.ID)
			require.NoError(t, getErr)
			require.NotNil(t, got)
			assert.Equal(t, tt.status, got.Status)
			require.NotNil(t, got.Category)
			assert.Equal(t, protectedCategory, *got.Category)
			require.NotNil(t, got.Metadata)
			assert.Equal(t, protectedMetadata, *got.Metadata)
		})
	}
}

func TestResetStaleItems_UpdatedAtFieldUpdated(t *testing.T) {
	// Setup
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	setupQueueSchema(t, db)

	now := time.Now()
	insertQueueItemWithTime(t, db, 1, "old.nzb", "processing", now.Add(-20*time.Minute))

	// Get original updated_at
	var originalUpdatedAt time.Time
	err = db.QueryRow("SELECT updated_at FROM import_queue WHERE id = 1").Scan(&originalUpdatedAt)
	require.NoError(t, err)

	// Wait 1 second to ensure time difference (SQLite datetime has second precision)
	time.Sleep(1 * time.Second)

	repo := NewQueueRepository(db, DialectSQLite)

	// Test: Reset stale items
	err = repo.ResetStaleItems(context.Background())
	require.NoError(t, err)

	// Verify: updated_at was changed
	var newUpdatedAt time.Time
	err = db.QueryRow("SELECT updated_at FROM import_queue WHERE id = 1").Scan(&newUpdatedAt)
	require.NoError(t, err)

	assert.True(t, newUpdatedAt.After(originalUpdatedAt),
		"updated_at should be updated when item is reset")
}
