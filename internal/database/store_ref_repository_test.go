package database

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func storeRefOperationKey(value string) string {
	digest := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func setupStoreRefTestDB(t *testing.T) *StoreRefRepository {
	t.Helper()
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name()))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS nzb_store_refs (
			store_path TEXT NOT NULL PRIMARY KEY,
			ref_count  INTEGER NOT NULL DEFAULT 0,
			updated_at DATETIME NOT NULL DEFAULT (datetime('now'))
		)
		;
		CREATE TABLE IF NOT EXISTS nzb_store_ref_operations (
			operation_key TEXT NOT NULL PRIMARY KEY,
			store_path_hash TEXT NOT NULL,
			delta INTEGER NOT NULL CHECK(delta IN (-1, 1)),
			resulting_ref_count BIGINT NOT NULL CHECK(resulting_ref_count >= 0),
			applied_at DATETIME NOT NULL
		)
	`)
	require.NoError(t, err)

	return NewStoreRefRepository(db, DialectSQLite)
}

func TestStoreRefRepository_IncStoreRef(t *testing.T) {
	repo := setupStoreRefTestDB(t)
	ctx := context.Background()

	storePath := "/store/abc.nzbz"

	// First increment: row should be inserted with ref_count = 1.
	require.NoError(t, repo.IncStoreRef(ctx, storePath))
	count, err := repo.GetStoreRefCount(ctx, storePath)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// Second increment: ref_count should become 2.
	require.NoError(t, repo.IncStoreRef(ctx, storePath))
	count, err = repo.GetStoreRefCount(ctx, storePath)
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// Third increment: ref_count should become 3.
	require.NoError(t, repo.IncStoreRef(ctx, storePath))
	count, err = repo.GetStoreRefCount(ctx, storePath)
	require.NoError(t, err)
	assert.Equal(t, int64(3), count)
}

func TestStoreRefRepository_GetStoreRefCount_NotFound(t *testing.T) {
	repo := setupStoreRefTestDB(t)
	ctx := context.Background()

	count, err := repo.GetStoreRefCount(ctx, "/store/nonexistent.nzbz")
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestStoreRefRepository_DecStoreRef_Decrement(t *testing.T) {
	repo := setupStoreRefTestDB(t)
	ctx := context.Background()

	storePath := "/store/dec.nzbz"

	// Set up: increment 3 times.
	require.NoError(t, repo.IncStoreRef(ctx, storePath))
	require.NoError(t, repo.IncStoreRef(ctx, storePath))
	require.NoError(t, repo.IncStoreRef(ctx, storePath))

	// Decrement: should return 2.
	count, err := repo.DecStoreRef(ctx, storePath)
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// Verify via Get.
	got, err := repo.GetStoreRefCount(ctx, storePath)
	require.NoError(t, err)
	assert.Equal(t, int64(2), got)
}

func TestStoreRefRepository_DecStoreRef_DeletesRowAtZero(t *testing.T) {
	repo := setupStoreRefTestDB(t)
	ctx := context.Background()

	storePath := "/store/zero.nzbz"

	// Set up: increment once, then decrement back to zero.
	require.NoError(t, repo.IncStoreRef(ctx, storePath))

	count, err := repo.DecStoreRef(ctx, storePath)
	require.NoError(t, err)
	assert.Equal(t, int64(0), count, "decrementing to zero should return 0")

	// Row must be gone.
	got, err := repo.GetStoreRefCount(ctx, storePath)
	require.NoError(t, err)
	assert.Equal(t, int64(0), got, "row should have been deleted")
}

func TestStoreRefRepository_DecStoreRef_NoRow(t *testing.T) {
	repo := setupStoreRefTestDB(t)
	ctx := context.Background()

	// Decrementing a non-existent row should not error and should return 0.
	count, err := repo.DecStoreRef(ctx, "/store/ghost.nzbz")
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestStoreRefRepository_MultipleStores(t *testing.T) {
	repo := setupStoreRefTestDB(t)
	ctx := context.Background()

	pathA := "/store/a.nzbz"
	pathB := "/store/b.nzbz"

	require.NoError(t, repo.IncStoreRef(ctx, pathA))
	require.NoError(t, repo.IncStoreRef(ctx, pathA))
	require.NoError(t, repo.IncStoreRef(ctx, pathB))

	countA, err := repo.GetStoreRefCount(ctx, pathA)
	require.NoError(t, err)
	assert.Equal(t, int64(2), countA)

	countB, err := repo.GetStoreRefCount(ctx, pathB)
	require.NoError(t, err)
	assert.Equal(t, int64(1), countB)

	// Decrement A once; B should be unaffected.
	newA, err := repo.DecStoreRef(ctx, pathA)
	require.NoError(t, err)
	assert.Equal(t, int64(1), newA)

	countB, err = repo.GetStoreRefCount(ctx, pathB)
	require.NoError(t, err)
	assert.Equal(t, int64(1), countB, "store B must be unaffected")
}

func TestStoreRefRepository_ApplyStoreRefDeltaOnceIsDurableAndRawFree(t *testing.T) {
	repo := setupStoreRefTestDB(t)
	ctx := context.Background()
	path := "/store/private-local-name.nzbz"
	require.NoError(t, repo.IncStoreRef(ctx, path))
	require.NoError(t, repo.IncStoreRef(ctx, path))
	key := storeRefOperationKey("queue-7:rollback:candidate-a")

	count, applied, err := repo.ApplyStoreRefDeltaOnce(ctx, key, path, -1)
	require.NoError(t, err)
	assert.True(t, applied)
	assert.Equal(t, int64(1), count)
	count, applied, err = repo.ApplyStoreRefDeltaOnce(ctx, key, path, -1)
	require.NoError(t, err)
	assert.False(t, applied)
	assert.Equal(t, int64(1), count)
	actual, err := repo.GetStoreRefCount(ctx, path)
	require.NoError(t, err)
	assert.Equal(t, int64(1), actual)

	var retainedHash string
	require.NoError(t, repo.db.QueryRowContext(ctx, `
		SELECT store_path_hash FROM nzb_store_ref_operations WHERE operation_key = ?
	`, key).Scan(&retainedHash))
	assert.NotEqual(t, path, retainedHash)
	assert.NotContains(t, retainedHash, "private-local-name")
	_, _, err = repo.ApplyStoreRefDeltaOnce(ctx, key, path, 1)
	require.ErrorIs(t, err, ErrStoreRefOperationConflict)
}

func TestStoreRefRepository_ApplyStoreRefDeltaOnceConvergesConcurrently(t *testing.T) {
	repo := setupStoreRefTestDB(t)
	ctx := context.Background()
	key := storeRefOperationKey("concurrent-increment")
	path := "/store/concurrent.nzbz"
	var appliedCount atomic.Int64
	var wg sync.WaitGroup
	errorsOut := make(chan error, 12)
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			count, applied, err := repo.ApplyStoreRefDeltaOnce(ctx, key, path, 1)
			if err == nil && count != 1 {
				err = fmt.Errorf("unexpected converged count %d", count)
			}
			if applied {
				appliedCount.Add(1)
			}
			errorsOut <- err
		}()
	}
	wg.Wait()
	close(errorsOut)
	for err := range errorsOut {
		require.NoError(t, err)
	}
	assert.Equal(t, int64(1), appliedCount.Load())
	count, err := repo.GetStoreRefCount(ctx, path)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}
