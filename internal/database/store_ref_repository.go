package database

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrStoreRefOperationConflict = errors.New("store reference operation conflicts with retained transition")

// StoreRefRepository tracks reference counts on .nzbz store files.
// When all .meta files that reference a given store are deleted,
// the refcount hits 0 and the caller can delete the store file.
type StoreRefRepository struct {
	db      *dialectAwareDB
	dialect dialectHelper
}

// NewStoreRefRepository creates a new StoreRefRepository.
func NewStoreRefRepository(db *sql.DB, d Dialect) *StoreRefRepository {
	return &StoreRefRepository{
		db:      newDialectAwareDB(db, d),
		dialect: dialectHelper{d: d},
	}
}

// IncStoreRef increments the ref_count for storePath by 1.
// If no row exists yet, it inserts one with ref_count = 1.
func (r *StoreRefRepository) IncStoreRef(ctx context.Context, storePath string) error {
	query := r.dialect.q(`
		INSERT INTO nzb_store_refs (store_path, ref_count)
		VALUES (?, 1)
		ON CONFLICT(store_path) DO UPDATE SET
			ref_count = nzb_store_refs.ref_count + 1
	`)
	_, err := r.db.ExecContext(ctx, query, storePath)
	if err != nil {
		return fmt.Errorf("inc store ref %q: %w", storePath, err)
	}
	return nil
}

// DecStoreRef decrements the ref_count for storePath by 1.
// If the resulting count is ≤ 0, the row is deleted and 0 is returned.
// Returns the new count (0 if the row was deleted or did not exist).
func (r *StoreRefRepository) DecStoreRef(ctx context.Context, storePath string) (int64, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("dec store ref %q: begin tx: %w", storePath, err)
	}
	defer tx.Rollback()

	updateQuery := r.dialect.q(`
		UPDATE nzb_store_refs
		SET ref_count = ref_count - 1
		WHERE store_path = ?
	`)
	if _, err := tx.ExecContext(ctx, updateQuery, storePath); err != nil {
		return 0, fmt.Errorf("dec store ref %q: update: %w", storePath, err)
	}

	var count int64
	selectQuery := r.dialect.q(`SELECT ref_count FROM nzb_store_refs WHERE store_path = ?`)
	err = tx.QueryRowContext(ctx, selectQuery, storePath).Scan(&count)
	if errors.Is(err, sql.ErrNoRows) {
		// Row didn't exist before the decrement; nothing to do.
		if err := tx.Commit(); err != nil {
			return 0, fmt.Errorf("dec store ref %q: commit: %w", storePath, err)
		}
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("dec store ref %q: select: %w", storePath, err)
	}

	if count <= 0 {
		deleteQuery := r.dialect.q(`DELETE FROM nzb_store_refs WHERE store_path = ?`)
		if _, err := tx.ExecContext(ctx, deleteQuery, storePath); err != nil {
			return 0, fmt.Errorf("dec store ref %q: delete: %w", storePath, err)
		}
		count = 0
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("dec store ref %q: commit: %w", storePath, err)
	}
	return count, nil
}

// GetStoreRefCount returns the current ref_count for storePath.
// Returns 0 (and no error) if the row does not exist.
func (r *StoreRefRepository) GetStoreRefCount(ctx context.Context, storePath string) (int64, error) {
	query := r.dialect.q(`SELECT ref_count FROM nzb_store_refs WHERE store_path = ?`)
	var count int64
	err := r.db.QueryRowContext(ctx, query, storePath).Scan(&count)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get store ref count %q: %w", storePath, err)
	}
	return count, nil
}

// ApplyStoreRefDeltaOnce applies one ref transition under an opaque SHA-256
// operation key. The operation ledger retains only the operation key, a hash
// of the local store path, the delta, and the resulting count; it never stores
// article identities or metadata payloads.
func (r *StoreRefRepository) ApplyStoreRefDeltaOnce(
	ctx context.Context,
	operationKey, storePath string,
	delta int64,
) (count int64, applied bool, err error) {
	if !validStoreRefOperationKey(operationKey) || strings.TrimSpace(storePath) == "" ||
		(delta != -1 && delta != 1) {
		return 0, false, fmt.Errorf("valid hashed operation, store path, and unit delta are required")
	}
	pathDigest := sha256.Sum256([]byte(storePath))
	pathHash := "sha256:" + hex.EncodeToString(pathDigest[:])
	var lastErr error
	for attempt := 0; attempt < 8; attempt++ {
		count, applied, err = r.applyStoreRefDeltaOnce(ctx, operationKey, pathHash, storePath, delta)
		if err == nil || errors.Is(err, ErrStoreRefOperationConflict) ||
			!retryableHealthScheduleConflict(err) {
			return count, applied, err
		}
		lastErr = err
		timer := time.NewTimer(time.Duration(attempt+1) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, false, ctx.Err()
		case <-timer.C:
		}
	}
	return 0, false, fmt.Errorf("converge idempotent store reference transition: %w", lastErr)
}

func (r *StoreRefRepository) applyStoreRefDeltaOnce(
	ctx context.Context,
	operationKey, pathHash, storePath string,
	delta int64,
) (count int64, applied bool, err error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, fmt.Errorf("begin idempotent store reference transition: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var retainedHash string
	var retainedDelta, retainedCount int64
	err = tx.QueryRowContext(ctx, `
		SELECT store_path_hash, delta, resulting_ref_count
		FROM nzb_store_ref_operations WHERE operation_key = ?
	`, operationKey).Scan(&retainedHash, &retainedDelta, &retainedCount)
	if err == nil {
		if retainedHash != pathHash || retainedDelta != delta {
			return 0, false, ErrStoreRefOperationConflict
		}
		if err := tx.Commit(); err != nil {
			return 0, false, fmt.Errorf("commit retained store reference transition: %w", err)
		}
		return retainedCount, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, false, fmt.Errorf("read retained store reference transition: %w", err)
	}

	if delta > 0 {
		err = tx.QueryRowContext(ctx, `
			INSERT INTO nzb_store_refs (store_path, ref_count, updated_at)
			VALUES (?, 1, CURRENT_TIMESTAMP)
			ON CONFLICT(store_path) DO UPDATE SET
				ref_count = nzb_store_refs.ref_count + 1,
				updated_at = CURRENT_TIMESTAMP
			RETURNING ref_count
		`, storePath).Scan(&count)
		if err != nil {
			return 0, false, fmt.Errorf("increment idempotent store reference: %w", err)
		}
	} else {
		err = tx.QueryRowContext(ctx, `
			UPDATE nzb_store_refs
			SET ref_count = ref_count - 1, updated_at = CURRENT_TIMESTAMP
			WHERE store_path = ? AND ref_count > 0
			RETURNING ref_count
		`, storePath).Scan(&count)
		if errors.Is(err, sql.ErrNoRows) {
			count = 0
		} else if err != nil {
			return 0, false, fmt.Errorf("decrement idempotent store reference: %w", err)
		}
		if count <= 0 {
			count = 0
			if _, err := tx.ExecContext(ctx, `
				DELETE FROM nzb_store_refs WHERE store_path = ? AND ref_count <= 0
			`, storePath); err != nil {
				return 0, false, fmt.Errorf("retire zero idempotent store reference: %w", err)
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO nzb_store_ref_operations
			(operation_key, store_path_hash, delta, resulting_ref_count, applied_at)
		VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
	`, operationKey, pathHash, delta, count); err != nil {
		return 0, false, fmt.Errorf("record idempotent store reference transition: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, false, fmt.Errorf("commit idempotent store reference transition: %w", err)
	}
	return count, true, nil
}

func validStoreRefOperationKey(value string) bool {
	if len(value) != len("sha256:")+sha256.Size*2 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil && len(decoded) == sha256.Size
}
