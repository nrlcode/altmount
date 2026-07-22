package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

func validateGapWrite(gap GapRangeWrite) error {
	if gap.FileRevisionID == "" || gap.StartSegment < 0 || gap.SegmentCount <= 0 || gap.CreatedAt.IsZero() {
		return fmt.Errorf("gap revision, positive range, and creation time are required")
	}
	if gap.StartSegment > int64(^uint64(0)>>1)-gap.SegmentCount {
		return fmt.Errorf("gap segment range overflows")
	}
	switch gap.Kind {
	case GapKindProvisional, GapKindConfirmedAbsent, GapKindConfirmedUnusable, GapKindLegacyUnverified:
	default:
		return fmt.Errorf("invalid gap kind %q", gap.Kind)
	}
	switch gap.Status {
	case GapStatusActive, GapStatusCleared, GapStatusDormant:
	default:
		return fmt.Errorf("invalid gap status %q", gap.Status)
	}
	for _, cause := range gap.Causes {
		if cause.ProviderID == "" || cause.ProviderGeneration <= 0 || cause.ConfirmationCount < 0 ||
			(cause.Cause != GapCauseAbsent && cause.Cause != GapCauseCorrupt) {
			return fmt.Errorf("invalid provider gap cause")
		}
	}
	return nil
}

func (r *HealthStateRepository) UpsertGapRange(ctx context.Context, write GapRangeWrite) (*HealthGapRange, error) {
	if err := validateGapWrite(write); err != nil {
		return nil, err
	}
	requestedID := write.ID
	write.CreatedAt = write.CreatedAt.UTC()
	clearedAt := write.ClearedAt
	if write.Status == GapStatusActive {
		clearedAt = nil
	}
	var gap HealthGapRange
	err := r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		var revisionSegments int64
		if err := tx.QueryRowContext(ctx, `
			UPDATE health_file_revisions SET active = active WHERE id = ?
			RETURNING segment_count
		`, write.FileRevisionID).Scan(&revisionSegments); err != nil {
			return fmt.Errorf("read gap file revision bounds: %w", err)
		}
		if write.SegmentCount > revisionSegments || write.StartSegment > revisionSegments-write.SegmentCount {
			return fmt.Errorf("gap range exceeds file revision segment count")
		}
		var naturalID string
		var naturalCreated time.Time
		err := tx.QueryRowContext(ctx, `
			SELECT id, created_at FROM health_gap_ranges
			WHERE file_revision_id = ? AND kind = ? AND start_segment = ? AND segment_count = ?
		`, write.FileRevisionID, write.Kind, write.StartSegment, write.SegmentCount).
			Scan(&naturalID, &naturalCreated)
		switch {
		case err == nil:
			if requestedID == naturalID && !naturalCreated.Equal(write.CreatedAt) {
				return ErrHealthChunkConflict
			}
			write.ID = naturalID
		case !errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("read natural health gap identity: %w", err)
		case requestedID == "":
			write.ID = uuid.NewString()
		default:
			var existingID string
			err = tx.QueryRowContext(ctx,
				`SELECT id FROM health_gap_ranges WHERE id = ?`, requestedID).Scan(&existingID)
			switch {
			case err == nil:
				return ErrHealthChunkConflict
			case !errors.Is(err, sql.ErrNoRows):
				return fmt.Errorf("read health gap identity: %w", err)
			}
		}

		var canonicalID string
		err = tx.QueryRowContext(ctx, `
			INSERT INTO health_gap_ranges
				(id, file_revision_id, kind, start_segment, segment_count, status,
				 created_at, confirmed_at, cleared_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(file_revision_id, kind, start_segment, segment_count) DO UPDATE SET
				status = excluded.status,
				confirmed_at = CASE
					WHEN excluded.confirmed_at IS NOT NULL THEN excluded.confirmed_at
					ELSE health_gap_ranges.confirmed_at END,
				cleared_at = CASE
					WHEN excluded.status = 'active' THEN NULL
					ELSE COALESCE(health_gap_ranges.cleared_at, excluded.cleared_at) END
			RETURNING id
		`, write.ID, write.FileRevisionID, write.Kind, write.StartSegment,
			write.SegmentCount, write.Status, write.CreatedAt, write.ConfirmedAt, clearedAt).Scan(&canonicalID)
		if err != nil {
			return fmt.Errorf("upsert health gap range: %w", err)
		}
		for _, cause := range write.Causes {
			var confirmedAt any
			if !cause.ConfirmedAt.IsZero() {
				confirmedAt = cause.ConfirmedAt.UTC()
			}
			_, err := tx.ExecContext(ctx, `
				INSERT INTO health_gap_provider_causes
					(gap_id, provider_id, provider_generation, cause, confirmation_count, confirmed_at)
				VALUES (?, ?, ?, ?, ?, ?)
				ON CONFLICT(gap_id, provider_id, provider_generation) DO UPDATE SET
					cause = CASE
						WHEN health_gap_provider_causes.confirmed_at IS NULL
						  OR (excluded.confirmed_at IS NOT NULL AND health_gap_provider_causes.confirmed_at <= excluded.confirmed_at)
						THEN excluded.cause ELSE health_gap_provider_causes.cause END,
					confirmation_count = CASE
						WHEN health_gap_provider_causes.cause = excluded.cause THEN
							CASE WHEN health_gap_provider_causes.confirmation_count > excluded.confirmation_count
								THEN health_gap_provider_causes.confirmation_count ELSE excluded.confirmation_count END
						WHEN health_gap_provider_causes.confirmed_at IS NULL
						  OR (excluded.confirmed_at IS NOT NULL AND health_gap_provider_causes.confirmed_at <= excluded.confirmed_at)
						THEN excluded.confirmation_count ELSE health_gap_provider_causes.confirmation_count END,
					confirmed_at = CASE
						WHEN health_gap_provider_causes.confirmed_at IS NULL
						  OR (excluded.confirmed_at IS NOT NULL AND health_gap_provider_causes.confirmed_at <= excluded.confirmed_at)
						THEN excluded.confirmed_at ELSE health_gap_provider_causes.confirmed_at END
			`, canonicalID, cause.ProviderID, cause.ProviderGeneration, cause.Cause,
				cause.ConfirmationCount, confirmedAt)
			if err != nil {
				return fmt.Errorf("insert health gap provider cause: %w", err)
			}
		}
		if err := tx.QueryRowContext(ctx, `
			SELECT id, file_revision_id, kind, start_segment, segment_count, status,
			       created_at, confirmed_at, cleared_at
			FROM health_gap_ranges WHERE id = ?
		`, canonicalID).Scan(&gap.ID, &gap.FileRevisionID, &gap.Kind, &gap.StartSegment,
			&gap.SegmentCount, &gap.Status, &gap.CreatedAt, &gap.ConfirmedAt, &gap.ClearedAt); err != nil {
			return fmt.Errorf("read persisted health gap: %w", err)
		}
		rows, err := tx.QueryContext(ctx, `
			SELECT provider_id, provider_generation, cause, confirmation_count, confirmed_at
			FROM health_gap_provider_causes WHERE gap_id = ?
			ORDER BY provider_id, provider_generation
		`, canonicalID)
		if err != nil {
			return fmt.Errorf("read persisted health gap causes: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var cause GapProviderCause
			var confirmedAt *time.Time
			if err := rows.Scan(&cause.ProviderID, &cause.ProviderGeneration, &cause.Cause,
				&cause.ConfirmationCount, &confirmedAt); err != nil {
				return fmt.Errorf("scan persisted health gap cause: %w", err)
			}
			if confirmedAt != nil {
				cause.ConfirmedAt = confirmedAt.UTC()
			}
			gap.Causes = append(gap.Causes, cause)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return &gap, nil
}

func (r *HealthStateRepository) RecordSyntheticOutput(ctx context.Context, write SyntheticOutputWrite) (*CacheRecoveryState, error) {
	if write.ID == "" || write.GapID == "" || write.FileRevisionID == "" ||
		write.ByteStart < 0 || write.ByteEnd < write.ByteStart || write.EmittedAt.IsZero() {
		return nil, fmt.Errorf("synthetic output identity, revision, range, and emission time are required")
	}
	write.EmittedAt = write.EmittedAt.UTC()
	var state CacheRecoveryState
	err := r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		var gapRevision string
		var virtualSize int64
		if err := tx.QueryRowContext(ctx, `
			SELECT g.file_revision_id, r.virtual_size
			FROM health_gap_ranges g
			JOIN health_file_revisions r ON r.id = g.file_revision_id
			WHERE g.id = ?
		`, write.GapID).Scan(&gapRevision, &virtualSize); err != nil {
			return fmt.Errorf("read synthetic output gap: %w", err)
		}
		if gapRevision != write.FileRevisionID {
			return fmt.Errorf("synthetic output gap belongs to a different file revision")
		}
		if virtualSize == 0 || write.ByteStart >= virtualSize || write.ByteEnd >= virtualSize {
			return fmt.Errorf("synthetic output range exceeds file revision size")
		}

		var existingGap, existingRevision string
		var existingStart, existingEnd int64
		var existingAt time.Time
		err := tx.QueryRowContext(ctx, `
			SELECT gap_id, file_revision_id, byte_start, byte_end, emitted_at
			FROM health_synthetic_ranges WHERE id = ?
		`, write.ID).Scan(&existingGap, &existingRevision, &existingStart, &existingEnd, &existingAt)
		if err == nil {
			if existingGap != write.GapID || existingRevision != write.FileRevisionID || existingStart != write.ByteStart ||
				existingEnd != write.ByteEnd || !existingAt.Equal(write.EmittedAt) {
				return ErrHealthChunkConflict
			}
			return scanCacheRecoveryState(tx.QueryRowContext(ctx, `
				SELECT file_revision_id, status, retry_count, next_retry_at, last_error,
				       content_revision, updated_at
				FROM health_cache_recovery WHERE file_revision_id = ?
			`, write.FileRevisionID), &state)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read synthetic output identity: %w", err)
		}

		_, err = tx.ExecContext(ctx, `
			INSERT INTO health_synthetic_ranges
				(id, gap_id, file_revision_id, byte_start, byte_end, emitted_at)
			VALUES (?, ?, ?, ?, ?, ?)
		`, write.ID, write.GapID, write.FileRevisionID, write.ByteStart, write.ByteEnd, write.EmittedAt)
		if err != nil {
			return fmt.Errorf("record synthetic output range: %w", err)
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO health_cache_recovery
				(file_revision_id, status, retry_count, next_retry_at, last_error,
				 content_revision, updated_at)
			VALUES (?, 'synthetic', 0, NULL, NULL, 0, ?)
			ON CONFLICT(file_revision_id) DO UPDATE SET
				status = CASE
					WHEN health_cache_recovery.status IN ('pending', 'in_progress', 'failed')
					THEN health_cache_recovery.status
					ELSE 'synthetic'
				END,
				updated_at = CASE
					WHEN health_cache_recovery.updated_at > excluded.updated_at
					THEN health_cache_recovery.updated_at ELSE excluded.updated_at END
		`, write.FileRevisionID, write.EmittedAt)
		if err != nil {
			return fmt.Errorf("mark cache as containing synthetic output: %w", err)
		}
		return scanCacheRecoveryState(tx.QueryRowContext(ctx, `
			SELECT file_revision_id, status, retry_count, next_retry_at, last_error,
			       content_revision, updated_at
			FROM health_cache_recovery WHERE file_revision_id = ?
		`, write.FileRevisionID), &state)
	})
	if err != nil {
		return nil, err
	}
	return &state, nil
}

// MarkSyntheticRangeRecovered records that validated source bytes are now
// available for a range that was previously emitted synthetically. It queues
// cache recovery without advancing content_revision; PR8 owns that serialized
// transition and its mounted-path verification.
func (r *HealthStateRepository) MarkSyntheticRangeRecovered(ctx context.Context, syntheticID string, recoveredAt time.Time) (*CacheRecoveryState, error) {
	if syntheticID == "" || recoveredAt.IsZero() {
		return nil, fmt.Errorf("synthetic range identity and recovery time are required")
	}
	recoveredAt = recoveredAt.UTC()
	var state CacheRecoveryState
	err := r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		var revisionID string
		err := tx.QueryRowContext(ctx, `
			SELECT file_revision_id
			FROM health_synthetic_ranges WHERE id = ?
		`, syntheticID).Scan(&revisionID)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("synthetic output range does not exist")
		}
		if err != nil {
			return fmt.Errorf("read synthetic output recovery state: %w", err)
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE health_synthetic_ranges SET recovered_at = ?
			WHERE id = ? AND recovered_at IS NULL
		`, recoveredAt, syntheticID)
		if err != nil {
			return fmt.Errorf("mark synthetic output recovered: %w", err)
		}
		updated, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("read synthetic recovery update result: %w", err)
		}
		if updated == 0 {
			return scanCacheRecoveryState(tx.QueryRowContext(ctx, `
				SELECT file_revision_id, status, retry_count, next_retry_at, last_error,
				       content_revision, updated_at
				FROM health_cache_recovery WHERE file_revision_id = ?
			`, revisionID), &state)
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO health_cache_recovery
				(file_revision_id, status, retry_count, next_retry_at, last_error,
				 content_revision, updated_at)
			VALUES (?, 'pending', 0, NULL, NULL, 0, ?)
			ON CONFLICT(file_revision_id) DO UPDATE SET
				status = CASE
					WHEN health_cache_recovery.status IN ('in_progress', 'failed')
					THEN health_cache_recovery.status ELSE 'pending' END,
				next_retry_at = CASE
					WHEN health_cache_recovery.status IN ('in_progress', 'failed')
					THEN health_cache_recovery.next_retry_at ELSE NULL END,
				last_error = CASE
					WHEN health_cache_recovery.status IN ('in_progress', 'failed')
					THEN health_cache_recovery.last_error ELSE NULL END,
				updated_at = CASE
					WHEN health_cache_recovery.status IN ('in_progress', 'failed')
					THEN health_cache_recovery.updated_at ELSE excluded.updated_at END
		`, revisionID, recoveredAt)
		if err != nil {
			return fmt.Errorf("queue recovered synthetic output for cache recovery: %w", err)
		}
		return scanCacheRecoveryState(tx.QueryRowContext(ctx, `
			SELECT file_revision_id, status, retry_count, next_retry_at, last_error,
			       content_revision, updated_at
			FROM health_cache_recovery WHERE file_revision_id = ?
		`, revisionID), &state)
	})
	if err != nil {
		return nil, err
	}
	return &state, nil
}

func scanCacheRecoveryState(row rowScanner, state *CacheRecoveryState) error {
	return row.Scan(&state.FileRevisionID, &state.Status, &state.RetryCount,
		&state.NextRetryAt, &state.LastError, &state.ContentRevision, &state.UpdatedAt)
}

func (r *HealthStateRepository) GetCacheRecoveryState(ctx context.Context, revisionID string) (*CacheRecoveryState, error) {
	var state CacheRecoveryState
	err := scanCacheRecoveryState(r.db.QueryRowContext(ctx, `
		SELECT file_revision_id, status, retry_count, next_retry_at, last_error,
		       content_revision, updated_at
		FROM health_cache_recovery WHERE file_revision_id = ?
	`, revisionID), &state)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get cache recovery state: %w", err)
	}
	return &state, nil
}
