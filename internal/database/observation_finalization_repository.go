package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// FinalizeObservationHealthRun publishes every terminal ordinary-observation
// gap and retires its run in one provider-snapshot- and lease-fenced
// transaction. A provider activation between gap derivation and settlement
// therefore cannot leave a globally visible gap attributed to a stale set.
func (r *HealthStateRepository) FinalizeObservationHealthRun(
	ctx context.Context,
	runID, owner string,
	fencingToken int64,
	gaps []GapRangeWrite,
	at time.Time,
) error {
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(owner) == "" ||
		fencingToken <= 0 || at.IsZero() {
		return fmt.Errorf("observation run, owner, fence, and finalization time are required")
	}
	at = at.UTC()
	for i := range gaps {
		if err := validateGapWrite(gaps[i]); err != nil {
			return err
		}
		if gaps[i].ID == "" {
			return fmt.Errorf("terminal observation gap requires a stable identity")
		}
		if gaps[i].RunID != "" && (gaps[i].RunID != runID ||
			gaps[i].LeaseOwner != owner || gaps[i].FencingToken != fencingToken) {
			return ErrStaleHealthLease
		}
	}

	return r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		var revisionID, snapshotID, trigger string
		var leaseExpiresAt time.Time
		err := tx.QueryRowContext(ctx, `
			UPDATE health_runs SET updated_at = updated_at
			WHERE id = ? AND status = 'running' AND lease_owner = ? AND fencing_token = ?
			  AND lease_expires_at > ? AND cancel_requested = FALSE
			RETURNING file_revision_id, provider_snapshot_id, trigger, lease_expires_at
		`, runID, owner, fencingToken, r.now().UTC()).Scan(
			&revisionID, &snapshotID, &trigger, &leaseExpiresAt,
		)
		if errors.Is(err, sql.ErrNoRows) || (err == nil && !leaseExpiresAt.After(r.now().UTC())) {
			return ErrStaleHealthLease
		}
		if err != nil {
			return fmt.Errorf("lock terminal observation run: %w", err)
		}
		if trigger == "import" || trigger == "health_pending" ||
			strings.HasPrefix(trigger, "gap_revalidation_") ||
			strings.HasPrefix(trigger, "provider_activation") {
			return fmt.Errorf("targeted observation run requires its dedicated finalizer")
		}
		current, err := providerSnapshotMembershipMatchesCurrentTx(ctx, tx, snapshotID)
		if err != nil {
			return err
		}
		if !current {
			return ErrProviderSnapshotMismatch
		}

		var scheduleActive bool
		var targetProviderID, targetGapID sql.NullString
		if err := tx.QueryRowContext(ctx, `
			UPDATE health_run_schedule SET active = active
			WHERE run_id = ?
			RETURNING active, target_provider_id, target_gap_id
		`, runID).Scan(&scheduleActive, &targetProviderID, &targetGapID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrStaleHealthSchedule
			}
			return fmt.Errorf("lock terminal observation schedule: %w", err)
		}
		if !scheduleActive || targetProviderID.Valid || targetGapID.Valid {
			return ErrStaleHealthSchedule
		}

		var revisionSegments int64
		if err := tx.QueryRowContext(ctx, `
			UPDATE health_file_revisions SET active = active
			WHERE id = ? AND active = TRUE
			RETURNING segment_count
		`, revisionID).Scan(&revisionSegments); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrStaleHealthSchedule
			}
			return fmt.Errorf("lock terminal observation revision: %w", err)
		}

		for i := range gaps {
			write := gaps[i]
			if write.FileRevisionID != revisionID || write.SegmentCount > revisionSegments ||
				write.StartSegment > revisionSegments-write.SegmentCount {
				return ErrStaleHealthSchedule
			}
			if err := r.persistFinalObservationGapTx(ctx, tx, write); err != nil {
				return err
			}
		}

		finished, err := tx.ExecContext(ctx, `
			UPDATE health_runs
			SET status = 'completed', resolved_segments = total_segments,
			    lease_owner = NULL, lease_expires_at = NULL,
			    last_error = NULL, updated_at = ?, completed_at = ?
			WHERE id = ? AND status = 'running' AND lease_owner = ? AND fencing_token = ?
			  AND lease_expires_at > ? AND cancel_requested = FALSE
		`, at, at, runID, owner, fencingToken, r.now().UTC())
		if err != nil {
			return fmt.Errorf("complete terminal observation run: %w", err)
		}
		if rows, err := finished.RowsAffected(); err != nil {
			return fmt.Errorf("read terminal observation completion: %w", err)
		} else if rows != 1 {
			return ErrStaleHealthLease
		}
		retired, err := tx.ExecContext(ctx, `
			UPDATE health_run_schedule SET active = FALSE, updated_at = ?
			WHERE run_id = ? AND active = TRUE
		`, at, runID)
		if err != nil {
			return fmt.Errorf("retire terminal observation schedule: %w", err)
		}
		if rows, err := retired.RowsAffected(); err != nil {
			return fmt.Errorf("read retired terminal observation schedule: %w", err)
		} else if rows != 1 {
			return ErrStaleHealthSchedule
		}
		return nil
	})
}

func (r *HealthStateRepository) persistFinalObservationGapTx(
	ctx context.Context,
	tx *dialectAwareTx,
	write GapRangeWrite,
) error {
	write.CreatedAt = write.CreatedAt.UTC()
	causes, confirmedAt, err := r.authoritativeGapProviderCauses(ctx, tx, write)
	if err != nil {
		return err
	}
	write.Causes = causes
	write.ConfirmedAt = confirmedAt

	var existingRevision string
	var existingKind GapKind
	var existingStatus GapStatus
	var existingStart, existingCount, episode int64
	var existingCreated time.Time
	err = tx.QueryRowContext(ctx, `
		SELECT file_revision_id, kind, start_segment, segment_count, episode, status, created_at
		FROM health_gap_ranges WHERE id = ?
	`, write.ID).Scan(&existingRevision, &existingKind, &existingStart,
		&existingCount, &episode, &existingStatus, &existingCreated)
	switch {
	case err == nil:
		if existingRevision != write.FileRevisionID || existingKind != write.Kind ||
			existingStart != write.StartSegment || existingCount != write.SegmentCount ||
			!existingCreated.Equal(write.CreatedAt) {
			return ErrHealthChunkConflict
		}
		// A newer validated-BODY recovery is authoritative. The ordinary run
		// may have derived the same stable gap before that targeted clear
		// committed; finalization must retire the stale run without resurrecting
		// the cleared episode.
		if existingStatus == GapStatusCleared {
			return nil
		}
		if existingStatus != GapStatusActive {
			return ErrHealthChunkConflict
		}
	case errors.Is(err, sql.ErrNoRows):
		if err := tx.QueryRowContext(ctx, `
			SELECT COALESCE(MAX(episode), 0) + 1
			FROM health_gap_ranges
			WHERE file_revision_id = ? AND kind = ?
			  AND start_segment = ? AND segment_count = ?
		`, write.FileRevisionID, write.Kind, write.StartSegment,
			write.SegmentCount).Scan(&episode); err != nil {
			return fmt.Errorf("allocate terminal observation gap episode: %w", err)
		}
	default:
		return fmt.Errorf("read terminal observation gap identity: %w", err)
	}

	var nextRevalidationAt any
	if write.ConfirmedAt != nil {
		nextRevalidationAt = write.ConfirmedAt.UTC().Add(24 * time.Hour)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO health_gap_ranges
			(id, file_revision_id, kind, start_segment, segment_count, episode, status,
			 created_at, confirmed_at, cleared_at, revalidation_step,
			 next_revalidation_at, last_revalidation_at)
		VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?, NULL, 0, ?, NULL)
		ON CONFLICT(id) DO UPDATE SET
			confirmed_at = COALESCE(health_gap_ranges.confirmed_at, excluded.confirmed_at),
			next_revalidation_at = COALESCE(
				health_gap_ranges.next_revalidation_at, excluded.next_revalidation_at)
	`, write.ID, write.FileRevisionID, write.Kind, write.StartSegment,
		write.SegmentCount, episode, write.CreatedAt, write.ConfirmedAt,
		nextRevalidationAt); err != nil {
		return fmt.Errorf("persist terminal observation gap: %w", err)
	}
	for _, cause := range write.Causes {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO health_gap_provider_causes
				(gap_id, provider_id, provider_generation, provider_activation_epoch,
				 cause, confirmation_count, confirmed_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(gap_id, provider_id, provider_generation, provider_activation_epoch)
			DO UPDATE SET cause = excluded.cause,
			              confirmation_count = excluded.confirmation_count,
			              confirmed_at = excluded.confirmed_at
		`, write.ID, cause.ProviderID, cause.ProviderGeneration,
			cause.ProviderActivationEpoch, cause.Cause,
			cause.ConfirmationCount, cause.ConfirmedAt.UTC()); err != nil {
			return fmt.Errorf("persist terminal observation gap cause: %w", err)
		}
	}
	return nil
}
