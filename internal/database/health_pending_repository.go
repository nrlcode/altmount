package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// FinalizeHealthPendingObservation consumes the exact unresolved import set
// only after every position either becomes present or belongs to a persistent
// gap whose causes cover the current provider activations. Inconclusive runs
// are failed and re-armed without rewriting temporary evidence as absence.
func (r *HealthStateRepository) FinalizeHealthPendingObservation(
	ctx context.Context,
	runID, owner string,
	fencingToken int64,
	at time.Time,
) (*HealthPendingFinalization, error) {
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(owner) == "" ||
		fencingToken <= 0 || at.IsZero() {
		return nil, fmt.Errorf("health-pending run, owner, fence, and finalization time are required")
	}
	at = at.UTC()
	result := &HealthPendingFinalization{}
	err := r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		var revisionID, snapshotID, trigger string
		var workTotal int64
		var leaseExpiresAt time.Time
		err := tx.QueryRowContext(ctx, `
			UPDATE health_runs SET updated_at = updated_at
			WHERE id = ? AND status = 'running' AND lease_owner = ? AND fencing_token = ?
			RETURNING file_revision_id, provider_snapshot_id, trigger,
			          total_segments, lease_expires_at
		`, runID, owner, fencingToken).Scan(
			&revisionID, &snapshotID, &trigger, &workTotal, &leaseExpiresAt,
		)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrStaleHealthLease
		}
		if err != nil {
			return fmt.Errorf("lock health-pending run: %w", err)
		}
		if !leaseExpiresAt.After(r.now().UTC()) {
			return ErrStaleHealthLease
		}
		if trigger != "health_pending" {
			return fmt.Errorf("run is not a health-pending continuation")
		}
		var revisionTotal int64
		if err := tx.QueryRowContext(ctx, `
			SELECT segment_count FROM health_file_revisions
			WHERE id = ? AND active = TRUE
		`, revisionID).Scan(&revisionTotal); err != nil {
			return fmt.Errorf("read health-pending revision bounds: %w", err)
		}
		current, err := providerSnapshotMembershipMatchesCurrentTx(ctx, tx, snapshotID)
		if err != nil {
			return err
		}
		if !current {
			return ErrProviderSnapshotMismatch
		}
		var scheduleActive bool
		if err := tx.QueryRowContext(ctx, `
			UPDATE health_run_schedule SET active = active
			WHERE run_id = ? RETURNING active
		`, runID).Scan(&scheduleActive); err != nil {
			return fmt.Errorf("lock health-pending schedule: %w", err)
		}
		if !scheduleActive {
			return ErrStaleHealthSchedule
		}

		var validationID string
		var unresolvedCount int64
		var unresolved []byte
		err = tx.QueryRowContext(ctx, `
			SELECT id, unresolved_segments, unresolved_bitmap
			FROM health_import_validations
			WHERE file_revision_id = ? AND phase = 'health_pending'
			  AND health_pending_settled_at IS NULL
			ORDER BY updated_at DESC, id DESC LIMIT 1
		`, revisionID).Scan(&validationID, &unresolvedCount, &unresolved)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrStaleHealthSchedule
		}
		if err != nil {
			return fmt.Errorf("read health-pending validation: %w", err)
		}
		if len(unresolved) != bitmapByteLength(revisionTotal) ||
			bitmapPopulation(unresolved) != unresolvedCount || unresolvedCount <= 0 ||
			workTotal != unresolvedCount {
			return fmt.Errorf("health-pending validation bitmap is inconsistent")
		}

		allRecovered := true
		allResolved := true
		for position := int64(0); position < revisionTotal; position++ {
			if !bitmapSet(unresolved, position) {
				continue
			}
			var present int
			rows, err := tx.QueryContext(ctx, `
				SELECT segment_start, segment_count, present_bitmap
				FROM health_run_chunks
				WHERE run_id = ? AND segment_start <= ?
				  AND segment_start + segment_count > ?
			`, runID, position, position)
			if err != nil {
				return fmt.Errorf("read health-pending presence: %w", err)
			}
			for rows.Next() {
				var start, count int64
				var bitmap []byte
				if err := rows.Scan(&start, &count, &bitmap); err != nil {
					rows.Close()
					return fmt.Errorf("scan health-pending presence: %w", err)
				}
				if position-start < count && bitmapSet(bitmap, position-start) {
					present = 1
				}
			}
			if err := rows.Close(); err != nil {
				return fmt.Errorf("close health-pending presence: %w", err)
			}
			if present > 0 {
				continue
			}
			allRecovered = false
			var coveringGap int
			if err := tx.QueryRowContext(ctx, `
				SELECT COUNT(*)
				FROM health_gap_ranges gap
				WHERE gap.file_revision_id = ? AND gap.status IN ('active', 'dormant')
				  AND gap.kind IN ('confirmed_absent', 'confirmed_unusable')
				  AND gap.start_segment <= ?
				  AND gap.start_segment + gap.segment_count > ?
				  AND (SELECT COUNT(*) FROM health_providers WHERE active = TRUE) > 0
				  AND (SELECT COUNT(*) FROM health_providers WHERE active = TRUE) = (
				    SELECT COUNT(*) FROM health_gap_provider_causes cause
				    JOIN health_providers provider
				      ON provider.id = cause.provider_id AND provider.active = TRUE
				     AND provider.current_generation = cause.provider_generation
				     AND provider.activation_epoch = cause.provider_activation_epoch
				    WHERE cause.gap_id = gap.id
				  )
			`, revisionID, position, position).Scan(&coveringGap); err != nil {
				return fmt.Errorf("read health-pending persistent gap: %w", err)
			}
			if coveringGap == 0 {
				allResolved = false
			}
		}

		status := HealthRunCompleted
		lastError := any(nil)
		if allResolved {
			result.Settled = true
			result.Recovered = allRecovered
			if allRecovered {
				empty := make([]byte, bitmapByteLength(revisionTotal))
				updated, err := tx.ExecContext(ctx, `
					UPDATE health_import_validations
					SET phase = 'accepted', unresolved_segments = 0,
					    unresolved_bitmap = ?, health_pending_settled_at = ?,
					    coverage_reused_at = ?, updated_at = ?
					WHERE id = ? AND phase = 'health_pending'
					  AND health_pending_settled_at IS NULL
				`, empty, at, at, at, validationID)
				if err != nil {
					return fmt.Errorf("accept recovered health-pending validation: %w", err)
				}
				if rows, _ := updated.RowsAffected(); rows != 1 {
					return ErrStaleHealthSchedule
				}
			} else {
				updated, err := tx.ExecContext(ctx, `
					UPDATE health_import_validations
					SET health_pending_settled_at = ?, updated_at = ?
					WHERE id = ? AND phase = 'health_pending'
					  AND health_pending_settled_at IS NULL
				`, at, at, validationID)
				if err != nil {
					return fmt.Errorf("settle persistent health-pending gap: %w", err)
				}
				if rows, _ := updated.RowsAffected(); rows != 1 {
					return ErrStaleHealthSchedule
				}
			}
		} else {
			status = HealthRunFailed
			lastError = "inconclusive health pending observation"
			if _, err := tx.ExecContext(ctx, `
				UPDATE file_health
				SET status = 'pending', scheduled_check_at = ?, updated_at = ?
				WHERE id = (SELECT file_health_id FROM health_file_revisions WHERE id = ?)
			`, at.Add(time.Hour), at, revisionID); err != nil {
				return fmt.Errorf("rearm inconclusive health-pending observation: %w", err)
			}
		}
		progressAssignment := ""
		if result.Settled {
			progressAssignment = ", resolved_segments = total_segments"
		}
		finished, err := tx.ExecContext(ctx, `
			UPDATE health_runs
			SET status = ?`+progressAssignment+`, lease_owner = NULL, lease_expires_at = NULL,
			    last_error = ?, updated_at = ?, completed_at = ?
			WHERE id = ? AND status = 'running' AND lease_owner = ? AND fencing_token = ?
			  AND lease_expires_at > ? AND cancel_requested = FALSE
		`, status, lastError, at, at, runID, owner, fencingToken, r.now().UTC())
		if err != nil {
			return fmt.Errorf("finish health-pending observation run: %w", err)
		}
		if rows, _ := finished.RowsAffected(); rows != 1 {
			return ErrStaleHealthLease
		}
		retired, err := tx.ExecContext(ctx, `
			UPDATE health_run_schedule SET active = FALSE, updated_at = ?
			WHERE run_id = ? AND active = TRUE
		`, at, runID)
		if err != nil {
			return fmt.Errorf("retire health-pending observation schedule: %w", err)
		}
		if rows, _ := retired.RowsAffected(); rows != 1 {
			return ErrStaleHealthSchedule
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
