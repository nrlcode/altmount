package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// FinalizeProviderActivationGap incorporates two delayed confirmation waves
// from one newly active provider into an existing known gap. Cause history is
// immutable across provider removal and reactivation; conclusions join only
// the exact currently active generation and activation epoch.
func (r *HealthStateRepository) FinalizeProviderActivationGap(
	ctx context.Context,
	runID, owner string,
	fencingToken int64,
	at time.Time,
) (bool, error) {
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(owner) == "" ||
		fencingToken <= 0 || at.IsZero() {
		return false, fmt.Errorf("provider activation run, owner, fence, and finalization time are required")
	}
	at = at.UTC()
	conclusive := false
	err := r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		var revisionID, snapshotID, trigger string
		var leaseExpiresAt time.Time
		err := tx.QueryRowContext(ctx, `
			UPDATE health_runs SET updated_at = updated_at
			WHERE id = ? AND status = 'running' AND lease_owner = ? AND fencing_token = ?
			RETURNING file_revision_id, provider_snapshot_id, trigger, lease_expires_at
		`, runID, owner, fencingToken).Scan(
			&revisionID, &snapshotID, &trigger, &leaseExpiresAt,
		)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrStaleHealthLease
		}
		if err != nil {
			return fmt.Errorf("lock provider activation gap run: %w", err)
		}
		if !leaseExpiresAt.After(r.now().UTC()) {
			return ErrStaleHealthLease
		}
		if !strings.HasPrefix(trigger, "provider_activation") {
			return fmt.Errorf("run is not provider activation work")
		}
		current, err := providerSnapshotMembershipMatchesCurrentTx(ctx, tx, snapshotID)
		if err != nil {
			return err
		}
		if !current {
			return ErrProviderSnapshotMismatch
		}

		var active bool
		var providerID, gapID *string
		var generation, epoch *int64
		if err := tx.QueryRowContext(ctx, `
			UPDATE health_run_schedule SET active = active
			WHERE run_id = ?
			RETURNING active, target_provider_id, target_provider_generation,
			          target_provider_activation_epoch, target_gap_id
		`, runID).Scan(&active, &providerID, &generation, &epoch, &gapID); err != nil {
			return fmt.Errorf("lock provider activation gap schedule: %w", err)
		}
		if !active || providerID == nil || generation == nil || epoch == nil || gapID == nil {
			return ErrStaleHealthSchedule
		}
		var gap HealthGapRange
		if err := scanHealthGapRange(tx.QueryRowContext(ctx, `
			UPDATE health_gap_ranges SET status = status WHERE id = ?
			RETURNING id, file_revision_id, kind, start_segment, segment_count, episode,
			          status, created_at, confirmed_at, cleared_at, revalidation_step,
			          next_revalidation_at, last_revalidation_at
		`, *gapID), &gap); err != nil {
			return fmt.Errorf("lock provider activation gap: %w", err)
		}
		if gap.FileRevisionID != revisionID ||
			(gap.Status != GapStatusActive && gap.Status != GapStatusDormant) {
			return ErrStaleHealthSchedule
		}

		// A corrupt confirmation is usable only when its source BODY explicitly
		// proves the fresh-transport request survived durable commit.
		var staleCorrupt int
		if err := tx.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM health_confirmation_events event
			JOIN health_run_chunks chunk ON chunk.id = event.source_chunk_id
			WHERE chunk.run_id = ? AND event.provider_id = ?
			  AND event.provider_generation = ? AND event.provider_activation_epoch = ?
			  AND event.cause = 'corrupt'
			  AND (chunk.observation_kind <> 'validated_body' OR chunk.fresh_transport = FALSE)
		`, runID, *providerID, *generation, *epoch).Scan(&staleCorrupt); err != nil {
			return fmt.Errorf("verify provider activation corrupt transport freshness: %w", err)
		}
		if staleCorrupt == 0 {
			write := GapRangeWrite{
				FileRevisionID: revisionID, Kind: GapKindProvisional,
				StartSegment: gap.StartSegment, SegmentCount: gap.SegmentCount,
				Status: GapStatusActive, CreatedAt: gap.CreatedAt,
				Causes: []GapProviderCause{{
					ProviderID: *providerID, ProviderGeneration: *generation,
					ProviderActivationEpoch: *epoch, Cause: GapCauseAbsent,
				}},
			}
			derived, _, deriveErr := deriveTimeSeparatedGapCauses(
				ctx, tx, write, r.gapConfirmationMinimumDelay(),
			)
			if deriveErr != nil && !errors.Is(deriveErr, errIncompleteGapConfirmationEvidence) {
				return deriveErr
			}
			if deriveErr == nil && len(derived) == 1 && derived[0].ConfirmationCount >= 2 {
				cause := derived[0]
				if _, err := tx.ExecContext(ctx, `
					INSERT INTO health_gap_provider_causes
						(gap_id, provider_id, provider_generation, provider_activation_epoch,
						 cause, confirmation_count, confirmed_at)
					VALUES (?, ?, ?, ?, ?, ?, ?)
					ON CONFLICT(gap_id, provider_id, provider_generation, provider_activation_epoch)
					DO UPDATE SET cause = excluded.cause,
					              confirmation_count = excluded.confirmation_count,
					              confirmed_at = excluded.confirmed_at
				`, gap.ID, cause.ProviderID, cause.ProviderGeneration,
					cause.ProviderActivationEpoch, cause.Cause,
					cause.ConfirmationCount, cause.ConfirmedAt); err != nil {
					return fmt.Errorf("update provider activation known-gap cause: %w", err)
				}
				var activeCount, causeCount, corruptCount int
				if err := tx.QueryRowContext(ctx, `
					SELECT
					 (SELECT COUNT(*) FROM health_providers WHERE active = TRUE),
					 (SELECT COUNT(*) FROM health_gap_provider_causes cause
					  JOIN health_providers provider
					    ON provider.id = cause.provider_id AND provider.active = TRUE
					   AND provider.current_generation = cause.provider_generation
					   AND provider.activation_epoch = cause.provider_activation_epoch
					  WHERE cause.gap_id = ?),
					 (SELECT COUNT(*) FROM health_gap_provider_causes cause
					  JOIN health_providers provider
					    ON provider.id = cause.provider_id AND provider.active = TRUE
					   AND provider.current_generation = cause.provider_generation
					   AND provider.activation_epoch = cause.provider_activation_epoch
					  WHERE cause.gap_id = ? AND cause.cause = 'corrupt')
				`, gap.ID, gap.ID).Scan(
					&activeCount, &causeCount, &corruptCount,
				); err != nil {
					return fmt.Errorf("derive provider activation known-gap kind: %w", err)
				}
				kind := GapKindProvisional
				var confirmedAt, nextRevalidationAt any
				if activeCount > 0 && activeCount == causeCount {
					kind = GapKindConfirmedAbsent
					if corruptCount > 0 {
						kind = GapKindConfirmedUnusable
					}
					var latestConfirmedAt time.Time
					if err := tx.QueryRowContext(ctx, `
						SELECT cause.confirmed_at
						FROM health_gap_provider_causes cause
						JOIN health_providers provider
						  ON provider.id = cause.provider_id AND provider.active = TRUE
						 AND provider.current_generation = cause.provider_generation
						 AND provider.activation_epoch = cause.provider_activation_epoch
						WHERE cause.gap_id = ? AND cause.confirmed_at IS NOT NULL
						ORDER BY cause.confirmed_at DESC LIMIT 1
					`, gap.ID).Scan(&latestConfirmedAt); err != nil {
						return fmt.Errorf("read provider activation confirmation time: %w", err)
					}
					confirmationTime := &latestConfirmedAt
					if gap.ConfirmedAt != nil {
						confirmationTime = gap.ConfirmedAt
					}
					if confirmationTime == nil {
						return fmt.Errorf("complete provider activation gap lacks a confirmation time")
					}
					confirmedAt = confirmationTime.UTC()
					if gap.Status == GapStatusActive {
						if gap.NextRevalidationAt != nil {
							nextRevalidationAt = gap.NextRevalidationAt.UTC()
						} else {
							nextRevalidationAt = confirmationTime.UTC().Add(24 * time.Hour)
						}
					}
				}
				if nextRevalidationAt == nil {
					if _, err := tx.ExecContext(ctx, `
						UPDATE health_gap_ranges
						SET kind = ?, confirmed_at = COALESCE(confirmed_at, ?)
						WHERE id = ?
					`, kind, confirmedAt, gap.ID); err != nil {
						return fmt.Errorf("rederive dormant provider activation known-gap kind: %w", err)
					}
				} else if _, err := tx.ExecContext(ctx, `
					UPDATE health_gap_ranges
					SET kind = ?, confirmed_at = COALESCE(confirmed_at, ?),
					    next_revalidation_at = COALESCE(next_revalidation_at, ?)
					WHERE id = ?
				`, kind, confirmedAt, nextRevalidationAt, gap.ID); err != nil {
					return fmt.Errorf("rederive provider activation known-gap kind: %w", err)
				}
				conclusive = true
			}
		}

		status := HealthRunFailed
		lastError := any("inconclusive provider activation gap observation")
		if conclusive {
			status = HealthRunCompleted
			lastError = nil
		}
		progressAssignment := ""
		if conclusive {
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
			return fmt.Errorf("finish provider activation gap run: %w", err)
		}
		if rows, _ := finished.RowsAffected(); rows != 1 {
			return ErrStaleHealthLease
		}
		retired, err := tx.ExecContext(ctx, `
			UPDATE health_run_schedule SET active = FALSE, updated_at = ?
			WHERE run_id = ? AND active = TRUE
		`, at, runID)
		if err != nil {
			return fmt.Errorf("retire provider activation gap schedule: %w", err)
		}
		if rows, _ := retired.RowsAffected(); rows != 1 {
			return ErrStaleHealthSchedule
		}
		return nil
	})
	return conclusive, err
}
