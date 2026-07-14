package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func gapRevalidationStepFromTrigger(trigger string) (int, bool) {
	const prefix = "gap_revalidation_"
	if !strings.HasPrefix(trigger, prefix) {
		return 0, false
	}
	step, err := strconv.Atoi(strings.TrimPrefix(trigger, prefix))
	return step, err == nil && step >= 0 && step < 4
}

// FinalizeGapRevalidation derives the milestone result from committed chunks
// while holding the run fence, schedule target, gap episode, and active
// provider-set checks in one transaction. Callers cannot promote a temporary
// observation by supplying a boolean outcome.
func (r *HealthStateRepository) FinalizeGapRevalidation(
	ctx context.Context,
	runID, owner string,
	fencingToken int64,
	at time.Time,
) (*GapRevalidationFinalization, error) {
	if runID == "" || strings.TrimSpace(owner) == "" || fencingToken <= 0 || at.IsZero() {
		return nil, fmt.Errorf("run, lease owner, fencing token, and finalization time are required")
	}
	at = at.UTC()
	var finalized GapRevalidationFinalization
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
			return fmt.Errorf("verify gap revalidation fence: %w", err)
		}
		leaseCheckAt := r.now().UTC()
		if !leaseExpiresAt.After(leaseCheckAt) {
			return ErrStaleHealthLease
		}
		expectedStep, ok := gapRevalidationStepFromTrigger(trigger)
		if !ok {
			return fmt.Errorf("run is not a gap revalidation")
		}
		var scheduleActive bool
		var targetGapID *string
		if err := tx.QueryRowContext(ctx, `
			UPDATE health_run_schedule SET active = active
			WHERE run_id = ?
			RETURNING active, target_gap_id
		`, runID).Scan(&scheduleActive, &targetGapID); err != nil {
			return fmt.Errorf("read gap revalidation schedule: %w", err)
		}
		if !scheduleActive || targetGapID == nil || *targetGapID == "" {
			return ErrStaleHealthSchedule
		}

		var gap HealthGapRange
		if err := scanHealthGapRange(tx.QueryRowContext(ctx, `
			UPDATE health_gap_ranges SET status = status
			WHERE id = ?
			RETURNING id, file_revision_id, kind, start_segment, segment_count, episode,
			          status, created_at, confirmed_at, cleared_at, revalidation_step,
			          next_revalidation_at, last_revalidation_at
		`, *targetGapID), &gap); err != nil {
			return fmt.Errorf("read gap revalidation target: %w", err)
		}
		if gap.FileRevisionID != revisionID || gap.Status != GapStatusActive ||
			gap.RevalidationStep != expectedStep || gap.ConfirmedAt == nil {
			return ErrStaleHealthSchedule
		}

		var activeCount, snapshotCount, matchingCount int
		if err := tx.QueryRowContext(ctx, `
			SELECT
			  (SELECT COUNT(*) FROM health_providers WHERE active = TRUE),
			  (SELECT COUNT(*) FROM health_provider_snapshot_entries WHERE snapshot_id = ?),
			  (SELECT COUNT(*)
			   FROM health_provider_snapshot_entries entry
			   JOIN health_providers provider
			     ON provider.id = entry.provider_id AND provider.active = TRUE
			    AND provider.current_generation = entry.provider_generation
			    AND provider.activation_epoch = entry.provider_activation_epoch
			   WHERE entry.snapshot_id = ?)
		`, snapshotID, snapshotID).Scan(&activeCount, &snapshotCount, &matchingCount); err != nil {
			return fmt.Errorf("verify revalidation provider snapshot: %w", err)
		}
		if activeCount == 0 || activeCount != snapshotCount || snapshotCount != matchingCount {
			return ErrStaleHealthSchedule
		}

		type providerKey struct {
			id                string
			generation, epoch int64
		}
		causes := make(map[providerKey]GapCause, activeCount)
		rows, err := tx.QueryContext(ctx, `
			SELECT cause.provider_id, cause.provider_generation,
			       cause.provider_activation_epoch, cause.cause
			FROM health_gap_provider_causes cause
			JOIN health_providers provider
			  ON provider.id = cause.provider_id AND provider.active = TRUE
			 AND provider.current_generation = cause.provider_generation
			 AND provider.activation_epoch = cause.provider_activation_epoch
			WHERE cause.gap_id = ?
		`, gap.ID)
		if err != nil {
			return fmt.Errorf("read current gap revalidation causes: %w", err)
		}
		for rows.Next() {
			var key providerKey
			var cause GapCause
			if err := rows.Scan(&key.id, &key.generation, &key.epoch, &cause); err != nil {
				rows.Close()
				return fmt.Errorf("scan current gap revalidation cause: %w", err)
			}
			causes[key] = cause
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close current gap revalidation causes: %w", err)
		}
		if len(causes) != activeCount {
			return ErrStaleHealthSchedule
		}

		type observedKey struct {
			providerKey
			position int64
		}
		type observedValue struct {
			kind    HealthObservationKind
			outcome string
			fresh   bool
		}
		observed := make(map[observedKey]observedValue)
		rows, err = tx.QueryContext(ctx, `
			SELECT provider_id, provider_generation, provider_activation_epoch,
			       observation_kind, fresh_transport, segment_start, segment_count, tested_bitmap,
			       present_bitmap, absent_bitmap, corrupt_bitmap,
			       temporary_bitmap, inconclusive_bitmap
			FROM health_run_chunks
			WHERE run_id = ?
			ORDER BY committed_at, id
		`, runID)
		if err != nil {
			return fmt.Errorf("read gap revalidation chunks: %w", err)
		}
		for rows.Next() {
			var key providerKey
			var kind HealthObservationKind
			var fresh bool
			var start, count int64
			var tested, present, absent, corrupt, temporary, inconclusive []byte
			if err := rows.Scan(
				&key.id, &key.generation, &key.epoch, &kind, &fresh, &start, &count,
				&tested, &present, &absent, &corrupt, &temporary, &inconclusive,
			); err != nil {
				rows.Close()
				return fmt.Errorf("scan gap revalidation chunk: %w", err)
			}
			for relative := int64(0); relative < count; relative++ {
				position := start + relative
				if position < gap.StartSegment || position >= gap.StartSegment+gap.SegmentCount ||
					!bitmapSet(tested, relative) {
					continue
				}
				outcome := "inconclusive"
				switch {
				case bitmapSet(present, relative):
					outcome = "present"
				case bitmapSet(absent, relative):
					outcome = "absent"
				case bitmapSet(corrupt, relative):
					outcome = "corrupt"
				case bitmapSet(temporary, relative):
					outcome = "temporary"
				case bitmapSet(inconclusive, relative):
					outcome = "inconclusive"
				}
				observed[observedKey{providerKey: key, position: position}] = observedValue{
					kind: kind, outcome: outcome, fresh: fresh,
				}
			}
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close gap revalidation chunks: %w", err)
		}

		conclusive := true
		replacementCauses := make(map[providerKey]GapCause, len(causes))
		for provider, cause := range causes {
			replacement := GapCauseAbsent
			for position := gap.StartSegment; position < gap.StartSegment+gap.SegmentCount; position++ {
				value, ok := observed[observedKey{providerKey: provider, position: position}]
				if !ok {
					conclusive = false
					continue
				}
				if value.kind == HealthObservationValidatedBody && value.outcome == "present" {
					return fmt.Errorf("validated BODY recovery must clear the gap before finalization")
				}
				if value.kind == HealthObservationValidatedBody && !value.fresh {
					conclusive = false
					continue
				}
				switch cause {
				case GapCauseAbsent:
					if value.outcome == "corrupt" && value.kind == HealthObservationValidatedBody {
						replacement = GapCauseCorrupt
					} else if value.outcome != "absent" {
						conclusive = false
					}
				case GapCauseCorrupt:
					if value.kind != HealthObservationValidatedBody ||
						(value.outcome != "absent" && value.outcome != "corrupt") {
						conclusive = false
					} else if value.outcome == "corrupt" {
						replacement = GapCauseCorrupt
					}
				default:
					conclusive = false
				}
			}
			replacementCauses[provider] = replacement
		}

		status := HealthRunFailed
		lastError := "inconclusive revalidation"
		nextAt := at.Add(time.Hour)
		newStep := gap.RevalidationStep
		if conclusive {
			status = HealthRunCompleted
			lastError = ""
			newStep++
			finalized.Advanced = true
			newKind := GapKindConfirmedAbsent
			for provider, replacement := range replacementCauses {
				if replacement == GapCauseCorrupt {
					newKind = GapKindConfirmedUnusable
				}
				updated, err := tx.ExecContext(ctx, `
					UPDATE health_gap_provider_causes
					SET cause = ?, confirmation_count = CASE
					      WHEN confirmation_count < 2 THEN 2 ELSE confirmation_count END,
					    confirmed_at = ?
					WHERE gap_id = ? AND provider_id = ? AND provider_generation = ?
					  AND provider_activation_epoch = ?
				`, replacement, at, gap.ID, provider.id, provider.generation, provider.epoch)
				if err != nil {
					return fmt.Errorf("replace gap revalidation provider cause: %w", err)
				}
				if rows, err := updated.RowsAffected(); err != nil {
					return fmt.Errorf("read replaced gap revalidation provider cause: %w", err)
				} else if rows != 1 {
					return ErrStaleHealthSchedule
				}
			}
			if newStep >= 4 {
				finalized.Dormant = true
				updated, err := tx.ExecContext(ctx, `
					UPDATE health_gap_ranges
					SET status = 'dormant', kind = ?, revalidation_step = 4,
					    next_revalidation_at = NULL, last_revalidation_at = ?
					WHERE id = ? AND status = 'active' AND revalidation_step = ?
				`, newKind, at, gap.ID, expectedStep)
				if err != nil {
					return fmt.Errorf("dormant final gap revalidation: %w", err)
				}
				if rows, err := updated.RowsAffected(); err != nil {
					return fmt.Errorf("read dormant final gap revalidation result: %w", err)
				} else if rows != 1 {
					return ErrStaleHealthSchedule
				}
			} else {
				ages := [...]time.Duration{24 * time.Hour, 3 * 24 * time.Hour, 7 * 24 * time.Hour, 14 * 24 * time.Hour}
				nextAt = gap.ConfirmedAt.UTC().Add(ages[newStep])
				updated, err := tx.ExecContext(ctx, `
					UPDATE health_gap_ranges
					SET kind = ?, revalidation_step = ?, next_revalidation_at = ?, last_revalidation_at = ?
					WHERE id = ? AND status = 'active' AND revalidation_step = ?
				`, newKind, newStep, nextAt, at, gap.ID, expectedStep)
				if err != nil {
					return fmt.Errorf("advance gap revalidation milestone: %w", err)
				}
				if rows, err := updated.RowsAffected(); err != nil {
					return fmt.Errorf("read advanced gap revalidation result: %w", err)
				} else if rows != 1 {
					return ErrStaleHealthSchedule
				}
			}
		} else {
			updated, err := tx.ExecContext(ctx, `
				UPDATE health_gap_ranges
				SET next_revalidation_at = ?, last_revalidation_at = ?
				WHERE id = ? AND status = 'active' AND revalidation_step = ?
			`, nextAt, at, gap.ID, expectedStep)
			if err != nil {
				return fmt.Errorf("defer inconclusive gap revalidation: %w", err)
			}
			if rows, err := updated.RowsAffected(); err != nil {
				return fmt.Errorf("read deferred gap revalidation result: %w", err)
			} else if rows != 1 {
				return ErrStaleHealthSchedule
			}
		}
		progressAssignment := ""
		if conclusive {
			progressAssignment = ", resolved_segments = total_segments"
		}
		updatedRun, err := tx.ExecContext(ctx, `
			UPDATE health_runs
			SET status = ?`+progressAssignment+`, lease_owner = NULL, lease_expires_at = NULL,
			    last_error = ?, updated_at = ?, completed_at = ?
			WHERE id = ? AND status = 'running' AND lease_owner = ? AND fencing_token = ?
			  AND lease_expires_at > ?
		`, status, nullableString(lastError), at, at, runID, owner, fencingToken, leaseCheckAt)
		if err != nil {
			return fmt.Errorf("finish gap revalidation run: %w", err)
		}
		if rows, err := updatedRun.RowsAffected(); err != nil {
			return fmt.Errorf("read finished gap revalidation run result: %w", err)
		} else if rows != 1 {
			return ErrStaleHealthLease
		}
		retired, err := tx.ExecContext(ctx, `
			UPDATE health_run_schedule SET active = FALSE, updated_at = ?
			WHERE run_id = ? AND active = TRUE
		`, at, runID)
		if err != nil {
			return fmt.Errorf("retire gap revalidation schedule: %w", err)
		}
		if rows, err := retired.RowsAffected(); err != nil {
			return fmt.Errorf("read retired gap revalidation schedule result: %w", err)
		} else if rows != 1 {
			return ErrStaleHealthSchedule
		}
		if err := scanHealthGapRange(tx.QueryRowContext(ctx,
			healthGapRangeSelect+` WHERE id = ?`, gap.ID), &finalized.Gap); err != nil {
			return fmt.Errorf("read finalized gap revalidation: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	causes, err := r.listGapProviderCauses(ctx, finalized.Gap.ID)
	if err != nil {
		return nil, err
	}
	finalized.Gap.Causes = causes
	return &finalized, nil
}
