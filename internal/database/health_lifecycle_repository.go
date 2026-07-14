package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

func validHealthRunPriority(priority HealthRunPriority) bool {
	return priority >= HealthRunPriorityLow && priority <= HealthRunPriorityHigh
}

func normalizeScheduledHealthRunSpec(spec ScheduledHealthRunSpec, now time.Time) (ScheduledHealthRunSpec, error) {
	spec.DedupeKey = strings.TrimSpace(spec.DedupeKey)
	spec.TargetProviderID = strings.TrimSpace(spec.TargetProviderID)
	spec.TargetGapID = strings.TrimSpace(spec.TargetGapID)
	if spec.DedupeKey == "" {
		return ScheduledHealthRunSpec{}, fmt.Errorf("health run schedule dedupe key is required")
	}
	if !validHealthRunPriority(spec.Priority) {
		return ScheduledHealthRunSpec{}, fmt.Errorf("invalid health run priority %d", spec.Priority)
	}
	if spec.NotBefore.IsZero() {
		spec.NotBefore = now
	} else {
		spec.NotBefore = spec.NotBefore.UTC()
	}
	if spec.Run.FileRevisionID == "" || spec.Run.ProviderSnapshotID == "" ||
		spec.Run.Trigger == "" || spec.Run.Mode == "" {
		return ScheduledHealthRunSpec{}, fmt.Errorf("revision, provider snapshot, trigger, and mode are required")
	}
	if spec.Run.TotalSegments < 0 {
		return ScheduledHealthRunSpec{}, fmt.Errorf("total segments must be non-negative")
	}
	if spec.Run.ID == "" {
		spec.Run.ID = uuid.NewString()
	}
	if spec.Run.CreatedAt.IsZero() {
		spec.Run.CreatedAt = now
	} else {
		spec.Run.CreatedAt = spec.Run.CreatedAt.UTC()
	}
	providerTargeted := spec.TargetProviderID != "" || spec.TargetProviderGeneration != 0 ||
		spec.TargetProviderActivationEpoch != 0
	if providerTargeted && (spec.TargetProviderID == "" || spec.TargetProviderGeneration <= 0 ||
		spec.TargetProviderActivationEpoch <= 0) {
		return ScheduledHealthRunSpec{}, fmt.Errorf("target provider ID, generation, and activation epoch must be supplied together")
	}
	return spec, nil
}

func scanHealthRunSchedule(row rowScanner, schedule *HealthRunSchedule) error {
	var providerID, gapID sql.NullString
	var providerGeneration, providerActivationEpoch sql.NullInt64
	if err := row.Scan(
		&schedule.RunID, &schedule.DedupeKey, &schedule.Active,
		&providerID, &providerGeneration, &providerActivationEpoch, &gapID,
		&schedule.Priority, &schedule.NotBefore, &schedule.CreatedAt, &schedule.UpdatedAt,
	); err != nil {
		return err
	}
	if providerID.Valid {
		schedule.TargetProviderID = providerID.String
	}
	if providerGeneration.Valid {
		schedule.TargetProviderGeneration = providerGeneration.Int64
	}
	if providerActivationEpoch.Valid {
		schedule.TargetProviderActivationEpoch = providerActivationEpoch.Int64
	}
	if gapID.Valid {
		schedule.TargetGapID = gapID.String
	}
	return nil
}

const healthRunScheduleSelect = `
	SELECT run_id, dedupe_key, active, target_provider_id,
	       target_provider_generation, target_provider_activation_epoch,
	       target_gap_id, priority, not_before, created_at, updated_at
	FROM health_run_schedule
`

func (r *HealthStateRepository) GetHealthRunSchedule(ctx context.Context, runID string) (*HealthRunSchedule, error) {
	var schedule HealthRunSchedule
	err := scanHealthRunSchedule(r.db.QueryRowContext(ctx,
		healthRunScheduleSelect+` WHERE run_id = ?`, runID), &schedule)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get health run schedule: %w", err)
	}
	return &schedule, nil
}

// GetActiveScheduledHealthRun returns the durable winner for a logical
// schedule key. It is used by idempotent explicit requests whose callers may
// capture equivalent provider snapshots under different snapshot row IDs.
func (r *HealthStateRepository) GetActiveScheduledHealthRun(
	ctx context.Context,
	dedupeKey string,
) (*HealthRun, error) {
	dedupeKey = strings.TrimSpace(dedupeKey)
	if dedupeKey == "" {
		return nil, fmt.Errorf("health run schedule dedupe key is required")
	}
	var run HealthRun
	err := scanHealthRun(r.db.QueryRowContext(ctx, healthRunSelect+`
		WHERE health_runs.id = (
		  SELECT schedule.run_id FROM health_run_schedule schedule
		  WHERE schedule.dedupe_key = ? AND schedule.active = TRUE
		  LIMIT 1
		)
	`, dedupeKey), &run)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get active scheduled health run: %w", err)
	}
	return &run, nil
}

// GetActiveObservationHealthRunForFile returns one currently actionable
// non-import observation for the active revision of a file-health row. The
// active schedule and status predicates avoid scanning terminal run history;
// a running owner wins over queued siblings, followed by scheduler priority.
func (r *HealthStateRepository) GetActiveObservationHealthRunForFile(
	ctx context.Context,
	fileHealthID int64,
) (*HealthRun, error) {
	if fileHealthID <= 0 {
		return nil, fmt.Errorf("positive file health identifier is required")
	}
	var run HealthRun
	err := scanHealthRun(r.db.QueryRowContext(ctx, healthRunSelect+`
		WHERE health_runs.id = (
		  SELECT candidate.id
		  FROM health_file_revisions revision
		  JOIN health_runs candidate ON candidate.file_revision_id = revision.id
		  JOIN health_run_schedule schedule ON schedule.run_id = candidate.id
		  WHERE revision.file_health_id = ? AND revision.active = TRUE
		    AND schedule.active = TRUE
		    AND candidate.mode = 'observation' AND candidate.trigger <> 'import'
		    AND candidate.status IN ('pending', 'running', 'paused')
		  ORDER BY CASE WHEN candidate.status = 'running' THEN 0 ELSE 1 END,
		           schedule.priority DESC, schedule.not_before,
		           schedule.created_at, candidate.id
		  LIMIT 1
		)
	`, fileHealthID), &run)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get active file observation run: %w", err)
	}
	return &run, nil
}

// EnsureScheduledHealthRun creates one run for an active dedupe key or
// monotonically promotes the existing compatible run.
func (r *HealthStateRepository) EnsureScheduledHealthRun(
	ctx context.Context,
	spec ScheduledHealthRunSpec,
) (*HealthRun, bool, error) {
	now := r.now().UTC()
	spec, err := normalizeScheduledHealthRunSpec(spec, now)
	if err != nil {
		return nil, false, err
	}
	var lastErr error
	for attempt := 0; attempt < 16; attempt++ {
		run, created, err := r.ensureScheduledHealthRunOnce(ctx, spec, now)
		if err == nil {
			return run, created, nil
		}
		lastErr = err
		if !retryableHealthScheduleConflict(err) {
			return nil, false, err
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 2 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, false, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, false, fmt.Errorf("converge scheduled health run: %w", lastErr)
}

func retryableHealthScheduleConflict(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "unique") || strings.Contains(message, "duplicate key") ||
		strings.Contains(message, "database is locked") ||
		strings.Contains(message, "database table is locked") ||
		strings.Contains(message, "database is busy") ||
		strings.Contains(message, "serialization") || strings.Contains(message, "deadlock")
}

func (r *HealthStateRepository) ensureScheduledHealthRunOnce(
	ctx context.Context,
	spec ScheduledHealthRunSpec,
	now time.Time,
) (*HealthRun, bool, error) {
	var result HealthRun
	created := false
	err := r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		var existingRunID, revisionID, snapshotID, trigger, mode string
		var totalSegments int64
		var targetProviderID, targetGapID sql.NullString
		var targetGeneration, targetActivationEpoch sql.NullInt64
		err := tx.QueryRowContext(ctx, `
			SELECT r.id, r.file_revision_id, r.provider_snapshot_id, r.trigger, r.mode,
			       r.total_segments, s.target_provider_id, s.target_provider_generation,
			       s.target_provider_activation_epoch, s.target_gap_id
			FROM health_run_schedule s
			JOIN health_runs r ON r.id = s.run_id
			WHERE s.dedupe_key = ? AND s.active = TRUE
		`, spec.DedupeKey).Scan(
			&existingRunID, &revisionID, &snapshotID, &trigger, &mode, &totalSegments,
			&targetProviderID, &targetGeneration, &targetActivationEpoch, &targetGapID,
		)
		if err == nil {
			if revisionID != spec.Run.FileRevisionID || snapshotID != spec.Run.ProviderSnapshotID ||
				trigger != spec.Run.Trigger || mode != spec.Run.Mode || totalSegments != spec.Run.TotalSegments ||
				nullStringValue(targetProviderID) != spec.TargetProviderID ||
				nullInt64Value(targetGeneration) != spec.TargetProviderGeneration ||
				nullInt64Value(targetActivationEpoch) != spec.TargetProviderActivationEpoch ||
				nullStringValue(targetGapID) != spec.TargetGapID {
				return fmt.Errorf("active health run dedupe key is bound to a different target")
			}
			_, err = tx.ExecContext(ctx, `
				UPDATE health_run_schedule
				SET priority = CASE WHEN priority < ? THEN ? ELSE priority END,
				    not_before = CASE WHEN not_before > ? THEN ? ELSE not_before END,
				    updated_at = ?
				WHERE run_id = ? AND active = TRUE
			`, spec.Priority, spec.Priority, spec.NotBefore, spec.NotBefore, now, existingRunID)
			if err != nil {
				return fmt.Errorf("promote scheduled health run: %w", err)
			}
			return scanHealthRun(tx.QueryRowContext(ctx,
				healthRunSelect+` WHERE id = ?`, existingRunID), &result)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("find active scheduled health run: %w", err)
		}

		var revisionSegments int64
		if err := tx.QueryRowContext(ctx, `
			SELECT segment_count FROM health_file_revisions WHERE id = ?
		`, spec.Run.FileRevisionID).Scan(&revisionSegments); err != nil {
			return fmt.Errorf("read scheduled health run file revision: %w", err)
		}
		if spec.TargetGapID == "" {
			sparseTarget := spec.TargetProviderID != "" || spec.Run.Trigger == "health_pending"
			if sparseTarget {
				if spec.Run.TotalSegments <= 0 || spec.Run.TotalSegments > revisionSegments {
					return fmt.Errorf("targeted health run work total exceeds file revision bounds")
				}
			} else if revisionSegments != spec.Run.TotalSegments {
				return fmt.Errorf("health run total does not match file revision segment count")
			}
		}
		if spec.TargetProviderID != "" {
			var snapshotEntry int
			err := tx.QueryRowContext(ctx, `
				SELECT 1
				FROM health_provider_snapshot_entries
				WHERE snapshot_id = ? AND provider_id = ? AND provider_generation = ?
				  AND provider_activation_epoch = ?
			`, spec.Run.ProviderSnapshotID, spec.TargetProviderID,
				spec.TargetProviderGeneration, spec.TargetProviderActivationEpoch).Scan(&snapshotEntry)
			if errors.Is(err, sql.ErrNoRows) {
				return ErrProviderSnapshotMismatch
			}
			if err != nil {
				return fmt.Errorf("verify scheduled provider target: %w", err)
			}
		}
		if spec.TargetGapID != "" {
			var gapRevisionID string
			var gapStatus GapStatus
			var gapSegments int64
			if err := tx.QueryRowContext(ctx, `
					SELECT file_revision_id, status, segment_count FROM health_gap_ranges WHERE id = ?
				`, spec.TargetGapID).Scan(&gapRevisionID, &gapStatus, &gapSegments); err != nil {
				return fmt.Errorf("verify scheduled gap target: %w", err)
			}
			if gapRevisionID != spec.Run.FileRevisionID {
				return fmt.Errorf("scheduled gap target belongs to a different file revision")
			}
			if spec.Run.TotalSegments != gapSegments {
				return fmt.Errorf("targeted health run total does not match gap work size")
			}
			allowDormant := strings.HasPrefix(spec.Run.Trigger, "provider_activation") || spec.Run.Trigger == "manual"
			if gapStatus != GapStatusActive && (!allowDormant || gapStatus != GapStatusDormant) {
				return fmt.Errorf("scheduled gap target is not eligible")
			}
		}

		_, err = tx.ExecContext(ctx, `
			INSERT INTO health_runs
				(id, file_revision_id, provider_snapshot_id, trigger, mode, status,
				 total_segments, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?)
		`, spec.Run.ID, spec.Run.FileRevisionID, spec.Run.ProviderSnapshotID,
			spec.Run.Trigger, spec.Run.Mode, spec.Run.TotalSegments,
			spec.Run.CreatedAt, spec.Run.CreatedAt)
		if err != nil {
			return fmt.Errorf("create scheduled health run: %w", err)
		}
		var providerID, gapID any
		var providerGeneration, providerActivationEpoch any
		if spec.TargetProviderID != "" {
			providerID = spec.TargetProviderID
			providerGeneration = spec.TargetProviderGeneration
			providerActivationEpoch = spec.TargetProviderActivationEpoch
		}
		if spec.TargetGapID != "" {
			gapID = spec.TargetGapID
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO health_run_schedule
				(run_id, dedupe_key, active, target_provider_id,
				 target_provider_generation, target_provider_activation_epoch,
				 target_gap_id, priority, not_before, created_at, updated_at)
			VALUES (?, ?, TRUE, ?, ?, ?, ?, ?, ?, ?, ?)
		`, spec.Run.ID, spec.DedupeKey, providerID, providerGeneration,
			providerActivationEpoch, gapID, spec.Priority, spec.NotBefore,
			spec.Run.CreatedAt, spec.Run.CreatedAt)
		if err != nil {
			return fmt.Errorf("create health run schedule: %w", err)
		}
		created = true
		return scanHealthRun(tx.QueryRowContext(ctx,
			healthRunSelect+` WHERE id = ?`, spec.Run.ID), &result)
	})
	if err != nil {
		return nil, false, err
	}
	return &result, created, nil
}

func nullStringValue(value sql.NullString) string {
	if value.Valid {
		return value.String
	}
	return ""
}

func nullInt64Value(value sql.NullInt64) int64 {
	if value.Valid {
		return value.Int64
	}
	return 0
}

// ClaimDueHealthRun leases one due, unpaused run. Expired running leases are
// eligible, and every acquisition advances the fencing token.
func (r *HealthStateRepository) ClaimDueHealthRun(
	ctx context.Context,
	owner string,
	ttl time.Duration,
) (*HealthRun, error) {
	return r.claimDueHealthRun(ctx, owner, ttl, false)
}

// ClaimDueObservationHealthRun is the background-health claim domain. Import
// admission owns trigger=import schedules and must never have its fence
// advanced by the ordinary observation service.
func (r *HealthStateRepository) ClaimDueObservationHealthRun(
	ctx context.Context,
	owner string,
	ttl time.Duration,
) (*HealthRun, error) {
	return r.claimDueHealthRun(ctx, owner, ttl, true)
}

// AbandonStaleObservationRun atomically retires a leased background run whose
// frozen provider activation membership no longer matches current config. A
// fresh ordinary scheduler pass is armed immediately; order/role-only changes
// do not abandon evidence.
func (r *HealthStateRepository) AbandonStaleObservationRun(
	ctx context.Context,
	runID, owner string,
	fencingToken int64,
	at time.Time,
) (bool, error) {
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(owner) == "" ||
		fencingToken <= 0 || at.IsZero() {
		return false, fmt.Errorf("run, owner, fence, and abandonment time are required")
	}
	at = at.UTC()
	abandoned := false
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
			return fmt.Errorf("lock stale observation run: %w", err)
		}
		if !leaseExpiresAt.After(r.now().UTC()) {
			return ErrStaleHealthLease
		}
		if trigger == "import" {
			return ErrImportRunControl
		}
		current, err := providerSnapshotMembershipMatchesCurrentTx(ctx, tx, snapshotID)
		if err != nil {
			return err
		}
		if current {
			return nil
		}
		updated, err := tx.ExecContext(ctx, `
			UPDATE health_runs
			SET status = 'canceled', lease_owner = NULL, lease_expires_at = NULL,
			    cancel_requested = TRUE, last_error = 'provider set changed',
			    updated_at = ?, completed_at = ?
			WHERE id = ? AND status = 'running' AND lease_owner = ? AND fencing_token = ?
		`, at, at, runID, owner, fencingToken)
		if err != nil {
			return fmt.Errorf("abandon stale observation run: %w", err)
		}
		if rows, err := updated.RowsAffected(); err != nil {
			return fmt.Errorf("read stale observation abandonment result: %w", err)
		} else if rows != 1 {
			return ErrStaleHealthLease
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE health_run_schedule SET active = FALSE, updated_at = ?
			WHERE run_id = ? AND active = TRUE
		`, at, runID); err != nil {
			return fmt.Errorf("retire stale observation schedule: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE file_health
			SET status = 'pending', scheduled_check_at = CASE
			      WHEN scheduled_check_at IS NULL OR scheduled_check_at > ? THEN ?
			      ELSE scheduled_check_at END,
			    updated_at = ?
			WHERE id = (SELECT file_health_id FROM health_file_revisions WHERE id = ?)
		`, at, at, at, revisionID); err != nil {
			return fmt.Errorf("arm replacement observation run: %w", err)
		}
		abandoned = true
		return nil
	})
	return abandoned, err
}

func (r *HealthStateRepository) claimDueHealthRun(
	ctx context.Context,
	owner string,
	ttl time.Duration,
	observationOnly bool,
) (*HealthRun, error) {
	owner = strings.TrimSpace(owner)
	if owner == "" || ttl <= 0 {
		return nil, fmt.Errorf("lease owner and positive TTL are required")
	}
	now := r.now().UTC()
	expires := now.Add(ttl)
	if err := r.retireStaleScheduledHealthRuns(ctx, now); err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 24; attempt++ {
		run, err := r.claimDueHealthRunOnce(ctx, owner, now, expires, observationOnly)
		if err == nil && run != nil {
			return run, nil
		}
		if err != nil && !retryableHealthScheduleConflict(err) {
			return nil, err
		}
		if err == nil {
			var due int
			domainClause := ""
			if observationOnly {
				domainClause = " AND r.trigger <> 'import'"
			}
			checkErr := r.db.QueryRowContext(ctx, `
				SELECT COUNT(*)
				FROM health_run_schedule s
				JOIN health_runs r ON r.id = s.run_id
				WHERE s.active = TRUE AND s.not_before <= ?
				  AND r.status IN ('pending', 'running', 'paused')
				  AND r.pause_requested = FALSE AND r.cancel_requested = FALSE
				  AND (r.lease_owner IS NULL OR r.lease_expires_at <= ?)
				  AND NOT EXISTS (
				    SELECT 1 FROM health_runs active_run
				    WHERE active_run.file_revision_id = r.file_revision_id
				      AND active_run.id <> r.id AND active_run.status = 'running'
				      AND active_run.lease_owner IS NOT NULL
				      AND active_run.lease_expires_at > ?
				  )
			`+domainClause, now, now, now).Scan(&due)
			if checkErr == nil && due == 0 {
				return nil, nil
			}
		}
		timer := time.NewTimer(time.Duration(attempt+1) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, nil
}

func (r *HealthStateRepository) retireStaleScheduledHealthRuns(ctx context.Context, at time.Time) error {
	return r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		rows, err := tx.QueryContext(ctx, `
			UPDATE health_run_schedule
			SET active = FALSE, updated_at = ?
			WHERE active = TRUE AND (
			  (target_gap_id IS NOT NULL AND NOT EXISTS (
			    SELECT 1
			    FROM health_gap_ranges g
			    JOIN health_runs target_run ON target_run.id = health_run_schedule.run_id
			    WHERE g.id = health_run_schedule.target_gap_id
			      AND (g.status = 'active' OR (
			        g.status = 'dormant' AND (
			          target_run.trigger LIKE 'provider_activation%' OR target_run.trigger = 'manual'
			        )
			      ))
			  ))
			  OR
			  (target_provider_id IS NOT NULL AND NOT EXISTS (
			    SELECT 1 FROM health_providers p
			    WHERE p.id = health_run_schedule.target_provider_id AND p.active = TRUE
			      AND p.current_generation = health_run_schedule.target_provider_generation
			      AND p.activation_epoch = health_run_schedule.target_provider_activation_epoch
			  ))
			)
			RETURNING run_id
		`, at)
		if err != nil {
			return fmt.Errorf("retire stale scheduled health runs: %w", err)
		}
		var runIDs []string
		for rows.Next() {
			var runID string
			if err := rows.Scan(&runID); err != nil {
				rows.Close()
				return fmt.Errorf("scan retired stale health run: %w", err)
			}
			runIDs = append(runIDs, runID)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close retired stale health runs: %w", err)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate retired stale health runs: %w", err)
		}
		return terminalizeStaleTargetedHealthRunsTx(ctx, tx, runIDs, "", at)
	})
}

func terminalizeStaleTargetedHealthRunsTx(
	ctx context.Context,
	tx *dialectAwareTx,
	runIDs []string,
	exceptRunID string,
	at time.Time,
) error {
	for _, runID := range runIDs {
		if runID == exceptRunID {
			continue
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE health_runs
			SET status = 'canceled', lease_owner = NULL, lease_expires_at = NULL,
			    cancel_requested = TRUE, last_error = 'stale health target',
			    updated_at = ?, completed_at = ?
			WHERE id = ? AND trigger <> 'import'
			  AND status IN ('pending', 'running', 'paused')
		`, at, at, runID); err != nil {
			return fmt.Errorf("terminalize stale targeted health run: %w", err)
		}
	}
	return nil
}

func (r *HealthStateRepository) claimDueHealthRunOnce(
	ctx context.Context,
	owner string,
	now, expires time.Time,
	observationOnly bool,
) (*HealthRun, error) {
	domainClause := ""
	if observationOnly {
		domainClause = " AND r.trigger <> 'import'"
	}
	query := `
		UPDATE health_runs
		SET lease_owner = ?, lease_expires_at = ?, fencing_token = fencing_token + 1,
		    status = 'running', started_at = COALESCE(started_at, ?), updated_at = ?
		WHERE id = (
			SELECT r.id
			FROM health_run_schedule s
			JOIN health_runs r ON r.id = s.run_id
			WHERE s.active = TRUE AND s.not_before <= ?
			  AND r.status IN ('pending', 'running', 'paused')
			  AND r.pause_requested = FALSE AND r.cancel_requested = FALSE
			  AND (r.lease_owner IS NULL OR r.lease_expires_at <= ?)
			  AND NOT EXISTS (
			    SELECT 1 FROM health_runs active_run
			    WHERE active_run.file_revision_id = r.file_revision_id
			      AND active_run.id <> r.id AND active_run.status = 'running'
			      AND active_run.lease_owner IS NOT NULL
			      AND active_run.lease_expires_at > ?
			  )
			  ` + domainClause + `
			ORDER BY s.priority DESC, s.not_before, s.created_at, r.id
			LIMIT 1
		)
		  AND status IN ('pending', 'running', 'paused')
		  AND pause_requested = FALSE AND cancel_requested = FALSE
		  AND (lease_owner IS NULL OR lease_expires_at <= ?)
		RETURNING id, file_revision_id, provider_snapshot_id, trigger, mode, status,
		          lease_owner, lease_expires_at, fencing_token, total_segments,
		          resolved_segments, provider_checks, missing_candidates, inconclusive_count,
		          stage, current_provider_id, current_provider_generation, cursor_segment,
		          pause_requested, cancel_requested, created_at, started_at, updated_at, completed_at,
		          COALESCE(last_error, '')
	`
	if r.dialect.IsPostgres() {
		query = `
			WITH candidate AS (
				SELECT r.id
				FROM health_run_schedule s
				JOIN health_runs r ON r.id = s.run_id
				JOIN health_file_revisions revision ON revision.id = r.file_revision_id
				WHERE s.active = TRUE AND s.not_before <= ?
				  AND r.status IN ('pending', 'running', 'paused')
				  AND r.pause_requested = FALSE AND r.cancel_requested = FALSE
				  AND (r.lease_owner IS NULL OR r.lease_expires_at <= ?)
				  AND NOT EXISTS (
				    SELECT 1 FROM health_runs active_run
				    WHERE active_run.file_revision_id = r.file_revision_id
				      AND active_run.id <> r.id AND active_run.status = 'running'
				      AND active_run.lease_owner IS NOT NULL
				      AND active_run.lease_expires_at > ?
				  )
				  ` + domainClause + `
				ORDER BY s.priority DESC, s.not_before, s.created_at, r.id
				FOR UPDATE OF revision SKIP LOCKED
				LIMIT 1
			)
			UPDATE health_runs AS r
			SET lease_owner = ?, lease_expires_at = ?, fencing_token = r.fencing_token + 1,
			    status = 'running', started_at = COALESCE(r.started_at, ?), updated_at = ?
			FROM candidate
			WHERE r.id = candidate.id
			  AND r.status IN ('pending', 'running', 'paused')
			  AND r.pause_requested = FALSE AND r.cancel_requested = FALSE
			  AND (r.lease_owner IS NULL OR r.lease_expires_at <= ?)
			RETURNING r.id, r.file_revision_id, r.provider_snapshot_id, r.trigger, r.mode, r.status,
			          r.lease_owner, r.lease_expires_at, r.fencing_token, r.total_segments,
			          r.resolved_segments, r.provider_checks, r.missing_candidates, r.inconclusive_count,
			          r.stage, r.current_provider_id, r.current_provider_generation, r.cursor_segment,
			          r.pause_requested, r.cancel_requested, r.created_at, r.started_at,
			          r.updated_at, r.completed_at, COALESCE(r.last_error, '')
		`
	}
	var run HealthRun
	var err error
	if r.dialect.IsPostgres() {
		err = r.withTransaction(ctx, func(tx *dialectAwareTx) error {
			return scanHealthRun(tx.QueryRowContext(ctx, query,
				now, now, now, owner, expires, now, now, now), &run)
		})
	} else {
		err = scanHealthRun(r.db.QueryRowContext(ctx, query,
			owner, expires, now, now, now, now, now, now), &run)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim due health run: %w", err)
	}
	return &run, nil
}

func (r *HealthStateRepository) RenewHealthRunLease(
	ctx context.Context,
	runID, owner string,
	fencingToken int64,
	ttl time.Duration,
) (*HealthRun, error) {
	if runID == "" || strings.TrimSpace(owner) == "" || fencingToken <= 0 || ttl <= 0 {
		return nil, fmt.Errorf("run ID, lease owner, fencing token, and positive TTL are required")
	}
	now := r.now().UTC()
	query := `
		UPDATE health_runs
		SET lease_expires_at = ?, updated_at = ?
		WHERE id = ? AND status = 'running' AND lease_owner = ? AND fencing_token = ?
		  AND lease_expires_at > ? AND pause_requested = FALSE AND cancel_requested = FALSE
		RETURNING id, file_revision_id, provider_snapshot_id, trigger, mode, status,
		          lease_owner, lease_expires_at, fencing_token, total_segments,
		          resolved_segments, provider_checks, missing_candidates, inconclusive_count,
		          stage, current_provider_id, current_provider_generation, cursor_segment,
		          pause_requested, cancel_requested, created_at, started_at, updated_at, completed_at,
		          COALESCE(last_error, '')
	`
	var run HealthRun
	err := scanHealthRun(r.db.QueryRowContext(ctx, query,
		now.Add(ttl), now, runID, owner, fencingToken, now), &run)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrStaleHealthLease
	}
	if err != nil {
		return nil, fmt.Errorf("renew health run lease: %w", err)
	}
	return &run, nil
}

func (r *HealthStateRepository) ParkHealthRun(
	ctx context.Context,
	runID, owner string,
	fencingToken int64,
	notBefore, at time.Time,
) error {
	return r.parkHealthRun(ctx, runID, owner, fencingToken, notBefore, at, false)
}

func (r *HealthStateRepository) ParkObservationHealthRun(
	ctx context.Context,
	runID, owner string,
	fencingToken int64,
	notBefore, at time.Time,
) error {
	return r.parkHealthRun(ctx, runID, owner, fencingToken, notBefore, at, true)
}

func (r *HealthStateRepository) parkHealthRun(
	ctx context.Context,
	runID, owner string,
	fencingToken int64,
	notBefore, at time.Time,
	requireCurrentSnapshot bool,
) error {
	if runID == "" || strings.TrimSpace(owner) == "" || fencingToken <= 0 || notBefore.IsZero() {
		return fmt.Errorf("run ID, lease owner, fencing token, and next admission time are required")
	}
	if at.IsZero() {
		at = r.now().UTC()
	} else {
		at = at.UTC()
	}
	notBefore = notBefore.UTC()
	return r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		if requireCurrentSnapshot {
			var snapshotID string
			if err := tx.QueryRowContext(ctx, `
				UPDATE health_runs SET updated_at = updated_at
				WHERE id = ? AND status = 'running' AND lease_owner = ? AND fencing_token = ?
				  AND lease_expires_at > ? AND cancel_requested = FALSE
				RETURNING provider_snapshot_id
			`, runID, owner, fencingToken, r.now().UTC()).Scan(&snapshotID); errors.Is(err, sql.ErrNoRows) {
				return ErrStaleHealthLease
			} else if err != nil {
				return fmt.Errorf("lock parked observation run: %w", err)
			}
			current, err := providerSnapshotMembershipMatchesCurrentTx(ctx, tx, snapshotID)
			if err != nil {
				return err
			}
			if !current {
				return ErrProviderSnapshotMismatch
			}
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE health_runs
			SET status = 'pending', lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
			WHERE id = ? AND status = 'running' AND lease_owner = ? AND fencing_token = ?
			  AND lease_expires_at > ? AND cancel_requested = FALSE
		`, at, runID, owner, fencingToken, r.now().UTC())
		if err != nil {
			return fmt.Errorf("park health run: %w", err)
		}
		if rows, err := result.RowsAffected(); err != nil {
			return fmt.Errorf("read parked health run result: %w", err)
		} else if rows == 0 {
			return ErrStaleHealthLease
		}
		result, err = tx.ExecContext(ctx, `
			UPDATE health_run_schedule
			SET not_before = ?, updated_at = ?
			WHERE run_id = ? AND active = TRUE
		`, notBefore, at, runID)
		if err != nil {
			return fmt.Errorf("reschedule parked health run: %w", err)
		}
		if rows, err := result.RowsAffected(); err != nil {
			return fmt.Errorf("read parked health run schedule result: %w", err)
		} else if rows == 0 {
			return fmt.Errorf("parked health run has no active schedule")
		}
		return nil
	})
}

func (r *HealthStateRepository) CompleteHealthRun(
	ctx context.Context,
	runID, owner string,
	fencingToken int64,
	at time.Time,
) error {
	return r.finishHealthRun(ctx, runID, owner, fencingToken, HealthRunCompleted, "", at, false, false)
}

func (r *HealthStateRepository) CompleteObservationHealthRun(
	ctx context.Context,
	runID, owner string,
	fencingToken int64,
	at time.Time,
) error {
	return r.finishHealthRun(ctx, runID, owner, fencingToken, HealthRunCompleted, "", at, true, true)
}

func (r *HealthStateRepository) FailHealthRun(
	ctx context.Context,
	runID, owner string,
	fencingToken int64,
	reason string,
	at time.Time,
) error {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return fmt.Errorf("health run failure reason is required")
	}
	reason = sanitizeHealthFailureReason(reason)
	return r.finishHealthRun(ctx, runID, owner, fencingToken, HealthRunFailed, reason, at, false, false)
}

// sanitizeHealthFailureReason retains a small typed/operator-safe class while
// ensuring transport payloads, article identifiers, and provider details
// cannot become durable API-visible state through a free-form error string.
func sanitizeHealthFailureReason(reason string) string {
	if len(reason) > 128 {
		return "health run failed"
	}
	for _, value := range reason {
		if value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' ||
			value >= '0' && value <= '9' || value == ' ' || value == '_' ||
			value == '-' || value == '.' {
			continue
		}
		return "health run failed"
	}
	return reason
}

func (r *HealthStateRepository) finishHealthRun(
	ctx context.Context,
	runID, owner string,
	fencingToken int64,
	status HealthRunStatus,
	reason string,
	at time.Time,
	requireCurrentSnapshot bool,
	reconcileCompletedProgress bool,
) error {
	if runID == "" || strings.TrimSpace(owner) == "" || fencingToken <= 0 {
		return fmt.Errorf("run ID, lease owner, and fencing token are required")
	}
	if status != HealthRunCompleted && status != HealthRunFailed {
		return fmt.Errorf("invalid terminal health run status %q", status)
	}
	if at.IsZero() {
		at = r.now().UTC()
	} else {
		at = at.UTC()
	}
	return r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		if requireCurrentSnapshot {
			var snapshotID string
			if err := tx.QueryRowContext(ctx, `
				UPDATE health_runs SET updated_at = updated_at
				WHERE id = ? AND status = 'running' AND lease_owner = ? AND fencing_token = ?
				  AND lease_expires_at > ? AND cancel_requested = FALSE
				RETURNING provider_snapshot_id
			`, runID, owner, fencingToken, r.now().UTC()).Scan(&snapshotID); errors.Is(err, sql.ErrNoRows) {
				return ErrStaleHealthLease
			} else if err != nil {
				return fmt.Errorf("lock completed observation run: %w", err)
			}
			current, err := providerSnapshotMembershipMatchesCurrentTx(ctx, tx, snapshotID)
			if err != nil {
				return err
			}
			if !current {
				return ErrProviderSnapshotMismatch
			}
		}
		progressAssignment := ""
		if reconcileCompletedProgress && status == HealthRunCompleted {
			progressAssignment = ", resolved_segments = total_segments"
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE health_runs
			SET status = ?`+progressAssignment+`, lease_owner = NULL, lease_expires_at = NULL,
			    last_error = ?, updated_at = ?, completed_at = ?
			WHERE id = ? AND status = 'running' AND lease_owner = ? AND fencing_token = ?
			  AND lease_expires_at > ? AND cancel_requested = FALSE
		`, status, nullableString(reason), at, at, runID, owner, fencingToken, r.now().UTC())
		if err != nil {
			return fmt.Errorf("finish health run: %w", err)
		}
		if rows, err := result.RowsAffected(); err != nil {
			return fmt.Errorf("read finished health run result: %w", err)
		} else if rows == 0 {
			return ErrStaleHealthLease
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE health_run_schedule SET active = FALSE, updated_at = ? WHERE run_id = ?
		`, at, runID); err != nil {
			return fmt.Errorf("retire health run schedule: %w", err)
		}
		return nil
	})
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

// GetHealthRunResumeState reconstructs only state made durable by successful
// chunk transactions.
func (r *HealthStateRepository) GetHealthRunResumeState(
	ctx context.Context,
	runID string,
) (*HealthRunResumeState, error) {
	run, err := r.GetHealthRun(ctx, runID)
	if err != nil || run == nil {
		return nil, err
	}
	state := &HealthRunResumeState{Run: *run}

	rows, err := r.db.QueryContext(ctx, `
		SELECT id, run_id, provider_id, provider_generation, provider_activation_epoch,
		       stage, observation_kind, fresh_transport,
		       segment_start, segment_count, tested_bitmap, present_bitmap, absent_bitmap,
		       corrupt_bitmap, temporary_bitmap, inconclusive_bitmap, resolved_bitmap, fencing_token,
		       resolved_delta, provider_checks_delta, missing_candidates_delta,
		       inconclusive_delta, committed_at
		FROM health_run_chunks
		WHERE run_id = ?
		ORDER BY committed_at, segment_start, id
	`, runID)
	if err != nil {
		return nil, fmt.Errorf("list committed health run chunks: %w", err)
	}
	for rows.Next() {
		var chunk HealthRunChunkState
		if err := rows.Scan(
			&chunk.ID, &chunk.RunID, &chunk.ProviderID, &chunk.ProviderGeneration,
			&chunk.ProviderActivationEpoch, &chunk.Stage, &chunk.ObservationKind,
			&chunk.FreshTransport,
			&chunk.SegmentStart, &chunk.SegmentCount,
			&chunk.TestedBitmap, &chunk.PresentBitmap, &chunk.AbsentBitmap,
			&chunk.CorruptBitmap, &chunk.TemporaryBitmap, &chunk.InconclusiveBitmap,
			&chunk.ResolvedBitmap, &chunk.FencingToken, &chunk.ResolvedDelta, &chunk.ProviderChecksDelta,
			&chunk.MissingCandidatesDelta, &chunk.InconclusiveDelta, &chunk.CommittedAt,
		); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan committed health run chunk: %w", err)
		}
		state.Chunks = append(state.Chunks, chunk)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close committed health run chunks: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate committed health run chunks: %w", err)
	}

	rows, err = r.db.QueryContext(ctx, `
		SELECT c.id, c.file_revision_id, c.provider_id, c.provider_generation,
		       c.provider_activation_epoch,
		       c.observation_kind, c.segment_start, c.segment_count, c.tested_bitmap,
		       c.present_bitmap, c.resolved_bitmap, c.source_chunk_id, c.observed_at
		FROM health_provider_coverage c
		JOIN health_run_chunks rc ON rc.id = c.source_chunk_id
		WHERE rc.run_id = ?
		ORDER BY c.observed_at, c.segment_start, c.id
	`, runID)
	if err != nil {
		return nil, fmt.Errorf("list committed health run coverage: %w", err)
	}
	for rows.Next() {
		var coverage HealthProviderCoverageState
		if err := rows.Scan(
			&coverage.ID, &coverage.FileRevisionID, &coverage.ProviderID,
			&coverage.ProviderGeneration, &coverage.ProviderActivationEpoch, &coverage.ObservationKind,
			&coverage.SegmentStart, &coverage.SegmentCount, &coverage.TestedBitmap,
			&coverage.PresentBitmap, &coverage.ResolvedBitmap,
			&coverage.SourceChunkID, &coverage.ObservedAt,
		); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan committed health run coverage: %w", err)
		}
		state.Coverage = append(state.Coverage, coverage)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close committed health run coverage: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate committed health run coverage: %w", err)
	}

	rows, err = r.db.QueryContext(ctx, `
		SELECT rs.retry_key, rs.source_chunk_id, rs.file_revision_id, rs.provider_id,
		       rs.provider_generation, rs.provider_activation_epoch,
		       rs.segment_start, rs.segment_count, rs.outcome,
		       rs.attempt, rs.next_attempt_at, rs.exhausted, rs.updated_at
		FROM health_retry_states rs
		JOIN health_run_chunks rc ON rc.id = rs.source_chunk_id
		WHERE rc.run_id = ?
		ORDER BY rs.updated_at, rs.retry_key
	`, runID)
	if err != nil {
		return nil, fmt.Errorf("list committed health run retries: %w", err)
	}
	for rows.Next() {
		var retry HealthRunRetryState
		var nextAttemptAt *time.Time
		if err := rows.Scan(
			&retry.RetryKey, &retry.SourceChunkID, &retry.FileRevisionID,
			&retry.ProviderID, &retry.ProviderGeneration, &retry.ProviderActivationEpoch,
			&retry.SegmentStart,
			&retry.SegmentCount, &retry.Outcome, &retry.Attempt, &nextAttemptAt,
			&retry.Exhausted, &retry.UpdatedAt,
		); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan committed health run retry: %w", err)
		}
		if nextAttemptAt != nil {
			retry.NextAttemptAt = nextAttemptAt.UTC()
		}
		state.Retries = append(state.Retries, retry)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close committed health run retries: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate committed health run retries: %w", err)
	}
	return state, nil
}
