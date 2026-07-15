package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ListInactiveImportQueueCandidates returns admitted candidates whose
// metadata publication may have crashed before DB activation created an
// activation-journal row. Terminal filesystem cleanup uses the private intent
// journal to decide whether any visible bytes/ref reservation exist.
func (r *HealthStateRepository) ListInactiveImportQueueCandidates(
	ctx context.Context,
	queueItemID int64,
) ([]InactiveImportCandidate, error) {
	if queueItemID <= 0 {
		return nil, fmt.Errorf("positive import queue item ID is required")
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT DISTINCT validation.queue_item_id, health.file_path,
		       candidate.id, candidate.layout_fingerprint
		FROM health_import_validations validation
		JOIN health_file_revisions candidate
		  ON candidate.id = validation.file_revision_id
		JOIN file_health health ON health.id = candidate.file_health_id
		WHERE validation.queue_item_id = ?
		  AND validation.phase IN ('accepted', 'health_pending')
		  AND NOT EXISTS (
		      SELECT 1 FROM health_import_activation_journal journal
		      WHERE journal.queue_item_id = validation.queue_item_id
		        AND journal.candidate_revision_id = candidate.id
		  )
		ORDER BY health.file_path, candidate.id
	`, queueItemID)
	if err != nil {
		return nil, fmt.Errorf("list inactive import candidates: %w", err)
	}
	defer rows.Close()
	var candidates []InactiveImportCandidate
	for rows.Next() {
		var candidate InactiveImportCandidate
		if err := rows.Scan(
			&candidate.QueueItemID, &candidate.FilePath,
			&candidate.CandidateRevisionID, &candidate.LayoutFingerprint,
		); err != nil {
			return nil, fmt.Errorf("scan inactive import candidate: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate inactive import candidates: %w", err)
	}
	return candidates, nil
}

// ClaimInactiveImportCandidateCleanup fences one exact pre-activation
// filesystem restore with the existing activation journal. The file identity
// lock serializes this claim against ActivateImportFileRevision. A false claim
// is returned only when the same shared candidate is already owned by another
// queue and its visible metadata must be preserved.
func (r *HealthStateRepository) ClaimInactiveImportCandidateCleanup(
	ctx context.Context,
	queueItemID int64,
	candidateRevisionID string,
	priorLayoutFingerprint string,
	priorExists bool,
	at time.Time,
) (bool, error) {
	candidateRevisionID = strings.TrimSpace(candidateRevisionID)
	priorLayoutFingerprint = strings.TrimSpace(priorLayoutFingerprint)
	if queueItemID <= 0 || candidateRevisionID == "" || at.IsZero() ||
		(priorExists && priorLayoutFingerprint == "") ||
		(!priorExists && priorLayoutFingerprint != "") {
		return false, fmt.Errorf("exact inactive candidate cleanup identity is required")
	}
	at = at.UTC()
	claimed := false
	err := r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		if err := lockImportActivationQueue(ctx, tx, queueItemID); err != nil {
			return err
		}
		var fileHealthID int64
		var candidateFingerprint string
		var candidateActive bool
		if err := tx.QueryRowContext(ctx, `
			UPDATE health_file_revisions SET active = active
			WHERE id = ?
			RETURNING file_health_id, layout_fingerprint, active
		`, candidateRevisionID).Scan(
			&fileHealthID, &candidateFingerprint, &candidateActive,
		); errors.Is(err, sql.ErrNoRows) {
			return ErrFileRevisionNotAdmitted
		} else if err != nil {
			return fmt.Errorf("lock inactive import candidate: %w", err)
		}

		var admitted int
		if err := tx.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM health_import_validations validation
			JOIN health_runs run ON run.id = validation.run_id
			WHERE validation.queue_item_id = ?
			  AND validation.file_revision_id = ?
			  AND validation.phase IN ('accepted', 'health_pending')
			  AND run.status = 'completed' AND run.trigger = 'import'
			  AND run.mode = 'observation'
		`, queueItemID, candidateRevisionID).Scan(&admitted); err != nil {
			return fmt.Errorf("read inactive import admission: %w", err)
		}
		if admitted != 1 {
			return ErrFileRevisionNotAdmitted
		}

		var priorStatus HealthStatus
		var priorScheduled *time.Time
		var priorPriority HealthPriority
		var priorRetry, priorRepairRetry int
		if err := tx.QueryRowContext(ctx, `
			UPDATE file_health SET updated_at = updated_at
			WHERE id = ?
			RETURNING status, scheduled_check_at, priority,
			          retry_count, repair_retry_count
		`, fileHealthID).Scan(
			&priorStatus, &priorScheduled, &priorPriority,
			&priorRetry, &priorRepairRetry,
		); err != nil {
			return fmt.Errorf("lock inactive candidate file identity: %w", err)
		}

		var ownState string
		err := tx.QueryRowContext(ctx, `
			SELECT state FROM health_import_activation_journal
			WHERE queue_item_id = ? AND candidate_revision_id = ?
		`, queueItemID, candidateRevisionID).Scan(&ownState)
		if err == nil {
			if ownState != "cleanup_pending" {
				return ErrStaleRevisionActivation
			}
			claimed = true
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read inactive candidate cleanup claim: %w", err)
		}

		var activeRevisionID, activeFingerprint string
		err = tx.QueryRowContext(ctx, `
			SELECT id, layout_fingerprint
			FROM health_file_revisions
			WHERE file_health_id = ? AND active = TRUE
		`, fileHealthID).Scan(&activeRevisionID, &activeFingerprint)
		if err == nil && activeRevisionID == candidateRevisionID {
			// A committed or otherwise resolved owner already published the
			// identical shared revision. This queue may release only its own ref.
			return nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read inactive cleanup prior revision: %w", err)
		}

		var foreignCandidateID string
		foreignErr := tx.QueryRowContext(ctx, `
			SELECT candidate_revision_id
			FROM health_import_activation_journal
			WHERE file_health_id = ? AND queue_item_id <> ?
			  AND state IN ('active', 'cleanup_pending', 'compensated')
			ORDER BY created_at, queue_item_id
			LIMIT 1
		`, fileHealthID, queueItemID).Scan(&foreignCandidateID)
		if foreignErr == nil {
			// The same shared candidate is already durably owned elsewhere, so
			// this queue may release only its own exact ref without touching the
			// visible bytes. A different foreign owner (including the currently
			// active prior) is not safe to discard: retain this queue's recovery
			// state and retry after that ownership window resolves.
			if foreignCandidateID == candidateRevisionID {
				return nil
			}
			return ErrStaleRevisionActivation
		}
		if !errors.Is(foreignErr, sql.ErrNoRows) {
			return fmt.Errorf("read foreign inactive cleanup owner: %w", foreignErr)
		}
		if priorExists {
			if errors.Is(err, sql.ErrNoRows) || activeFingerprint != priorLayoutFingerprint {
				return ErrStaleRevisionActivation
			}
		} else if err == nil {
			return ErrStaleRevisionActivation
		}

		var priorRevisionID *string
		if activeRevisionID != "" {
			priorRevisionID = &activeRevisionID
		}
		candidateScheduled := at
		if priorScheduled != nil {
			candidateScheduled = priorScheduled.UTC()
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO health_import_activation_journal
				(queue_item_id, candidate_revision_id, file_health_id, prior_revision_id,
				 prior_status, prior_scheduled_check_at, prior_priority,
				 prior_retry_count, prior_repair_retry_count,
				 candidate_scheduled_check_at, candidate_priority,
				 state, created_at, updated_at, resolved_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'cleanup_pending', ?, ?, NULL)
		`, queueItemID, candidateRevisionID, fileHealthID, priorRevisionID,
			priorStatus, priorScheduled, priorPriority, priorRetry, priorRepairRetry,
			candidateScheduled, priorPriority, at, at); err != nil {
			return fmt.Errorf("claim inactive candidate cleanup: %w", err)
		}
		claimed = true
		return nil
	})
	return claimed, err
}

// RollbackImportQueueActivations is retained as the path-oriented compatibility
// seam. The durable journal is authoritative and makes an empty list mean all
// queue-owned activations, as before.
func (r *HealthStateRepository) RollbackImportQueueActivations(
	ctx context.Context,
	queueItemID int64,
	paths []string,
	at time.Time,
) ([]string, error) {
	records, err := r.beginImportQueueActivationRollback(ctx, queueItemID, paths, at)
	if err != nil {
		return nil, err
	}
	rolledBack := make([]string, 0, len(records))
	for _, record := range records {
		rolledBack = append(rolledBack, record.FilePath)
	}
	return rolledBack, nil
}

// BeginImportQueueActivationRollback atomically switches every exact active
// candidate back to its journaled prior revision (or no revision for a new
// path), restores prior coarse health scheduling, and marks filesystem cleanup
// pending. Repeated calls return the same pending records after a restart.
func (r *HealthStateRepository) BeginImportQueueActivationRollback(
	ctx context.Context,
	queueItemID int64,
	at time.Time,
) ([]ImportActivationRollback, error) {
	return r.beginImportQueueActivationRollback(ctx, queueItemID, nil, at)
}

func (r *HealthStateRepository) beginImportQueueActivationRollback(
	ctx context.Context,
	queueItemID int64,
	paths []string,
	at time.Time,
) ([]ImportActivationRollback, error) {
	if queueItemID <= 0 || at.IsZero() {
		return nil, fmt.Errorf("queue item and activation rollback time are required")
	}
	at = at.UTC()
	requested := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		if path = normalizeHealthPath(path); path != "" {
			requested[path] = struct{}{}
		}
	}
	var records []ImportActivationRollback
	err := r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		if err := lockImportActivationQueue(ctx, tx, queueItemID); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `
			SELECT journal.candidate_revision_id, journal.file_health_id,
			       journal.prior_revision_id, journal.prior_status,
			       journal.prior_scheduled_check_at, journal.prior_priority,
			       journal.prior_retry_count, journal.prior_repair_retry_count,
			       journal.state, health.file_path, candidate.layout_fingerprint,
			       prior.layout_fingerprint
			FROM health_import_activation_journal journal
			JOIN file_health health ON health.id = journal.file_health_id
			JOIN health_file_revisions candidate
			  ON candidate.id = journal.candidate_revision_id
			LEFT JOIN health_file_revisions prior ON prior.id = journal.prior_revision_id
			WHERE journal.queue_item_id = ?
			  AND journal.state IN ('active', 'compensated', 'cleanup_pending')
			ORDER BY health.file_path, journal.candidate_revision_id
		`, queueItemID)
		if err != nil {
			return fmt.Errorf("list import activation rollback journal: %w", err)
		}
		type journalRow struct {
			candidateID, filePath, candidateFingerprint, state string
			fileHealthID                                       int64
			priorID, priorFingerprint                          *string
			priorStatus                                        HealthStatus
			priorScheduled                                     *time.Time
			priorPriority                                      HealthPriority
			priorRetry, priorRepairRetry                       int
		}
		var journalRows []journalRow
		for rows.Next() {
			var row journalRow
			if err := rows.Scan(
				&row.candidateID, &row.fileHealthID, &row.priorID,
				&row.priorStatus, &row.priorScheduled, &row.priorPriority,
				&row.priorRetry, &row.priorRepairRetry, &row.state,
				&row.filePath, &row.candidateFingerprint, &row.priorFingerprint,
			); err != nil {
				rows.Close()
				return fmt.Errorf("scan import activation rollback journal: %w", err)
			}
			row.filePath = normalizeHealthPath(row.filePath)
			if len(requested) > 0 {
				if _, ok := requested[row.filePath]; !ok {
					continue
				}
			}
			journalRows = append(journalRows, row)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close import activation rollback journal: %w", err)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate import activation rollback journal: %w", err)
		}

		for _, row := range journalRows {
			activeID, err := lockCurrentFileRevision(ctx, tx, row.fileHealthID)
			if err != nil {
				return err
			}
			priorID := nullablePointerString(row.priorID)
			if row.state == "cleanup_pending" {
				if activeID != priorID {
					return ErrStaleRevisionActivation
				}
			} else {
				if activeID != row.candidateID {
					return ErrStaleRevisionActivation
				}
				if priorID != row.candidateID {
					if _, err := tx.ExecContext(ctx, `
						UPDATE health_file_revisions SET active = FALSE
						WHERE id = ? AND file_health_id = ? AND active = TRUE
					`, row.candidateID, row.fileHealthID); err != nil {
						return fmt.Errorf("deactivate journaled import candidate: %w", err)
					}
					if priorID != "" {
						updated, err := tx.ExecContext(ctx, `
							UPDATE health_file_revisions SET active = TRUE
							WHERE id = ? AND file_health_id = ? AND active = FALSE
						`, priorID, row.fileHealthID)
						if err != nil {
							return fmt.Errorf("restore journaled prior file revision: %w", err)
						}
						if count, _ := updated.RowsAffected(); count != 1 {
							return ErrStaleRevisionActivation
						}
					}
				}
				if _, err := tx.ExecContext(ctx, `
					UPDATE file_health
					SET status = ?, scheduled_check_at = ?, priority = ?, retry_count = ?,
					    repair_retry_count = ?, last_error = NULL, error_details = NULL,
					    updated_at = ?
					WHERE id = ?
				`, row.priorStatus, row.priorScheduled, row.priorPriority,
					row.priorRetry, row.priorRepairRetry, at, row.fileHealthID); err != nil {
					return fmt.Errorf("restore prior file health schedule: %w", err)
				}
				if _, err := tx.ExecContext(ctx, `
					UPDATE health_import_activation_journal
					SET state = 'cleanup_pending', updated_at = ?, resolved_at = NULL
					WHERE queue_item_id = ? AND candidate_revision_id = ?
					  AND state IN ('active', 'compensated')
				`, at, queueItemID, row.candidateID); err != nil {
					return fmt.Errorf("mark import activation cleanup pending: %w", err)
				}
			}
			record := ImportActivationRollback{
				QueueItemID: queueItemID, FilePath: row.filePath,
				CandidateRevisionID:        row.candidateID,
				CandidateLayoutFingerprint: row.candidateFingerprint,
				PriorRevisionID:            priorID,
			}
			if row.priorFingerprint != nil {
				record.PriorLayoutFingerprint = *row.priorFingerprint
			}
			records = append(records, record)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return records, nil
}

// CompleteImportQueueActivationRollback records that the filesystem journal
// has restored/deleted the exact pending candidate artifacts.
func (r *HealthStateRepository) CompleteImportQueueActivationRollback(
	ctx context.Context,
	queueItemID int64,
	candidateRevisionIDs []string,
	at time.Time,
) error {
	return r.resolveImportActivationJournal(
		ctx, queueItemID, candidateRevisionIDs, at, false,
	)
}

// CompensateImportQueueActivationRollback republishes only an exact pending
// candidate when filesystem cleanup failed and no unrelated revision became
// active after Begin. Its candidate health schedule is restored verbatim.
func (r *HealthStateRepository) CompensateImportQueueActivationRollback(
	ctx context.Context,
	queueItemID int64,
	candidateRevisionIDs []string,
	at time.Time,
) error {
	return r.resolveImportActivationJournal(
		ctx, queueItemID, candidateRevisionIDs, at, true,
	)
}

func (r *HealthStateRepository) resolveImportActivationJournal(
	ctx context.Context,
	queueItemID int64,
	candidateRevisionIDs []string,
	at time.Time,
	compensate bool,
) error {
	if queueItemID <= 0 || at.IsZero() {
		return fmt.Errorf("queue item and activation resolution time are required")
	}
	at = at.UTC()
	requested := normalizedRevisionIDs(candidateRevisionIDs)
	return r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		if err := lockImportActivationQueue(ctx, tx, queueItemID); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `
			SELECT candidate_revision_id, file_health_id, prior_revision_id,
			       candidate_scheduled_check_at, candidate_priority, state
			FROM health_import_activation_journal
			WHERE queue_item_id = ?
			ORDER BY candidate_revision_id
		`, queueItemID)
		if err != nil {
			return fmt.Errorf("list import activation resolutions: %w", err)
		}
		type resolution struct {
			candidateID, priorID, state string
			fileHealthID                int64
			candidateScheduled          time.Time
			candidatePriority           HealthPriority
		}
		var resolutions []resolution
		for rows.Next() {
			var value resolution
			var priorID *string
			if err := rows.Scan(&value.candidateID, &value.fileHealthID, &priorID,
				&value.candidateScheduled, &value.candidatePriority, &value.state); err != nil {
				rows.Close()
				return fmt.Errorf("scan import activation resolution: %w", err)
			}
			value.priorID = nullablePointerString(priorID)
			if len(requested) > 0 {
				if _, ok := requested[value.candidateID]; !ok {
					continue
				}
			}
			resolutions = append(resolutions, value)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close import activation resolutions: %w", err)
		}
		if len(requested) > 0 && len(resolutions) != len(requested) {
			return ErrStaleRevisionActivation
		}
		for _, value := range resolutions {
			terminalState := "cleanup_completed"
			if compensate {
				terminalState = "compensated"
			}
			if value.state == terminalState {
				continue
			}
			if value.state != "cleanup_pending" {
				return ErrStaleRevisionActivation
			}
			if compensate {
				activeID, err := lockCurrentFileRevision(ctx, tx, value.fileHealthID)
				if err != nil {
					return err
				}
				if activeID != value.priorID {
					return ErrStaleRevisionActivation
				}
				if value.priorID != value.candidateID {
					if value.priorID != "" {
						if _, err := tx.ExecContext(ctx, `
							UPDATE health_file_revisions SET active = FALSE
							WHERE id = ? AND file_health_id = ? AND active = TRUE
						`, value.priorID, value.fileHealthID); err != nil {
							return fmt.Errorf("deactivate compensated prior revision: %w", err)
						}
					}
					updated, err := tx.ExecContext(ctx, `
						UPDATE health_file_revisions SET active = TRUE, activated_at = ?
						WHERE id = ? AND file_health_id = ? AND active = FALSE
					`, at, value.candidateID, value.fileHealthID)
					if err != nil {
						return fmt.Errorf("republish compensated candidate revision: %w", err)
					}
					if count, _ := updated.RowsAffected(); count != 1 {
						return ErrStaleRevisionActivation
					}
				}
				if _, err := tx.ExecContext(ctx, `
					UPDATE file_health
					SET status = 'pending', scheduled_check_at = ?, priority = ?,
					    retry_count = 0, repair_retry_count = 0,
					    last_error = NULL, error_details = NULL, updated_at = ?
					WHERE id = ?
				`, value.candidateScheduled, value.candidatePriority, at,
					value.fileHealthID); err != nil {
					return fmt.Errorf("restore compensated candidate health schedule: %w", err)
				}
			}
			if _, err := tx.ExecContext(ctx, `
				UPDATE health_import_activation_journal
				SET state = ?, updated_at = ?, resolved_at = ?
				WHERE queue_item_id = ? AND candidate_revision_id = ?
				  AND state = 'cleanup_pending'
			`, terminalState, at, at, queueItemID, value.candidateID); err != nil {
				return fmt.Errorf("resolve import activation journal: %w", err)
			}
		}
		return nil
	})
}

// CommitImportQueueActivations closes the rollback window only after the queue
// success path and filesystem journal have both committed.
func (r *HealthStateRepository) CommitImportQueueActivations(
	ctx context.Context,
	queueItemID int64,
	at time.Time,
) error {
	if queueItemID <= 0 || at.IsZero() {
		return fmt.Errorf("queue item and activation commit time are required")
	}
	at = at.UTC()
	return r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		if err := lockImportActivationQueue(ctx, tx, queueItemID); err != nil {
			return err
		}
		var pending int
		if err := tx.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM health_import_activation_journal
			WHERE queue_item_id = ? AND state = 'cleanup_pending'
		`, queueItemID).Scan(&pending); err != nil {
			return fmt.Errorf("read pending activation cleanup: %w", err)
		}
		if pending != 0 {
			return ErrStaleRevisionActivation
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE health_import_activation_journal
			SET state = 'committed', updated_at = ?, resolved_at = ?
			WHERE queue_item_id = ? AND state IN ('active', 'compensated')
		`, at, at, queueItemID); err != nil {
			return fmt.Errorf("commit import activation journal: %w", err)
		}
		return nil
	})
}

func lockImportActivationQueue(ctx context.Context, tx *dialectAwareTx, queueItemID int64) error {
	var locked int64
	if err := tx.QueryRowContext(ctx, `
		UPDATE import_queue SET updated_at = updated_at
		WHERE id = ? RETURNING id
	`, queueItemID).Scan(&locked); errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("import queue item does not exist")
	} else if err != nil {
		return fmt.Errorf("lock import activation queue: %w", err)
	}
	return nil
}

func lockCurrentFileRevision(
	ctx context.Context,
	tx *dialectAwareTx,
	fileHealthID int64,
) (string, error) {
	var locked int64
	if err := tx.QueryRowContext(ctx, `
		UPDATE file_health SET updated_at = updated_at
		WHERE id = ? RETURNING id
	`, fileHealthID).Scan(&locked); err != nil {
		return "", fmt.Errorf("lock import activation file identity: %w", err)
	}
	var activeID string
	err := tx.QueryRowContext(ctx, `
		SELECT id FROM health_file_revisions
		WHERE file_health_id = ? AND active = TRUE
	`, fileHealthID).Scan(&activeID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read current import activation revision: %w", err)
	}
	return activeID, nil
}

func normalizedRevisionIDs(ids []string) map[string]struct{} {
	result := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id = strings.TrimSpace(id); id != "" {
			result[id] = struct{}{}
		}
	}
	return result
}

func nullablePointerString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
