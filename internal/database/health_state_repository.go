package database

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

var (
	ErrStaleHealthLease                  = errors.New("stale or expired health run lease")
	ErrStaleHealthSchedule               = errors.New("stale or inactive health run schedule")
	ErrHealthChunkConflict               = errors.New("health chunk identity conflicts with committed content")
	ErrProviderSnapshotMismatch          = errors.New("provider generation is not in the run dispatch snapshot")
	ErrFileRevisionNotAdmitted           = errors.New("file revision has not completed import admission")
	ErrStaleRevisionActivation           = errors.New("a newer admitted file revision supersedes this activation")
	ErrStaleImportValidation             = errors.New("import validation is bound to a different run")
	ErrInactiveFileRevision              = errors.New("file layout is an inactive import candidate")
	ErrImportDamagePolicy                = errors.New("import queue damage policy is already frozen")
	ErrImportRunControl                  = errors.New("import admission run is not controlled by health operations")
	errIncompleteGapConfirmationEvidence = errors.New("incomplete gap confirmation evidence")
)

const DefaultGapConfirmationMinimumDelay = 10 * time.Minute

// HealthStateRepository owns the additive PR4 durable provider, revision, run,
// observation, gap, and recovery state. The PR3 health engine is intentionally
// not wired to it until PR5 observation mode.
type HealthStateRepository struct {
	db                               *dialectAwareDB
	dialect                          dialectHelper
	now                              func() time.Time
	gapConfirmationMinimumDelayNanos atomic.Int64
}

func NewHealthStateRepository(db *sql.DB, d Dialect) *HealthStateRepository {
	repository := &HealthStateRepository{
		db: newDialectAwareDB(db, d), dialect: dialectHelper{d: d},
		now: func() time.Time { return time.Now().UTC() },
	}
	repository.gapConfirmationMinimumDelayNanos.Store(
		int64(DefaultGapConfirmationMinimumDelay),
	)
	return repository
}

// SetGapConfirmationMinimumDelay configures how far apart two complete,
// coherent confirmation waves must be. The setting is local to this repository
// instance and can be updated safely while workers are active.
func (r *HealthStateRepository) SetGapConfirmationMinimumDelay(delay time.Duration) error {
	if delay <= 0 {
		return fmt.Errorf("gap confirmation minimum delay must be positive")
	}
	r.gapConfirmationMinimumDelayNanos.Store(int64(delay))
	return nil
}

func (r *HealthStateRepository) gapConfirmationMinimumDelay() time.Duration {
	delay := time.Duration(r.gapConfirmationMinimumDelayNanos.Load())
	if delay <= 0 {
		return DefaultGapConfirmationMinimumDelay
	}
	return delay
}

func (r *HealthStateRepository) withTransaction(ctx context.Context, fn func(*dialectAwareTx) error) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin health state transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit health state transaction: %w", err)
	}
	return nil
}

func (r *HealthStateRepository) EnsureFileRevision(ctx context.Context, spec FileRevisionSpec) (*HealthFileRevision, error) {
	spec.FilePath = normalizeHealthPath(spec.FilePath)
	if spec.FilePath == "" || spec.LayoutFingerprint == "" {
		return nil, fmt.Errorf("file path and layout fingerprint are required")
	}
	if spec.VirtualSize < 0 || spec.SegmentCount < 0 {
		return nil, fmt.Errorf("file revision sizes must be non-negative")
	}
	now := time.Now().UTC()
	var revision HealthFileRevision
	err := r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO file_health (file_path, status, created_at, updated_at)
			VALUES (?, 'pending', ?, ?)
			ON CONFLICT(file_path) DO NOTHING
		`, spec.FilePath, now, now)
		if err != nil {
			return fmt.Errorf("ensure file health identity: %w", err)
		}
		var fileHealthID int64
		if err := tx.QueryRowContext(ctx, `SELECT id FROM file_health WHERE file_path = ?`, spec.FilePath).Scan(&fileHealthID); err != nil {
			return fmt.Errorf("read file health identity: %w", err)
		}

		// Deactivate first so the partial unique index never observes two active
		// revisions, including when a retained historical layout is reactivated.
		if _, err := tx.ExecContext(ctx, `UPDATE health_file_revisions SET active = FALSE WHERE file_health_id = ? AND active = TRUE`, fileHealthID); err != nil {
			return fmt.Errorf("deactivate prior file revision: %w", err)
		}

		err = scanHealthFileRevision(tx.QueryRowContext(ctx, `
			SELECT id, file_health_id, layout_fingerprint, virtual_size, segment_count,
			       active, created_at, activated_at
			FROM health_file_revisions
			WHERE file_health_id = ? AND layout_fingerprint = ?
		`, fileHealthID, spec.LayoutFingerprint), &revision)
		switch {
		case err == nil:
			if revision.VirtualSize != spec.VirtualSize || revision.SegmentCount != spec.SegmentCount {
				return fmt.Errorf("retained layout fingerprint has different file dimensions")
			}
			_, err = tx.ExecContext(ctx, `
				UPDATE health_file_revisions
				SET active = TRUE, activated_at = ?, virtual_size = ?, segment_count = ?
				WHERE id = ?
			`, now, spec.VirtualSize, spec.SegmentCount, revision.ID)
			if err != nil {
				return fmt.Errorf("reactivate file revision: %w", err)
			}
			revision.Active = true
			revision.ActivatedAt = now
			revision.VirtualSize = spec.VirtualSize
			revision.SegmentCount = spec.SegmentCount
			return nil
		case !errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("find file revision: %w", err)
		}

		revision = HealthFileRevision{
			ID: uuid.NewString(), FileHealthID: fileHealthID,
			LayoutFingerprint: spec.LayoutFingerprint, VirtualSize: spec.VirtualSize,
			SegmentCount: spec.SegmentCount, Active: true, CreatedAt: now, ActivatedAt: now,
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO health_file_revisions
				(id, file_health_id, layout_fingerprint, virtual_size, segment_count, active, created_at, activated_at)
			VALUES (?, ?, ?, ?, ?, TRUE, ?, ?)
		`, revision.ID, revision.FileHealthID, revision.LayoutFingerprint, revision.VirtualSize,
			revision.SegmentCount, revision.CreatedAt, revision.ActivatedAt)
		if err != nil {
			return fmt.Errorf("insert file revision: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &revision, nil
}

// EnsureCandidateFileRevision persists a canonical layout without publishing
// it to ordinary health scheduling. Import validation may safely restart
// against the returned identity; only ActivateFileRevision changes which
// retained layout is visible as the active revision for the file.
func (r *HealthStateRepository) EnsureCandidateFileRevision(
	ctx context.Context,
	spec FileRevisionSpec,
) (*HealthFileRevision, error) {
	spec.FilePath = normalizeHealthPath(spec.FilePath)
	if spec.FilePath == "" || spec.LayoutFingerprint == "" {
		return nil, fmt.Errorf("file path and layout fingerprint are required")
	}
	if spec.VirtualSize < 0 || spec.SegmentCount < 0 {
		return nil, fmt.Errorf("file revision sizes must be non-negative")
	}
	now := r.now().UTC()
	var revision HealthFileRevision
	err := r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO file_health (file_path, status, created_at, updated_at)
			VALUES (?, 'pending', ?, ?)
			ON CONFLICT(file_path) DO NOTHING
		`, spec.FilePath, now, now); err != nil {
			return fmt.Errorf("ensure candidate file health identity: %w", err)
		}
		var fileHealthID int64
		if err := tx.QueryRowContext(ctx, `
			SELECT id FROM file_health WHERE file_path = ?
		`, spec.FilePath).Scan(&fileHealthID); err != nil {
			return fmt.Errorf("read candidate file health identity: %w", err)
		}

		err := scanHealthFileRevision(tx.QueryRowContext(ctx, `
			SELECT id, file_health_id, layout_fingerprint, virtual_size, segment_count,
			       active, created_at, activated_at
			FROM health_file_revisions
			WHERE file_health_id = ? AND layout_fingerprint = ?
		`, fileHealthID, spec.LayoutFingerprint), &revision)
		switch {
		case err == nil:
			if revision.VirtualSize != spec.VirtualSize || revision.SegmentCount != spec.SegmentCount {
				return fmt.Errorf("retained layout fingerprint has different file dimensions")
			}
			return nil
		case !errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("find candidate file revision: %w", err)
		}

		revision = HealthFileRevision{
			ID: uuid.NewString(), FileHealthID: fileHealthID,
			LayoutFingerprint: spec.LayoutFingerprint, VirtualSize: spec.VirtualSize,
			SegmentCount: spec.SegmentCount, Active: false, CreatedAt: now, ActivatedAt: now,
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO health_file_revisions
				(id, file_health_id, layout_fingerprint, virtual_size, segment_count,
				 active, created_at, activated_at)
			VALUES (?, ?, ?, ?, ?, FALSE, ?, ?)
		`, revision.ID, revision.FileHealthID, revision.LayoutFingerprint,
			revision.VirtualSize, revision.SegmentCount, revision.CreatedAt,
			revision.ActivatedAt); err != nil {
			return fmt.Errorf("insert candidate file revision: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &revision, nil
}

// EnsureObservationFileRevision resolves only a published layout. It may
// bootstrap the very first revision for genuinely legacy metadata, but it
// never activates a different retained layout or an import candidate.
func (r *HealthStateRepository) EnsureObservationFileRevision(
	ctx context.Context,
	spec FileRevisionSpec,
) (*HealthFileRevision, error) {
	spec.FilePath = normalizeHealthPath(spec.FilePath)
	if spec.FilePath == "" || spec.LayoutFingerprint == "" {
		return nil, fmt.Errorf("file path and layout fingerprint are required")
	}
	if spec.VirtualSize < 0 || spec.SegmentCount < 0 {
		return nil, fmt.Errorf("file revision sizes must be non-negative")
	}
	now := r.now().UTC()
	var revision HealthFileRevision
	err := r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO file_health (file_path, status, created_at, updated_at)
			VALUES (?, 'pending', ?, ?)
			ON CONFLICT(file_path) DO NOTHING
		`, spec.FilePath, now, now); err != nil {
			return fmt.Errorf("ensure observation file health identity: %w", err)
		}
		var fileHealthID int64
		if err := tx.QueryRowContext(ctx, `
			UPDATE file_health SET updated_at = updated_at
			WHERE file_path = ? RETURNING id
		`, spec.FilePath).Scan(&fileHealthID); err != nil {
			return fmt.Errorf("lock observation file health identity: %w", err)
		}

		err := scanHealthFileRevision(tx.QueryRowContext(ctx, `
			SELECT id, file_health_id, layout_fingerprint, virtual_size, segment_count,
			       active, created_at, activated_at
			FROM health_file_revisions
			WHERE file_health_id = ? AND layout_fingerprint = ?
		`, fileHealthID, spec.LayoutFingerprint), &revision)
		if err == nil {
			if revision.VirtualSize != spec.VirtualSize || revision.SegmentCount != spec.SegmentCount {
				return fmt.Errorf("retained layout fingerprint has different file dimensions")
			}
			if !revision.Active {
				return ErrInactiveFileRevision
			}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("find observation file revision: %w", err)
		}

		var retainedCount int
		if err := tx.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM health_file_revisions WHERE file_health_id = ?
		`, fileHealthID).Scan(&retainedCount); err != nil {
			return fmt.Errorf("count retained observation file revisions: %w", err)
		}
		if retainedCount != 0 {
			return ErrInactiveFileRevision
		}
		revision = HealthFileRevision{
			ID: uuid.NewString(), FileHealthID: fileHealthID,
			LayoutFingerprint: spec.LayoutFingerprint, VirtualSize: spec.VirtualSize,
			SegmentCount: spec.SegmentCount, Active: true, CreatedAt: now, ActivatedAt: now,
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO health_file_revisions
				(id, file_health_id, layout_fingerprint, virtual_size, segment_count,
				 active, created_at, activated_at)
			VALUES (?, ?, ?, ?, ?, TRUE, ?, ?)
		`, revision.ID, revision.FileHealthID, revision.LayoutFingerprint,
			revision.VirtualSize, revision.SegmentCount, revision.CreatedAt,
			revision.ActivatedAt); err != nil {
			return fmt.Errorf("bootstrap legacy observation file revision: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &revision, nil
}

// ActivateFileRevision atomically publishes an admitted candidate and arms
// its next health work. Clean imports enter the ordinary cadence; tolerant
// health-pending imports are due immediately for their exact unresolved set.
func (r *HealthStateRepository) ActivateFileRevision(
	ctx context.Context,
	revisionID string,
) (*HealthFileRevision, error) {
	return r.activateFileRevision(ctx, 0, revisionID)
}

// ActivateImportFileRevision publishes only the admission owned by the exact
// queue/revision pair. Revision IDs are shared by path+layout, so selecting the
// latest admission by revision alone can otherwise journal one queue's write
// under another queue's rollback authority.
func (r *HealthStateRepository) ActivateImportFileRevision(
	ctx context.Context,
	queueItemID int64,
	revisionID string,
) (*HealthFileRevision, error) {
	if queueItemID <= 0 {
		return nil, fmt.Errorf("positive import queue item ID is required")
	}
	return r.activateFileRevision(ctx, queueItemID, revisionID)
}

func (r *HealthStateRepository) activateFileRevision(
	ctx context.Context,
	exactQueueItemID int64,
	revisionID string,
) (*HealthFileRevision, error) {
	revisionID = strings.TrimSpace(revisionID)
	if revisionID == "" {
		return nil, fmt.Errorf("file revision ID is required")
	}
	now := r.now().UTC()
	var revision HealthFileRevision
	err := r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		if err := scanHealthFileRevision(tx.QueryRowContext(ctx, `
			UPDATE health_file_revisions SET active = active
			WHERE id = ?
			RETURNING id, file_health_id, layout_fingerprint, virtual_size,
			          segment_count, active, created_at, activated_at
		`, revisionID), &revision); errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("file revision does not exist")
		} else if err != nil {
			return fmt.Errorf("lock candidate file revision: %w", err)
		}
		var lockedFileHealthID int64
		var priorStatus HealthStatus
		var priorScheduledCheckAt *time.Time
		var priorPriority HealthPriority
		var priorRetryCount, priorRepairRetryCount int
		if err := tx.QueryRowContext(ctx, `
			UPDATE file_health SET updated_at = updated_at
			WHERE id = ?
			RETURNING id, status, scheduled_check_at, priority,
			          retry_count, repair_retry_count
		`, revision.FileHealthID).Scan(
			&lockedFileHealthID, &priorStatus, &priorScheduledCheckAt, &priorPriority,
			&priorRetryCount, &priorRepairRetryCount,
		); err != nil {
			return fmt.Errorf("lock candidate file identity: %w", err)
		}

		var phase ImportValidationPhase
		var snapshotID string
		var queueItemID int64
		var err error
		if exactQueueItemID > 0 {
			err = tx.QueryRowContext(ctx, `
				SELECT validation.phase, run.provider_snapshot_id, validation.queue_item_id
				FROM health_import_validations validation
				JOIN health_runs run ON run.id = validation.run_id
				WHERE validation.queue_item_id = ? AND validation.file_revision_id = ?
				  AND validation.phase IN ('accepted', 'health_pending')
				  AND run.status = 'completed' AND run.trigger = 'import'
				  AND run.mode = 'observation'
				LIMIT 1
			`, exactQueueItemID, revision.ID).Scan(&phase, &snapshotID, &queueItemID)
		} else {
			err = tx.QueryRowContext(ctx, `
				SELECT validation.phase, run.provider_snapshot_id, validation.queue_item_id
				FROM health_import_validations validation
				JOIN health_runs run ON run.id = validation.run_id
				WHERE validation.file_revision_id = ?
				  AND validation.phase IN ('accepted', 'health_pending')
				  AND run.status = 'completed' AND run.trigger = 'import'
				  AND run.mode = 'observation'
				ORDER BY validation.updated_at DESC, validation.id DESC
				LIMIT 1
			`, revision.ID).Scan(&phase, &snapshotID, &queueItemID)
		}
		if errors.Is(err, sql.ErrNoRows) {
			return ErrFileRevisionNotAdmitted
		}
		if err != nil {
			return fmt.Errorf("read candidate import admission: %w", err)
		}
		current, err := providerSnapshotMembershipMatchesCurrentTx(ctx, tx, snapshotID)
		if err != nil {
			return err
		}
		if !current {
			return ErrProviderSnapshotMismatch
		}

		var newestRevisionID string
		err = tx.QueryRowContext(ctx, `
			SELECT validation.file_revision_id
			FROM health_import_validations validation
			JOIN health_runs run ON run.id = validation.run_id
			JOIN health_file_revisions candidate
			  ON candidate.id = validation.file_revision_id
			WHERE candidate.file_health_id = ?
			  AND validation.phase IN ('accepted', 'health_pending')
			  AND run.status = 'completed' AND run.trigger = 'import'
			  AND run.mode = 'observation'
			ORDER BY validation.updated_at DESC, candidate.created_at DESC,
			         validation.id DESC, candidate.id DESC
			LIMIT 1
		`, revision.FileHealthID).Scan(&newestRevisionID)
		if err != nil {
			return fmt.Errorf("read newest admitted file revision: %w", err)
		}
		if newestRevisionID != revision.ID {
			return ErrStaleRevisionActivation
		}
		var foreignQueueItemID int64
		err = tx.QueryRowContext(ctx, `
			SELECT queue_item_id
			FROM health_import_activation_journal
			WHERE file_health_id = ? AND queue_item_id <> ?
			  AND state IN ('active', 'cleanup_pending', 'compensated')
			ORDER BY created_at, queue_item_id
			LIMIT 1
		`, revision.FileHealthID, queueItemID).Scan(&foreignQueueItemID)
		if err == nil {
			return ErrStaleRevisionActivation
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read unresolved import activation owner: %w", err)
		}
		nextCheck := now.Add(24 * time.Hour)
		priority := HealthPriorityNormal
		if phase == ImportValidationPhaseHealthPending {
			nextCheck = now
			priority = HealthPriorityHigh
		}
		var priorRevisionID *string
		var activeRevisionID string
		err = tx.QueryRowContext(ctx, `
			SELECT id FROM health_file_revisions
			WHERE file_health_id = ? AND active = TRUE
		`, revision.FileHealthID).Scan(&activeRevisionID)
		if err == nil {
			priorRevisionID = &activeRevisionID
		} else if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read prior published file revision: %w", err)
		}

		if _, err := tx.ExecContext(ctx, `
			INSERT INTO health_import_activation_journal
				(queue_item_id, candidate_revision_id, file_health_id, prior_revision_id,
				 prior_status, prior_scheduled_check_at, prior_priority,
				 prior_retry_count, prior_repair_retry_count,
				 candidate_scheduled_check_at, candidate_priority,
				 state, created_at, updated_at, resolved_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, NULL)
			ON CONFLICT(queue_item_id, candidate_revision_id) DO UPDATE SET
				file_health_id = excluded.file_health_id,
				prior_revision_id = excluded.prior_revision_id,
				prior_status = excluded.prior_status,
				prior_scheduled_check_at = excluded.prior_scheduled_check_at,
				prior_priority = excluded.prior_priority,
				prior_retry_count = excluded.prior_retry_count,
				prior_repair_retry_count = excluded.prior_repair_retry_count,
				candidate_scheduled_check_at = excluded.candidate_scheduled_check_at,
				candidate_priority = excluded.candidate_priority,
				state = 'active', updated_at = excluded.updated_at, resolved_at = NULL
			WHERE health_import_activation_journal.state IN ('cleanup_completed', 'compensated')
		`, queueItemID, revision.ID, revision.FileHealthID, priorRevisionID,
			priorStatus, priorScheduledCheckAt, priorPriority,
			priorRetryCount, priorRepairRetryCount, nextCheck, priority, now, now); err != nil {
			return fmt.Errorf("journal candidate file activation: %w", err)
		}
		var journalFileHealthID int64
		var journalState string
		if err := tx.QueryRowContext(ctx, `
			SELECT file_health_id, state
			FROM health_import_activation_journal
			WHERE queue_item_id = ? AND candidate_revision_id = ?
		`, queueItemID, revision.ID).Scan(&journalFileHealthID, &journalState); err != nil {
			return fmt.Errorf("read candidate activation journal: %w", err)
		}
		if journalFileHealthID != revision.FileHealthID || journalState == "cleanup_pending" ||
			journalState == "cleanup_completed" {
			return ErrStaleRevisionActivation
		}
		if revision.Active {
			return nil
		}
		if priorRevisionID != nil && *priorRevisionID != revision.ID {
			var journalPriorRevisionID *string
			if err := tx.QueryRowContext(ctx, `
				SELECT prior_revision_id FROM health_import_activation_journal
				WHERE queue_item_id = ? AND candidate_revision_id = ?
			`, queueItemID, revision.ID).Scan(&journalPriorRevisionID); err != nil {
				return fmt.Errorf("read journaled prior file revision: %w", err)
			}
			if journalPriorRevisionID == nil || *journalPriorRevisionID != *priorRevisionID {
				return ErrStaleRevisionActivation
			}
		}

		if _, err := tx.ExecContext(ctx, `
			UPDATE health_file_revisions
			SET active = FALSE
			WHERE file_health_id = ? AND active = TRUE AND id <> ?
		`, revision.FileHealthID, revision.ID); err != nil {
			return fmt.Errorf("deactivate prior published file revision: %w", err)
		}
		activated, err := tx.ExecContext(ctx, `
			UPDATE health_file_revisions
			SET active = TRUE, activated_at = ?
			WHERE id = ? AND active = FALSE
		`, now, revision.ID)
		if err != nil {
			return fmt.Errorf("publish candidate file revision: %w", err)
		}
		if rows, err := activated.RowsAffected(); err != nil {
			return fmt.Errorf("read candidate publication result: %w", err)
		} else if rows != 1 {
			return ErrStaleRevisionActivation
		}

		updated, err := tx.ExecContext(ctx, `
			UPDATE file_health
			SET status = 'pending', scheduled_check_at = ?, priority = ?,
			    retry_count = 0, repair_retry_count = 0,
			    last_error = NULL, error_details = NULL, updated_at = ?
			WHERE id = ?
		`, nextCheck, priority, now, revision.FileHealthID)
		if err != nil {
			return fmt.Errorf("arm published file revision health work: %w", err)
		}
		if rows, err := updated.RowsAffected(); err != nil {
			return fmt.Errorf("read published file health result: %w", err)
		} else if rows != 1 {
			return ErrStaleRevisionActivation
		}
		revision.Active = true
		revision.ActivatedAt = now
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &revision, nil
}

func scanHealthFileRevision(row rowScanner, revision *HealthFileRevision) error {
	return row.Scan(&revision.ID, &revision.FileHealthID, &revision.LayoutFingerprint,
		&revision.VirtualSize, &revision.SegmentCount, &revision.Active,
		&revision.CreatedAt, &revision.ActivatedAt)
}

func (r *HealthStateRepository) ListFileRevisions(ctx context.Context, filePath string) ([]HealthFileRevision, error) {
	filePath = normalizeHealthPath(filePath)
	rows, err := r.db.QueryContext(ctx, `
		SELECT r.id, r.file_health_id, r.layout_fingerprint, r.virtual_size,
		       r.segment_count, r.active, r.created_at, r.activated_at
		FROM health_file_revisions r
		JOIN file_health f ON f.id = r.file_health_id
		WHERE f.file_path = ?
		ORDER BY r.created_at, r.id
	`, filePath)
	if err != nil {
		return nil, fmt.Errorf("list file revisions: %w", err)
	}
	defer rows.Close()
	var revisions []HealthFileRevision
	for rows.Next() {
		var revision HealthFileRevision
		if err := scanHealthFileRevision(rows, &revision); err != nil {
			return nil, fmt.Errorf("scan file revision: %w", err)
		}
		revisions = append(revisions, revision)
	}
	return revisions, rows.Err()
}

func normalizeProviderSpec(spec ProviderSpec) (ProviderSpec, string, error) {
	spec.StableID = strings.TrimSpace(spec.StableID)
	spec.DisplayName = strings.TrimSpace(spec.DisplayName)
	spec.Endpoint = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(spec.Endpoint)), ".")
	spec.Account = strings.TrimSpace(spec.Account)
	if spec.DisplayName == "" || spec.Endpoint == "" {
		return ProviderSpec{}, "", fmt.Errorf("provider display name and endpoint are required")
	}
	if spec.Port <= 0 || spec.Port > 65535 {
		return ProviderSpec{}, "", fmt.Errorf("provider port is outside 1..65535")
	}
	if spec.Role != ProviderRolePrimary && spec.Role != ProviderRoleBackup {
		return ProviderSpec{}, "", fmt.Errorf("invalid provider role %q", spec.Role)
	}
	if spec.Order < 0 {
		return ProviderSpec{}, "", fmt.Errorf("provider order must be non-negative")
	}
	identity := sha256.New()
	fmt.Fprintf(identity, "%d:%s|%d|%d:%s", len(spec.Endpoint), spec.Endpoint, spec.Port, len(spec.Account), spec.Account)
	return spec, "sha256:" + hex.EncodeToString(identity.Sum(nil)), nil
}

func (r *HealthStateRepository) ReconcileProviders(ctx context.Context, specs []ProviderSpec) ([]HealthProvider, error) {
	type normalizedProvider struct {
		spec     ProviderSpec
		identity string
	}
	normalized := make([]normalizedProvider, len(specs))
	orders := make(map[int]struct{}, len(specs))
	stableIDs := make(map[string]struct{}, len(specs))
	for i, spec := range specs {
		n, identity, err := normalizeProviderSpec(spec)
		if err != nil {
			return nil, fmt.Errorf("provider %d: %w", i, err)
		}
		if _, exists := orders[n.Order]; exists {
			return nil, fmt.Errorf("duplicate configured provider order %d", n.Order)
		}
		orders[n.Order] = struct{}{}
		if n.StableID != "" {
			if _, exists := stableIDs[n.StableID]; exists {
				return nil, fmt.Errorf("duplicate stable provider ID")
			}
			stableIDs[n.StableID] = struct{}{}
		}
		normalized[i] = normalizedProvider{spec: n, identity: identity}
	}
	sort.SliceStable(normalized, func(i, j int) bool { return normalized[i].spec.Order < normalized[j].spec.Order })

	now := r.now().UTC()
	seenIDs := make(map[string]struct{}, len(normalized))
	activeIDs := make([]string, 0, len(normalized))
	reservedIDs := make(map[string]struct{}, len(stableIDs))
	for id := range stableIDs {
		reservedIDs[id] = struct{}{}
	}
	err := r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		for _, desired := range normalized {
			providerID := desired.spec.StableID
			if providerID == "" {
				matches, err := providerIDsForIdentity(ctx, tx, desired.identity)
				if err != nil {
					return err
				}
				if len(matches) == 1 {
					_, alreadyClaimed := seenIDs[matches[0]]
					_, explicitlyReserved := reservedIDs[matches[0]]
					if !alreadyClaimed && !explicitlyReserved {
						providerID = matches[0]
					}
				}
				if providerID == "" {
					providerID = uuid.NewString()
				}
			}
			if _, duplicate := seenIDs[providerID]; duplicate {
				return fmt.Errorf("provider configuration resolves to duplicate stable identity")
			}
			seenIDs[providerID] = struct{}{}
			activeIDs = append(activeIDs, providerID)

			var generation, activationEpoch int64
			var currentIdentity string
			var wasActive bool
			var activatedAt time.Time
			err := tx.QueryRowContext(ctx, `
				SELECT p.current_generation, p.activation_epoch, p.activated_at,
				       p.active, g.identity_fingerprint
				FROM health_providers p
				JOIN health_provider_generations g
				  ON g.provider_id = p.id AND g.generation = p.current_generation
				WHERE p.id = ?
			`, providerID).Scan(&generation, &activationEpoch, &activatedAt, &wasActive, &currentIdentity)
			switch {
			case errors.Is(err, sql.ErrNoRows):
				generation = 1
				activationEpoch = 1
				activatedAt = now
				_, err = tx.ExecContext(ctx, `
					INSERT INTO health_providers
						(id, display_name, role, configured_order, active, current_generation,
						 activation_epoch, activated_at, tombstoned_at, created_at, updated_at)
					VALUES (?, ?, ?, ?, TRUE, 1, 1, ?, NULL, ?, ?)
				`, providerID, desired.spec.DisplayName, desired.spec.Role, desired.spec.Order,
					activatedAt, now, now)
				if err != nil {
					return fmt.Errorf("insert provider registry row: %w", err)
				}
				if err := insertProviderGeneration(ctx, tx, providerID, generation, desired, now); err != nil {
					return err
				}
			case err != nil:
				return fmt.Errorf("read provider registry row: %w", err)
			default:
				if !wasActive {
					activationEpoch++
					activatedAt = now
				}
				if currentIdentity != desired.identity {
					generation++
					if err := insertProviderGeneration(ctx, tx, providerID, generation, desired, now); err != nil {
						return err
					}
				}
				_, err = tx.ExecContext(ctx, `
					UPDATE health_providers
					SET display_name = ?, role = ?, configured_order = ?, active = TRUE,
					    current_generation = ?, activation_epoch = ?, activated_at = ?,
					    tombstoned_at = NULL, updated_at = ?
					WHERE id = ?
				`, desired.spec.DisplayName, desired.spec.Role, desired.spec.Order, generation,
					activationEpoch, activatedAt, now, providerID)
				if err != nil {
					return fmt.Errorf("update provider registry row: %w", err)
				}
			}
		}

		query := `
			UPDATE health_providers
			SET active = FALSE, tombstoned_at = ?, updated_at = ?
			WHERE active = TRUE
		`
		args := []any{now, now}
		if len(activeIDs) > 0 {
			query += ` AND id NOT IN (` + strings.TrimSuffix(strings.Repeat("?,", len(activeIDs)), ",") + `)`
			for _, providerID := range activeIDs {
				args = append(args, providerID)
			}
		}
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("tombstone removed providers: %w", err)
		}
		// A gap conclusion is meaningful only for the complete current
		// activation set. Adding/reactivating a provider immediately makes an
		// old conclusion provisional; removing one rederives the kind from the
		// retained, still-current confirmed causes.
		if _, err := tx.ExecContext(ctx, `
			UPDATE health_gap_ranges
			SET kind = CASE
			  WHEN (SELECT COUNT(*) FROM health_providers WHERE active = TRUE) > 0
			   AND (SELECT COUNT(*) FROM health_providers WHERE active = TRUE) = (
			     SELECT COUNT(*)
			     FROM health_gap_provider_causes cause
			     JOIN health_providers provider
			       ON provider.id = cause.provider_id AND provider.active = TRUE
			      AND provider.current_generation = cause.provider_generation
			      AND provider.activation_epoch = cause.provider_activation_epoch
			     WHERE cause.gap_id = health_gap_ranges.id
			       AND cause.confirmation_count >= 2 AND cause.confirmed_at IS NOT NULL
			   )
			  THEN CASE WHEN EXISTS (
			     SELECT 1
			     FROM health_gap_provider_causes cause
			     JOIN health_providers provider
			       ON provider.id = cause.provider_id AND provider.active = TRUE
			      AND provider.current_generation = cause.provider_generation
			      AND provider.activation_epoch = cause.provider_activation_epoch
			     WHERE cause.gap_id = health_gap_ranges.id AND cause.cause = 'corrupt'
			   ) THEN 'confirmed_unusable' ELSE 'confirmed_absent' END
			  ELSE 'provisional'
			END
			WHERE status IN ('active', 'dormant')
			  AND kind IN ('provisional', 'confirmed_absent', 'confirmed_unusable')
		`); err != nil {
			return fmt.Errorf("rederive gaps for current provider activations: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return r.ListProviders(ctx, false)
}

func providerIDsForIdentity(ctx context.Context, tx *dialectAwareTx, identity string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT provider_id
		FROM health_provider_generations
		WHERE identity_fingerprint = ?
		ORDER BY provider_id
	`, identity)
	if err != nil {
		return nil, fmt.Errorf("find retained provider identity: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan retained provider identity: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func insertProviderGeneration(ctx context.Context, tx *dialectAwareTx, providerID string, generation int64, desired struct {
	spec     ProviderSpec
	identity string
}, now time.Time) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO health_provider_generations
			(provider_id, generation, endpoint, port, account, identity_fingerprint, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, providerID, generation, desired.spec.Endpoint, desired.spec.Port, desired.spec.Account, desired.identity, now)
	if err != nil {
		return fmt.Errorf("insert provider generation: %w", err)
	}
	return nil
}

func scanHealthProvider(row rowScanner, provider *HealthProvider) error {
	return row.Scan(&provider.ID, &provider.DisplayName, &provider.Role, &provider.Order,
		&provider.Active, &provider.CurrentGeneration, &provider.ActivationEpoch,
		&provider.ActivatedAt, &provider.TombstonedAt, &provider.CreatedAt, &provider.UpdatedAt)
}

func (r *HealthStateRepository) ListProviders(ctx context.Context, includeTombstoned bool) ([]HealthProvider, error) {
	query := `
		SELECT id, display_name, role, configured_order, active, current_generation,
		       activation_epoch, activated_at, tombstoned_at, created_at, updated_at
		FROM health_providers
	`
	if !includeTombstoned {
		query += ` WHERE active = TRUE`
	}
	query += ` ORDER BY active DESC, configured_order, id`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list providers: %w", err)
	}
	defer rows.Close()
	var providers []HealthProvider
	for rows.Next() {
		var provider HealthProvider
		if err := scanHealthProvider(rows, &provider); err != nil {
			return nil, fmt.Errorf("scan provider: %w", err)
		}
		providers = append(providers, provider)
	}
	return providers, rows.Err()
}

func (r *HealthStateRepository) ListProviderGenerations(ctx context.Context, providerID string) ([]HealthProviderGeneration, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT provider_id, generation, endpoint, port, account, identity_fingerprint, created_at
		FROM health_provider_generations
		WHERE provider_id = ?
		ORDER BY generation
	`, providerID)
	if err != nil {
		return nil, fmt.Errorf("list provider generations: %w", err)
	}
	defer rows.Close()
	var generations []HealthProviderGeneration
	for rows.Next() {
		var generation HealthProviderGeneration
		if err := rows.Scan(&generation.ProviderID, &generation.Generation, &generation.Endpoint,
			&generation.Port, &generation.Account, &generation.IdentityFingerprint,
			&generation.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan provider generation: %w", err)
		}
		generations = append(generations, generation)
	}
	return generations, rows.Err()
}

func (r *HealthStateRepository) CaptureActiveProviderSnapshot(ctx context.Context, at time.Time) (*ProviderSnapshot, error) {
	if at.IsZero() {
		return nil, fmt.Errorf("snapshot time is required")
	}
	snapshot := &ProviderSnapshot{ID: uuid.NewString(), CreatedAt: at.UTC()}
	err := r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO health_provider_snapshots (id, created_at) VALUES (?, ?)`, snapshot.ID, snapshot.CreatedAt); err != nil {
			return fmt.Errorf("insert provider snapshot: %w", err)
		}
		rows, err := tx.QueryContext(ctx, `
			SELECT id, current_generation, activation_epoch, role, configured_order
			FROM health_providers
			WHERE active = TRUE
			ORDER BY configured_order, id
		`)
		if err != nil {
			return fmt.Errorf("read active providers for snapshot: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var entry ProviderSnapshotEntry
			if err := rows.Scan(&entry.ProviderID, &entry.ProviderGeneration,
				&entry.ProviderActivationEpoch, &entry.Role, &entry.Order); err != nil {
				return fmt.Errorf("scan provider snapshot entry: %w", err)
			}
			snapshot.Entries = append(snapshot.Entries, entry)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		for _, entry := range snapshot.Entries {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO health_provider_snapshot_entries
					(snapshot_id, provider_id, provider_generation, provider_activation_epoch,
					 role, configured_order)
				VALUES (?, ?, ?, ?, ?, ?)
			`, snapshot.ID, entry.ProviderID, entry.ProviderGeneration,
				entry.ProviderActivationEpoch, entry.Role, entry.Order); err != nil {
				return fmt.Errorf("insert provider snapshot entry: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (r *HealthStateRepository) GetProviderSnapshot(ctx context.Context, id string) (*ProviderSnapshot, error) {
	var snapshot ProviderSnapshot
	if err := r.db.QueryRowContext(ctx, `SELECT id, created_at FROM health_provider_snapshots WHERE id = ?`, id).Scan(&snapshot.ID, &snapshot.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get provider snapshot: %w", err)
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT provider_id, provider_generation, provider_activation_epoch, role, configured_order
		FROM health_provider_snapshot_entries
		WHERE snapshot_id = ?
		ORDER BY configured_order, provider_id
	`, id)
	if err != nil {
		return nil, fmt.Errorf("get provider snapshot entries: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var entry ProviderSnapshotEntry
		if err := rows.Scan(&entry.ProviderID, &entry.ProviderGeneration,
			&entry.ProviderActivationEpoch, &entry.Role, &entry.Order); err != nil {
			return nil, err
		}
		snapshot.Entries = append(snapshot.Entries, entry)
	}
	return &snapshot, rows.Err()
}

// providerSnapshotMembershipMatchesCurrentTx compares dispatch identity only.
// Provider role/order changes are scheduling changes, not evidence changes, so
// a pure reorder remains compatible with in-flight observation work.
func providerSnapshotMembershipMatchesCurrentTx(
	ctx context.Context,
	tx *dialectAwareTx,
	snapshotID string,
) (bool, error) {
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
		return false, fmt.Errorf("compare provider snapshot membership: %w", err)
	}
	return activeCount > 0 && activeCount == snapshotCount && snapshotCount == matchingCount, nil
}

func (r *HealthStateRepository) providerSnapshotMembershipMatchesCurrent(
	ctx context.Context,
	snapshotID string,
) (bool, error) {
	var activeCount, snapshotCount, matchingCount int
	if err := r.db.QueryRowContext(ctx, `
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
		return false, fmt.Errorf("compare provider snapshot membership: %w", err)
	}
	return activeCount > 0 && activeCount == snapshotCount && snapshotCount == matchingCount, nil
}
