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

func validImportDamagePolicy(policy ImportDamagePolicy) bool {
	return policy == ImportDamagePolicyStrict || policy == ImportDamagePolicyTolerant
}

func validImportValidationPhase(phase ImportValidationPhase) bool {
	switch phase {
	case ImportValidationPhaseInitialPass,
		ImportValidationPhaseConfirmationWait,
		ImportValidationPhaseConfirmationPass,
		ImportValidationPhaseAccepted,
		ImportValidationPhaseHealthPending,
		ImportValidationPhaseRejected:
		return true
	default:
		return false
	}
}

func normalizeImportValidationWrite(
	write ImportValidationWrite,
	now time.Time,
) (ImportValidationWrite, error) {
	if write.ID == "" {
		write.ID = uuid.NewString()
	}
	if write.CreatedAt.IsZero() {
		write.CreatedAt = now
	} else {
		write.CreatedAt = write.CreatedAt.UTC()
	}
	if write.UpdatedAt.IsZero() {
		write.UpdatedAt = write.CreatedAt
	} else {
		write.UpdatedAt = write.UpdatedAt.UTC()
	}
	if write.ConfirmationDueAt != nil {
		due := write.ConfirmationDueAt.UTC()
		write.ConfirmationDueAt = &due
	}
	if write.QueueItemID <= 0 || write.FileRevisionID == "" || write.RunID == "" {
		return ImportValidationWrite{}, fmt.Errorf("queue item, final file revision, and health run are required")
	}
	if write.LeaseOwner == "" || write.FencingToken <= 0 {
		return ImportValidationWrite{}, fmt.Errorf("import validation requires an active lease owner and fencing token")
	}
	if !validImportDamagePolicy(write.DamagePolicy) {
		return ImportValidationWrite{}, fmt.Errorf("invalid import damage policy %q", write.DamagePolicy)
	}
	if !validImportValidationPhase(write.Phase) {
		return ImportValidationWrite{}, fmt.Errorf("invalid import validation phase %q", write.Phase)
	}
	if write.UnresolvedSegments < 0 {
		return ImportValidationWrite{}, fmt.Errorf("unresolved import segment count must be non-negative")
	}
	if write.UpdatedAt.Before(write.CreatedAt) {
		return ImportValidationWrite{}, fmt.Errorf("import validation update predates creation")
	}
	if write.UpdatedAt.After(now.Add(5 * time.Minute)) {
		return ImportValidationWrite{}, fmt.Errorf("import validation update time is too far in the future")
	}
	if write.Phase == ImportValidationPhaseConfirmationWait {
		if write.ConfirmationDueAt == nil {
			return ImportValidationWrite{}, fmt.Errorf("confirmation wait requires a durable deadline")
		}
	} else if write.ConfirmationDueAt != nil {
		return ImportValidationWrite{}, fmt.Errorf("confirmation deadline is valid only while waiting")
	}
	if write.Phase == ImportValidationPhaseHealthPending && write.DamagePolicy != ImportDamagePolicyTolerant {
		return ImportValidationWrite{}, fmt.Errorf("health-pending import requires tolerant damage policy")
	}
	return write, nil
}

func validImportValidationTransition(from, to ImportValidationPhase) bool {
	if from == to {
		return true
	}
	switch from {
	case ImportValidationPhaseInitialPass:
		return to == ImportValidationPhaseConfirmationWait || to == ImportValidationPhaseAccepted
	case ImportValidationPhaseConfirmationWait:
		return to == ImportValidationPhaseConfirmationPass
	case ImportValidationPhaseConfirmationPass:
		return to == ImportValidationPhaseAccepted || to == ImportValidationPhaseHealthPending ||
			to == ImportValidationPhaseRejected
	default:
		return false
	}
}

const importValidationSelect = `
	SELECT id, queue_item_id, file_revision_id, run_id, phase, damage_policy,
	       confirmation_due_at, unresolved_segments, unresolved_bitmap,
	       initial_pass_complete, second_pass_complete, created_at, updated_at
	FROM health_import_validations
`

func scanImportValidation(row rowScanner, validation *ImportValidation) error {
	return row.Scan(
		&validation.ID, &validation.QueueItemID, &validation.FileRevisionID,
		&validation.RunID, &validation.Phase, &validation.DamagePolicy,
		&validation.ConfirmationDueAt, &validation.UnresolvedSegments,
		&validation.UnresolvedBitmap, &validation.InitialPassComplete,
		&validation.SecondPassComplete, &validation.CreatedAt, &validation.UpdatedAt,
	)
}

func (r *HealthStateRepository) GetImportValidation(
	ctx context.Context,
	queueItemID int64,
	fileRevisionID string,
) (*ImportValidation, error) {
	var validation ImportValidation
	err := scanImportValidation(r.db.QueryRowContext(ctx,
		importValidationSelect+` WHERE queue_item_id = ? AND file_revision_id = ?`,
		queueItemID, fileRevisionID), &validation)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get import validation: %w", err)
	}
	return &validation, nil
}

type importValidationRun struct {
	RevisionID string
	SnapshotID string
	Trigger    string
	Mode       string
	Total      int64
	ExpiresAt  time.Time
}

func (r *HealthStateRepository) lockImportValidationRun(
	ctx context.Context,
	tx *dialectAwareTx,
	write ImportValidationWrite,
) (importValidationRun, error) {
	var run importValidationRun
	err := tx.QueryRowContext(ctx, `
		UPDATE health_runs SET updated_at = updated_at
		WHERE id = ? AND status = 'running' AND lease_owner = ? AND fencing_token = ?
		RETURNING file_revision_id, provider_snapshot_id, trigger, mode,
		          total_segments, lease_expires_at
	`, write.RunID, write.LeaseOwner, write.FencingToken).Scan(
		&run.RevisionID, &run.SnapshotID, &run.Trigger, &run.Mode, &run.Total, &run.ExpiresAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return importValidationRun{}, ErrStaleHealthLease
	}
	if err != nil {
		return importValidationRun{}, fmt.Errorf("lock import validation health run: %w", err)
	}
	if !run.ExpiresAt.After(r.now().UTC()) {
		return importValidationRun{}, ErrStaleHealthLease
	}
	if run.RevisionID != write.FileRevisionID {
		return importValidationRun{}, fmt.Errorf("import validation run is bound to a different final file revision")
	}
	if run.Trigger != "import" || run.Mode != "observation" {
		return importValidationRun{}, fmt.Errorf("import validation requires an import-triggered observation run")
	}
	return run, nil
}

// UpsertImportValidation advances one queue-item/final-layout lifecycle while
// freezing its policy/run binding. Pass completion, unresolved positions, and
// terminal admission are derived from fenced committed chunks in this same
// transaction; caller booleans and counts are never authoritative.
func (r *HealthStateRepository) UpsertImportValidation(
	ctx context.Context,
	write ImportValidationWrite,
) (*ImportValidation, error) {
	now := r.now().UTC()
	write, err := normalizeImportValidationWrite(write, now)
	if err != nil {
		return nil, err
	}
	var result ImportValidation
	err = r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		run, err := r.lockImportValidationRun(ctx, tx, write)
		if err != nil {
			return err
		}
		var lockedQueueID int64
		if err := tx.QueryRowContext(ctx, `
			UPDATE import_queue SET updated_at = updated_at
			WHERE id = ? RETURNING id
		`, write.QueueItemID).Scan(&lockedQueueID); errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("import validation queue item does not exist")
		} else if err != nil {
			return fmt.Errorf("lock import validation queue item: %w", err)
		}
		var frozenPolicy ImportDamagePolicy
		err = tx.QueryRowContext(ctx, `
			SELECT damage_policy FROM health_import_validations
			WHERE queue_item_id = ?
			ORDER BY created_at, id LIMIT 1
		`, write.QueueItemID).Scan(&frozenPolicy)
		if err == nil && frozenPolicy != write.DamagePolicy {
			return ErrImportDamagePolicy
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read frozen import queue damage policy: %w", err)
		}

		var boundID string
		var boundQueue int64
		var boundRevision string
		err = tx.QueryRowContext(ctx, `
			SELECT id, queue_item_id, file_revision_id
			FROM health_import_validations WHERE run_id = ?
		`, write.RunID).Scan(&boundID, &boundQueue, &boundRevision)
		if err == nil && (boundID != write.ID || boundQueue != write.QueueItemID ||
			boundRevision != write.FileRevisionID) {
			return fmt.Errorf("one import health run cannot be rebound to another validation")
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read import validation run binding: %w", err)
		}

		var existing ImportValidation
		err = scanImportValidation(tx.QueryRowContext(ctx,
			importValidationSelect+` WHERE queue_item_id = ? AND file_revision_id = ?`,
			write.QueueItemID, write.FileRevisionID), &existing)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			if write.Phase != ImportValidationPhaseInitialPass || write.InitialPassComplete ||
				write.SecondPassComplete || write.ConfirmationDueAt != nil {
				return fmt.Errorf("an import validation must begin in an incomplete initial pass")
			}
			empty := make([]byte, bitmapByteLength(run.Total))
			_, err = tx.ExecContext(ctx, `
				INSERT INTO health_import_validations
					(id, queue_item_id, file_revision_id, run_id, phase, damage_policy,
					 confirmation_due_at, unresolved_segments, unresolved_bitmap,
					 initial_pass_complete, second_pass_complete, created_at, updated_at)
				VALUES (?, ?, ?, ?, 'initial_pass', ?, NULL, 0, ?, FALSE, FALSE, ?, ?)
			`, write.ID, write.QueueItemID, write.FileRevisionID, write.RunID,
				write.DamagePolicy, empty, write.CreatedAt, write.UpdatedAt)
			if err != nil {
				return fmt.Errorf("create import validation: %w", err)
			}
		case err != nil:
			return fmt.Errorf("find import validation: %w", err)
		default:
			if err := validateImportValidationIdentity(existing, write); err != nil {
				return err
			}
			if !validImportValidationTransition(existing.Phase, write.Phase) {
				return fmt.Errorf("invalid import validation transition from %q to %q", existing.Phase, write.Phase)
			}
			if existing.Phase == ImportValidationPhaseConfirmationWait &&
				write.Phase == ImportValidationPhaseConfirmationWait &&
				!sameTimePointers(existing.ConfirmationDueAt, write.ConfirmationDueAt) {
				return fmt.Errorf("import confirmation deadline is immutable while waiting")
			}

			requestedUnresolvedSegments := write.UnresolvedSegments
			requestedUnresolvedBitmap := append([]byte(nil), write.UnresolvedBitmap...)
			write.InitialPassComplete = existing.InitialPassComplete
			write.SecondPassComplete = existing.SecondPassComplete
			write.UnresolvedSegments = existing.UnresolvedSegments
			write.UnresolvedBitmap = append([]byte(nil), existing.UnresolvedBitmap...)
			if err := r.deriveImportValidationTransition(
				ctx, tx, run, existing, &write, now,
				requestedUnresolvedSegments, requestedUnresolvedBitmap,
			); err != nil {
				return err
			}
			if terminalImportValidationPhase(write.Phase) {
				current, err := providerSnapshotMembershipMatchesCurrentTx(ctx, tx, run.SnapshotID)
				if err != nil {
					return err
				}
				if !current {
					return ErrProviderSnapshotMismatch
				}
			}
			updated, err := tx.ExecContext(ctx, `
				UPDATE health_import_validations
				SET phase = ?, confirmation_due_at = ?, unresolved_segments = ?,
				    unresolved_bitmap = ?, initial_pass_complete = ?,
				    second_pass_complete = ?, updated_at = ?
				WHERE id = ? AND phase = ? AND updated_at = ?
			`, write.Phase, write.ConfirmationDueAt, write.UnresolvedSegments,
				write.UnresolvedBitmap, write.InitialPassComplete, write.SecondPassComplete,
				write.UpdatedAt, write.ID, existing.Phase, existing.UpdatedAt)
			if err != nil {
				return fmt.Errorf("advance import validation: %w", err)
			}
			if rows, err := updated.RowsAffected(); err != nil {
				return fmt.Errorf("read import validation update result: %w", err)
			} else if rows == 0 {
				return fmt.Errorf("import validation changed concurrently")
			}
			if terminalImportValidationPhase(write.Phase) {
				if err := finishImportValidationRun(ctx, tx, write, now); err != nil {
					return err
				}
			}
		}
		return scanImportValidation(tx.QueryRowContext(ctx,
			importValidationSelect+` WHERE id = ?`, write.ID), &result)
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// GetImportQueueDamagePolicy returns the first policy durably frozen for a
// queue item. Every final file produced by that queue item must use it.
func (r *HealthStateRepository) GetImportQueueDamagePolicy(
	ctx context.Context,
	queueItemID int64,
) (ImportDamagePolicy, bool, error) {
	if queueItemID <= 0 {
		return "", false, fmt.Errorf("positive import queue item ID is required")
	}
	var policy ImportDamagePolicy
	err := r.db.QueryRowContext(ctx, `
		SELECT damage_policy FROM health_import_validations
		WHERE queue_item_id = ?
		ORDER BY created_at, id LIMIT 1
	`, queueItemID).Scan(&policy)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("get import queue damage policy: %w", err)
	}
	return policy, true, nil
}

// ValidateImportProviderSnapshotCurrent fences terminal admission against the
// current provider membership while deliberately tolerating order/role-only
// changes. UpsertImportValidation performs the same check atomically at its
// terminal transition; this read seam lets a resumable importer decide whether
// it must abandon and reseed before doing further network work.
func (r *HealthStateRepository) ValidateImportProviderSnapshotCurrent(
	ctx context.Context,
	runID string,
) error {
	if strings.TrimSpace(runID) == "" {
		return fmt.Errorf("import health run ID is required")
	}
	var snapshotID, trigger, mode string
	if err := r.db.QueryRowContext(ctx, `
		SELECT provider_snapshot_id, trigger, mode FROM health_runs WHERE id = ?
	`, runID).Scan(&snapshotID, &trigger, &mode); errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("import health run does not exist")
	} else if err != nil {
		return fmt.Errorf("read import health run snapshot: %w", err)
	}
	if trigger != "import" || mode != "observation" {
		return fmt.Errorf("provider snapshot validation requires an import observation run")
	}
	current, err := r.providerSnapshotMembershipMatchesCurrent(ctx, snapshotID)
	if err != nil {
		return err
	}
	if !current {
		return ErrProviderSnapshotMismatch
	}
	return nil
}

// AbandonImportValidation detaches only the current nonterminal validation
// after a provider-set change. The old run/chunks/attempts remain immutable
// history and a replacement run must capture its own provider snapshot.
func (r *HealthStateRepository) AbandonImportValidation(
	ctx context.Context,
	queueItemID int64,
	fileRevisionID, expectedRunID string,
	at time.Time,
) error {
	if queueItemID <= 0 || strings.TrimSpace(fileRevisionID) == "" ||
		strings.TrimSpace(expectedRunID) == "" || at.IsZero() {
		return fmt.Errorf("queue item, file revision, expected run, and abandonment time are required")
	}
	at = at.UTC()
	return r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		var validationID, currentRunID string
		var phase ImportValidationPhase
		err := tx.QueryRowContext(ctx, `
			UPDATE health_import_validations SET updated_at = updated_at
			WHERE queue_item_id = ? AND file_revision_id = ?
			RETURNING id, run_id, phase
		`, queueItemID, fileRevisionID).Scan(&validationID, &currentRunID, &phase)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("lock import validation for abandonment: %w", err)
		}
		if currentRunID != expectedRunID {
			return ErrStaleImportValidation
		}
		if phase == ImportValidationPhaseRejected {
			return fmt.Errorf("rejected import validation cannot be abandoned")
		}
		if phase == ImportValidationPhaseAccepted || phase == ImportValidationPhaseHealthPending {
			var active bool
			if err := tx.QueryRowContext(ctx, `
				SELECT active FROM health_file_revisions WHERE id = ?
			`, fileRevisionID).Scan(&active); err != nil {
				return fmt.Errorf("read admitted candidate publication state: %w", err)
			}
			if active {
				return fmt.Errorf("published import validation cannot be abandoned")
			}
		}
		var trigger, mode string
		if err := tx.QueryRowContext(ctx, `
			SELECT trigger, mode FROM health_runs WHERE id = ?
		`, currentRunID).Scan(&trigger, &mode); err != nil {
			return fmt.Errorf("read abandoned import health run: %w", err)
		}
		if trigger != "import" || mode != "observation" {
			return fmt.Errorf("only an import observation run may be abandoned")
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE health_runs
			SET status = 'canceled', lease_owner = NULL, lease_expires_at = NULL,
			    cancel_requested = TRUE, updated_at = ?, completed_at = ?
			WHERE id = ? AND status IN ('pending', 'running', 'paused')
		`, at, at, currentRunID); err != nil {
			return fmt.Errorf("cancel abandoned import health run: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE health_run_schedule SET active = FALSE, updated_at = ?
			WHERE run_id = ? AND active = TRUE
		`, at, currentRunID); err != nil {
			return fmt.Errorf("retire abandoned import schedule: %w", err)
		}
		deleted, err := tx.ExecContext(ctx, `
			DELETE FROM health_import_validations WHERE id = ? AND run_id = ?
		`, validationID, currentRunID)
		if err != nil {
			return fmt.Errorf("reset abandoned import validation: %w", err)
		}
		if rows, err := deleted.RowsAffected(); err != nil {
			return fmt.Errorf("read abandoned import validation result: %w", err)
		} else if rows != 1 {
			return ErrStaleImportValidation
		}
		return nil
	})
}

// RetireUnboundImportRun removes schedulable ownership from a provisional
// import run that lost the queue-policy race before any validation bound to it.
// Any already-committed transport evidence remains immutable history.
func (r *HealthStateRepository) RetireUnboundImportRun(
	ctx context.Context,
	runID, revisionID string,
	at time.Time,
) error {
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(revisionID) == "" || at.IsZero() {
		return fmt.Errorf("import run, revision, and retirement time are required")
	}
	at = at.UTC()
	return r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		var boundRevision, trigger, mode string
		err := tx.QueryRowContext(ctx, `
			UPDATE health_runs SET updated_at = updated_at
			WHERE id = ? RETURNING file_revision_id, trigger, mode
		`, runID).Scan(&boundRevision, &trigger, &mode)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("lock unbound import run: %w", err)
		}
		if boundRevision != revisionID || trigger != "import" || mode != "observation" {
			return ErrStaleImportValidation
		}
		var references int
		if err := tx.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM health_import_validations WHERE run_id = ?
		`, runID).Scan(&references); err != nil {
			return fmt.Errorf("check provisional import run binding: %w", err)
		}
		if references != 0 {
			return ErrStaleImportValidation
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE health_runs
			SET status = CASE WHEN status IN ('completed', 'failed', 'canceled') THEN status ELSE 'canceled' END,
			    lease_owner = NULL, lease_expires_at = NULL, cancel_requested = TRUE,
			    updated_at = ?, completed_at = COALESCE(completed_at, ?)
			WHERE id = ?
		`, at, at, runID); err != nil {
			return fmt.Errorf("retire unbound import run: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE health_run_schedule SET active = FALSE, updated_at = ?
			WHERE run_id = ? AND active = TRUE
		`, at, runID); err != nil {
			return fmt.Errorf("retire unbound import schedule: %w", err)
		}
		return nil
	})
}

func validateImportValidationIdentity(existing ImportValidation, write ImportValidationWrite) error {
	if existing.ID != write.ID {
		return fmt.Errorf("final file already has a different import validation identity")
	}
	if existing.RunID != write.RunID {
		return fmt.Errorf("import validation cannot be rebound to another health run")
	}
	if existing.DamagePolicy != write.DamagePolicy {
		return fmt.Errorf("import validation damage policy is immutable")
	}
	if !existing.CreatedAt.Equal(write.CreatedAt) {
		return fmt.Errorf("import validation creation time is immutable")
	}
	if write.UpdatedAt.Before(existing.UpdatedAt) {
		return fmt.Errorf("stale import validation update")
	}
	return nil
}

func (r *HealthStateRepository) deriveImportValidationTransition(
	ctx context.Context,
	tx *dialectAwareTx,
	run importValidationRun,
	existing ImportValidation,
	write *ImportValidationWrite,
	now time.Time,
	requestedUnresolvedSegments int64,
	requestedUnresolvedBitmap []byte,
) error {
	if existing.Phase == write.Phase {
		return nil
	}
	switch existing.Phase {
	case ImportValidationPhaseInitialPass:
		unresolved, err := deriveInitialImportUnresolved(ctx, tx, write.RunID, run.SnapshotID, run.Total)
		if err != nil {
			return err
		}
		write.InitialPassComplete = true
		write.SecondPassComplete = false
		write.UnresolvedBitmap = unresolved
		write.UnresolvedSegments = bitmapPopulation(unresolved)
		if err := requireExactImportUnresolved(
			run.Total, requestedUnresolvedSegments, requestedUnresolvedBitmap, unresolved,
		); err != nil {
			return fmt.Errorf("initial import unresolved state: %w", err)
		}
		if write.UnresolvedSegments == 0 {
			if write.Phase != ImportValidationPhaseAccepted {
				return fmt.Errorf("a complete available initial pass must be accepted")
			}
			write.ConfirmationDueAt = nil
			return nil
		}
		if write.Phase != ImportValidationPhaseConfirmationWait || write.ConfirmationDueAt == nil {
			return fmt.Errorf("a complete unresolved initial pass must enter durable confirmation wait")
		}
		if !write.ConfirmationDueAt.After(now) {
			return fmt.Errorf("import confirmation deadline must be in the future")
		}
		return nil
	case ImportValidationPhaseConfirmationWait:
		if write.Phase != ImportValidationPhaseConfirmationPass {
			return fmt.Errorf("a waiting import validation must enter its confirmation pass")
		}
		if existing.ConfirmationDueAt == nil || now.Before(existing.ConfirmationDueAt.UTC()) {
			return fmt.Errorf("import confirmation pass is not due")
		}
		if write.UpdatedAt.Before(existing.ConfirmationDueAt.UTC()) {
			return fmt.Errorf("import confirmation pass timestamp predates its durable wait")
		}
		write.ConfirmationDueAt = nil
		return nil
	case ImportValidationPhaseConfirmationPass:
		finalUnresolved, err := deriveConfirmationImportUnresolved(
			ctx, tx, write.RunID, run.SnapshotID, run.Total,
			existing.UnresolvedBitmap, existing.UpdatedAt,
		)
		if err != nil {
			return err
		}
		write.InitialPassComplete = true
		write.SecondPassComplete = true
		write.UnresolvedBitmap = finalUnresolved
		write.UnresolvedSegments = bitmapPopulation(finalUnresolved)
		if err := requireExactImportUnresolved(
			run.Total, requestedUnresolvedSegments, requestedUnresolvedBitmap, finalUnresolved,
		); err != nil {
			return fmt.Errorf("terminal import unresolved state: %w", err)
		}
		if write.UnresolvedSegments == 0 {
			if write.Phase != ImportValidationPhaseAccepted {
				return fmt.Errorf("a fully available confirmation pass must be accepted")
			}
			return nil
		}
		if write.Phase == ImportValidationPhaseAccepted {
			return fmt.Errorf("accepted import validation cannot retain unresolved positions")
		}
		if write.Phase == ImportValidationPhaseHealthPending && write.DamagePolicy != ImportDamagePolicyTolerant {
			return fmt.Errorf("health-pending import requires tolerant damage policy")
		}
		return nil
	default:
		return fmt.Errorf("terminal import validation cannot advance")
	}
}

func requireExactImportUnresolved(
	total, requestedCount int64,
	requested, authoritative []byte,
) error {
	if len(requested) == 0 && requestedCount == 0 {
		requested = make([]byte, bitmapByteLength(total))
	}
	if len(requested) != bitmapByteLength(total) ||
		bitmapPopulation(requested) != requestedCount || !sameBitmap(requested, authoritative) {
		return fmt.Errorf("caller state does not match durable positional evidence")
	}
	return nil
}

type importProviderKey struct {
	ID              string
	Generation      int64
	ActivationEpoch int64
}

type importStageCoverage struct {
	providers    []importProviderKey
	testedBy     map[importProviderKey][]byte
	testedUnion  []byte
	presentUnion []byte
}

func loadImportStageCoverage(
	ctx context.Context,
	tx *dialectAwareTx,
	runID, snapshotID, stage string,
	total int64,
	notBefore time.Time,
) (importStageCoverage, error) {
	coverage := importStageCoverage{
		testedBy:     make(map[importProviderKey][]byte),
		testedUnion:  make([]byte, bitmapByteLength(total)),
		presentUnion: make([]byte, bitmapByteLength(total)),
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT provider_id, provider_generation, provider_activation_epoch
		FROM health_provider_snapshot_entries
		WHERE snapshot_id = ?
		ORDER BY CASE role WHEN 'primary' THEN 0 ELSE 1 END, configured_order, provider_id
	`, snapshotID)
	if err != nil {
		return importStageCoverage{}, fmt.Errorf("read import provider snapshot: %w", err)
	}
	for rows.Next() {
		var provider importProviderKey
		if err := rows.Scan(&provider.ID, &provider.Generation, &provider.ActivationEpoch); err != nil {
			rows.Close()
			return importStageCoverage{}, fmt.Errorf("scan import provider snapshot: %w", err)
		}
		coverage.providers = append(coverage.providers, provider)
		coverage.testedBy[provider] = make([]byte, bitmapByteLength(total))
	}
	if err := rows.Close(); err != nil {
		return importStageCoverage{}, fmt.Errorf("close import provider snapshot: %w", err)
	}
	if err := rows.Err(); err != nil {
		return importStageCoverage{}, fmt.Errorf("iterate import provider snapshot: %w", err)
	}
	if len(coverage.providers) == 0 {
		return importStageCoverage{}, fmt.Errorf("import validation provider snapshot is empty")
	}

	rows, err = tx.QueryContext(ctx, `
		SELECT provider_id, provider_generation, provider_activation_epoch,
		       observation_kind, segment_start, segment_count, tested_bitmap, present_bitmap
		FROM health_run_chunks
		WHERE run_id = ? AND stage = ? AND committed_at >= ?
		ORDER BY committed_at, segment_start, id
	`, runID, stage, notBefore.UTC())
	if err != nil {
		return importStageCoverage{}, fmt.Errorf("read import stage chunks: %w", err)
	}
	for rows.Next() {
		var provider importProviderKey
		var kind HealthObservationKind
		var start, count int64
		var tested, present []byte
		if err := rows.Scan(&provider.ID, &provider.Generation, &provider.ActivationEpoch,
			&kind, &start, &count, &tested, &present); err != nil {
			rows.Close()
			return importStageCoverage{}, fmt.Errorf("scan import stage chunk: %w", err)
		}
		providerTested, current := coverage.testedBy[provider]
		if !current {
			rows.Close()
			return importStageCoverage{}, ErrProviderSnapshotMismatch
		}
		if kind != HealthObservationSTAT {
			rows.Close()
			return importStageCoverage{}, fmt.Errorf("import availability stages require STAT observations")
		}
		orRelativeBitmap(providerTested, tested, start, count)
		orRelativeBitmap(coverage.testedUnion, tested, start, count)
		orRelativeBitmap(coverage.presentUnion, present, start, count)
	}
	if err := rows.Close(); err != nil {
		return importStageCoverage{}, fmt.Errorf("close import stage chunks: %w", err)
	}
	if err := rows.Err(); err != nil {
		return importStageCoverage{}, fmt.Errorf("iterate import stage chunks: %w", err)
	}
	return coverage, nil
}

func deriveInitialImportUnresolved(
	ctx context.Context,
	tx *dialectAwareTx,
	runID, snapshotID string,
	total int64,
) ([]byte, error) {
	coverage, err := loadImportStageCoverage(
		ctx, tx, runID, snapshotID, HealthRunStageImportInitialSTAT, total,
		time.Unix(0, 0).UTC(),
	)
	if err != nil {
		return nil, err
	}
	all := fullBitmap(total)
	if !sameBitmap(coverage.testedUnion, all) {
		return nil, fmt.Errorf("initial import pass does not cover every canonical position")
	}
	if !sameBitmap(coverage.testedBy[coverage.providers[0]], all) {
		return nil, fmt.Errorf("initial import pass does not fully test the first configured provider")
	}
	unresolved := bitmapDifference(all, coverage.presentUnion)
	for _, provider := range coverage.providers {
		if !bitmapContains(coverage.testedBy[provider], unresolved) {
			return nil, fmt.Errorf("initial import pass omits an intended provider for an unresolved position")
		}
	}
	return unresolved, nil
}

func deriveConfirmationImportUnresolved(
	ctx context.Context,
	tx *dialectAwareTx,
	runID, snapshotID string,
	total int64,
	initialUnresolved []byte,
	notBefore time.Time,
) ([]byte, error) {
	if len(initialUnresolved) != bitmapByteLength(total) {
		return nil, fmt.Errorf("persisted initial unresolved bitmap has the wrong canonical size")
	}
	coverage, err := loadImportStageCoverage(
		ctx, tx, runID, snapshotID, HealthRunStageImportConfirmationSTAT, total, notBefore,
	)
	if err != nil {
		return nil, err
	}
	if !bitmapContains(initialUnresolved, coverage.testedUnion) {
		return nil, fmt.Errorf("confirmation pass tested positions outside its durable target set")
	}
	for _, provider := range coverage.providers {
		if !sameBitmap(coverage.testedBy[provider], initialUnresolved) {
			return nil, fmt.Errorf("confirmation pass omits or exceeds an intended provider target")
		}
	}
	return bitmapDifference(initialUnresolved, coverage.presentUnion), nil
}

func bitmapByteLength(total int64) int {
	if total <= 0 {
		return 0
	}
	return int((total + 7) / 8)
}

func fullBitmap(total int64) []byte {
	bitmap := make([]byte, bitmapByteLength(total))
	for position := int64(0); position < total; position++ {
		bitmap[position/8] |= 1 << uint(position%8)
	}
	return bitmap
}

func orRelativeBitmap(destination, source []byte, start, count int64) {
	for relative := int64(0); relative < count; relative++ {
		if relative/8 < int64(len(source)) && bitmapSet(source, relative) {
			position := start + relative
			if position >= 0 && position/8 < int64(len(destination)) {
				destination[position/8] |= 1 << uint(position%8)
			}
		}
	}
}

func sameBitmap(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func bitmapContains(superset, subset []byte) bool {
	if len(superset) != len(subset) {
		return false
	}
	for i := range superset {
		if subset[i]&^superset[i] != 0 {
			return false
		}
	}
	return true
}

func bitmapDifference(left, right []byte) []byte {
	result := append([]byte(nil), left...)
	for i := range result {
		if i < len(right) {
			result[i] &^= right[i]
		}
	}
	return result
}

func terminalImportValidationPhase(phase ImportValidationPhase) bool {
	return phase == ImportValidationPhaseAccepted || phase == ImportValidationPhaseHealthPending ||
		phase == ImportValidationPhaseRejected
}

func finishImportValidationRun(
	ctx context.Context,
	tx *dialectAwareTx,
	write ImportValidationWrite,
	at time.Time,
) error {
	updated, err := tx.ExecContext(ctx, `
		UPDATE health_runs
		SET status = 'completed', lease_owner = NULL, lease_expires_at = NULL,
		    last_error = NULL, updated_at = ?, completed_at = ?
		WHERE id = ? AND status = 'running' AND lease_owner = ? AND fencing_token = ?
		  AND lease_expires_at > ? AND cancel_requested = FALSE
	`, at, at, write.RunID, write.LeaseOwner, write.FencingToken, at)
	if err != nil {
		return fmt.Errorf("complete import validation health run: %w", err)
	}
	if rows, err := updated.RowsAffected(); err != nil {
		return fmt.Errorf("read completed import validation run result: %w", err)
	} else if rows == 0 {
		return ErrStaleHealthLease
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE health_run_schedule SET active = FALSE, updated_at = ? WHERE run_id = ?
	`, at, write.RunID); err != nil {
		return fmt.Errorf("retire completed import validation schedule: %w", err)
	}
	return nil
}

func sameTimePointers(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(right.UTC())
}

// ListDueImportValidations discovers both due waits and interrupted active
// confirmation passes after restart.
func (r *HealthStateRepository) ListDueImportValidations(
	ctx context.Context,
	at time.Time,
	limit int,
) ([]ImportValidation, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("positive import validation limit is required")
	}
	if at.IsZero() {
		at = r.now().UTC()
	} else {
		at = at.UTC()
	}
	rows, err := r.db.QueryContext(ctx, importValidationSelect+`
		WHERE (phase = 'confirmation_wait' AND confirmation_due_at <= ?)
		   OR phase = 'confirmation_pass'
		ORDER BY CASE WHEN phase = 'confirmation_pass' THEN 0 ELSE 1 END,
		         confirmation_due_at, created_at, id
		LIMIT ?
	`, at, limit)
	if err != nil {
		return nil, fmt.Errorf("list due import validations: %w", err)
	}
	defer rows.Close()
	var validations []ImportValidation
	for rows.Next() {
		var validation ImportValidation
		if err := scanImportValidation(rows, &validation); err != nil {
			return nil, fmt.Errorf("scan due import validation: %w", err)
		}
		validations = append(validations, validation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate due import validations: %w", err)
	}
	return validations, nil
}
