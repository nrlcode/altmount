package database

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"strings"
	"time"

	"github.com/google/uuid"
)

func (r *HealthStateRepository) CreateHealthRun(ctx context.Context, spec HealthRunSpec) (*HealthRun, error) {
	if spec.FileRevisionID == "" || spec.ProviderSnapshotID == "" || spec.Trigger == "" || spec.Mode == "" {
		return nil, fmt.Errorf("revision, provider snapshot, trigger, and mode are required")
	}
	if spec.TotalSegments < 0 {
		return nil, fmt.Errorf("total segments must be non-negative")
	}
	if spec.ID == "" {
		spec.ID = uuid.NewString()
	}
	if spec.CreatedAt.IsZero() {
		spec.CreatedAt = time.Now().UTC()
	} else {
		spec.CreatedAt = spec.CreatedAt.UTC()
	}
	err := r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		var revisionSegments int64
		if err := tx.QueryRowContext(ctx, `
			SELECT segment_count FROM health_file_revisions WHERE id = ?
		`, spec.FileRevisionID).Scan(&revisionSegments); err != nil {
			return fmt.Errorf("read health run file revision: %w", err)
		}
		if revisionSegments != spec.TotalSegments {
			return fmt.Errorf("health run total does not match file revision segment count")
		}
		_, err := tx.ExecContext(ctx, `
			INSERT INTO health_runs
				(id, file_revision_id, provider_snapshot_id, trigger, mode, status,
				 total_segments, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?)
		`, spec.ID, spec.FileRevisionID, spec.ProviderSnapshotID, spec.Trigger, spec.Mode,
			spec.TotalSegments, spec.CreatedAt, spec.CreatedAt)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("create health run: %w", err)
	}
	return r.GetHealthRun(ctx, spec.ID)
}

const healthRunSelect = `
	SELECT id, file_revision_id, provider_snapshot_id, trigger, mode, status,
	       lease_owner, lease_expires_at, fencing_token, total_segments,
	       resolved_segments, provider_checks, missing_candidates, inconclusive_count,
	       stage, current_provider_id, current_provider_generation, cursor_segment,
	       pause_requested, cancel_requested, created_at, started_at, updated_at, completed_at,
	       COALESCE(last_error, '')
	FROM health_runs
`

func scanHealthRun(row rowScanner, run *HealthRun) error {
	return row.Scan(&run.ID, &run.FileRevisionID, &run.ProviderSnapshotID, &run.Trigger,
		&run.Mode, &run.Status, &run.LeaseOwner, &run.LeaseExpiresAt, &run.FencingToken,
		&run.TotalSegments, &run.ResolvedSegments, &run.ProviderChecks,
		&run.MissingCandidates, &run.InconclusiveCount, &run.Stage,
		&run.CurrentProviderID, &run.CurrentProviderGeneration, &run.CursorSegment,
		&run.PauseRequested, &run.CancelRequested, &run.CreatedAt, &run.StartedAt,
		&run.UpdatedAt, &run.CompletedAt, &run.LastError)
}

func (r *HealthStateRepository) GetHealthRun(ctx context.Context, id string) (*HealthRun, error) {
	var run HealthRun
	if err := scanHealthRun(r.db.QueryRowContext(ctx, healthRunSelect+` WHERE id = ?`, id), &run); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get health run: %w", err)
	}
	return &run, nil
}

// ListHealthRuns returns committed progress snapshots, newest first. The
// bounded limit keeps the operator progress API from becoming an unbounded
// history scan.
func (r *HealthStateRepository) ListHealthRuns(ctx context.Context, limit int) ([]HealthRun, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("positive health run limit is required")
	}
	if limit > 500 {
		limit = 500
	}
	rows, err := r.db.QueryContext(ctx, healthRunSelect+`
		ORDER BY updated_at DESC, id DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list health runs: %w", err)
	}
	defer rows.Close()

	runs := make([]HealthRun, 0, limit)
	for rows.Next() {
		var run HealthRun
		if err := scanHealthRun(rows, &run); err != nil {
			return nil, fmt.Errorf("scan health run: %w", err)
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate health runs: %w", err)
	}
	return runs, nil
}

// HasReusableCompletedImportSTATCoverage reports whether an accepted import
// already committed complete positional STAT coverage for the current provider
// activation set. It never promotes BODY integrity and deliberately rejects a
// stale provider snapshot so newly activated providers still receive work.
func (r *HealthStateRepository) HasReusableCompletedImportSTATCoverage(
	ctx context.Context,
	revisionID string,
	totalSegments int64,
) (bool, error) {
	coverage, err := r.GetCompletedImportSTATCoverage(ctx, revisionID, totalSegments)
	if err != nil || coverage == nil {
		return false, err
	}
	return coverage.Reusable, nil
}

func (r *HealthStateRepository) GetCompletedImportSTATCoverage(
	ctx context.Context,
	revisionID string,
	totalSegments int64,
) (*CompletedImportSTATCoverage, error) {
	if strings.TrimSpace(revisionID) == "" || totalSegments <= 0 {
		return nil, nil
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT validation.id, r.id, r.provider_snapshot_id, validation.phase,
		       validation.unresolved_segments, validation.unresolved_bitmap
		FROM health_import_validations validation
		JOIN health_runs r ON r.id = validation.run_id
		JOIN health_file_revisions revision ON revision.id = r.file_revision_id
		WHERE validation.file_revision_id = ?
		  AND validation.phase IN ('accepted', 'health_pending')
		  AND r.trigger = 'import' AND r.mode = 'observation'
		  AND r.status = 'completed' AND r.total_segments = ?
		  AND revision.active = TRUE
		  AND (validation.phase <> 'accepted' OR validation.coverage_reused_at IS NULL)
		  AND (validation.phase <> 'accepted' OR NOT EXISTS (
		    SELECT 1 FROM health_runs newer
		    WHERE newer.file_revision_id = r.file_revision_id
		      AND newer.mode = 'observation' AND newer.trigger <> 'import'
		      AND newer.created_at >= r.completed_at
		  ))
		  AND (validation.phase <> 'health_pending' OR validation.health_pending_settled_at IS NULL)
		ORDER BY r.completed_at DESC, r.id DESC
	`, revisionID, totalSegments)
	if err != nil {
		return nil, fmt.Errorf("list completed import coverage: %w", err)
	}
	type candidate struct {
		validationID     string
		runID            string
		snapshotID       string
		phase            ImportValidationPhase
		unresolvedCount  int64
		unresolvedBitmap []byte
	}
	var candidates []candidate
	for rows.Next() {
		var value candidate
		if err := rows.Scan(
			&value.validationID, &value.runID, &value.snapshotID, &value.phase,
			&value.unresolvedCount, &value.unresolvedBitmap,
		); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan completed import coverage: %w", err)
		}
		candidates = append(candidates, value)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate completed import coverage: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close completed import coverage: %w", err)
	}

	for _, value := range candidates {
		currentSnapshot, err := r.providerSnapshotMembershipMatchesCurrent(ctx, value.snapshotID)
		if err != nil {
			return nil, err
		}
		if !currentSnapshot {
			continue
		}
		complete, err := r.importRunHasFullSTATCoverage(ctx, value.runID, totalSegments)
		if err != nil {
			return nil, err
		}
		if !complete {
			continue
		}
		coverage := &CompletedImportSTATCoverage{
			ValidationID: value.validationID, RunID: value.runID, ProviderSnapshotID: value.snapshotID,
		}
		switch value.phase {
		case ImportValidationPhaseAccepted:
			coverage.Reusable = true
			return coverage, nil
		case ImportValidationPhaseHealthPending:
			if len(value.unresolvedBitmap) != int((totalSegments+7)/8) {
				return nil, fmt.Errorf("health-pending unresolved bitmap is malformed")
			}
			for position := int64(0); position < totalSegments; position++ {
				if bitmapSet(value.unresolvedBitmap, position) {
					coverage.UnresolvedPositions = append(coverage.UnresolvedPositions, position)
				}
			}
			if int64(len(coverage.UnresolvedPositions)) != value.unresolvedCount ||
				value.unresolvedCount <= 0 {
				return nil, fmt.Errorf("health-pending unresolved evidence is inconsistent")
			}
			coverage.HealthPending = true
			return coverage, nil
		}
	}
	return nil, nil
}

// ConsumeReusableCompletedImportSTATCoverage claims the accepted import pass
// exactly once. It suppresses only the immediate duplicate sweep; later
// ordinary cadences must perform fresh observation work, including after a
// restart.
func (r *HealthStateRepository) ConsumeReusableCompletedImportSTATCoverage(
	ctx context.Context,
	revisionID string,
	totalSegments int64,
	at time.Time,
) (*CompletedImportSTATCoverage, error) {
	return r.consumeReusableCompletedImportSTATCoverage(
		ctx, revisionID, totalSegments, at, nil,
	)
}

type reusableImportCoverageDeferral struct {
	filePath    string
	nextCheckAt time.Time
}

// ConsumeReusableCompletedImportSTATCoverageAndDeferHealth atomically claims
// the accepted import pass and moves the next ordinary observation time. A
// missing or mismatched active file-health identity rolls the claim back, so a
// scheduler failure cannot permanently burn the one-shot reuse marker.
func (r *HealthStateRepository) ConsumeReusableCompletedImportSTATCoverageAndDeferHealth(
	ctx context.Context,
	revisionID string,
	totalSegments int64,
	filePath string,
	nextCheckAt time.Time,
	at time.Time,
) (*CompletedImportSTATCoverage, error) {
	filePath = normalizeHealthPath(filePath)
	if filePath == "" || nextCheckAt.IsZero() {
		return nil, fmt.Errorf("file path and next health check time are required")
	}
	return r.consumeReusableCompletedImportSTATCoverage(
		ctx, revisionID, totalSegments, at,
		&reusableImportCoverageDeferral{
			filePath: filePath, nextCheckAt: nextCheckAt.UTC(),
		},
	)
}

func (r *HealthStateRepository) consumeReusableCompletedImportSTATCoverage(
	ctx context.Context,
	revisionID string,
	totalSegments int64,
	at time.Time,
	deferral *reusableImportCoverageDeferral,
) (*CompletedImportSTATCoverage, error) {
	if at.IsZero() {
		at = r.now().UTC()
	} else {
		at = at.UTC()
	}
	for attempt := 0; attempt < 2; attempt++ {
		coverage, err := r.GetCompletedImportSTATCoverage(ctx, revisionID, totalSegments)
		if err != nil || coverage == nil || !coverage.Reusable {
			return nil, err
		}
		claimed := false
		err = r.withTransaction(ctx, func(tx *dialectAwareTx) error {
			var snapshotID string
			err := tx.QueryRowContext(ctx, `
				SELECT run.provider_snapshot_id
				FROM health_import_validations validation
				JOIN health_runs run ON run.id = validation.run_id
				JOIN health_file_revisions revision ON revision.id = validation.file_revision_id
				WHERE validation.id = ? AND validation.file_revision_id = ?
				  AND validation.phase = 'accepted' AND validation.coverage_reused_at IS NULL
				  AND run.status = 'completed' AND run.trigger = 'import'
				  AND run.mode = 'observation' AND run.total_segments = ?
				  AND revision.active = TRUE
				  AND NOT EXISTS (
				    SELECT 1 FROM health_runs newer
				    WHERE newer.file_revision_id = run.file_revision_id
				      AND newer.mode = 'observation' AND newer.trigger <> 'import'
				      AND newer.created_at >= run.completed_at
				  )
			`, coverage.ValidationID, revisionID, totalSegments).Scan(&snapshotID)
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			if err != nil {
				return fmt.Errorf("lock reusable import coverage: %w", err)
			}
			current, err := providerSnapshotMembershipMatchesCurrentTx(ctx, tx, snapshotID)
			if err != nil {
				return err
			}
			if !current {
				return nil
			}
			updated, err := tx.ExecContext(ctx, `
				UPDATE health_import_validations SET coverage_reused_at = ?, updated_at = CASE
				  WHEN updated_at > ? THEN updated_at ELSE ? END
				WHERE id = ? AND coverage_reused_at IS NULL AND phase = 'accepted'
			`, at, at, at, coverage.ValidationID)
			if err != nil {
				return fmt.Errorf("consume reusable import coverage: %w", err)
			}
			rows, err := updated.RowsAffected()
			if err != nil {
				return fmt.Errorf("read reusable import coverage consumption: %w", err)
			}
			claimed = rows == 1
			if !claimed || deferral == nil {
				return nil
			}
			deferred, err := tx.ExecContext(ctx, `
				UPDATE file_health
				SET scheduled_check_at = ?,
				    updated_at = CASE WHEN updated_at > ? THEN updated_at ELSE ? END
				WHERE file_path = ?
				  AND id = (
				    SELECT file_health_id FROM health_file_revisions
				    WHERE id = ? AND active = TRUE
				  )
			`, deferral.nextCheckAt, at, at, deferral.filePath, revisionID)
			if err != nil {
				return fmt.Errorf("defer health after reusable import coverage: %w", err)
			}
			deferredRows, err := deferred.RowsAffected()
			if err != nil {
				return fmt.Errorf("read reusable import coverage health deferral: %w", err)
			}
			if deferredRows != 1 {
				return fmt.Errorf("active file health identity does not match reusable import coverage")
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		if claimed {
			return coverage, nil
		}
	}
	return nil, nil
}

func (r *HealthStateRepository) importRunHasFullSTATCoverage(
	ctx context.Context,
	runID string,
	totalSegments int64,
) (bool, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT segment_start, segment_count, tested_bitmap
		FROM health_run_chunks
		WHERE run_id = ? AND stage = ? AND observation_kind = 'stat'
	`, runID, HealthRunStageImportInitialSTAT)
	if err != nil {
		return false, fmt.Errorf("read reusable import STAT coverage: %w", err)
	}
	defer rows.Close()

	covered := make([]bool, totalSegments)
	for rows.Next() {
		var start, count int64
		var tested []byte
		if err := rows.Scan(&start, &count, &tested); err != nil {
			return false, fmt.Errorf("scan reusable import STAT coverage: %w", err)
		}
		if start < 0 || count <= 0 || start > totalSegments-count ||
			len(tested) != int((count+7)/8) {
			return false, fmt.Errorf("reusable import STAT coverage is malformed")
		}
		for relative := int64(0); relative < count; relative++ {
			if bitmapSet(tested, relative) {
				covered[start+relative] = true
			}
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate reusable import STAT coverage: %w", err)
	}
	for _, present := range covered {
		if !present {
			return false, nil
		}
	}
	return true, nil
}

func (r *HealthStateRepository) AcquireRunLease(ctx context.Context, runID, owner string, ttl time.Duration) (*HealthRun, error) {
	if runID == "" || owner == "" || ttl <= 0 {
		return nil, fmt.Errorf("run ID, lease owner, and positive TTL are required")
	}
	at := r.now().UTC()
	expires := at.Add(ttl)
	query := `
		UPDATE health_runs
		SET lease_owner = ?, lease_expires_at = ?, fencing_token = fencing_token + 1,
		    status = 'running', started_at = COALESCE(started_at, ?), updated_at = ?
		WHERE id = ?
		  AND status IN ('pending', 'running', 'paused')
		  AND pause_requested = FALSE AND cancel_requested = FALSE
		  AND (lease_owner IS NULL OR lease_expires_at <= ? OR lease_owner = ?)
		  AND NOT EXISTS (
		    SELECT 1 FROM health_runs active_run
		    WHERE active_run.file_revision_id = health_runs.file_revision_id
		      AND active_run.id <> health_runs.id AND active_run.status = 'running'
		      AND active_run.lease_owner IS NOT NULL
		      AND active_run.lease_expires_at > ?
		  )
		RETURNING id, file_revision_id, provider_snapshot_id, trigger, mode, status,
		          lease_owner, lease_expires_at, fencing_token, total_segments,
		          resolved_segments, provider_checks, missing_candidates, inconclusive_count,
		          stage, current_provider_id, current_provider_generation, cursor_segment,
		          pause_requested, cancel_requested, created_at, started_at, updated_at, completed_at,
		          COALESCE(last_error, '')
	`
	var run HealthRun
	var err error
	if r.dialect.IsPostgres() {
		err = r.withTransaction(ctx, func(tx *dialectAwareTx) error {
			var revisionID string
			if err := tx.QueryRowContext(ctx, `
				SELECT revision.id
				FROM health_file_revisions revision
				JOIN health_runs run ON run.file_revision_id = revision.id
				WHERE run.id = ?
				FOR UPDATE OF revision
			`, runID).Scan(&revisionID); err != nil {
				return err
			}
			return scanHealthRun(tx.QueryRowContext(ctx, query,
				owner, expires, at, at, runID, at, owner, at), &run)
		})
	} else {
		err = scanHealthRun(r.db.QueryRowContext(ctx, query,
			owner, expires, at, at, runID, at, owner, at), &run)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrStaleHealthLease
	}
	if err != nil {
		return nil, fmt.Errorf("acquire health run lease: %w", err)
	}
	return &run, nil
}

func (r *HealthStateRepository) GetFileRevisionForRun(ctx context.Context, runID string) (*HealthFileRevision, error) {
	var revision HealthFileRevision
	err := scanHealthFileRevision(r.db.QueryRowContext(ctx, `
		SELECT fr.id, fr.file_health_id, fr.layout_fingerprint, fr.virtual_size,
		       fr.segment_count, fr.active, fr.created_at, fr.activated_at
		FROM health_file_revisions fr
		JOIN health_runs r ON r.file_revision_id = fr.id
		WHERE r.id = ?
	`, runID), &revision)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get file revision for run: %w", err)
	}
	return &revision, nil
}

func validateHealthChunk(commit HealthChunkCommit) error {
	if commit.ChunkID == "" || commit.RunID == "" || commit.LeaseOwner == "" ||
		commit.ProviderID == "" || commit.Stage == "" || commit.CommittedAt.IsZero() {
		return fmt.Errorf("chunk, run, lease, provider, stage, and commit time are required")
	}
	if commit.ObservationKind != HealthObservationSTAT && commit.ObservationKind != HealthObservationValidatedBody {
		return fmt.Errorf("invalid health observation kind %q", commit.ObservationKind)
	}
	if commit.FreshTransport && commit.ObservationKind != HealthObservationValidatedBody {
		return fmt.Errorf("fresh transport is meaningful only for validated BODY observations")
	}
	if !validDurableHealthKey(commit.ChunkID) || !validDurableHealthClass(commit.Stage, true) {
		return fmt.Errorf("chunk identity or stage is not safe durable health metadata")
	}
	if commit.FencingToken <= 0 || commit.ProviderGeneration <= 0 || commit.ProviderActivationEpoch < 0 ||
		commit.SegmentStart < 0 || commit.SegmentCount <= 0 {
		return fmt.Errorf("invalid chunk token, generation, or segment range")
	}
	if commit.SegmentStart > math.MaxInt64-commit.SegmentCount {
		return fmt.Errorf("chunk segment range overflows")
	}
	segmentEnd := commit.SegmentStart + commit.SegmentCount
	if commit.CursorSegment < 0 || commit.ResolvedDelta < 0 || commit.ProviderChecksDelta < 0 ||
		commit.MissingCandidatesDelta < 0 || commit.InconclusiveDelta < 0 {
		return fmt.Errorf("chunk progress deltas must be non-negative")
	}
	if commit.CursorSegment > segmentEnd {
		return fmt.Errorf("chunk cursor advances beyond committed range")
	}
	if commit.SegmentCount > int64(math.MaxInt)/8 || commit.SegmentCount > math.MaxInt64-7 {
		return fmt.Errorf("chunk bitmap range is too large")
	}
	bitmapBytes := int((commit.SegmentCount + 7) / 8)
	bitmaps := [][]byte{
		commit.TestedBitmap, commit.PresentBitmap, commit.AbsentBitmap,
		commit.CorruptBitmap, commit.TemporaryBitmap, commit.InconclusiveBitmap,
		commit.ResolvedBitmap,
	}
	for _, bitmap := range bitmaps {
		if len(bitmap) != bitmapBytes {
			return fmt.Errorf("bitmap length does not match segment range")
		}
		if remainder := commit.SegmentCount % 8; remainder != 0 {
			allowed := byte((1 << remainder) - 1)
			if bitmap[len(bitmap)-1]&^allowed != 0 {
				return fmt.Errorf("bitmap sets bits outside segment range")
			}
		}
	}
	for i := range bitmapBytes {
		outcomes := []byte{
			commit.PresentBitmap[i], commit.AbsentBitmap[i], commit.CorruptBitmap[i],
			commit.TemporaryBitmap[i], commit.InconclusiveBitmap[i],
		}
		var union byte
		for _, outcome := range outcomes {
			if union&outcome != 0 {
				return fmt.Errorf("chunk outcome bitmaps overlap")
			}
			union |= outcome
		}
		if union != commit.TestedBitmap[i] {
			return fmt.Errorf("every tested chunk position must have exactly one outcome")
		}
		conclusive := commit.PresentBitmap[i] | commit.AbsentBitmap[i] | commit.CorruptBitmap[i]
		if commit.ResolvedBitmap[i]&^conclusive != 0 {
			return fmt.Errorf("resolved positions must have a conclusive committed outcome")
		}
	}
	if commit.ObservationKind == HealthObservationSTAT && bitmapPopulation(commit.CorruptBitmap) != 0 {
		return fmt.Errorf("STAT observations cannot report corrupt BODY outcomes")
	}
	testedCount := bitmapPopulation(commit.TestedBitmap)
	missingCount := bitmapPopulation(commit.AbsentBitmap) + bitmapPopulation(commit.CorruptBitmap)
	inconclusiveCount := bitmapPopulation(commit.TemporaryBitmap) + bitmapPopulation(commit.InconclusiveBitmap)
	conclusiveCount := testedCount - inconclusiveCount
	if commit.ProviderChecksDelta != testedCount || commit.MissingCandidatesDelta != missingCount ||
		commit.InconclusiveDelta != inconclusiveCount {
		return fmt.Errorf("chunk progress deltas do not match committed outcomes")
	}
	if commit.ResolvedDelta != bitmapPopulation(commit.ResolvedBitmap) || commit.ResolvedDelta > conclusiveCount {
		return fmt.Errorf("chunk progress exceeds segment range")
	}
	for _, attempt := range commit.Attempts {
		if attempt.IdempotencyKey == "" || attempt.Operation == "" || attempt.Outcome == "" ||
			attempt.BodyValidation == "" || attempt.ObservedAt.IsZero() ||
			attempt.SegmentIndex < commit.SegmentStart || attempt.SegmentIndex >= segmentEnd ||
			!bitmapSet(commit.TestedBitmap, attempt.SegmentIndex-commit.SegmentStart) {
			return fmt.Errorf("invalid attempt evidence")
		}
		if !validDurableHealthKey(attempt.IdempotencyKey) ||
			!validDurableHealthClass(attempt.Operation, true) ||
			!validDurableHealthClass(attempt.Outcome, true) ||
			!validDurableHealthClass(attempt.BodyValidation, true) ||
			!validDurableHealthClass(attempt.CauseClass, false) {
			return fmt.Errorf("attempt evidence contains an unsafe durable class")
		}
		if attempt.AdmissionWait < 0 || attempt.PoolQueue < 0 || attempt.PipelineWait < 0 || attempt.ResponseService < 0 {
			return fmt.Errorf("attempt durations must be non-negative")
		}
	}
	for _, confirmation := range commit.Confirmations {
		if confirmation.IdempotencyKey == "" || confirmation.ObservedAt.IsZero() ||
			(confirmation.Cause != GapCauseAbsent && confirmation.Cause != GapCauseCorrupt) ||
			confirmation.SegmentIndex < commit.SegmentStart || confirmation.SegmentIndex >= segmentEnd {
			return fmt.Errorf("invalid confirmation event")
		}
		if !validDurableHealthKey(confirmation.IdempotencyKey) {
			return fmt.Errorf("confirmation identity is not safe durable metadata")
		}
		relative := confirmation.SegmentIndex - commit.SegmentStart
		if confirmation.Cause == GapCauseAbsent && !bitmapSet(commit.AbsentBitmap, relative) ||
			confirmation.Cause == GapCauseCorrupt && !bitmapSet(commit.CorruptBitmap, relative) {
			return fmt.Errorf("confirmation cause does not match committed outcome")
		}
	}
	if retry := commit.Retry; retry != nil {
		if retry.RetryKey == "" || retry.Outcome == "" || retry.SegmentStart < commit.SegmentStart ||
			retry.SegmentCount <= 0 || retry.SegmentStart > math.MaxInt64-retry.SegmentCount ||
			retry.SegmentStart+retry.SegmentCount > segmentEnd || retry.Attempt < 0 {
			return fmt.Errorf("invalid retry state")
		}
		if !validDurableHealthKey(retry.RetryKey) || !validDurableHealthClass(retry.Outcome, true) {
			return fmt.Errorf("retry state contains unsafe durable metadata")
		}
		if !retry.Exhausted && retry.NextAttemptAt.IsZero() {
			return fmt.Errorf("non-exhausted retry requires next attempt time")
		}
	}
	return nil
}

func validDurableHealthClass(value string, required bool) bool {
	if value == "" {
		return !required
	}
	if len(value) > 64 {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || character == '_' || character == '-' ||
			character == '.' {
			continue
		}
		return false
	}
	return true
}

func validDurableHealthKey(value string) bool {
	if value == "" || len(value) > 256 {
		return false
	}
	for _, character := range value {
		if character <= ' ' || character == '<' || character == '>' || character == '@' {
			return false
		}
	}
	return true
}

func bitmapPopulation(bitmap []byte) int64 {
	var count int64
	for _, value := range bitmap {
		count += int64(bits.OnesCount8(value))
	}
	return count
}

func healthChunkDigest(commit HealthChunkCommit) (string, error) {
	encoded, err := json.Marshal(commit)
	if err != nil {
		return "", fmt.Errorf("encode health chunk digest: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func (r *HealthStateRepository) CommitHealthChunk(ctx context.Context, commit HealthChunkCommit) (*HealthRun, error) {
	if err := validateHealthChunk(commit); err != nil {
		return nil, err
	}
	commit.CommittedAt = commit.CommittedAt.UTC()
	now := r.now().UTC()
	const maximumEvidenceClockSkew = 5 * time.Minute
	if commit.CommittedAt.After(now.Add(maximumEvidenceClockSkew)) {
		return nil, fmt.Errorf("health chunk commit time is too far in the future")
	}
	for _, attempt := range commit.Attempts {
		observedAt := attempt.ObservedAt.UTC()
		if observedAt.After(now.Add(maximumEvidenceClockSkew)) ||
			observedAt.After(commit.CommittedAt.Add(maximumEvidenceClockSkew)) ||
			observedAt.Before(commit.CommittedAt.Add(-maximumEvidenceClockSkew)) {
			return nil, fmt.Errorf("attempt evidence time is outside the committed chunk window")
		}
	}
	for _, confirmation := range commit.Confirmations {
		observedAt := confirmation.ObservedAt.UTC()
		if observedAt.After(now.Add(maximumEvidenceClockSkew)) ||
			observedAt.After(commit.CommittedAt.Add(maximumEvidenceClockSkew)) ||
			observedAt.Before(commit.CommittedAt.Add(-maximumEvidenceClockSkew)) {
			return nil, fmt.Errorf("confirmation evidence time is outside the committed chunk window")
		}
	}
	var result HealthRun
	err := r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		var revisionID, snapshotID string
		var totalSegments int64
		var leaseExpiresAt, runUpdatedAt time.Time
		// A conditional write both locks the run row and proves that this exact
		// owner/token is still current and unexpired before idempotency is checked.
		err := tx.QueryRowContext(ctx, `
			UPDATE health_runs SET updated_at = updated_at
			WHERE id = ? AND status = 'running' AND lease_owner = ?
			  AND fencing_token = ?
			RETURNING file_revision_id, provider_snapshot_id, total_segments,
			          lease_expires_at, updated_at
		`, commit.RunID, commit.LeaseOwner, commit.FencingToken).Scan(
			&revisionID, &snapshotID, &totalSegments, &leaseExpiresAt, &runUpdatedAt,
		)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrStaleHealthLease
		}
		if err != nil {
			return fmt.Errorf("verify health run fence: %w", err)
		}
		if !leaseExpiresAt.After(r.now().UTC()) {
			return ErrStaleHealthLease
		}
		var lockedRevision string
		var revisionSegments int64
		if err := tx.QueryRowContext(ctx, `
			UPDATE health_file_revisions SET active = active WHERE id = ?
			RETURNING id, segment_count
		`, revisionID).Scan(&lockedRevision, &revisionSegments); err != nil {
			return fmt.Errorf("lock health file revision for observation commit: %w", err)
		}
		if commit.SegmentCount > revisionSegments ||
			commit.SegmentStart > revisionSegments-commit.SegmentCount ||
			commit.CursorSegment > revisionSegments {
			return fmt.Errorf("chunk range or cursor exceeds file revision bounds")
		}
		var snapshotActivationEpoch int64
		err = tx.QueryRowContext(ctx, `
			SELECT provider_activation_epoch FROM health_provider_snapshot_entries
			WHERE snapshot_id = ? AND provider_id = ? AND provider_generation = ?
		`, snapshotID, commit.ProviderID, commit.ProviderGeneration).Scan(&snapshotActivationEpoch)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrProviderSnapshotMismatch
		}
		if err != nil {
			return fmt.Errorf("verify provider dispatch snapshot: %w", err)
		}
		if commit.ProviderActivationEpoch == 0 {
			commit.ProviderActivationEpoch = snapshotActivationEpoch
		} else if commit.ProviderActivationEpoch != snapshotActivationEpoch {
			return ErrProviderSnapshotMismatch
		}
		if err := validateLiveHealthScheduleTarget(
			ctx, tx, commit, revisionID,
		); err != nil {
			return err
		}
		digest, err := healthChunkDigest(commit)
		if err != nil {
			return err
		}

		var existingDigest string
		err = tx.QueryRowContext(ctx, `SELECT commit_digest FROM health_run_chunks WHERE id = ?`, commit.ChunkID).Scan(&existingDigest)
		if err == nil {
			if existingDigest != digest {
				return ErrHealthChunkConflict
			}
			return scanHealthRun(tx.QueryRowContext(ctx, healthRunSelect+` WHERE id = ?`, commit.RunID), &result)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read committed health chunk: %w", err)
		}
		if commit.CommittedAt.Before(runUpdatedAt.UTC()) {
			return fmt.Errorf("health chunk commit time predates the active run checkpoint")
		}
		var logicalChunkID string
		err = tx.QueryRowContext(ctx, `
			SELECT id FROM health_run_chunks
			WHERE run_id = ? AND provider_id = ? AND provider_generation = ?
			  AND stage = ? AND segment_start < ? AND ? < segment_start + segment_count
		`, commit.RunID, commit.ProviderID, commit.ProviderGeneration, commit.Stage,
			commit.SegmentStart+commit.SegmentCount, commit.SegmentStart).Scan(&logicalChunkID)
		if err == nil {
			return ErrHealthChunkConflict
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read logical health chunk identity: %w", err)
		}
		resolvedDelta, err := countNewResolvedPositions(ctx, tx, commit)
		if err != nil {
			return err
		}

		var retryJSON any
		if commit.Retry != nil {
			encoded, err := json.Marshal(commit.Retry)
			if err != nil {
				return fmt.Errorf("encode chunk retry state: %w", err)
			}
			retryJSON = string(encoded)
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO health_run_chunks
				(id, run_id, provider_id, provider_generation, provider_activation_epoch,
				 stage, observation_kind, fresh_transport, segment_start,
				 segment_count, tested_bitmap, present_bitmap, absent_bitmap, corrupt_bitmap,
				 temporary_bitmap, inconclusive_bitmap, resolved_bitmap, retry_state, commit_digest,
				 fencing_token, resolved_delta, provider_checks_delta, missing_candidates_delta,
				 inconclusive_delta, committed_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, commit.ChunkID, commit.RunID, commit.ProviderID, commit.ProviderGeneration,
			commit.ProviderActivationEpoch, commit.Stage, commit.ObservationKind,
			commit.FreshTransport, commit.SegmentStart, commit.SegmentCount, commit.TestedBitmap,
			commit.PresentBitmap, commit.AbsentBitmap, commit.CorruptBitmap,
			commit.TemporaryBitmap, commit.InconclusiveBitmap, commit.ResolvedBitmap, retryJSON, digest,
			commit.FencingToken, resolvedDelta, commit.ProviderChecksDelta,
			commit.MissingCandidatesDelta, commit.InconclusiveDelta,
			commit.CommittedAt)
		if err != nil {
			return fmt.Errorf("insert health run chunk: %w", err)
		}

		_, err = tx.ExecContext(ctx, `
			INSERT INTO health_provider_coverage
				(id, file_revision_id, provider_id, provider_generation, provider_activation_epoch,
				 observation_kind, segment_start, segment_count, tested_bitmap, present_bitmap,
				 resolved_bitmap, source_chunk_id, observed_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, uuid.NewString(), revisionID, commit.ProviderID, commit.ProviderGeneration,
			commit.ProviderActivationEpoch, commit.ObservationKind, commit.SegmentStart,
			commit.SegmentCount, commit.TestedBitmap, commit.PresentBitmap,
			commit.ResolvedBitmap, commit.ChunkID, commit.CommittedAt)
		if err != nil {
			return fmt.Errorf("insert provider coverage: %w", err)
		}

		if err := persistChunkExceptions(ctx, tx, revisionID, commit); err != nil {
			return err
		}
		if err := persistAttemptEvidence(ctx, tx, revisionID, commit); err != nil {
			return err
		}
		if err := persistConfirmationEvents(ctx, tx, revisionID, commit); err != nil {
			return err
		}
		if err := persistRetryState(ctx, tx, revisionID, commit); err != nil {
			return err
		}

		update := `
			UPDATE health_runs
			SET resolved_segments = resolved_segments + ?,
			    provider_checks = provider_checks + ?,
			    missing_candidates = missing_candidates + ?,
			    inconclusive_count = inconclusive_count + ?,
			    cursor_segment = CASE
			      WHEN stage = ? THEN CASE WHEN cursor_segment > ? THEN cursor_segment ELSE ? END
			      ELSE ?
			    END,
			    stage = ?, current_provider_id = ?, current_provider_generation = ?,
			    updated_at = ?
			WHERE id = ? AND lease_owner = ? AND fencing_token = ?
			  AND resolved_segments + ? <= total_segments
			RETURNING id, file_revision_id, provider_snapshot_id, trigger, mode, status,
			          lease_owner, lease_expires_at, fencing_token, total_segments,
			          resolved_segments, provider_checks, missing_candidates, inconclusive_count,
			          stage, current_provider_id, current_provider_generation, cursor_segment,
			          pause_requested, cancel_requested, created_at, started_at, updated_at, completed_at,
			          COALESCE(last_error, '')
		`
		err = scanHealthRun(tx.QueryRowContext(ctx, update,
			resolvedDelta, commit.ProviderChecksDelta, commit.MissingCandidatesDelta,
			commit.InconclusiveDelta, commit.Stage, commit.CursorSegment, commit.CursorSegment,
			commit.CursorSegment,
			commit.Stage, commit.ProviderID, commit.ProviderGeneration, commit.CommittedAt,
			commit.RunID, commit.LeaseOwner, commit.FencingToken, resolvedDelta,
		), &result)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("health chunk progress violates active run bounds")
		}
		if err != nil {
			return fmt.Errorf("advance health run progress: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func validateLiveHealthScheduleTarget(
	ctx context.Context,
	tx *dialectAwareTx,
	commit HealthChunkCommit,
	revisionID string,
) error {
	var active bool
	var targetProviderID, targetGapID sql.NullString
	var targetGeneration, targetActivationEpoch sql.NullInt64
	var trigger string
	err := tx.QueryRowContext(ctx, `
		SELECT s.active, s.target_provider_id, s.target_provider_generation,
		       s.target_provider_activation_epoch, s.target_gap_id, r.trigger
		FROM health_run_schedule s
		JOIN health_runs r ON r.id = s.run_id
		WHERE s.run_id = ?
	`, commit.RunID).Scan(
		&active, &targetProviderID, &targetGeneration,
		&targetActivationEpoch, &targetGapID, &trigger,
	)
	if errors.Is(err, sql.ErrNoRows) {
		// Import admission and the PR4 compatibility APIs create explicitly
		// leased runs without scheduler metadata.
		return nil
	}
	if err != nil {
		return fmt.Errorf("verify health run schedule: %w", err)
	}
	if !active {
		return ErrStaleHealthSchedule
	}
	if targetProviderID.Valid {
		if commit.ProviderID != targetProviderID.String ||
			commit.ProviderGeneration != targetGeneration.Int64 ||
			commit.ProviderActivationEpoch != targetActivationEpoch.Int64 {
			return ErrStaleHealthSchedule
		}
		var current int
		err := tx.QueryRowContext(ctx, `
			SELECT 1 FROM health_providers
			WHERE id = ? AND active = TRUE AND current_generation = ?
			  AND activation_epoch = ?
		`, targetProviderID.String, targetGeneration.Int64,
			targetActivationEpoch.Int64).Scan(&current)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrStaleHealthSchedule
		}
		if err != nil {
			return fmt.Errorf("verify current scheduled provider activation: %w", err)
		}
	}
	if targetGapID.Valid {
		var gapRevisionID string
		var start, count int64
		var status GapStatus
		err := tx.QueryRowContext(ctx, `
			SELECT file_revision_id, start_segment, segment_count, status
			FROM health_gap_ranges WHERE id = ?
		`, targetGapID.String).Scan(&gapRevisionID, &start, &count, &status)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrStaleHealthSchedule
		}
		if err != nil {
			return fmt.Errorf("verify scheduled gap target: %w", err)
		}
		allowDormant := strings.HasPrefix(trigger, "provider_activation") || trigger == "manual"
		if gapRevisionID != revisionID ||
			(status != GapStatusActive && (!allowDormant || status != GapStatusDormant)) ||
			commit.SegmentStart < start ||
			commit.SegmentStart+commit.SegmentCount > start+count {
			return ErrStaleHealthSchedule
		}
	}
	return nil
}

// countNewResolvedPositions computes the positional union under the run-row
// lock held by CommitHealthChunk. Provider/stage attempt counts remain
// additive, but a file position can advance resolved_segments only once.
func countNewResolvedPositions(
	ctx context.Context,
	tx *dialectAwareTx,
	commit HealthChunkCommit,
) (int64, error) {
	seen := make([]byte, len(commit.ResolvedBitmap))
	rows, err := tx.QueryContext(ctx, `
		SELECT segment_start, segment_count, resolved_bitmap
		FROM health_run_chunks
		WHERE run_id = ? AND segment_start < ? AND ? < segment_start + segment_count
	`, commit.RunID, commit.SegmentStart+commit.SegmentCount, commit.SegmentStart)
	if err != nil {
		return 0, fmt.Errorf("read prior resolved health positions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var start, count int64
		var resolved []byte
		if err := rows.Scan(&start, &count, &resolved); err != nil {
			return 0, fmt.Errorf("scan prior resolved health positions: %w", err)
		}
		for relative := int64(0); relative < commit.SegmentCount; relative++ {
			position := commit.SegmentStart + relative
			priorRelative := position - start
			if priorRelative >= 0 && priorRelative < count && priorRelative/8 < int64(len(resolved)) &&
				bitmapSet(resolved, priorRelative) {
				seen[relative/8] |= 1 << uint(relative%8)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate prior resolved health positions: %w", err)
	}
	var added int64
	for relative := int64(0); relative < commit.SegmentCount; relative++ {
		if bitmapSet(commit.ResolvedBitmap, relative) && !bitmapSet(seen, relative) {
			added++
		}
	}
	return added, nil
}

func bitmapSet(bitmap []byte, index int64) bool {
	return bitmap[index/8]&(1<<uint(index%8)) != 0
}

func persistChunkExceptions(ctx context.Context, tx *dialectAwareTx, revisionID string, commit HealthChunkCommit) error {
	for relative := int64(0); relative < commit.SegmentCount; relative++ {
		segmentIndex := commit.SegmentStart + relative
		if bitmapSet(commit.PresentBitmap, relative) {
			query := `
				DELETE FROM health_segment_exceptions
				WHERE file_revision_id = ? AND provider_id = ? AND provider_generation = ?
				  AND provider_activation_epoch = ?
				  AND segment_index = ? AND observed_at <= ?
			`
			if commit.ObservationKind == HealthObservationSTAT {
				query += ` AND outcome <> 'corrupt_body'`
			}
			_, err := tx.ExecContext(ctx, query, revisionID, commit.ProviderID,
				commit.ProviderGeneration, commit.ProviderActivationEpoch,
				segmentIndex, commit.CommittedAt)
			if err != nil {
				return fmt.Errorf("clear provider segment exception: %w", err)
			}
			continue
		}
		outcome := ""
		switch {
		case bitmapSet(commit.AbsentBitmap, relative):
			outcome = "hard_absence"
		case bitmapSet(commit.CorruptBitmap, relative):
			outcome = "corrupt_body"
		case bitmapSet(commit.TemporaryBitmap, relative):
			outcome = "temporary_failure"
		case bitmapSet(commit.InconclusiveBitmap, relative):
			outcome = "inconclusive"
		}
		if outcome == "" {
			continue
		}
		newerPresent, err := hasNewerApplicablePresence(ctx, tx, revisionID, commit, segmentIndex, outcome)
		if err != nil {
			return err
		}
		if newerPresent {
			continue
		}
		var nextRetry any
		if outcome == "temporary_failure" && commit.Retry != nil && !commit.Retry.Exhausted &&
			segmentIndex >= commit.Retry.SegmentStart && segmentIndex < commit.Retry.SegmentStart+commit.Retry.SegmentCount {
			nextRetry = commit.Retry.NextAttemptAt.UTC()
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO health_segment_exceptions
				(file_revision_id, provider_id, provider_generation, provider_activation_epoch, segment_index,
				 outcome, source_chunk_id, observed_at, next_retry_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(file_revision_id, provider_id, provider_generation, provider_activation_epoch, segment_index)
			DO UPDATE SET outcome = excluded.outcome, source_chunk_id = excluded.source_chunk_id,
			              observed_at = excluded.observed_at, next_retry_at = excluded.next_retry_at
			WHERE health_segment_exceptions.observed_at <= excluded.observed_at
			  AND (
			    excluded.outcome = 'corrupt_body'
			    OR health_segment_exceptions.outcome <> 'corrupt_body'
			  )
			  AND (
			    excluded.outcome IN ('hard_absence', 'corrupt_body')
			    OR health_segment_exceptions.outcome NOT IN ('hard_absence', 'corrupt_body')
			  )
		`, revisionID, commit.ProviderID, commit.ProviderGeneration,
			commit.ProviderActivationEpoch, segmentIndex,
			outcome, commit.ChunkID, commit.CommittedAt, nextRetry)
		if err != nil {
			return fmt.Errorf("persist provider segment exception: %w", err)
		}
	}
	return nil
}

func hasNewerApplicablePresence(
	ctx context.Context,
	tx *dialectAwareTx,
	revisionID string,
	commit HealthChunkCommit,
	segmentIndex int64,
	outcome string,
) (bool, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT observation_kind, segment_start, present_bitmap
		FROM health_provider_coverage
		WHERE file_revision_id = ? AND provider_id = ? AND provider_generation = ?
		  AND provider_activation_epoch = ?
		  AND observed_at >= ? AND segment_start <= ? AND segment_start + segment_count > ?
	`, revisionID, commit.ProviderID, commit.ProviderGeneration, commit.ProviderActivationEpoch, commit.CommittedAt,
		segmentIndex, segmentIndex)
	if err != nil {
		return false, fmt.Errorf("read newer provider presence: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var kind HealthObservationKind
		var start int64
		var present []byte
		if err := rows.Scan(&kind, &start, &present); err != nil {
			return false, fmt.Errorf("scan newer provider presence: %w", err)
		}
		if bitmapSet(present, segmentIndex-start) &&
			(outcome != "corrupt_body" || kind == HealthObservationValidatedBody) {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate newer provider presence: %w", err)
	}
	return false, nil
}

func persistAttemptEvidence(ctx context.Context, tx *dialectAwareTx, revisionID string, commit HealthChunkCommit) error {
	for _, attempt := range commit.Attempts {
		var existingChunkID string
		err := tx.QueryRowContext(ctx, `
			SELECT source_chunk_id FROM health_attempt_evidence WHERE idempotency_key = ?
		`, attempt.IdempotencyKey).Scan(&existingChunkID)
		if err == nil {
			// Exact enclosing-chunk replay returned before reaching this path. A
			// stable attempt key appearing in another chunk is therefore a
			// conflicting write, not an idempotent retry.
			return ErrHealthChunkConflict
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read attempt evidence identity: %w", err)
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO health_attempt_evidence
				(idempotency_key, source_chunk_id, file_revision_id, provider_id,
				 provider_generation, provider_activation_epoch, segment_index, operation, outcome, response_code,
				 body_validation, cause_class, admission_wait_ns, pool_queue_ns,
				 pipeline_wait_ns, response_service_ns, observed_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, attempt.IdempotencyKey, commit.ChunkID, revisionID, commit.ProviderID,
			commit.ProviderGeneration, commit.ProviderActivationEpoch,
			attempt.SegmentIndex, attempt.Operation,
			attempt.Outcome, attempt.ResponseCode, attempt.BodyValidation, attempt.CauseClass,
			attempt.AdmissionWait.Nanoseconds(), attempt.PoolQueue.Nanoseconds(),
			attempt.PipelineWait.Nanoseconds(), attempt.ResponseService.Nanoseconds(),
			attempt.ObservedAt.UTC())
		if err != nil {
			return fmt.Errorf("persist attempt evidence: %w", err)
		}
	}
	return nil
}

func persistConfirmationEvents(ctx context.Context, tx *dialectAwareTx, revisionID string, commit HealthChunkCommit) error {
	for _, confirmation := range commit.Confirmations {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO health_confirmation_events
				(idempotency_key, source_chunk_id, file_revision_id, provider_id,
				 provider_generation, provider_activation_epoch, segment_index, cause, observed_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(idempotency_key) DO NOTHING
		`, confirmation.IdempotencyKey, commit.ChunkID, revisionID, commit.ProviderID,
			commit.ProviderGeneration, commit.ProviderActivationEpoch,
			confirmation.SegmentIndex, confirmation.Cause,
			confirmation.ObservedAt.UTC())
		if err != nil {
			return fmt.Errorf("persist confirmation event: %w", err)
		}
		var existingRevision, existingProvider string
		var existingGeneration, existingActivationEpoch, existingSegment int64
		var existingCause GapCause
		var existingObservedAt time.Time
		err = tx.QueryRowContext(ctx, `
			SELECT file_revision_id, provider_id, provider_generation, provider_activation_epoch,
			       segment_index, cause, observed_at
			FROM health_confirmation_events WHERE idempotency_key = ?
		`, confirmation.IdempotencyKey).Scan(
			&existingRevision, &existingProvider, &existingGeneration,
			&existingActivationEpoch, &existingSegment,
			&existingCause, &existingObservedAt,
		)
		if err != nil {
			return fmt.Errorf("read confirmation event identity: %w", err)
		}
		if existingRevision != revisionID || existingProvider != commit.ProviderID ||
			existingGeneration != commit.ProviderGeneration ||
			existingActivationEpoch != commit.ProviderActivationEpoch ||
			existingSegment != confirmation.SegmentIndex ||
			existingCause != confirmation.Cause || !existingObservedAt.Equal(confirmation.ObservedAt.UTC()) {
			return ErrHealthChunkConflict
		}
	}
	return nil
}

func persistRetryState(ctx context.Context, tx *dialectAwareTx, revisionID string, commit HealthChunkCommit) error {
	if commit.Retry == nil {
		return nil
	}
	retry := commit.Retry
	var nextAttempt any
	if !retry.NextAttemptAt.IsZero() {
		nextAttempt = retry.NextAttemptAt.UTC()
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO health_retry_states
			(retry_key, source_chunk_id, file_revision_id, provider_id, provider_generation,
			 provider_activation_epoch,
			 segment_start, segment_count, outcome, attempt, next_attempt_at, exhausted, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(retry_key) DO NOTHING
	`, retry.RetryKey, commit.ChunkID, revisionID, commit.ProviderID,
		commit.ProviderGeneration, commit.ProviderActivationEpoch,
		retry.SegmentStart, retry.SegmentCount,
		retry.Outcome, retry.Attempt, nextAttempt, retry.Exhausted, commit.CommittedAt)
	if err != nil {
		return fmt.Errorf("persist health retry state: %w", err)
	}
	var existingRevision, existingProvider, existingOutcome string
	var existingGeneration, existingActivationEpoch, existingStart, existingCount int64
	var existingAttempt int
	var existingNextAttempt *time.Time
	var existingExhausted bool
	var existingUpdatedAt time.Time
	err = tx.QueryRowContext(ctx, `
		SELECT file_revision_id, provider_id, provider_generation, provider_activation_epoch, segment_start,
		       segment_count, outcome, attempt, next_attempt_at, exhausted, updated_at
		FROM health_retry_states WHERE retry_key = ?
	`, retry.RetryKey).Scan(
		&existingRevision, &existingProvider, &existingGeneration,
		&existingActivationEpoch, &existingStart,
		&existingCount, &existingOutcome, &existingAttempt, &existingNextAttempt,
		&existingExhausted, &existingUpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("read health retry identity: %w", err)
	}
	if existingRevision != revisionID || existingProvider != commit.ProviderID ||
		existingGeneration != commit.ProviderGeneration ||
		existingActivationEpoch != commit.ProviderActivationEpoch || existingStart != retry.SegmentStart ||
		existingCount != retry.SegmentCount || existingOutcome != retry.Outcome {
		return ErrHealthChunkConflict
	}
	if existingAttempt > retry.Attempt || existingUpdatedAt.After(commit.CommittedAt) {
		return nil
	}
	if existingAttempt == retry.Attempt {
		if existingExhausted != retry.Exhausted || !sameOptionalTime(existingNextAttempt, retry.NextAttemptAt) {
			return ErrHealthChunkConflict
		}
		return nil
	}
	if existingExhausted {
		return ErrHealthChunkConflict
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE health_retry_states
		SET source_chunk_id = ?, attempt = ?, next_attempt_at = ?, exhausted = ?, updated_at = ?
		WHERE retry_key = ? AND attempt < ? AND exhausted = FALSE AND updated_at <= ?
	`, commit.ChunkID, retry.Attempt, nextAttempt, retry.Exhausted, commit.CommittedAt,
		retry.RetryKey, retry.Attempt, commit.CommittedAt)
	if err != nil {
		return fmt.Errorf("advance health retry state: %w", err)
	}
	if rows, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("read health retry update result: %w", err)
	} else if rows == 0 {
		return nil
	}
	return nil
}

func sameOptionalTime(existing *time.Time, desired time.Time) bool {
	if existing == nil {
		return desired.IsZero()
	}
	return !desired.IsZero() && existing.Equal(desired.UTC())
}

func (r *HealthStateRepository) RequestRunPause(ctx context.Context, runID string, requested bool, at time.Time) error {
	return r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		var trigger string
		if err := tx.QueryRowContext(ctx, `
			UPDATE health_runs SET updated_at = updated_at
			WHERE id = ? RETURNING trigger
		`, runID).Scan(&trigger); errors.Is(err, sql.ErrNoRows) {
			return sql.ErrNoRows
		} else if err != nil {
			return fmt.Errorf("lock health run control target: %w", err)
		}
		if trigger == "import" {
			return ErrImportRunControl
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE health_runs SET pause_requested = ?, updated_at = ? WHERE id = ?
		`, requested, at.UTC(), runID)
		if err != nil {
			return fmt.Errorf("request run pause: %w", err)
		}
		if rows, err := result.RowsAffected(); err != nil {
			return fmt.Errorf("read health run pause result: %w", err)
		} else if rows != 1 {
			return sql.ErrNoRows
		}
		return nil
	})
}

func (r *HealthStateRepository) RequestRunCancel(ctx context.Context, runID string, at time.Time) error {
	at = at.UTC()
	return r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		var trigger string
		if err := tx.QueryRowContext(ctx, `
			UPDATE health_runs SET updated_at = updated_at
			WHERE id = ? RETURNING trigger
		`, runID).Scan(&trigger); errors.Is(err, sql.ErrNoRows) {
			return sql.ErrNoRows
		} else if err != nil {
			return fmt.Errorf("lock health run cancel target: %w", err)
		}
		if trigger == "import" {
			return ErrImportRunControl
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE health_runs
			SET cancel_requested = TRUE, status = 'canceled', lease_owner = NULL,
			    lease_expires_at = NULL, updated_at = ?, completed_at = ?
			WHERE id = ? AND status NOT IN ('completed', 'canceled', 'failed')
		`, at, at, runID)
		if err != nil {
			return fmt.Errorf("request run cancel: %w", err)
		}
		if rows, _ := result.RowsAffected(); rows == 0 {
			return sql.ErrNoRows
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE health_run_schedule SET active = FALSE, updated_at = ? WHERE run_id = ?
		`, at, runID); err != nil {
			return fmt.Errorf("retire canceled health run schedule: %w", err)
		}
		return nil
	})
}
