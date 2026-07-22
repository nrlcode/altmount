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
	"time"

	"github.com/google/uuid"
)

var ErrAmbiguousLegacyHealthChunk = errors.New("legacy health chunk has ambiguous resolved progress")

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
	       stage, current_provider_id, current_provider_generation, cursor_sequence, cursor_segment,
	       pause_requested, cancel_requested, created_at, started_at, updated_at, completed_at
	FROM health_runs
`

func scanHealthRun(row rowScanner, run *HealthRun) error {
	return row.Scan(&run.ID, &run.FileRevisionID, &run.ProviderSnapshotID, &run.Trigger,
		&run.Mode, &run.Status, &run.LeaseOwner, &run.LeaseExpiresAt, &run.FencingToken,
		&run.TotalSegments, &run.ResolvedSegments, &run.ProviderChecks,
		&run.MissingCandidates, &run.InconclusiveCount, &run.Stage,
		&run.CurrentProviderID, &run.CurrentProviderGeneration, &run.CursorSequence, &run.CursorSegment,
		&run.PauseRequested, &run.CancelRequested, &run.CreatedAt, &run.StartedAt,
		&run.UpdatedAt, &run.CompletedAt)
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
		  AND cancel_requested = FALSE
		  AND (lease_owner IS NULL OR lease_expires_at <= ? OR lease_owner = ?)
		RETURNING id, file_revision_id, provider_snapshot_id, trigger, mode, status,
		          lease_owner, lease_expires_at, fencing_token, total_segments,
		          resolved_segments, provider_checks, missing_candidates, inconclusive_count,
		          stage, current_provider_id, current_provider_generation, cursor_sequence, cursor_segment,
		          pause_requested, cancel_requested, created_at, started_at, updated_at, completed_at
	`
	var run HealthRun
	err := scanHealthRun(r.db.QueryRowContext(ctx, query, owner, expires, at, at, runID, at, owner), &run)
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

func prepareHealthChunk(commit HealthChunkCommit) (HealthChunkCommit, error) {
	commit.TestedBitmap = append([]byte(nil), commit.TestedBitmap...)
	commit.PresentBitmap = append([]byte(nil), commit.PresentBitmap...)
	commit.AbsentBitmap = append([]byte(nil), commit.AbsentBitmap...)
	commit.CorruptBitmap = append([]byte(nil), commit.CorruptBitmap...)
	commit.TemporaryBitmap = append([]byte(nil), commit.TemporaryBitmap...)
	commit.InconclusiveBitmap = append([]byte(nil), commit.InconclusiveBitmap...)
	if commit.ResolvedBitmap != nil {
		resolved := make([]byte, len(commit.ResolvedBitmap))
		copy(resolved, commit.ResolvedBitmap)
		commit.ResolvedBitmap = resolved
	}
	commit.CommittedAt = commit.CommittedAt.UTC()
	commit.Attempts = append([]HealthAttemptEvidence(nil), commit.Attempts...)
	for i := range commit.Attempts {
		commit.Attempts[i].ObservedAt = commit.Attempts[i].ObservedAt.UTC()
		if commit.Attempts[i].ResponseCode != nil {
			code := *commit.Attempts[i].ResponseCode
			commit.Attempts[i].ResponseCode = &code
		}
	}
	commit.Confirmations = append([]HealthConfirmationEvent(nil), commit.Confirmations...)
	for i := range commit.Confirmations {
		commit.Confirmations[i].ObservedAt = commit.Confirmations[i].ObservedAt.UTC()
	}
	if commit.Retry != nil {
		retry := *commit.Retry
		retry.NextAttemptAt = retry.NextAttemptAt.UTC()
		commit.Retry = &retry
	}

	if commit.CursorSequence == 0 && commit.ResolvedBitmap == nil &&
		commit.SegmentCount > 0 && commit.SegmentCount <= int64(math.MaxInt)/8 &&
		commit.SegmentCount <= math.MaxInt64-7 {
		bitmapBytes := int((commit.SegmentCount + 7) / 8)
		base := [][]byte{
			commit.TestedBitmap, commit.PresentBitmap, commit.AbsentBitmap,
			commit.CorruptBitmap, commit.TemporaryBitmap, commit.InconclusiveBitmap,
		}
		validLengths := true
		for _, bitmap := range base {
			validLengths = validLengths && len(bitmap) == bitmapBytes
		}
		if validLengths {
			conclusive := make([]byte, bitmapBytes)
			for i := range conclusive {
				conclusive[i] = commit.TestedBitmap[i] &^ (commit.TemporaryBitmap[i] | commit.InconclusiveBitmap[i])
			}
			switch {
			case commit.ResolvedDelta == 0:
				commit.ResolvedBitmap = make([]byte, bitmapBytes)
			case commit.ResolvedDelta == bitmapPopulation(conclusive):
				commit.ResolvedBitmap = conclusive
			}
		}
	}
	if err := validateHealthChunk(commit); err != nil {
		return HealthChunkCommit{}, err
	}
	return commit, nil
}

func validateHealthChunk(commit HealthChunkCommit) error {
	if commit.ChunkID == "" || commit.RunID == "" || commit.LeaseOwner == "" ||
		commit.ProviderID == "" || commit.Stage == "" || commit.CommittedAt.IsZero() {
		return fmt.Errorf("chunk, run, lease, provider, stage, and commit time are required")
	}
	if commit.ObservationKind != HealthObservationSTAT && commit.ObservationKind != HealthObservationValidatedBody {
		return fmt.Errorf("invalid health observation kind %q", commit.ObservationKind)
	}
	if commit.FencingToken <= 0 || commit.ProviderGeneration <= 0 || commit.SegmentStart < 0 || commit.SegmentCount <= 0 {
		return fmt.Errorf("invalid chunk token, generation, or segment range")
	}
	if commit.SegmentStart > math.MaxInt64-commit.SegmentCount {
		return fmt.Errorf("chunk segment range overflows")
	}
	segmentEnd := commit.SegmentStart + commit.SegmentCount
	if commit.CursorSequence < 0 || commit.CursorSegment < 0 || commit.ResolvedDelta < 0 || commit.ProviderChecksDelta < 0 ||
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
	if commit.ResolvedDelta > conclusiveCount {
		return fmt.Errorf("chunk progress exceeds segment range")
	}
	if len(commit.ResolvedBitmap) != bitmapBytes {
		return fmt.Errorf("resolved bitmap length does not match segment range")
	}
	if remainder := commit.SegmentCount % 8; remainder != 0 {
		allowed := byte((1 << remainder) - 1)
		if commit.ResolvedBitmap[len(commit.ResolvedBitmap)-1]&^allowed != 0 {
			return fmt.Errorf("resolved bitmap sets bits outside segment range")
		}
	}
	for i := range bitmapBytes {
		conclusive := commit.TestedBitmap[i] &^ (commit.TemporaryBitmap[i] | commit.InconclusiveBitmap[i])
		if commit.ResolvedBitmap[i]&^conclusive != 0 {
			return fmt.Errorf("resolved bitmap contains an inconclusive or untested position")
		}
	}
	if bitmapPopulation(commit.ResolvedBitmap) != commit.ResolvedDelta {
		return fmt.Errorf("resolved bitmap population does not match progress delta")
	}
	for _, attempt := range commit.Attempts {
		if attempt.IdempotencyKey == "" || attempt.Operation == "" || attempt.Outcome == "" ||
			attempt.BodyValidation == "" || attempt.ObservedAt.IsZero() ||
			attempt.SegmentIndex < commit.SegmentStart || attempt.SegmentIndex >= segmentEnd ||
			!bitmapSet(commit.TestedBitmap, attempt.SegmentIndex-commit.SegmentStart) {
			return fmt.Errorf("invalid attempt evidence")
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
		if !retry.Exhausted && retry.NextAttemptAt.IsZero() {
			return fmt.Errorf("non-exhausted retry requires next attempt time")
		}
	}
	return nil
}

func bitmapPopulation(bitmap []byte) int64 {
	var count int64
	for _, value := range bitmap {
		count += int64(bits.OnesCount8(value))
	}
	return count
}

func healthChunkDigest(commit HealthChunkCommit) (string, error) {
	commit.LeaseOwner = ""
	commit.FencingToken = 0
	encoded, err := json.Marshal(commit)
	if err != nil {
		return "", fmt.Errorf("encode health chunk digest: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func normalizeHealthCursor(
	commit *HealthChunkCommit,
	stage string,
	providerID sql.NullString,
	providerGeneration sql.NullInt64,
	storedSequence int64,
	storedCursor int64,
) (int64, error) {
	if storedSequence < 0 || storedCursor < 0 {
		return 0, fmt.Errorf("stored health cursor is invalid")
	}
	established := stage != "" || providerID.Valid || providerGeneration.Valid
	if !established {
		if commit.CursorSequence == 0 {
			commit.CursorSequence = 1
		}
		if commit.CursorSequence != 1 {
			return 0, fmt.Errorf("initial health cursor sequence must be one")
		}
		return commit.CursorSegment, nil
	}
	if stage == "" || !providerID.Valid || !providerGeneration.Valid {
		return 0, fmt.Errorf("stored health cursor tuple is incomplete")
	}

	currentSequence := storedSequence
	if currentSequence == 0 {
		currentSequence = 1
	}
	sameTuple := stage == commit.Stage && providerID.Valid && providerID.String == commit.ProviderID &&
		providerGeneration.Valid && providerGeneration.Int64 == commit.ProviderGeneration
	if sameTuple {
		if commit.CursorSequence == 0 {
			commit.CursorSequence = currentSequence
		}
		if commit.CursorSequence != currentSequence {
			return 0, fmt.Errorf("health cursor sequence does not match the active tuple")
		}
		if storedCursor > commit.CursorSegment {
			return storedCursor, nil
		}
		return commit.CursorSegment, nil
	}
	if commit.CursorSequence == 0 || currentSequence == math.MaxInt64 ||
		commit.CursorSequence != currentSequence+1 {
		return 0, fmt.Errorf("health cursor tuple transition requires the next sequence")
	}
	return commit.CursorSegment, nil
}

func storedResolvedBitmap(
	chunkID string,
	segmentCount int64,
	tested, temporary, inconclusive []byte,
	resolvedDelta int64,
	resolved []byte,
) ([]byte, error) {
	if segmentCount <= 0 || segmentCount > int64(math.MaxInt)/8 || segmentCount > math.MaxInt64-7 {
		return nil, fmt.Errorf("stored health chunk %s has an invalid segment range", chunkID)
	}
	bitmapBytes := int((segmentCount + 7) / 8)
	for _, bitmap := range [][]byte{tested, temporary, inconclusive} {
		if len(bitmap) != bitmapBytes {
			return nil, fmt.Errorf("stored health chunk %s has an invalid bitmap length", chunkID)
		}
	}
	if remainder := segmentCount % 8; remainder != 0 {
		allowed := byte((1 << remainder) - 1)
		for _, bitmap := range [][]byte{tested, temporary, inconclusive} {
			if bitmap[len(bitmap)-1]&^allowed != 0 {
				return nil, fmt.Errorf("stored health chunk %s sets bits outside its range", chunkID)
			}
		}
	}
	conclusive := make([]byte, bitmapBytes)
	for i := range bitmapBytes {
		if temporary[i]&inconclusive[i] != 0 ||
			(temporary[i]|inconclusive[i])&^tested[i] != 0 {
			return nil, fmt.Errorf("stored health chunk %s has invalid inconclusive outcomes", chunkID)
		}
		conclusive[i] = tested[i] &^ (temporary[i] | inconclusive[i])
	}
	if resolved == nil {
		switch {
		case resolvedDelta == 0:
			return make([]byte, bitmapBytes), nil
		case resolvedDelta == bitmapPopulation(conclusive):
			return conclusive, nil
		default:
			return nil, fmt.Errorf("%w: %s", ErrAmbiguousLegacyHealthChunk, chunkID)
		}
	}
	if len(resolved) != bitmapBytes {
		return nil, fmt.Errorf("stored health chunk %s has an invalid resolved bitmap length", chunkID)
	}
	if remainder := segmentCount % 8; remainder != 0 {
		allowed := byte((1 << remainder) - 1)
		if resolved[len(resolved)-1]&^allowed != 0 {
			return nil, fmt.Errorf("stored health chunk %s has resolved bits outside its range", chunkID)
		}
	}
	for i := range bitmapBytes {
		if resolved[i]&^conclusive[i] != 0 {
			return nil, fmt.Errorf("stored health chunk %s resolves an inconclusive or untested position", chunkID)
		}
	}
	if resolvedDelta < 0 || bitmapPopulation(resolved) != resolvedDelta {
		return nil, fmt.Errorf("stored health chunk %s has inconsistent resolved progress", chunkID)
	}
	return resolved, nil
}

func healthRunResolvedCount(
	ctx context.Context,
	tx *dialectAwareTx,
	runID string,
	totalSegments int64,
	pending HealthChunkCommit,
) (int64, error) {
	resolvedPositions := make(map[int64]struct{})
	add := func(start, count int64, bitmap []byte) error {
		if start < 0 || count <= 0 || start > math.MaxInt64-count ||
			count > totalSegments || start > totalSegments-count {
			return fmt.Errorf("stored health chunk range exceeds run total")
		}
		for byteIndex, value := range bitmap {
			for value != 0 {
				bit := bits.TrailingZeros8(value)
				relative := int64(byteIndex*8 + bit)
				if relative >= count {
					return fmt.Errorf("stored health chunk resolved bit exceeds its range")
				}
				resolvedPositions[start+relative] = struct{}{}
				value &= value - 1
			}
		}
		return nil
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT id, segment_start, segment_count, tested_bitmap, temporary_bitmap,
		       inconclusive_bitmap, resolved_delta, resolved_bitmap
		FROM health_run_chunks
		WHERE run_id = ?
	`, runID)
	if err != nil {
		return 0, fmt.Errorf("read health run resolved progress: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var chunkID string
		var start, count, resolvedDelta int64
		var tested, temporary, inconclusive, resolved []byte
		if err := rows.Scan(&chunkID, &start, &count, &tested, &temporary,
			&inconclusive, &resolvedDelta, &resolved); err != nil {
			return 0, fmt.Errorf("scan health run resolved progress: %w", err)
		}
		bitmap, err := storedResolvedBitmap(
			chunkID, count, tested, temporary, inconclusive, resolvedDelta, resolved,
		)
		if err != nil {
			return 0, err
		}
		if err := add(start, count, bitmap); err != nil {
			return 0, err
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate health run resolved progress: %w", err)
	}
	if err := add(pending.SegmentStart, pending.SegmentCount, pending.ResolvedBitmap); err != nil {
		return 0, err
	}
	return int64(len(resolvedPositions)), nil
}

func healthStatementTime(ctx context.Context, tx *dialectAwareTx) (time.Time, error) {
	if tx.dialect.IsPostgres() {
		var at time.Time
		if err := tx.QueryRowContext(ctx, `SELECT statement_timestamp()`).Scan(&at); err != nil {
			return time.Time{}, fmt.Errorf("read database health commit time: %w", err)
		}
		return at.UTC(), nil
	}
	var encoded string
	if err := tx.QueryRowContext(ctx,
		`SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now')`,
	).Scan(&encoded); err != nil {
		return time.Time{}, fmt.Errorf("read database health commit time: %w", err)
	}
	at, err := time.Parse(time.RFC3339Nano, encoded)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse database health commit time: %w", err)
	}
	return at, nil
}

func (r *HealthStateRepository) CommitHealthChunk(ctx context.Context, commit HealthChunkCommit) (*HealthRun, error) {
	prepared, err := prepareHealthChunk(commit)
	if err != nil {
		return nil, err
	}
	commit = prepared
	var result HealthRun
	err = r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		var revisionID, snapshotID string
		var totalSegments, storedSequence, storedCursor int64
		var leaseExpiresAt time.Time
		var stage string
		var providerID sql.NullString
		var providerGeneration sql.NullInt64
		// A conditional write both locks the run row and proves that this exact
		// owner/token is still current and unexpired before idempotency is checked.
		err := tx.QueryRowContext(ctx, `
			UPDATE health_runs SET updated_at = updated_at
			WHERE id = ? AND status = 'running' AND lease_owner = ?
			  AND fencing_token = ?
			RETURNING file_revision_id, provider_snapshot_id, total_segments, lease_expires_at,
			          stage, current_provider_id, current_provider_generation,
			          cursor_sequence, cursor_segment
		`, commit.RunID, commit.LeaseOwner, commit.FencingToken).Scan(
			&revisionID, &snapshotID, &totalSegments, &leaseExpiresAt, &stage,
			&providerID, &providerGeneration, &storedSequence, &storedCursor,
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
		if err := tx.QueryRowContext(ctx, `
			UPDATE health_file_revisions SET active = active WHERE id = ? RETURNING id
		`, revisionID).Scan(&lockedRevision); err != nil {
			return fmt.Errorf("lock health file revision for observation commit: %w", err)
		}
		if commit.SegmentCount > totalSegments || commit.SegmentStart > totalSegments-commit.SegmentCount ||
			commit.CursorSegment > totalSegments {
			return fmt.Errorf("chunk range or cursor exceeds run total")
		}
		nextCursor, err := normalizeHealthCursor(
			&commit, stage, providerID, providerGeneration, storedSequence, storedCursor,
		)
		if err != nil {
			return err
		}
		digest, err := healthChunkDigest(commit)
		if err != nil {
			return err
		}
		var snapshotEntry int
		err = tx.QueryRowContext(ctx, `
			SELECT 1 FROM health_provider_snapshot_entries
			WHERE snapshot_id = ? AND provider_id = ? AND provider_generation = ?
		`, snapshotID, commit.ProviderID, commit.ProviderGeneration).Scan(&snapshotEntry)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrProviderSnapshotMismatch
		}
		if err != nil {
			return fmt.Errorf("verify provider dispatch snapshot: %w", err)
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
		resolvedSegments, err := healthRunResolvedCount(ctx, tx, commit.RunID, totalSegments, commit)
		if err != nil {
			return err
		}
		applyTime, err := healthStatementTime(ctx, tx)
		if err != nil {
			return err
		}
		appliedCommit := commit
		appliedCommit.CommittedAt = applyTime

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
				(id, run_id, provider_id, provider_generation, stage, observation_kind, segment_start,
				 segment_count, tested_bitmap, present_bitmap, absent_bitmap, corrupt_bitmap,
				 temporary_bitmap, inconclusive_bitmap, resolved_bitmap, retry_state, commit_digest,
				 fencing_token, resolved_delta, provider_checks_delta, missing_candidates_delta,
				 inconclusive_delta, committed_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, commit.ChunkID, commit.RunID, commit.ProviderID, commit.ProviderGeneration,
			commit.Stage, commit.ObservationKind, commit.SegmentStart, commit.SegmentCount, commit.TestedBitmap,
			commit.PresentBitmap, commit.AbsentBitmap, commit.CorruptBitmap,
			commit.TemporaryBitmap, commit.InconclusiveBitmap, commit.ResolvedBitmap, retryJSON, digest,
			commit.FencingToken, commit.ResolvedDelta, commit.ProviderChecksDelta,
			commit.MissingCandidatesDelta, commit.InconclusiveDelta,
			applyTime)
		if err != nil {
			return fmt.Errorf("insert health run chunk: %w", err)
		}

		_, err = tx.ExecContext(ctx, `
			INSERT INTO health_provider_coverage
				(id, file_revision_id, provider_id, provider_generation, observation_kind, segment_start,
				 segment_count, tested_bitmap, present_bitmap, source_chunk_id, observed_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, uuid.NewString(), revisionID, commit.ProviderID, commit.ProviderGeneration,
			commit.ObservationKind, commit.SegmentStart, commit.SegmentCount, commit.TestedBitmap,
			commit.PresentBitmap, commit.ChunkID, applyTime)
		if err != nil {
			return fmt.Errorf("insert provider coverage: %w", err)
		}

		if err := persistChunkExceptions(ctx, tx, revisionID, appliedCommit); err != nil {
			return err
		}
		if err := persistAttemptEvidence(ctx, tx, revisionID, appliedCommit); err != nil {
			return err
		}
		if err := persistConfirmationEvents(ctx, tx, revisionID, appliedCommit); err != nil {
			return err
		}
		if err := persistRetryState(ctx, tx, revisionID, appliedCommit); err != nil {
			return err
		}

		update := `
			UPDATE health_runs
			SET resolved_segments = ?,
			    provider_checks = provider_checks + ?,
			    missing_candidates = missing_candidates + ?,
			    inconclusive_count = inconclusive_count + ?,
			    cursor_sequence = ?, cursor_segment = ?,
			    stage = ?, current_provider_id = ?, current_provider_generation = ?,
			    updated_at = ?
			WHERE id = ? AND lease_owner = ? AND fencing_token = ?
			  AND ? <= total_segments
			RETURNING id, file_revision_id, provider_snapshot_id, trigger, mode, status,
			          lease_owner, lease_expires_at, fencing_token, total_segments,
			          resolved_segments, provider_checks, missing_candidates, inconclusive_count,
			          stage, current_provider_id, current_provider_generation, cursor_sequence, cursor_segment,
			          pause_requested, cancel_requested, created_at, started_at, updated_at, completed_at
		`
		err = scanHealthRun(tx.QueryRowContext(ctx, update,
			resolvedSegments, commit.ProviderChecksDelta, commit.MissingCandidatesDelta,
			commit.InconclusiveDelta, commit.CursorSequence, nextCursor,
			commit.Stage, commit.ProviderID, commit.ProviderGeneration, applyTime,
			commit.RunID, commit.LeaseOwner, commit.FencingToken, resolvedSegments,
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
				  AND segment_index = ? AND observed_at <= ?
			`
			if commit.ObservationKind == HealthObservationSTAT {
				query += ` AND outcome <> 'corrupt_body'`
			}
			_, err := tx.ExecContext(ctx, query, revisionID, commit.ProviderID,
				commit.ProviderGeneration, segmentIndex, commit.CommittedAt)
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
				(file_revision_id, provider_id, provider_generation, segment_index,
				 outcome, source_chunk_id, observed_at, next_retry_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(file_revision_id, provider_id, provider_generation, segment_index)
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
		`, revisionID, commit.ProviderID, commit.ProviderGeneration, segmentIndex,
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
		  AND observed_at > ? AND segment_start <= ? AND segment_start + segment_count > ?
	`, revisionID, commit.ProviderID, commit.ProviderGeneration, commit.CommittedAt,
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
				 provider_generation, segment_index, operation, outcome, response_code,
				 body_validation, cause_class, admission_wait_ns, pool_queue_ns,
				 pipeline_wait_ns, response_service_ns, observed_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, attempt.IdempotencyKey, commit.ChunkID, revisionID, commit.ProviderID,
			commit.ProviderGeneration, attempt.SegmentIndex, attempt.Operation,
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
				 provider_generation, segment_index, cause, observed_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(idempotency_key) DO NOTHING
		`, confirmation.IdempotencyKey, commit.ChunkID, revisionID, commit.ProviderID,
			commit.ProviderGeneration, confirmation.SegmentIndex, confirmation.Cause,
			confirmation.ObservedAt.UTC())
		if err != nil {
			return fmt.Errorf("persist confirmation event: %w", err)
		}
		var existingRevision, existingProvider string
		var existingGeneration, existingSegment int64
		var existingCause GapCause
		var existingObservedAt time.Time
		err = tx.QueryRowContext(ctx, `
			SELECT file_revision_id, provider_id, provider_generation, segment_index, cause, observed_at
			FROM health_confirmation_events WHERE idempotency_key = ?
		`, confirmation.IdempotencyKey).Scan(
			&existingRevision, &existingProvider, &existingGeneration, &existingSegment,
			&existingCause, &existingObservedAt,
		)
		if err != nil {
			return fmt.Errorf("read confirmation event identity: %w", err)
		}
		if existingRevision != revisionID || existingProvider != commit.ProviderID ||
			existingGeneration != commit.ProviderGeneration || existingSegment != confirmation.SegmentIndex ||
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
			 segment_start, segment_count, outcome, attempt, next_attempt_at, exhausted, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(retry_key) DO NOTHING
	`, retry.RetryKey, commit.ChunkID, revisionID, commit.ProviderID,
		commit.ProviderGeneration, retry.SegmentStart, retry.SegmentCount,
		retry.Outcome, retry.Attempt, nextAttempt, retry.Exhausted, commit.CommittedAt)
	if err != nil {
		return fmt.Errorf("persist health retry state: %w", err)
	}
	var existingRevision, existingProvider, existingOutcome string
	var existingGeneration, existingStart, existingCount int64
	var existingAttempt int
	var existingNextAttempt *time.Time
	var existingExhausted bool
	var existingUpdatedAt time.Time
	err = tx.QueryRowContext(ctx, `
		SELECT file_revision_id, provider_id, provider_generation, segment_start,
		       segment_count, outcome, attempt, next_attempt_at, exhausted, updated_at
		FROM health_retry_states WHERE retry_key = ?
	`, retry.RetryKey).Scan(
		&existingRevision, &existingProvider, &existingGeneration, &existingStart,
		&existingCount, &existingOutcome, &existingAttempt, &existingNextAttempt,
		&existingExhausted, &existingUpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("read health retry identity: %w", err)
	}
	if existingRevision != revisionID || existingProvider != commit.ProviderID ||
		existingGeneration != commit.ProviderGeneration || existingStart != retry.SegmentStart ||
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
	result, err := r.db.ExecContext(ctx, `UPDATE health_runs SET pause_requested = ?, updated_at = ? WHERE id = ?`, requested, at.UTC(), runID)
	if err != nil {
		return fmt.Errorf("request run pause: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (r *HealthStateRepository) RequestRunCancel(ctx context.Context, runID string, at time.Time) error {
	result, err := r.db.ExecContext(ctx, `
		UPDATE health_runs
		SET cancel_requested = TRUE, status = 'canceled', lease_owner = NULL,
		    lease_expires_at = NULL, updated_at = ?, completed_at = ?
		WHERE id = ? AND status NOT IN ('completed', 'canceled')
	`, at.UTC(), at.UTC(), runID)
	if err != nil {
		return fmt.Errorf("request run cancel: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows == 0 {
		return sql.ErrNoRows
	}
	return nil
}
