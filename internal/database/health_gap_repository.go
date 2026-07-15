package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

func validateGapWrite(gap GapRangeWrite) error {
	if gap.FileRevisionID == "" || gap.StartSegment < 0 || gap.SegmentCount <= 0 || gap.CreatedAt.IsZero() {
		return fmt.Errorf("gap revision, positive range, and creation time are required")
	}
	if gap.StartSegment > int64(^uint64(0)>>1)-gap.SegmentCount {
		return fmt.Errorf("gap segment range overflows")
	}
	switch gap.Kind {
	case GapKindProvisional, GapKindConfirmedAbsent, GapKindConfirmedUnusable, GapKindLegacyUnverified:
	default:
		return fmt.Errorf("invalid gap kind %q", gap.Kind)
	}
	if gap.Status != GapStatusActive {
		return fmt.Errorf("generic gap persistence may create or refresh only active gaps")
	}
	if gap.ClearedAt != nil {
		return fmt.Errorf("generic gap persistence cannot clear an episode")
	}
	for _, cause := range gap.Causes {
		if cause.ProviderID == "" || cause.ProviderGeneration <= 0 || cause.ProviderActivationEpoch <= 0 ||
			cause.ConfirmationCount < 0 ||
			(cause.Cause != GapCauseAbsent && cause.Cause != GapCauseCorrupt) {
			return fmt.Errorf("invalid provider gap cause")
		}
	}
	fenced := gap.RunID != "" || gap.LeaseOwner != "" || gap.FencingToken != 0
	if fenced && (gap.RunID == "" || gap.LeaseOwner == "" || gap.FencingToken <= 0) {
		return fmt.Errorf("gap run fence must supply run, owner, and fencing token together")
	}
	return nil
}

func (r *HealthStateRepository) UpsertGapRange(ctx context.Context, write GapRangeWrite) (*HealthGapRange, error) {
	if err := validateGapWrite(write); err != nil {
		return nil, err
	}
	if write.ID == "" {
		write.ID = uuid.NewString()
	}
	write.CreatedAt = write.CreatedAt.UTC()
	var gap HealthGapRange
	err := r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		if write.RunID != "" {
			var revisionID, snapshotID string
			var leaseExpiresAt time.Time
			err := tx.QueryRowContext(ctx, `
				UPDATE health_runs SET updated_at = updated_at
				WHERE id = ? AND status = 'running' AND lease_owner = ? AND fencing_token = ?
				RETURNING file_revision_id, provider_snapshot_id, lease_expires_at
			`, write.RunID, write.LeaseOwner, write.FencingToken).Scan(
				&revisionID, &snapshotID, &leaseExpiresAt,
			)
			if errors.Is(err, sql.ErrNoRows) || (err == nil && !leaseExpiresAt.After(r.now().UTC())) {
				return ErrStaleHealthLease
			}
			if err != nil {
				return fmt.Errorf("verify gap observation run fence: %w", err)
			}
			if revisionID != write.FileRevisionID {
				return ErrStaleHealthSchedule
			}
			current, err := providerSnapshotMembershipMatchesCurrentTx(ctx, tx, snapshotID)
			if err != nil {
				return err
			}
			if !current {
				return ErrProviderSnapshotMismatch
			}
			var active bool
			if err := tx.QueryRowContext(ctx, `
				UPDATE health_run_schedule SET active = active
				WHERE run_id = ? RETURNING active
			`, write.RunID).Scan(&active); errors.Is(err, sql.ErrNoRows) || (err == nil && !active) {
				return ErrStaleHealthSchedule
			} else if err != nil {
				return fmt.Errorf("verify gap observation schedule: %w", err)
			}
		}
		var revisionSegments int64
		if err := tx.QueryRowContext(ctx, `
			UPDATE health_file_revisions SET active = active WHERE id = ?
			RETURNING segment_count
		`, write.FileRevisionID).Scan(&revisionSegments); err != nil {
			return fmt.Errorf("read gap file revision bounds: %w", err)
		}
		if write.SegmentCount > revisionSegments || write.StartSegment > revisionSegments-write.SegmentCount {
			return fmt.Errorf("gap range exceeds file revision segment count")
		}
		causes, confirmedAt, err := r.authoritativeGapProviderCauses(ctx, tx, write)
		if err != nil {
			return err
		}
		write.Causes = causes
		write.ConfirmedAt = confirmedAt
		var existingRevision string
		var existingKind GapKind
		var existingStart, existingCount, episode int64
		var existingCreated time.Time
		err = tx.QueryRowContext(ctx, `
			SELECT file_revision_id, kind, start_segment, segment_count, episode, created_at
			FROM health_gap_ranges WHERE id = ?
		`, write.ID).Scan(&existingRevision, &existingKind, &existingStart,
			&existingCount, &episode, &existingCreated)
		switch {
		case err == nil:
			if existingRevision != write.FileRevisionID || existingKind != write.Kind ||
				existingStart != write.StartSegment || existingCount != write.SegmentCount ||
				!existingCreated.Equal(write.CreatedAt) {
				return ErrHealthChunkConflict
			}
			var existingStatus GapStatus
			if err := tx.QueryRowContext(ctx, `
				SELECT status FROM health_gap_ranges WHERE id = ?
			`, write.ID).Scan(&existingStatus); err != nil {
				return fmt.Errorf("read health gap status: %w", err)
			}
			if existingStatus != GapStatusActive {
				return fmt.Errorf("generic gap persistence cannot transition a closed episode")
			}
		case errors.Is(err, sql.ErrNoRows):
			if err := tx.QueryRowContext(ctx, `
				SELECT COALESCE(MAX(episode), 0) + 1
				FROM health_gap_ranges
				WHERE file_revision_id = ? AND kind = ?
				  AND start_segment = ? AND segment_count = ?
			`, write.FileRevisionID, write.Kind, write.StartSegment, write.SegmentCount).Scan(&episode); err != nil {
				return fmt.Errorf("allocate health gap episode: %w", err)
			}
		case err != nil:
			return fmt.Errorf("read health gap identity: %w", err)
		}

		var nextRevalidationAt any
		if write.ConfirmedAt != nil &&
			(write.Kind == GapKindConfirmedAbsent || write.Kind == GapKindConfirmedUnusable) {
			nextRevalidationAt = write.ConfirmedAt.UTC().Add(24 * time.Hour)
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO health_gap_ranges
				(id, file_revision_id, kind, start_segment, segment_count, episode, status,
				 created_at, confirmed_at, cleared_at, revalidation_step,
				 next_revalidation_at, last_revalidation_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, NULL)
			ON CONFLICT(id) DO UPDATE SET
				confirmed_at = COALESCE(health_gap_ranges.confirmed_at, excluded.confirmed_at),
				next_revalidation_at = COALESCE(
					health_gap_ranges.next_revalidation_at, excluded.next_revalidation_at),
				cleared_at = health_gap_ranges.cleared_at
		`, write.ID, write.FileRevisionID, write.Kind, write.StartSegment,
			write.SegmentCount, episode, write.Status, write.CreatedAt, write.ConfirmedAt,
			write.ClearedAt, nextRevalidationAt)
		if err != nil {
			return fmt.Errorf("upsert health gap range: %w", err)
		}
		var retainedRevision string
		var retainedKind GapKind
		var retainedStart, retainedCount, retainedEpisode int64
		var retainedCreated time.Time
		if err := tx.QueryRowContext(ctx, `
			SELECT file_revision_id, kind, start_segment, segment_count, episode, created_at
			FROM health_gap_ranges WHERE id = ?
		`, write.ID).Scan(&retainedRevision, &retainedKind, &retainedStart,
			&retainedCount, &retainedEpisode, &retainedCreated); err != nil {
			return fmt.Errorf("verify health gap identity: %w", err)
		}
		if retainedRevision != write.FileRevisionID || retainedKind != write.Kind ||
			retainedStart != write.StartSegment || retainedCount != write.SegmentCount || retainedEpisode != episode ||
			!retainedCreated.Equal(write.CreatedAt) {
			return ErrHealthChunkConflict
		}
		for _, cause := range write.Causes {
			var confirmedAt any
			if !cause.ConfirmedAt.IsZero() {
				confirmedAt = cause.ConfirmedAt.UTC()
			}
			_, err = tx.ExecContext(ctx, `
				INSERT INTO health_gap_provider_causes
					(gap_id, provider_id, provider_generation, provider_activation_epoch,
					 cause, confirmation_count, confirmed_at)
				VALUES (?, ?, ?, ?, ?, ?, ?)
				ON CONFLICT(gap_id, provider_id, provider_generation, provider_activation_epoch) DO UPDATE SET
					cause = CASE
						WHEN health_gap_provider_causes.confirmed_at IS NULL
						  OR (excluded.confirmed_at IS NOT NULL AND health_gap_provider_causes.confirmed_at <= excluded.confirmed_at)
						THEN excluded.cause ELSE health_gap_provider_causes.cause END,
					confirmation_count = CASE
						WHEN health_gap_provider_causes.cause = excluded.cause
						  AND health_gap_provider_causes.confirmation_count > excluded.confirmation_count
						THEN health_gap_provider_causes.confirmation_count ELSE excluded.confirmation_count END,
					confirmed_at = CASE
						WHEN health_gap_provider_causes.confirmed_at IS NULL
						  OR (excluded.confirmed_at IS NOT NULL AND health_gap_provider_causes.confirmed_at <= excluded.confirmed_at)
						THEN excluded.confirmed_at ELSE health_gap_provider_causes.confirmed_at END
			`, write.ID, cause.ProviderID, cause.ProviderGeneration,
				cause.ProviderActivationEpoch, cause.Cause,
				cause.ConfirmationCount, confirmedAt)
			if err != nil {
				return fmt.Errorf("insert health gap provider cause: %w", err)
			}
		}
		if err := tx.QueryRowContext(ctx, `
			SELECT id, file_revision_id, kind, start_segment, segment_count, episode, status,
			       created_at, confirmed_at, cleared_at, revalidation_step,
			       next_revalidation_at, last_revalidation_at
			FROM health_gap_ranges WHERE id = ?
		`, write.ID).Scan(&gap.ID, &gap.FileRevisionID, &gap.Kind, &gap.StartSegment,
			&gap.SegmentCount, &gap.Episode, &gap.Status, &gap.CreatedAt,
			&gap.ConfirmedAt, &gap.ClearedAt, &gap.RevalidationStep,
			&gap.NextRevalidationAt, &gap.LastRevalidationAt); err != nil {
			return fmt.Errorf("read persisted health gap: %w", err)
		}
		rows, err := tx.QueryContext(ctx, `
			SELECT provider_id, provider_generation, provider_activation_epoch,
			       cause, confirmation_count, confirmed_at
			FROM health_gap_provider_causes WHERE gap_id = ?
			ORDER BY provider_id, provider_generation, provider_activation_epoch
		`, write.ID)
		if err != nil {
			return fmt.Errorf("read persisted health gap causes: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var cause GapProviderCause
			var confirmedAt *time.Time
			if err := rows.Scan(&cause.ProviderID, &cause.ProviderGeneration,
				&cause.ProviderActivationEpoch, &cause.Cause, &cause.ConfirmationCount, &confirmedAt); err != nil {
				return fmt.Errorf("scan persisted health gap cause: %w", err)
			}
			if confirmedAt != nil {
				cause.ConfirmedAt = confirmedAt.UTC()
			}
			gap.Causes = append(gap.Causes, cause)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return &gap, nil
}

func scanHealthGapRange(row rowScanner, gap *HealthGapRange) error {
	return row.Scan(
		&gap.ID, &gap.FileRevisionID, &gap.Kind, &gap.StartSegment,
		&gap.SegmentCount, &gap.Episode, &gap.Status, &gap.CreatedAt,
		&gap.ConfirmedAt, &gap.ClearedAt, &gap.RevalidationStep,
		&gap.NextRevalidationAt, &gap.LastRevalidationAt,
	)
}

const healthGapRangeSelect = `
	SELECT id, file_revision_id, kind, start_segment, segment_count, episode,
	       status, created_at, confirmed_at, cleared_at, revalidation_step,
	       next_revalidation_at, last_revalidation_at
	FROM health_gap_ranges
`

func (r *HealthStateRepository) GetHealthGapRange(ctx context.Context, gapID string) (*HealthGapRange, error) {
	var gap HealthGapRange
	err := scanHealthGapRange(r.db.QueryRowContext(ctx,
		healthGapRangeSelect+` WHERE id = ?`, gapID), &gap)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get health gap range: %w", err)
	}
	causes, err := r.listGapProviderCauses(ctx, gap.ID)
	if err != nil {
		return nil, err
	}
	gap.Causes = causes
	return &gap, nil
}

func (r *HealthStateRepository) ListDueGapRevalidations(
	ctx context.Context,
	now time.Time,
	limit int,
) ([]GapRevalidationWork, error) {
	if limit <= 0 {
		limit = 32
	}
	rows, err := r.db.QueryContext(ctx, healthGapRangeSelect+`
		WHERE status = 'active'
		  AND kind IN ('confirmed_absent', 'confirmed_unusable')
		  AND revalidation_step < 4
		  AND next_revalidation_at IS NOT NULL
		  AND next_revalidation_at <= ?
		  AND (SELECT COUNT(*) FROM health_providers WHERE active = TRUE) > 0
		  AND (SELECT COUNT(*) FROM health_providers WHERE active = TRUE) = (
		    SELECT COUNT(*)
		    FROM health_gap_provider_causes cause
		    JOIN health_providers provider
		      ON provider.id = cause.provider_id AND provider.active = TRUE
		     AND provider.current_generation = cause.provider_generation
		     AND provider.activation_epoch = cause.provider_activation_epoch
		    WHERE cause.gap_id = health_gap_ranges.id
		  )
		ORDER BY next_revalidation_at, id
		LIMIT ?
	`, now.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("list due gap revalidations: %w", err)
	}
	var gaps []HealthGapRange
	for rows.Next() {
		var gap HealthGapRange
		if err := scanHealthGapRange(rows, &gap); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan due gap revalidation: %w", err)
		}
		if gap.NextRevalidationAt == nil {
			continue
		}
		gaps = append(gaps, gap)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close due gap revalidations: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate due gap revalidations: %w", err)
	}
	var work []GapRevalidationWork
	for _, gap := range gaps {
		causes, err := r.listGapProviderCauses(ctx, gap.ID)
		if err != nil {
			return nil, err
		}
		gap.Causes = causes
		var revisionSegments int64
		if err := r.db.QueryRowContext(ctx, `
			SELECT segment_count FROM health_file_revisions WHERE id = ? AND active = TRUE
		`, gap.FileRevisionID).Scan(&revisionSegments); err != nil {
			return nil, fmt.Errorf("read due gap revision bounds: %w", err)
		}
		if gap.StartSegment > revisionSegments-gap.SegmentCount {
			return nil, fmt.Errorf("due gap exceeds active revision bounds")
		}
		work = append(work, GapRevalidationWork{
			Gap: gap, TotalSegments: gap.SegmentCount,
			Step: gap.RevalidationStep, NotBefore: gap.NextRevalidationAt.UTC(),
		})
	}
	return work, nil
}

func (r *HealthStateRepository) listGapProviderCauses(
	ctx context.Context,
	gapID string,
) ([]GapProviderCause, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT provider_id, provider_generation, provider_activation_epoch,
		       cause, confirmation_count, confirmed_at
		FROM health_gap_provider_causes
		WHERE gap_id = ?
		ORDER BY provider_id, provider_generation, provider_activation_epoch
	`, gapID)
	if err != nil {
		return nil, fmt.Errorf("list health gap provider causes: %w", err)
	}
	defer rows.Close()
	var causes []GapProviderCause
	for rows.Next() {
		var cause GapProviderCause
		var confirmedAt *time.Time
		if err := rows.Scan(
			&cause.ProviderID, &cause.ProviderGeneration, &cause.ProviderActivationEpoch,
			&cause.Cause, &cause.ConfirmationCount, &confirmedAt,
		); err != nil {
			return nil, fmt.Errorf("scan health gap provider cause: %w", err)
		}
		if confirmedAt != nil {
			cause.ConfirmedAt = confirmedAt.UTC()
		}
		causes = append(causes, cause)
	}
	return causes, rows.Err()
}

// authoritativeGapProviderCauses ignores caller counts/timestamps and derives
// one range-wide count shared by every requested active provider activation.
// Each increment requires another complete durable run/stage evidence wave at
// least the repository's configured minimum delay after the prior wave.
func (r *HealthStateRepository) authoritativeGapProviderCauses(
	ctx context.Context,
	tx *dialectAwareTx,
	write GapRangeWrite,
) ([]GapProviderCause, *time.Time, error) {
	active := make(map[importProviderKey]struct{})
	rows, err := tx.QueryContext(ctx, `
		SELECT id, current_generation, activation_epoch
		FROM health_providers WHERE active = TRUE
	`)
	if err != nil {
		return nil, nil, fmt.Errorf("read active providers for gap confirmation: %w", err)
	}
	for rows.Next() {
		var provider importProviderKey
		if err := rows.Scan(&provider.ID, &provider.Generation, &provider.ActivationEpoch); err != nil {
			rows.Close()
			return nil, nil, fmt.Errorf("scan active provider for gap confirmation: %w", err)
		}
		active[provider] = struct{}{}
	}
	if err := rows.Close(); err != nil {
		return nil, nil, fmt.Errorf("close active providers for gap confirmation: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate active providers for gap confirmation: %w", err)
	}

	seen := make(map[importProviderKey]struct{}, len(write.Causes))
	for _, requested := range write.Causes {
		provider := importProviderKey{
			ID: requested.ProviderID, Generation: requested.ProviderGeneration,
			ActivationEpoch: requested.ProviderActivationEpoch,
		}
		if provider.ActivationEpoch <= 0 {
			return nil, nil, fmt.Errorf("provider gap cause requires an explicit activation epoch")
		}
		if _, ok := active[provider]; !ok {
			return nil, nil, fmt.Errorf("provider gap cause is outside the active provider activation set")
		}
		if _, duplicate := seen[provider]; duplicate {
			return nil, nil, fmt.Errorf("provider gap cause duplicates an activation-scoped provider")
		}
		seen[provider] = struct{}{}
	}
	derived, gapConfirmedAt, err := deriveTimeSeparatedGapCauses(
		ctx, tx, write, r.gapConfirmationMinimumDelay(),
	)
	if err != nil {
		return nil, nil, err
	}

	if write.Kind == GapKindConfirmedAbsent || write.Kind == GapKindConfirmedUnusable {
		if len(active) == 0 || len(derived) != len(active) {
			return nil, nil, fmt.Errorf("confirmed gap requires evidence for every active provider activation")
		}
		hasCorrupt := false
		for _, cause := range derived {
			if cause.ConfirmationCount < 2 {
				return nil, nil, fmt.Errorf("confirmed gap requires two time-separated evidence waves")
			}
			if write.Kind == GapKindConfirmedAbsent && cause.Cause != GapCauseAbsent {
				return nil, nil, fmt.Errorf("confirmed absent gap cannot contain corrupt provider evidence")
			}
			if cause.Cause == GapCauseCorrupt {
				hasCorrupt = true
			}
		}
		if write.Kind == GapKindConfirmedUnusable && !hasCorrupt {
			return nil, nil, fmt.Errorf("confirmed unusable gap requires repeated corrupt BODY evidence")
		}
	}
	return derived, gapConfirmedAt, nil
}

type gapConfirmationTuple struct {
	provider importProviderKey
	position int64
}

type gapConfirmationWaveKey struct {
	runID string
	stage string
}

type gapConfirmationWaveEvidence struct {
	key      gapConfirmationWaveKey
	evidence map[gapConfirmationTuple]gapConfirmationObservation
}

type qualifiedGapConfirmationWave struct {
	key         gapConfirmationWaveKey
	startedAt   time.Time
	completedAt time.Time
	causes      map[importProviderKey]GapCause
}

type gapConfirmationObservation struct {
	cause      GapCause
	observedAt time.Time
}

func deriveTimeSeparatedGapCauses(
	ctx context.Context,
	tx *dialectAwareTx,
	write GapRangeWrite,
	minimumConfirmationSeparation time.Duration,
) ([]GapProviderCause, *time.Time, error) {
	if len(write.Causes) == 0 {
		return nil, nil, nil
	}
	requested := make(map[importProviderKey]struct{}, len(write.Causes))
	for _, cause := range write.Causes {
		requested[importProviderKey{
			ID: cause.ProviderID, Generation: cause.ProviderGeneration,
			ActivationEpoch: cause.ProviderActivationEpoch,
		}] = struct{}{}
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT c.run_id, c.stage, e.provider_id, e.provider_generation,
		       e.provider_activation_epoch, e.segment_index, e.cause, e.observed_at
		FROM health_confirmation_events e
		JOIN health_run_chunks c ON c.id = e.source_chunk_id
		JOIN health_runs r ON r.id = c.run_id
		WHERE e.file_revision_id = ? AND r.file_revision_id = ?
		  AND c.provider_id = e.provider_id
		  AND c.provider_generation = e.provider_generation
		  AND c.provider_activation_epoch = e.provider_activation_epoch
		  AND e.segment_index >= ? AND e.segment_index < ?
		  AND e.observed_at >= ?
		ORDER BY e.observed_at, c.run_id, c.stage, e.provider_id, e.segment_index, e.idempotency_key
	`, write.FileRevisionID, write.FileRevisionID, write.StartSegment,
		write.StartSegment+write.SegmentCount, write.CreatedAt.UTC())
	if err != nil {
		return nil, nil, fmt.Errorf("read range-wide gap confirmation evidence: %w", err)
	}
	byWave := make(map[gapConfirmationWaveKey]*gapConfirmationWaveEvidence)
	for rows.Next() {
		var runID, stage, providerID string
		var generation, activationEpoch, position int64
		var cause GapCause
		var observedAt time.Time
		if err := rows.Scan(&runID, &stage, &providerID, &generation, &activationEpoch,
			&position, &cause, &observedAt); err != nil {
			rows.Close()
			return nil, nil, fmt.Errorf("scan range-wide gap confirmation evidence: %w", err)
		}
		provider := importProviderKey{
			ID: providerID, Generation: generation, ActivationEpoch: activationEpoch,
		}
		if _, ok := requested[provider]; !ok {
			continue
		}
		waveKey := gapConfirmationWaveKey{runID: runID, stage: stage}
		wave := byWave[waveKey]
		if wave == nil {
			wave = &gapConfirmationWaveEvidence{
				key: waveKey, evidence: make(map[gapConfirmationTuple]gapConfirmationObservation),
			}
			byWave[waveKey] = wave
		}
		tuple := gapConfirmationTuple{provider: provider, position: position}
		observedAt = observedAt.UTC()
		if retained, ok := wave.evidence[tuple]; !ok || observedAt.After(retained.observedAt) {
			wave.evidence[tuple] = gapConfirmationObservation{cause: cause, observedAt: observedAt}
		}
	}
	if err := rows.Close(); err != nil {
		return nil, nil, fmt.Errorf("close range-wide gap confirmation evidence: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate range-wide gap confirmation evidence: %w", err)
	}

	waves := make([]qualifiedGapConfirmationWave, 0, len(byWave))
	for _, wave := range byWave {
		var startedAt time.Time
		var completedAt time.Time
		causes := make(map[importProviderKey]GapCause, len(requested))
		complete := true
		hasCorrupt := false
		for provider := range requested {
			providerCause := GapCauseAbsent
			for position := write.StartSegment; position < write.StartSegment+write.SegmentCount; position++ {
				observation, ok := wave.evidence[gapConfirmationTuple{
					provider: provider, position: position,
				}]
				if !ok {
					complete = false
					break
				}
				if startedAt.IsZero() || observation.observedAt.Before(startedAt) {
					startedAt = observation.observedAt
				}
				if observation.observedAt.After(completedAt) {
					completedAt = observation.observedAt
				}
				if observation.cause == GapCauseCorrupt {
					providerCause = GapCauseCorrupt
					hasCorrupt = true
				}
			}
			if !complete {
				break
			}
			causes[provider] = providerCause
		}
		if complete && write.Kind == GapKindConfirmedAbsent && hasCorrupt {
			complete = false
		}
		if complete && write.Kind == GapKindConfirmedUnusable && !hasCorrupt {
			complete = false
		}
		if complete {
			waves = append(waves, qualifiedGapConfirmationWave{
				key: wave.key, startedAt: startedAt, completedAt: completedAt, causes: causes,
			})
		}
	}
	if len(waves) == 0 {
		return nil, nil, fmt.Errorf("%w: provider causes lack a complete activation-scoped all-requested-provider wave",
			errIncompleteGapConfirmationEvidence)
	}
	sort.Slice(waves, func(i, j int) bool {
		if waves[i].completedAt.Equal(waves[j].completedAt) {
			if waves[i].key.runID == waves[j].key.runID {
				return waves[i].key.stage < waves[j].key.stage
			}
			return waves[i].key.runID < waves[j].key.runID
		}
		return waves[i].completedAt.Before(waves[j].completedAt)
	})

	qualified := make([]qualifiedGapConfirmationWave, 0, len(waves))
	for _, wave := range waves {
		if len(qualified) == 0 ||
			!wave.startedAt.Before(qualified[len(qualified)-1].completedAt.Add(minimumConfirmationSeparation)) {
			qualified = append(qualified, wave)
		}
	}
	confirmedAt := qualified[len(qualified)-1].completedAt
	derived := make([]GapProviderCause, 0, len(write.Causes))
	latestCauses := qualified[len(qualified)-1].causes
	for _, cause := range write.Causes {
		provider := importProviderKey{
			ID: cause.ProviderID, Generation: cause.ProviderGeneration,
			ActivationEpoch: cause.ProviderActivationEpoch,
		}
		cause.Cause = latestCauses[provider]
		cause.ConfirmationCount = len(qualified)
		cause.ConfirmedAt = confirmedAt
		derived = append(derived, cause)
	}
	return derived, &confirmedAt, nil
}

// ClearGapRangeFromChunk invalidates exactly those positions recovered by a
// post-episode validated BODY observation. Any unrecovered positions become
// new active subranges, so multiple chunks can safely converge on a gap.
func (r *HealthStateRepository) ClearGapRangeFromChunk(
	ctx context.Context,
	gapID string,
	chunkID string,
	clearedAt time.Time,
) (*HealthGapRange, error) {
	return r.clearGapRangeFromChunk(ctx, "", "", 0, gapID, chunkID, clearedAt)
}

// ClearGapRangeFromRunChunk clears/splits a targeted gap and completes the
// owning observation run in the same lease-fenced transaction.
func (r *HealthStateRepository) ClearGapRangeFromRunChunk(
	ctx context.Context,
	runID, owner string,
	fencingToken int64,
	gapID, chunkID string,
	clearedAt time.Time,
) (*HealthGapRange, error) {
	if runID == "" || owner == "" || fencingToken <= 0 {
		return nil, fmt.Errorf("gap recovery run fence is required")
	}
	return r.clearGapRangeFromChunk(
		ctx, runID, owner, fencingToken, gapID, chunkID, clearedAt,
	)
}

func (r *HealthStateRepository) clearGapRangeFromChunk(
	ctx context.Context,
	runID, owner string,
	fencingToken int64,
	gapID, chunkID string,
	clearedAt time.Time,
) (*HealthGapRange, error) {
	if gapID == "" || chunkID == "" || clearedAt.IsZero() {
		return nil, fmt.Errorf("gap, source chunk, and clear time are required")
	}
	clearedAt = clearedAt.UTC()
	if clearedAt.After(r.now().UTC().Add(5 * time.Minute)) {
		return nil, fmt.Errorf("gap clear time is too far in the future")
	}
	var gap HealthGapRange
	err := r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		var fencedRevisionID, snapshotID string
		var leaseExpiresAt time.Time
		if runID != "" {
			err := tx.QueryRowContext(ctx, `
				UPDATE health_runs SET updated_at = updated_at
				WHERE id = ? AND status = 'running' AND lease_owner = ? AND fencing_token = ?
				RETURNING file_revision_id, provider_snapshot_id, lease_expires_at
			`, runID, owner, fencingToken).Scan(
				&fencedRevisionID, &snapshotID, &leaseExpiresAt,
			)
			if errors.Is(err, sql.ErrNoRows) {
				return ErrStaleHealthLease
			}
			if err != nil {
				return fmt.Errorf("verify gap recovery run fence: %w", err)
			}
			if !leaseExpiresAt.After(r.now().UTC()) {
				return ErrStaleHealthLease
			}
			current, err := providerSnapshotMembershipMatchesCurrentTx(ctx, tx, snapshotID)
			if err != nil {
				return err
			}
			if !current {
				return ErrProviderSnapshotMismatch
			}
			var active bool
			var targetGapID *string
			if err := tx.QueryRowContext(ctx, `
				UPDATE health_run_schedule SET active = active
				WHERE run_id = ? RETURNING active, target_gap_id
			`, runID).Scan(&active, &targetGapID); err != nil {
				return fmt.Errorf("verify gap recovery schedule: %w", err)
			}
			if !active || targetGapID == nil || *targetGapID != gapID {
				return ErrStaleHealthSchedule
			}
		}
		var revisionID string
		if err := tx.QueryRowContext(ctx, `
			UPDATE health_gap_ranges SET status = status
			WHERE id = ?
			RETURNING id, file_revision_id, kind, start_segment, segment_count, episode,
			          status, created_at, confirmed_at, cleared_at, revalidation_step,
			          next_revalidation_at, last_revalidation_at
		`, gapID).Scan(&gap.ID, &revisionID, &gap.Kind, &gap.StartSegment,
			&gap.SegmentCount, &gap.Episode, &gap.Status, &gap.CreatedAt,
			&gap.ConfirmedAt, &gap.ClearedAt, &gap.RevalidationStep,
			&gap.NextRevalidationAt, &gap.LastRevalidationAt); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("health gap does not exist")
			}
			return fmt.Errorf("read health gap for clearing: %w", err)
		}
		gap.FileRevisionID = revisionID
		if runID != "" && fencedRevisionID != revisionID {
			return ErrStaleHealthSchedule
		}
		if gap.Status != GapStatusActive && gap.Status != GapStatusDormant {
			return fmt.Errorf("only an active or dormant health gap can be cleared")
		}

		rows, err := tx.QueryContext(ctx, `
			SELECT provider_id, provider_generation, provider_activation_epoch,
			       cause, confirmation_count, confirmed_at
			FROM health_gap_provider_causes WHERE gap_id = ?
			ORDER BY provider_id, provider_generation, provider_activation_epoch
		`, gapID)
		if err != nil {
			return fmt.Errorf("read health gap causes for recovery: %w", err)
		}
		for rows.Next() {
			var cause GapProviderCause
			var confirmedAt *time.Time
			if err := rows.Scan(&cause.ProviderID, &cause.ProviderGeneration,
				&cause.ProviderActivationEpoch, &cause.Cause,
				&cause.ConfirmationCount, &confirmedAt); err != nil {
				rows.Close()
				return fmt.Errorf("scan health gap cause for recovery: %w", err)
			}
			if confirmedAt != nil {
				cause.ConfirmedAt = confirmedAt.UTC()
			}
			gap.Causes = append(gap.Causes, cause)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close health gap causes for recovery: %w", err)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate health gap causes for recovery: %w", err)
		}

		var sourceRunID string
		var observationKind HealthObservationKind
		var freshTransport bool
		var chunkStart, chunkCount int64
		var tested, present []byte
		var observedAt time.Time
		if err := tx.QueryRowContext(ctx, `
			SELECT c.run_id, c.observation_kind, c.fresh_transport, c.segment_start, c.segment_count,
			       c.tested_bitmap, c.present_bitmap, c.committed_at
			FROM health_run_chunks c
			JOIN health_runs r ON r.id = c.run_id
			WHERE c.id = ? AND r.file_revision_id = ?
		`, chunkID, revisionID).Scan(&sourceRunID, &observationKind, &freshTransport, &chunkStart, &chunkCount,
			&tested, &present, &observedAt); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("source chunk does not belong to the gap revision")
			}
			return fmt.Errorf("read gap-clearing source chunk: %w", err)
		}
		if observationKind != HealthObservationValidatedBody {
			return fmt.Errorf("only validated BODY presence can clear a health gap")
		}
		if runID != "" && sourceRunID != runID {
			return ErrStaleHealthSchedule
		}
		if !freshTransport {
			return fmt.Errorf("gap recovery requires a fresh validated BODY transport")
		}
		if clearedAt.Before(observedAt) {
			return fmt.Errorf("gap clear time precedes its validated BODY evidence")
		}
		if observedAt.Before(gap.CreatedAt) ||
			(gap.ConfirmedAt != nil && observedAt.Before(gap.ConfirmedAt.UTC())) {
			return fmt.Errorf("validated BODY evidence predates the current gap episode")
		}
		recovered := make([]bool, gap.SegmentCount)
		var recoveredCount int64
		for segment := gap.StartSegment; segment < gap.StartSegment+gap.SegmentCount; segment++ {
			relative := segment - chunkStart
			if relative >= 0 && relative < chunkCount && relative/8 < int64(len(tested)) &&
				relative/8 < int64(len(present)) && bitmapSet(tested, relative) && bitmapSet(present, relative) {
				recovered[segment-gap.StartSegment] = true
				recoveredCount++
			}
		}
		if recoveredCount == 0 {
			return fmt.Errorf("validated BODY chunk recovers no position in the gap")
		}

		if _, err := tx.ExecContext(ctx, `
			UPDATE health_gap_ranges
			SET status = 'cleared', cleared_at = ?
			WHERE id = ? AND status IN ('active', 'dormant')
		`, clearedAt, gapID); err != nil {
			return fmt.Errorf("clear health gap episode: %w", err)
		}
		retiredRows, err := tx.QueryContext(ctx, `
			UPDATE health_run_schedule SET active = FALSE, updated_at = ?
			WHERE target_gap_id = ? AND active = TRUE
			RETURNING run_id
		`, clearedAt, gapID)
		if err != nil {
			return fmt.Errorf("retire recovered gap schedule: %w", err)
		}
		var retiredRunIDs []string
		for retiredRows.Next() {
			var retiredRunID string
			if err := retiredRows.Scan(&retiredRunID); err != nil {
				retiredRows.Close()
				return fmt.Errorf("scan recovered gap schedule: %w", err)
			}
			retiredRunIDs = append(retiredRunIDs, retiredRunID)
		}
		if err := retiredRows.Close(); err != nil {
			return fmt.Errorf("close recovered gap schedules: %w", err)
		}
		if err := retiredRows.Err(); err != nil {
			return fmt.Errorf("iterate recovered gap schedules: %w", err)
		}
		if err := terminalizeStaleTargetedHealthRunsTx(
			ctx, tx, retiredRunIDs, runID, clearedAt,
		); err != nil {
			return err
		}

		for offset := int64(0); offset < gap.SegmentCount; {
			for offset < gap.SegmentCount && recovered[offset] {
				offset++
			}
			if offset == gap.SegmentCount {
				break
			}
			startOffset := offset
			for offset < gap.SegmentCount && !recovered[offset] {
				offset++
			}
			if err := insertRecoveredGapRemainder(
				ctx, tx, gap, gap.StartSegment+startOffset, offset-startOffset,
			); err != nil {
				return err
			}
		}
		if err := tx.QueryRowContext(ctx, `
			SELECT status, cleared_at FROM health_gap_ranges WHERE id = ?
		`, gapID).Scan(&gap.Status, &gap.ClearedAt); err != nil {
			return fmt.Errorf("read cleared health gap: %w", err)
		}
		if runID != "" {
			completed, err := tx.ExecContext(ctx, `
				UPDATE health_runs
				SET status = 'completed', resolved_segments = total_segments,
				    lease_owner = NULL, lease_expires_at = NULL,
				    last_error = NULL, updated_at = ?, completed_at = ?
				WHERE id = ? AND status = 'running' AND lease_owner = ? AND fencing_token = ?
				  AND lease_expires_at > ? AND cancel_requested = FALSE
			`, clearedAt, clearedAt, runID, owner, fencingToken, r.now().UTC())
			if err != nil {
				return fmt.Errorf("complete gap recovery run: %w", err)
			}
			if rows, err := completed.RowsAffected(); err != nil {
				return fmt.Errorf("read gap recovery completion result: %w", err)
			} else if rows != 1 {
				return ErrStaleHealthLease
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &gap, nil
}

func insertRecoveredGapRemainder(
	ctx context.Context,
	tx *dialectAwareTx,
	parent HealthGapRange,
	start, count int64,
) error {
	var episode int64
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(episode), 0) + 1
		FROM health_gap_ranges
		WHERE file_revision_id = ? AND kind = ? AND start_segment = ? AND segment_count = ?
	`, parent.FileRevisionID, parent.Kind, start, count).Scan(&episode); err != nil {
		return fmt.Errorf("allocate recovered gap remainder episode: %w", err)
	}
	remainderID := uuid.NewString()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO health_gap_ranges
			(id, file_revision_id, kind, start_segment, segment_count, episode,
			 status, created_at, confirmed_at, cleared_at, revalidation_step,
			 next_revalidation_at, last_revalidation_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)
	`, remainderID, parent.FileRevisionID, parent.Kind, start, count, episode,
		parent.Status, parent.CreatedAt, parent.ConfirmedAt, parent.RevalidationStep,
		parent.NextRevalidationAt, parent.LastRevalidationAt); err != nil {
		return fmt.Errorf("persist recovered gap remainder: %w", err)
	}
	for _, cause := range parent.Causes {
		var confirmedAt any
		if !cause.ConfirmedAt.IsZero() {
			confirmedAt = cause.ConfirmedAt.UTC()
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO health_gap_provider_causes
				(gap_id, provider_id, provider_generation, provider_activation_epoch,
				 cause, confirmation_count, confirmed_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)
		`, remainderID, cause.ProviderID, cause.ProviderGeneration,
			cause.ProviderActivationEpoch, cause.Cause, cause.ConfirmationCount, confirmedAt); err != nil {
			return fmt.Errorf("copy recovered gap remainder cause: %w", err)
		}
	}
	return nil
}

func (r *HealthStateRepository) RecordSyntheticOutput(ctx context.Context, write SyntheticOutputWrite) (*CacheRecoveryState, error) {
	if write.ID == "" || write.GapID == "" || write.FileRevisionID == "" ||
		write.ByteStart < 0 || write.ByteEnd < write.ByteStart || write.EmittedAt.IsZero() {
		return nil, fmt.Errorf("synthetic output identity, revision, range, and emission time are required")
	}
	write.EmittedAt = write.EmittedAt.UTC()
	var state CacheRecoveryState
	err := r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		var gapRevision string
		var virtualSize int64
		if err := tx.QueryRowContext(ctx, `
			SELECT g.file_revision_id, r.virtual_size
			FROM health_gap_ranges g
			JOIN health_file_revisions r ON r.id = g.file_revision_id
			WHERE g.id = ?
		`, write.GapID).Scan(&gapRevision, &virtualSize); err != nil {
			return fmt.Errorf("read synthetic output gap: %w", err)
		}
		if gapRevision != write.FileRevisionID {
			return fmt.Errorf("synthetic output gap belongs to a different file revision")
		}
		if virtualSize == 0 || write.ByteStart >= virtualSize || write.ByteEnd >= virtualSize {
			return fmt.Errorf("synthetic output range exceeds file revision size")
		}

		var existingGap, existingRevision string
		var existingStart, existingEnd int64
		var existingAt time.Time
		err := tx.QueryRowContext(ctx, `
			SELECT gap_id, file_revision_id, byte_start, byte_end, emitted_at
			FROM health_synthetic_ranges WHERE id = ?
		`, write.ID).Scan(&existingGap, &existingRevision, &existingStart, &existingEnd, &existingAt)
		if err == nil {
			if existingGap != write.GapID || existingRevision != write.FileRevisionID || existingStart != write.ByteStart ||
				existingEnd != write.ByteEnd || !existingAt.Equal(write.EmittedAt) {
				return ErrHealthChunkConflict
			}
			return scanCacheRecoveryState(tx.QueryRowContext(ctx, `
				SELECT file_revision_id, status, retry_count, next_retry_at, last_error,
				       content_revision, updated_at
				FROM health_cache_recovery WHERE file_revision_id = ?
			`, write.FileRevisionID), &state)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read synthetic output identity: %w", err)
		}

		_, err = tx.ExecContext(ctx, `
			INSERT INTO health_synthetic_ranges
				(id, gap_id, file_revision_id, byte_start, byte_end, emitted_at)
			VALUES (?, ?, ?, ?, ?, ?)
		`, write.ID, write.GapID, write.FileRevisionID, write.ByteStart, write.ByteEnd, write.EmittedAt)
		if err != nil {
			return fmt.Errorf("record synthetic output range: %w", err)
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO health_cache_recovery
				(file_revision_id, status, retry_count, next_retry_at, last_error,
				 content_revision, updated_at)
			VALUES (?, 'synthetic', 0, NULL, NULL, 0, ?)
			ON CONFLICT(file_revision_id) DO UPDATE SET
				status = CASE
					WHEN health_cache_recovery.status IN ('pending', 'in_progress', 'failed')
					THEN health_cache_recovery.status
					ELSE 'synthetic'
				END,
				updated_at = CASE
					WHEN health_cache_recovery.updated_at > excluded.updated_at
					THEN health_cache_recovery.updated_at ELSE excluded.updated_at END
		`, write.FileRevisionID, write.EmittedAt)
		if err != nil {
			return fmt.Errorf("mark cache as containing synthetic output: %w", err)
		}
		return scanCacheRecoveryState(tx.QueryRowContext(ctx, `
			SELECT file_revision_id, status, retry_count, next_retry_at, last_error,
			       content_revision, updated_at
			FROM health_cache_recovery WHERE file_revision_id = ?
		`, write.FileRevisionID), &state)
	})
	if err != nil {
		return nil, err
	}
	return &state, nil
}

// MarkSyntheticRangeRecovered records that validated source bytes are now
// available for a range that was previously emitted synthetically. It queues
// cache recovery without advancing content_revision; PR8 owns that serialized
// transition and its mounted-path verification.
func (r *HealthStateRepository) MarkSyntheticRangeRecovered(ctx context.Context, syntheticID string, recoveredAt time.Time) (*CacheRecoveryState, error) {
	if syntheticID == "" || recoveredAt.IsZero() {
		return nil, fmt.Errorf("synthetic range identity and recovery time are required")
	}
	recoveredAt = recoveredAt.UTC()
	var state CacheRecoveryState
	err := r.withTransaction(ctx, func(tx *dialectAwareTx) error {
		var revisionID string
		err := tx.QueryRowContext(ctx, `
			SELECT file_revision_id
			FROM health_synthetic_ranges WHERE id = ?
		`, syntheticID).Scan(&revisionID)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("synthetic output range does not exist")
		}
		if err != nil {
			return fmt.Errorf("read synthetic output recovery state: %w", err)
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE health_synthetic_ranges SET recovered_at = ?
			WHERE id = ? AND recovered_at IS NULL
		`, recoveredAt, syntheticID)
		if err != nil {
			return fmt.Errorf("mark synthetic output recovered: %w", err)
		}
		updated, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("read synthetic recovery update result: %w", err)
		}
		if updated == 0 {
			return scanCacheRecoveryState(tx.QueryRowContext(ctx, `
				SELECT file_revision_id, status, retry_count, next_retry_at, last_error,
				       content_revision, updated_at
				FROM health_cache_recovery WHERE file_revision_id = ?
			`, revisionID), &state)
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO health_cache_recovery
				(file_revision_id, status, retry_count, next_retry_at, last_error,
				 content_revision, updated_at)
			VALUES (?, 'pending', 0, NULL, NULL, 0, ?)
			ON CONFLICT(file_revision_id) DO UPDATE SET
				status = CASE
					WHEN health_cache_recovery.status IN ('in_progress', 'failed')
					THEN health_cache_recovery.status ELSE 'pending' END,
				next_retry_at = CASE
					WHEN health_cache_recovery.status IN ('in_progress', 'failed')
					THEN health_cache_recovery.next_retry_at ELSE NULL END,
				last_error = CASE
					WHEN health_cache_recovery.status IN ('in_progress', 'failed')
					THEN health_cache_recovery.last_error ELSE NULL END,
				updated_at = CASE
					WHEN health_cache_recovery.status IN ('in_progress', 'failed')
					THEN health_cache_recovery.updated_at ELSE excluded.updated_at END
		`, revisionID, recoveredAt)
		if err != nil {
			return fmt.Errorf("queue recovered synthetic output for cache recovery: %w", err)
		}
		return scanCacheRecoveryState(tx.QueryRowContext(ctx, `
			SELECT file_revision_id, status, retry_count, next_retry_at, last_error,
			       content_revision, updated_at
			FROM health_cache_recovery WHERE file_revision_id = ?
		`, revisionID), &state)
	})
	if err != nil {
		return nil, err
	}
	return &state, nil
}

func scanCacheRecoveryState(row rowScanner, state *CacheRecoveryState) error {
	return row.Scan(&state.FileRevisionID, &state.Status, &state.RetryCount,
		&state.NextRetryAt, &state.LastError, &state.ContentRevision, &state.UpdatedAt)
}

func (r *HealthStateRepository) GetCacheRecoveryState(ctx context.Context, revisionID string) (*CacheRecoveryState, error) {
	var state CacheRecoveryState
	err := scanCacheRecoveryState(r.db.QueryRowContext(ctx, `
		SELECT file_revision_id, status, retry_count, next_retry_at, last_error,
		       content_revision, updated_at
		FROM health_cache_recovery WHERE file_revision_id = ?
	`, revisionID), &state)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get cache recovery state: %w", err)
	}
	return &state, nil
}
