package database

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

func (r *HealthStateRepository) ListProviderActivationWork(
	ctx context.Context,
	limit int,
) ([]ProviderActivationWork, error) {
	if limit <= 0 {
		limit = 64
	}
	work := make([]ProviderActivationWork, 0, limit)

	// Known gaps can be selected exactly in SQL. In particular, terminal
	// schedule history for this activation is an exhaustion marker: a fully
	// temporary attempt must not be recreated on every scheduler tick. A new
	// generation or activation epoch naturally becomes eligible again.
	rows, err := r.db.QueryContext(ctx, `
		SELECT g.id, g.file_revision_id, g.segment_count,
		       provider.id, provider.current_generation, provider.activation_epoch,
		       provider.role, provider.configured_order
		FROM health_gap_ranges g
		JOIN health_file_revisions revision
		  ON revision.id = g.file_revision_id AND revision.active = TRUE
		CROSS JOIN health_providers provider
		WHERE g.status IN ('active', 'dormant')
		  AND provider.active = TRUE
		  AND NOT EXISTS (
		    SELECT 1 FROM health_gap_provider_causes cause
		    WHERE cause.gap_id = g.id
		      AND cause.provider_id = provider.id
		      AND cause.provider_generation = provider.current_generation
		      AND cause.provider_activation_epoch = provider.activation_epoch
		  )
		  AND NOT EXISTS (
		    SELECT 1
		    FROM health_run_schedule schedule
		    JOIN health_runs run ON run.id = schedule.run_id
		    WHERE run.file_revision_id = g.file_revision_id
		      AND run.trigger LIKE 'provider_activation%'
		      AND schedule.target_gap_id = g.id
		      AND schedule.target_provider_id = provider.id
		      AND schedule.target_provider_generation = provider.current_generation
		      AND schedule.target_provider_activation_epoch = provider.activation_epoch
		      AND (
		        schedule.active = TRUE OR EXISTS (
		          SELECT 1
		          FROM health_run_chunks retry_chunk
		          JOIN health_retry_states retry
		            ON retry.source_chunk_id = retry_chunk.id AND retry.exhausted = TRUE
		          WHERE retry_chunk.run_id = run.id
		        )
		      )
		  )
		ORDER BY g.file_revision_id, g.start_segment, g.id,
		         provider.configured_order, provider.id
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list bounded provider activation gap work: %w", err)
	}
	for rows.Next() {
		var item ProviderActivationWork
		if err := rows.Scan(
			&item.GapID, &item.RevisionID, &item.TotalSegments,
			&item.Provider.ProviderID, &item.Provider.ProviderGeneration,
			&item.Provider.ProviderActivationEpoch, &item.Provider.Role,
			&item.Provider.Order,
		); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan bounded provider activation gap work: %w", err)
		}
		work = append(work, item)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close bounded provider activation gap work: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate bounded provider activation gap work: %w", err)
	}
	if len(work) >= limit {
		return work, nil
	}

	// Gapless activation work is also filtered exactly before LIMIT. The bitmap
	// predicates keep already-covered candidates ahead of an eligible pair from
	// starving it, without loading every revision/provider pair or issuing one
	// query per pair.
	present := r.bitmapPositionSetExpression(
		"coverage.present_bitmap", "exception.segment_index", "coverage.segment_start",
	)
	tested := r.bitmapPositionSetExpression(
		"coverage.tested_bitmap", "exception.segment_index", "coverage.segment_start",
	)
	eligiblePosition := fmt.Sprintf(`
		exception.segment_index >= 0
		AND exception.segment_index < revision.segment_count
		AND exception.outcome IN ('hard_absence', 'corrupt_body', 'temporary_failure',
		                          'provider_unavailable', 'transport_failure', 'inconclusive')
		AND NOT EXISTS (
		  SELECT 1 FROM health_gap_ranges known_gap
		  WHERE known_gap.file_revision_id = revision.id
		    AND known_gap.status IN ('active', 'dormant')
		    AND exception.segment_index >= known_gap.start_segment
		    AND exception.segment_index < known_gap.start_segment + known_gap.segment_count
		)
		AND NOT EXISTS (
		  SELECT 1
		  FROM health_provider_coverage coverage
		  JOIN health_providers current_provider
		    ON current_provider.id = coverage.provider_id
		   AND current_provider.active = TRUE
		   AND current_provider.current_generation = coverage.provider_generation
		   AND current_provider.activation_epoch = coverage.provider_activation_epoch
		  WHERE coverage.file_revision_id = revision.id
		    AND exception.segment_index >= coverage.segment_start
		    AND exception.segment_index < coverage.segment_start + coverage.segment_count
		    AND %s
		)
		AND NOT EXISTS (
		  SELECT 1
		  FROM health_provider_coverage coverage
		  WHERE coverage.file_revision_id = revision.id
		    AND coverage.provider_id = provider.id
		    AND coverage.provider_generation = provider.current_generation
		    AND coverage.provider_activation_epoch = provider.activation_epoch
		    AND exception.segment_index >= coverage.segment_start
		    AND exception.segment_index < coverage.segment_start + coverage.segment_count
		    AND %s
		)
	`, present, tested)
	query := fmt.Sprintf(`
		SELECT revision.id,
		       (SELECT COUNT(DISTINCT counted.segment_index)
		        FROM health_segment_exceptions counted
		        WHERE counted.file_revision_id = revision.id AND %s),
		       provider.id, provider.current_generation, provider.activation_epoch,
		       provider.role, provider.configured_order
		FROM health_file_revisions revision
		CROSS JOIN health_providers provider
		WHERE revision.active = TRUE AND provider.active = TRUE
		  AND EXISTS (
		    SELECT 1 FROM health_segment_exceptions exception
		    WHERE exception.file_revision_id = revision.id AND %s
		  )
		  AND NOT EXISTS (
		    SELECT 1
		    FROM health_run_schedule schedule
		    JOIN health_runs run ON run.id = schedule.run_id
		    WHERE run.file_revision_id = revision.id
		      AND run.trigger LIKE 'provider_activation%%'
		      AND schedule.target_gap_id IS NULL
		      AND schedule.target_provider_id = provider.id
		      AND schedule.target_provider_generation = provider.current_generation
		      AND schedule.target_provider_activation_epoch = provider.activation_epoch
		      AND (
		        schedule.active = TRUE OR EXISTS (
		          SELECT 1
		          FROM health_run_chunks retry_chunk
		          JOIN health_retry_states retry
		            ON retry.source_chunk_id = retry_chunk.id AND retry.exhausted = TRUE
		          WHERE retry_chunk.run_id = run.id
		        )
		      )
		  )
		ORDER BY revision.id, provider.configured_order, provider.id
		LIMIT ?
	`, strings.ReplaceAll(eligiblePosition, "exception.", "counted."), eligiblePosition)
	rows, err = r.db.QueryContext(ctx, query, limit-len(work))
	if err != nil {
		return nil, fmt.Errorf("list bounded unresolved provider activation work: %w", err)
	}
	for rows.Next() {
		var item ProviderActivationWork
		if err := rows.Scan(
			&item.RevisionID, &item.TotalSegments, &item.Provider.ProviderID,
			&item.Provider.ProviderGeneration, &item.Provider.ProviderActivationEpoch,
			&item.Provider.Role, &item.Provider.Order,
		); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan bounded unresolved provider activation work: %w", err)
		}
		work = append(work, item)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close bounded unresolved provider activation work: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate bounded unresolved provider activation work: %w", err)
	}
	return work, nil
}

// bitmapPositionSetExpression returns a backend-specific SQL predicate for a
// little-endian bitmap bit at (position-start). Bounds are checked by callers.
func (r *HealthStateRepository) bitmapPositionSetExpression(bitmap, position, start string) string {
	relative := fmt.Sprintf("(%s - %s)", position, start)
	if r.dialect.IsPostgres() {
		return fmt.Sprintf(`
			octet_length(%s) > CAST((%s) / 8 AS INTEGER)
			AND (get_byte(%s, CAST((%s) / 8 AS INTEGER)) &
			     (1 << CAST(mod(%s, 8) AS INTEGER))) <> 0
		`, bitmap, relative, bitmap, relative, relative)
	}
	hexByte := fmt.Sprintf(
		"hex(substr(%s, CAST((%s) / 8 AS INTEGER) + 1, 1))", bitmap, relative,
	)
	byteValue := fmt.Sprintf(
		"((instr('0123456789ABCDEF', substr(%s, 1, 1)) - 1) * 16 + "+
			"(instr('0123456789ABCDEF', substr(%s, 2, 1)) - 1))",
		hexByte, hexByte,
	)
	return fmt.Sprintf(`
		length(%s) > CAST((%s) / 8 AS INTEGER)
		AND ((%s) & (1 << ((%s) %% 8))) <> 0
	`, bitmap, relative, byteValue, relative)
}

// ListUnresolvedSegmentPositions returns only globally unresolved positions
// which have not already been tested by the requested current activation.
// Positive coverage on any active provider resolves a position for this
// provider-activation sweep; known gaps are scheduled separately by gap ID.
func (r *HealthStateRepository) ListUnresolvedSegmentPositions(
	ctx context.Context,
	revisionID, providerID string,
	generation, activationEpoch int64,
) ([]int64, error) {
	if revisionID == "" || providerID == "" || generation <= 0 || activationEpoch <= 0 {
		return nil, fmt.Errorf("revision and provider activation are required")
	}
	var total int64
	if err := r.db.QueryRowContext(ctx, `
		SELECT segment_count FROM health_file_revisions
		WHERE id = ? AND active = TRUE
	`, revisionID).Scan(&total); err != nil {
		return nil, fmt.Errorf("read unresolved revision bounds: %w", err)
	}
	candidates := make(map[int64]struct{})
	rows, err := r.db.QueryContext(ctx, `
		SELECT DISTINCT e.segment_index
		FROM health_segment_exceptions e
		WHERE e.file_revision_id = ?
		  AND e.outcome IN ('hard_absence', 'corrupt_body', 'temporary_failure',
		                    'provider_unavailable', 'transport_failure', 'inconclusive')
		  AND NOT EXISTS (
		    SELECT 1 FROM health_gap_ranges gap
		    WHERE gap.file_revision_id = e.file_revision_id
		      AND gap.status IN ('active', 'dormant')
		      AND e.segment_index >= gap.start_segment
		      AND e.segment_index < gap.start_segment + gap.segment_count
		  )
	`, revisionID)
	if err != nil {
		return nil, fmt.Errorf("list unresolved observation exceptions: %w", err)
	}
	for rows.Next() {
		var position int64
		if err := rows.Scan(&position); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan unresolved observation position: %w", err)
		}
		if position >= 0 && position < total {
			candidates[position] = struct{}{}
		}
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close unresolved observation positions: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate unresolved observation positions: %w", err)
	}
	if len(candidates) == 0 {
		return nil, nil
	}

	resolved := make(map[int64]struct{})
	testedByTarget := make(map[int64]struct{})
	rows, err = r.db.QueryContext(ctx, `
		SELECT c.provider_id, c.provider_generation, c.provider_activation_epoch,
		       c.segment_start, c.segment_count, c.tested_bitmap, c.present_bitmap
		FROM health_provider_coverage c
		JOIN health_providers p
		  ON p.id = c.provider_id AND p.active = TRUE
		 AND p.current_generation = c.provider_generation
		 AND p.activation_epoch = c.provider_activation_epoch
		WHERE c.file_revision_id = ?
	`, revisionID)
	if err != nil {
		return nil, fmt.Errorf("read current observation coverage: %w", err)
	}
	for rows.Next() {
		var id string
		var providerGeneration, providerEpoch, start, count int64
		var tested, present []byte
		if err := rows.Scan(&id, &providerGeneration, &providerEpoch, &start, &count, &tested, &present); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan current observation coverage: %w", err)
		}
		for position := range candidates {
			relative := position - start
			if relative < 0 || relative >= count {
				continue
			}
			if bitmapSet(present, relative) {
				resolved[position] = struct{}{}
			}
			if id == providerID && providerGeneration == generation &&
				providerEpoch == activationEpoch && bitmapSet(tested, relative) {
				testedByTarget[position] = struct{}{}
			}
		}
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close current observation coverage: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate current observation coverage: %w", err)
	}

	positions := make([]int64, 0, len(candidates))
	for position := range candidates {
		if _, ok := resolved[position]; ok {
			continue
		}
		if _, ok := testedByTarget[position]; ok {
			continue
		}
		positions = append(positions, position)
	}
	sort.Slice(positions, func(i, j int) bool { return positions[i] < positions[j] })
	return positions, nil
}
