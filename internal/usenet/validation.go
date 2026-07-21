package usenet

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sort"
	"time"

	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/javi11/altmount/internal/pool"
	"github.com/javi11/altmount/internal/progress"
	"github.com/javi11/nntppool/v4"
)

var randPerm = rand.Perm

// SelectSegmentsForValidation is the exported form of the sampling selector.
// It returns the subset of segments that should be validated based on samplePercentage,
// applying the same first-3 / last-2 / random-middle strategy used internally.
func SelectSegmentsForValidation(segments []*metapb.SegmentData, samplePercentage int) []*metapb.SegmentData {
	return selectSegmentsForValidation(segments, samplePercentage)
}

// MissingSegment identifies one unavailable segment together with the byte
// range it covers in file coordinates, enabling playback-impact classification.
type MissingSegment struct {
	Index int    // index into the original segments slice
	ID    string // Usenet message ID
	Start int64  // inclusive file-coordinate byte range
	End   int64
}

// ValidationTarget identifies one requested STAT position. Index is kept
// independently of ID because duplicate message IDs still represent distinct
// file positions.
type ValidationTarget struct {
	ID    string
	Index int
	Start int64
	End   int64
}

// ValidationResult holds detailed validation results. TotalExpected is the
// requested work while TotalChecked is the number of recognized results
// actually received. IncompleteCount covers returned non-conclusive outcomes
// plus requested work omitted when StatMany closes early.
type ValidationResult struct {
	TotalExpected   int
	TotalChecked    int
	IncompleteCount int
	MissingCount    int
	MissingIDs      []string
	MissingSegments []MissingSegment // complete positional set; only MissingIDs is capped
}

// ValidateSegmentAvailabilityDetailed validates segments and returns detailed results
// instead of failing fast on the first error.
func ValidateSegmentAvailabilityDetailed(
	ctx context.Context,
	segments []*metapb.SegmentData,
	poolManager pool.Manager,
	maxConnections int,
	samplePercentage int,
	progressTracker progress.ProgressTracker,
	timeout time.Duration,
) (ValidationResult, error) {
	if len(segments) == 0 {
		return ValidationResult{MissingIDs: []string{}}, nil
	}

	segmentsToValidate := selectSegmentsForValidation(segments, samplePercentage)
	indexBySegment := make(map[*metapb.SegmentData]int, len(segments))
	fileOffsets := make([]int64, len(segments))
	var pos int64
	for i, seg := range segments {
		indexBySegment[seg] = i
		fileOffsets[i] = pos
		pos += seg.EndOffset - seg.StartOffset + 1
	}

	targets := make([]ValidationTarget, 0, len(segmentsToValidate))
	for _, seg := range segmentsToValidate {
		idx := indexBySegment[seg]
		targets = append(targets, ValidationTarget{
			ID:    seg.Id,
			Index: idx,
			Start: fileOffsets[idx],
			End:   fileOffsets[idx] + (seg.EndOffset - seg.StartOffset),
		})
	}

	results, err := ValidateSegmentAvailabilityTargetsBatch(
		ctx, [][]ValidationTarget{targets}, poolManager, maxConnections, timeout,
	)
	if len(results) == 0 {
		return ValidationResult{MissingIDs: []string{}}, err
	}
	if progressTracker != nil {
		progressTracker.Update(results[0].TotalChecked, results[0].TotalExpected)
	}
	return results[0], err
}

// ValidateSegmentAvailabilityBatch checks pre-sampled segment IDs for many files
// in a single StatMany sweep. perFileIDs is index-aligned with the returned
// results: files with an empty ID list yield a zero ValidationResult. IDs are
// interleaved round-robin across files (every file's first sample, then every
// file's second, …) so one file with many segments cannot serialize the sweep
// for the others. An error is returned only for infrastructure failures (pool
// unavailable); per-segment misses are reported in the per-file results.
func ValidateSegmentAvailabilityBatch(
	ctx context.Context,
	perFileIDs [][]string,
	poolManager pool.Manager,
	maxConnections int,
	timeout time.Duration,
) ([]ValidationResult, error) {
	perFileTargets := make([][]ValidationTarget, len(perFileIDs))
	for fileIdx, ids := range perFileIDs {
		perFileTargets[fileIdx] = make([]ValidationTarget, len(ids))
		for idx, id := range ids {
			perFileTargets[fileIdx][idx] = ValidationTarget{ID: id, Index: idx}
		}
	}
	return ValidateSegmentAvailabilityTargetsBatch(ctx, perFileTargets, poolManager, maxConnections, timeout)
}

type validationOwner struct {
	fileIdx int
	target  ValidationTarget
}

// ValidateSegmentAvailabilityTargetsBatch is the positional form of the batch
// sweep. It preserves every hard-missing position for classification while
// limiting MissingIDs to diagnostic examples.
func ValidateSegmentAvailabilityTargetsBatch(
	ctx context.Context,
	perFileTargets [][]ValidationTarget,
	poolManager pool.Manager,
	maxConnections int,
	timeout time.Duration,
) ([]ValidationResult, error) {
	results := make([]ValidationResult, len(perFileTargets))
	total := 0
	maxSamples := 0
	nonEmptyFiles := 0
	for i, targets := range perFileTargets {
		results[i].MissingIDs = []string{}
		results[i].TotalExpected = len(targets)
		total += len(targets)
		if len(targets) > maxSamples {
			maxSamples = len(targets)
		}
		if len(targets) > 0 {
			nonEmptyFiles++
		}
	}
	if total == 0 {
		return results, nil
	}

	usenetPool, err := poolManager.GetPool()
	if err != nil {
		return results, fmt.Errorf("cannot validate segments: usenet connection pool unavailable: %w", err)
	}
	if usenetPool == nil {
		return results, fmt.Errorf("cannot validate segments: usenet connection pool is nil")
	}
	if maxConnections <= 0 {
		maxConnections = 1
	}

	ids := make([]string, 0, total)
	owners := make(map[string][]validationOwner, total)
	for round := 0; round < maxSamples; round++ {
		for fileIdx, targets := range perFileTargets {
			if round >= len(targets) {
				continue
			}
			target := targets[round]
			ids = append(ids, target.ID)
			owners[target.ID] = append(owners[target.ID], validationOwner{fileIdx: fileIdx, target: target})
		}
	}

	statCtx, cancel := context.WithTimeout(ctx, pool.StatManyTimeout(len(ids), maxConnections, timeout))
	defer cancel()

	sweepStart := time.Now()
	slog.InfoContext(ctx, "Starting STAT sweep",
		"files", nonEmptyFiles,
		"segments", len(ids),
		"concurrency", maxConnections,
	)

	var firstIncomplete error
	unexpectedResults := 0
	statResults := usenetPool.StatMany(statCtx, ids, nntppool.StatManyOptions{Concurrency: maxConnections})
receiveResults:
	for {
		var r nntppool.StatManyResult
		var ok bool
		select {
		case <-statCtx.Done():
			if firstIncomplete == nil {
				firstIncomplete = statCtx.Err()
			}
			break receiveResults
		case r, ok = <-statResults:
			if !ok {
				break receiveResults
			}
		}

		ownerQueue := owners[r.MessageID]
		if len(ownerQueue) == 0 {
			unexpectedResults++
			if firstIncomplete == nil {
				firstIncomplete = fmt.Errorf("provider returned an unrequested STAT result")
			}
			continue
		}
		owner := ownerQueue[0]
		owners[r.MessageID] = ownerQueue[1:]
		result := &results[owner.fileIdx]
		result.TotalChecked++

		outcome := ClassifyNNTPOutcome(r.Err)
		if r.Err == nil && r.Result == nil {
			outcome = nntppool.OutcomeInconclusive
		}
		switch outcome {
		case nntppool.OutcomeSuccess:
			poolManager.IncArticlesDownloaded()
			poolManager.UpdateDownloadProgress("", 100)
		case nntppool.OutcomeHardArticleAbsence:
			result.MissingCount++
			if len(result.MissingIDs) < 50 {
				result.MissingIDs = append(result.MissingIDs, owner.target.ID)
			}
			result.MissingSegments = append(result.MissingSegments, MissingSegment{
				Index: owner.target.Index,
				ID:    owner.target.ID,
				Start: owner.target.Start,
				End:   owner.target.End,
			})
		default:
			result.IncompleteCount++
			if firstIncomplete == nil {
				if r.Err != nil {
					firstIncomplete = r.Err
				} else {
					firstIncomplete = fmt.Errorf("provider returned an empty STAT result")
				}
			}
		}
	}

	missingTotal := 0
	globalIncomplete := unexpectedResults > 0
	incompleteTotal := unexpectedResults
	conclusiveTotal := 0
	checkedTotal := 0
	for i := range results {
		omitted := results[i].TotalExpected - results[i].TotalChecked
		if omitted > 0 {
			results[i].IncompleteCount += omitted
		}
		incompleteTotal += results[i].IncompleteCount
		conclusiveTotal += results[i].TotalExpected - results[i].IncompleteCount
		checkedTotal += results[i].TotalChecked
		missingTotal += results[i].MissingCount
		sort.Slice(results[i].MissingSegments, func(left, right int) bool {
			return results[i].MissingSegments[left].Index < results[i].MissingSegments[right].Index
		})
	}
	if statErr := statCtx.Err(); statErr != nil {
		globalIncomplete = true
		if firstIncomplete == nil {
			firstIncomplete = statErr
		}
	}
	if incompleteTotal > 0 && firstIncomplete == nil {
		firstIncomplete = fmt.Errorf("StatMany omitted requested work")
	}

	slog.InfoContext(ctx, "STAT sweep completed",
		"files", nonEmptyFiles,
		"expected", total,
		"checked", checkedTotal,
		"missing", missingTotal,
		"incomplete", incompleteTotal,
		"duration", time.Since(sweepStart),
	)

	if firstIncomplete != nil {
		return results, &IncompleteError{
			Expected: total, Completed: conclusiveTotal, Cause: firstIncomplete, Global: globalIncomplete,
		}
	}
	return results, nil
}

// selectSegmentsForValidation determines which segments to validate based on validation mode and sample percentage.
// For full validation, returns all segments. For sampling, uses a strategic approach that:
// - Validates first 3 segments (DMCA/takedown detection)
// - Validates last 2 segments (incomplete upload detection)
// - Validates random middle segments based on samplePercentage (general integrity check)
// A minimum of 5 segments are always validated for statistical validity when sampling.
func selectSegmentsForValidation(segments []*metapb.SegmentData, samplePercentage int) []*metapb.SegmentData {
	if samplePercentage == 100 {
		return segments
	}

	totalSegments := len(segments)

	// Min 5 for statistical validity, max 55 to cap network I/O on large files.
	targetSamples := min(max((totalSegments*samplePercentage)/100, 5), 55)

	if targetSamples >= totalSegments {
		return segments
	}

	var toValidate []*metapb.SegmentData

	// 1. First 3 segments (DMCA/takedown detection)
	firstCount := min(3, totalSegments)
	for i := range firstCount {
		toValidate = append(toValidate, segments[i])
	}

	// 2. Last 2 segments (incomplete upload detection)
	lastCount := 2
	if firstCount+lastCount > totalSegments {
		lastCount = totalSegments - firstCount
	}
	if lastCount > 0 {
		for i := totalSegments - lastCount; i < totalSegments; i++ {
			toValidate = append(toValidate, segments[i])
		}
	}

	// 3. Random middle segments to reach target sample size
	middleStart := firstCount
	middleEnd := totalSegments - lastCount
	middleRange := middleEnd - middleStart

	if middleRange > 0 {
		randomSamples := min(targetSamples-len(toValidate), middleRange)

		if randomSamples > 0 {
			perm := randPerm(middleRange)
			for i := range randomSamples {
				toValidate = append(toValidate, segments[middleStart+perm[i]])
			}
		}
	}

	return toValidate
}
