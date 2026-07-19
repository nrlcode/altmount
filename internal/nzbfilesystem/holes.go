package nzbfilesystem

import (
	"context"
	"log/slog"
	"math"
	"time"

	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/holes"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/javi11/altmount/internal/usenet"
)

// holeEligible reports whether this file's missing segments may be
// zero-filled: plain (unencrypted, non-nested, non-remuxed) video only.
// Padding anything else would silently corrupt copies/imports served
// through the mount.
func (mvf *MetadataVirtualFile) holeEligible() bool {
	return holes.EligibleFile(mvf.name) &&
		mvf.meta.Encryption == metapb.Encryption_NONE &&
		len(mvf.meta.NestedSources) == 0 &&
		len(mvf.meta.ClipBoundaries) == 0
}

// holeHooks returns the reader hooks that implement on-the-fly zero-fill for
// this handle, or nil when the file is ineligible. The hooks are built once
// per handle. Its accumulator starts empty: legacy persisted holes are
// quarantined and cannot authorize a fetch-free zero fill.
func (mvf *MetadataVirtualFile) holeHooks() *usenet.HoleHooks {
	mvf.holeOnce.Do(func() {
		if !mvf.holeEligible() {
			return
		}
		acc := &holes.Accumulator{}
		mvf.holeAcc = acc
		mvf.holeHooksVal = &usenet.HoleHooks{
			OnHole: mvf.onHole,
		}
	})
	return mvf.holeHooksVal
}

// onHole is the synchronous pad/fail verdict for a segment just confirmed
// missing on every provider. It merges the miss into the handle's
// accumulator, applies the threshold table, and — when the file remains
// within the padding caps — records the file as degraded off the hot path.
// Runs on download goroutines: no network, no blocking I/O.
func (mvf *MetadataVirtualFile) onHole(segIndex int, segID string) holes.Decision {
	idx := mvf.initSegmentIndex()
	if idx == nil || segIndex < 0 || segIndex >= len(idx.sizes) || mvf.holeAcc == nil {
		return holes.DecisionFail
	}

	mvf.holeMu.Lock()
	alreadyKnown := mvf.holeAcc.Has(segIndex)
	mvf.holeAcc.Add(segIndex)
	runs := mvf.holeAcc.Runs()
	total := mvf.holeAcc.Total()
	longest := mvf.holeAcc.LongestRun()
	mvf.holeMu.Unlock()

	totalSegments := len(mvf.meta.SegmentData)
	paddedBytes, ok := paddedBytesForRuns(runs, idx)
	if !ok {
		return holes.DecisionFail
	}
	verdict := holes.Classify(runs, mvf.meta.FileSize, paddedBytes)
	if verdict != holes.VerdictDegraded {
		// Caps exceeded: fail the stream; the DataCorruptionError path takes
		// over (repair trigger, safety-folder move) as it always has.
		slog.WarnContext(mvf.ctx, "Missing segment exceeds padding caps, failing stream",
			"file", mvf.name,
			"segment_id", segID,
			"total_missing", total,
			"longest_run", longest)
		return holes.DecisionFail
	}

	// Record the degradation without stalling the download goroutine. Replays
	// within this handle change nothing, so only new discoveries write.
	if !alreadyKnown {
		go mvf.recordDegradedPad(total, longest, totalSegments, paddedBytes)
	}
	return holes.DecisionPad
}

// recordDegradedPad marks a file degraded after a freshly confirmed, session-
// local pad. It deliberately does not persist padding authority into the
// legacy .meta hole field. There is NO repair trigger, NO
// safety-folder move and NO masking-counter increment — the file still plays.
// Status writes are debounced per file so a burst of pads writes once per
// window.
func (mvf *MetadataVirtualFile) recordDegradedPad(total, longest, totalSegments int, paddedBytes int64) {
	// Distinct debounce key from the repair path so pads never consume a
	// repair-trigger token.
	if !mvf.repairCoalescer.ShouldTrigger(mvf.name + "\x00degraded-pad") {
		return
	}

	ctx, cancel := context.WithTimeout(mvf.ctx, 5*time.Second)
	defer cancel()

	if err := mvf.metadataService.UpdateFileStatus(mvf.name, metapb.FileStatus_FILE_STATUS_DEGRADED); err != nil {
		slog.WarnContext(ctx, "Failed to update metadata status to degraded", "file", mvf.name, "error", err)
	}

	details := database.HealthErrorDetails{
		ErrorType:       "ArticleNotFound",
		MissingArticles: total,
		TotalArticles:   totalSegments,
		PlaybackImpact: &holes.Impact{
			Verdict:       holes.VerdictDegraded,
			TotalMissing:  total,
			LongestRun:    longest,
			TotalSegments: totalSegments,
			PaddedRatio:   paddedRatio(paddedBytes, mvf.meta.FileSize),
		},
	}
	errorMsg := "missing segments zero-filled during streaming"
	sourceNzbPath := &mvf.meta.SourceNzbPath
	if *sourceNzbPath == "" {
		sourceNzbPath = nil
	}

	slog.InfoContext(ctx, "Zero-filled missing segment during streaming, file marked degraded",
		"file", mvf.name,
		"total_missing", total,
		"longest_run", longest)

	if err := mvf.healthRepository.UpdateFileHealthScheduled(ctx,
		mvf.name,
		database.HealthStatusDegraded,
		&errorMsg,
		sourceNzbPath,
		details.Marshal(),
		false, // no immediate scheduling — periodic re-check refines the verdict
		time.Now().UTC(),
	); err != nil {
		slog.WarnContext(ctx, "Failed to record degraded status for padded file", "file", mvf.name, "error", err)
	}
}

// classifyStreamingFailure builds the playback-impact summary for a stream
// that FAILED on a missing article (hooks absent, or pad caps exceeded).
// Returns nil for ineligible files or non-hole failures (yEnc corruption,
// pool errors), which follow the plain corruption path.
func (mvf *MetadataVirtualFile) classifyStreamingFailure(dcErr *usenet.DataCorruptionError) *holes.Impact {
	if !mvf.holeEligible() || !usenet.IsArticleNotFound(dcErr.UnderlyingErr) {
		return nil
	}

	var acc holes.Accumulator
	mvf.holeMu.Lock()
	if mvf.holeAcc != nil {
		acc.Load(mvf.holeAcc.Runs())
	}
	mvf.holeMu.Unlock()

	idx := mvf.initSegmentIndex()
	if idx == nil {
		return &holes.Impact{Verdict: holes.VerdictFailed, TotalSegments: len(mvf.meta.SegmentData)}
	}

	// Fold in the failing segment when its position is known.
	if dcErr.FileOffset >= 0 {
		if segIdx := idx.findSegmentForOffset(dcErr.FileOffset); segIdx >= 0 {
			acc.Add(segIdx)
		}
	}

	totalSegments := len(mvf.meta.SegmentData)
	paddedBytes, ok := paddedBytesForRuns(acc.Runs(), idx)
	if !ok {
		return &holes.Impact{
			Verdict:       holes.VerdictFailed,
			TotalMissing:  acc.Total(),
			LongestRun:    acc.LongestRun(),
			TotalSegments: totalSegments,
		}
	}
	return &holes.Impact{
		Verdict:       holes.Classify(acc.Runs(), mvf.meta.FileSize, paddedBytes),
		TotalMissing:  acc.Total(),
		LongestRun:    acc.LongestRun(),
		TotalSegments: totalSegments,
		PaddedRatio:   paddedRatio(paddedBytes, mvf.meta.FileSize),
	}
}

func paddedBytesForRuns(runs []holes.Run, idx *segmentOffsetIndex) (int64, bool) {
	if idx == nil {
		return 0, false
	}
	var total int64
	for _, run := range runs {
		if run.Start < 0 || run.Count <= 0 || run.Start > len(idx.sizes)-run.Count {
			return 0, false
		}
		for i := run.Start; i < run.Start+run.Count; i++ {
			if idx.sizes[i] > math.MaxInt64-total {
				return 0, false
			}
			total += idx.sizes[i]
		}
	}
	return total, true
}

// paddedRatio is missing bytes over file bytes (0 when size is unknown).
func paddedRatio(paddedBytes, fileSize int64) float64 {
	if fileSize <= 0 {
		return 0
	}
	return float64(paddedBytes) / float64(fileSize)
}
