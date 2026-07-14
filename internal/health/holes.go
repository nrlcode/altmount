package health

import (
	"context"

	"github.com/javi11/altmount/internal/holes"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/javi11/altmount/internal/usenet"
)

// classifyHoles turns a check's missing segments into a playback-impact
// verdict using the hole model (see internal/holes). Only positions from the
// current conclusive sweep are authoritative; legacy .meta holes are
// quarantined pending migration and never participate in classification.
// A full sweep can prove clean; a sampled sweep only projects (never clean).
//
// It re-reads metadata rather than taking a healthCheckInput from the caller:
// prepareCheck deliberately drops the segment slice after sampling so the
// proto is collectible during the (possibly cross-file, batched) network
// sweep, and re-reading only pays a disk read in the rare case a file
// actually has missing segments.
//
// Returns nil when the file is ineligible (non-video, encrypted, remuxed) or
// metadata can no longer be read.
func (hc *HealthChecker) classifyHoles(
	ctx context.Context,
	filePath string,
	result usenet.ValidationResult,
) *holes.Impact {
	_ = ctx
	if len(result.MissingSegments) == 0 {
		return nil
	}
	input, ok := hc.loadClassificationInput(filePath)
	if !ok {
		return nil
	}

	var acc holes.Accumulator
	observed := missingRuns(result.MissingSegments, len(input.segments))
	acc.Load(observed)

	totalSegments := len(input.segments)
	segBytes := avgSegmentBytes(input.fileSize, totalSegments)
	fullCheck := result.IncompleteCount == 0 && result.TotalChecked >= totalSegments
	if result.TotalExpected > 0 {
		fullCheck = fullCheck && result.TotalChecked >= result.TotalExpected
	}

	var verdict holes.Verdict
	if fullCheck {
		verdict = holes.Classify(acc.Runs(), input.fileSize, segBytes)
	} else {
		// Sampled evidence projects from the complete positional sample.
		verdict = holes.ClassifyProjected(result.MissingCount, result.TotalChecked, totalSegments, acc.LongestRun())
		if holes.Classify(acc.Runs(), input.fileSize, segBytes) == holes.VerdictFailed {
			verdict = holes.VerdictFailed
		}
	}

	var ratio float64
	if input.fileSize > 0 {
		ratio = float64(int64(acc.Total())*segBytes) / float64(input.fileSize)
	}
	return &holes.Impact{
		Verdict:       verdict,
		TotalMissing:  acc.Total(),
		LongestRun:    acc.LongestRun(),
		Sampled:       result.TotalChecked,
		TotalSegments: totalSegments,
		PaddedRatio:   ratio,
	}
}

// loadClassificationInput re-reads metadata for hole classification and
// reports whether the file is eligible (plain unencrypted video).
func (hc *HealthChecker) loadClassificationInput(filePath string) (healthCheckInput, bool) {
	fileMeta, err := hc.metadataService.ReadFileMetadata(filePath)
	if err != nil || fileMeta == nil {
		return healthCheckInput{}, false
	}
	input := healthCheckInput{
		fileSize:      fileMeta.FileSize,
		sourceNzbPath: fileMeta.SourceNzbPath,
		segments:      fileMeta.SegmentData,
		encryption:    fileMeta.Encryption,
		hasNestedOrRemuxedSources: len(fileMeta.NestedSources) > 0 ||
			len(fileMeta.SharedOuterSources) > 0 ||
			len(fileMeta.ClipBoundaries) > 0,
	}
	if !holes.EligibleFile(filePath) ||
		input.encryption != metapb.Encryption_NONE ||
		input.hasNestedOrRemuxedSources {
		return healthCheckInput{}, false
	}
	return input, true
}

// missingRuns folds the complete positional missing set into hole runs.
func missingRuns(missing []usenet.MissingSegment, totalSegments int) []holes.Run {
	var acc holes.Accumulator
	for _, segment := range missing {
		if segment.Index >= 0 && segment.Index < totalSegments {
			acc.Add(segment.Index)
		}
	}
	return acc.Runs()
}

// avgSegmentBytes estimates the decoded segment size for the byte-ratio
// guard; encoded/decoded skew is negligible at 2%.
func avgSegmentBytes(fileSize int64, totalSegments int) int64 {
	if totalSegments <= 0 || fileSize <= 0 {
		return 1
	}
	return fileSize / int64(totalSegments)
}
