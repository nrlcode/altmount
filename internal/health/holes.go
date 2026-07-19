package health

import (
	"math"

	"github.com/javi11/altmount/internal/holes"
	"github.com/javi11/altmount/internal/usenet"
)

// classifyHoles turns a check's missing segments into a playback-impact
// verdict using the hole model (see internal/holes). Only positions from the
// current conclusive sweep are authoritative; legacy .meta holes are
// quarantined pending migration and never participate in classification.
// A full sweep can prove clean; a sampled sweep only projects (never clean).
//
// Preparation retains only scalar size/eligibility values from the validated
// layout, so positional results and their denominator remain one coherent
// snapshot without retaining the segment proto through the network sweep.
func (hc *HealthChecker) classifyHoles(
	prep preparedCheck,
	result usenet.ValidationResult,
) *holes.Impact {
	if len(result.MissingSegments) == 0 || !prep.holeEligible {
		return nil
	}

	var acc holes.Accumulator
	observed := missingRuns(result.MissingSegments, prep.totalSegments)
	acc.Load(observed)

	totalSegments := prep.totalSegments
	paddedBytes := missingBytes(result.MissingSegments)
	fullCheck := result.IncompleteCount == 0 && result.TotalChecked >= totalSegments
	if result.TotalExpected > 0 {
		fullCheck = fullCheck && result.TotalChecked >= result.TotalExpected
	}

	var verdict holes.Verdict
	if fullCheck {
		verdict = holes.Classify(acc.Runs(), prep.fileSize, paddedBytes)
	} else {
		// Sampled evidence projects from the complete positional sample.
		verdict = holes.ClassifyProjected(result.MissingCount, result.TotalChecked, totalSegments, acc.LongestRun())
		if holes.Classify(acc.Runs(), prep.fileSize, paddedBytes) == holes.VerdictFailed {
			verdict = holes.VerdictFailed
		}
	}

	var ratio float64
	if prep.fileSize > 0 {
		ratio = float64(paddedBytes) / float64(prep.fileSize)
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

// missingBytes sums the exact inclusive logical ranges attached to the
// complete positional missing set. Saturating malformed or overflowing input
// makes it fail closed at the byte-ratio guard without risking arithmetic
// wraparound; prepared health targets always carry valid disjoint ranges.
func missingBytes(missing []usenet.MissingSegment) int64 {
	var total int64
	for _, segment := range missing {
		if segment.Start < 0 || segment.End < segment.Start {
			return math.MaxInt64
		}
		span := segment.End - segment.Start
		if span == math.MaxInt64 {
			return math.MaxInt64
		}
		span++
		if total > math.MaxInt64-span {
			return math.MaxInt64
		}
		total += span
	}
	return total
}
