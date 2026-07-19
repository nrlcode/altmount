package usenet

import (
	"context"
)

type SegmentLoader interface {
	// GetSegment returns the segment with the given index.
	// If the segment is not found, it returns false.
	GetSegment(index int) (segment Segment, groups []string, ok bool)
}

// GetSegmentsInRange returns a segmentRange representing the requested byte range [start,end]
// across the underlying ordered segments provided by the SegmentLoader.
// Behaviour / rules:
//   - start and end are inclusive; caller guarantees 0 <= start <= end < filesize (filesize not passed here)
//   - Each loader Segment indicates:
//   - Start: offset (in bytes) within the physical segment where valid data for the file begins (can be > 0)
//   - End: offset (in bytes) within the physical segment where valid data for the file ends (inclusive)
//   - Size: full physical segment size (in bytes) for downloading
//     Therefore the usable data length contributed to the logical file by a loader segment is (End - Start + 1).
//   - We build output *segment objects (internal) with Start & End (inclusive) relative to the physical segment
//     so the reader will skip Start bytes and read up to End+1 bytes.
//   - First and last returned segments are trimmed so the concatenation of (End-Start+1) bytes across
//     returned segments equals the requested range length (unless the range lies fully outside available
//     data, in which case zero segments are returned).
func GetSegmentsInRange(ctx context.Context, start, end int64, ml SegmentLoader) *segmentRange {
	return GetSegmentsInRangeFromIndex(ctx, start, end, ml, 0, 0)
}

// GetSegmentsInRangeFromIndex is like GetSegmentsInRange but allows skipping directly to a
// known segment index with its corresponding file position. This enables O(1) skip to the
// correct segment when a pre-built index provides the starting segment via binary search.
//
// Parameters:
//   - startSegmentIndex: the segment index to start iteration from (use 0 for beginning)
//   - startFilePos: the cumulative file offset at the start of startSegmentIndex (use 0 for beginning)
func GetSegmentsInRangeFromIndex(ctx context.Context, start, end int64, ml SegmentLoader, startSegmentIndex int, startFilePos int64) *segmentRange {
	// Defensive handling of invalid input ranges
	if start < 0 || end < start {
		return &segmentRange{start: start, end: end, segments: nil, ctx: ctx}
	}

	requestedLen := end - start + 1
	segments := make([]*segment, 0, 4)
	var accumulatedLen int64 // running sum of readable bytes across appended segments

	// logicalFilePos tracks the starting file offset of the next loader segment's usable data
	// Start from the provided position to skip O(n) iteration
	logicalFilePos := startFilePos
	startIdx := startSegmentIndex
	if startIdx < 0 {
		startIdx = 0
		logicalFilePos = 0
	}

	for idx := startIdx; ; idx++ {
		src, groups, ok := ml.GetSegment(idx)
		if !ok { // no more segments
			break
		}

		// Usable data inside this segment starts at src.Start (may be >0) and ends at src.End inclusive.
		// Length contributed to file:
		usableLen := src.End - src.Start + 1
		if usableLen <= 0 { // nothing useful; skip
			continue
		}

		segFileStart := logicalFilePos             // first file offset covered by this segment's usable data
		segFileEnd := segFileStart + usableLen - 1 // last file offset covered

		// If this segment's data ends before the requested start, skip it and advance file position
		if segFileEnd < start {
			logicalFilePos += usableLen
			continue
		}
		// If this segment starts after the requested end, we are done
		if segFileStart > end {
			break
		}

		// Determine read window inside the physical segment
		// Start with full usable window (src.Start .. src.End)
		readStart := src.Start
		readEnd := src.End

		// Trim front if request starts inside this segment
		if start > segFileStart {
			// Offset (bytes) into this segment's usable data where we begin
			delta := start - segFileStart
			readStart = src.Start + delta
		}
		// Trim tail if request ends inside this segment
		if end < segFileEnd {
			delta := segFileEnd - end
			readEnd = src.End - delta
		}

		if readStart > readEnd { // safety; shouldn't happen
			logicalFilePos += usableLen
			continue
		}

		seg := newSegment(src.Id, readStart, readEnd, src.Size, groups, idx)
		segments = append(segments, seg)
		accumulatedLen += readEnd - readStart + 1

		logicalFilePos += usableLen

		// If we've satisfied the full request length, stop
		// (Check by seeing if this segment covered the end)
		if segFileEnd >= end {
			break
		}

		// If we've already accumulated requestedLen bytes across segments we could also break early
		if accumulatedLen >= requestedLen { // redundancy safeguard
			break
		}
	}

	return &segmentRange{segments: segments, start: start, end: end, ctx: ctx}
}

// NewLazySegmentRange creates a segmentRange that defers segment object creation until
// GetSegment() is called by the download manager. This is O(1) regardless of segment
// count, compared to O(N) for the eager GetSegmentsInRangeFromIndex path.
//
// Parameters:
//   - start, end: inclusive byte range in file coordinates
//   - ml: segment loader for on-demand segment info retrieval
//   - startSegIdx: loader index of the first segment covering 'start'
//   - startFilePos: cumulative file offset at the start of startSegIdx's usable data
//   - endSegIdx: loader index of the last segment covering 'end'
//   - endFilePos: cumulative file offset at the start of endSegIdx's usable data
func NewLazySegmentRange(ctx context.Context, start, end int64, ml SegmentLoader,
	startSegIdx int, startFilePos int64, endSegIdx int, endFilePos int64,
) *segmentRange {
	if start < 0 || end < start || startSegIdx < 0 || endSegIdx < startSegIdx {
		return &segmentRange{start: start, end: end, segments: nil, ctx: ctx}
	}

	count := endSegIdx - startSegIdx + 1
	return &segmentRange{
		start:        start,
		end:          end,
		segments:     make([]*segment, count),
		ctx:          ctx,
		loader:       ml,
		startSegIdx:  startSegIdx,
		startFilePos: startFilePos,
		endFilePos:   endFilePos,
	}
}

// BuildSegmentRange returns a lazy segment range when index bounds are provided
// (endSegIdx >= 0), giving O(1) initialization. Otherwise it falls back to the
// eager O(N) path via GetSegmentsInRangeFromIndex.
func BuildSegmentRange(ctx context.Context, start, end int64, ml SegmentLoader,
	startSegIdx int, startFilePos int64, endSegIdx int, endFilePos int64,
) *segmentRange {
	if startSegIdx >= 0 && endSegIdx >= 0 {
		return NewLazySegmentRange(ctx, start, end, ml, startSegIdx, startFilePos, endSegIdx, endFilePos)
	}
	if startSegIdx < 0 {
		startSegIdx = 0
		startFilePos = 0
	}
	return GetSegmentsInRangeFromIndex(ctx, start, end, ml, startSegIdx, startFilePos)
}
