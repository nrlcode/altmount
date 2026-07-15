package metadata

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"

	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"google.golang.org/protobuf/proto"
)

const (
	segmentLayoutFingerprintVersion  = "altmount-segment-layout-v1"
	nestedPlaybackFingerprintVersion = "altmount-nested-playback-layout-v2"
)

// CanonicalSegmentLayoutFingerprint returns a representation-independent digest
// of the final ordered article layout and virtual file size. Mutable health,
// access, encryption, and storage-compaction fields are intentionally excluded.
// Article identifiers are fed only into SHA-256 and are never returned or logged.
func CanonicalSegmentLayoutFingerprint(meta *metapb.FileMetadata) (string, error) {
	canonical, err := canonicalPlaybackMetadata(meta)
	if err != nil {
		return "", err
	}
	return fingerprintCanonicalPlaybackMetadata(canonical)
}

// canonicalPlaybackMetadata resolves storage compaction on a clone and selects
// the same representation as playback. Nested sources take precedence over
// main SegmentData; some valid stored metadata intentionally retains both.
func canonicalPlaybackMetadata(meta *metapb.FileMetadata) (*metapb.FileMetadata, error) {
	if meta == nil {
		return nil, fmt.Errorf("metadata is required")
	}
	if meta.FileSize < 0 {
		return nil, fmt.Errorf("virtual file size must be non-negative")
	}
	if err := validateResolvedFingerprintLayout(meta); err != nil {
		return nil, err
	}

	// Expansion happens on a clone so callers do not observe a storage-shape
	// mutation. The canonical encoding ignores SharedOuterSourceIndex itself,
	// making compact and already-expanded metadata structurally identical.
	canonical := proto.Clone(meta).(*metapb.FileMetadata)
	if err := ExpandSharedOuterSources(canonical); err != nil {
		return nil, fmt.Errorf("expand shared nested sources: %w", err)
	}
	if len(canonical.NestedSources) > 0 {
		canonical.SegmentData = nil
		canonical.SegmentRuns = nil
		canonical.SegmentRefs = nil
		if err := retainNestedPlaybackDependencies(canonical); err != nil {
			return nil, err
		}
	}
	return canonical, nil
}

func fingerprintCanonicalPlaybackMetadata(canonical *metapb.FileMetadata) (string, error) {
	digest := sha256.New()
	writeFingerprintString(digest, segmentLayoutFingerprintVersion)
	writeFingerprintInt64(digest, canonical.FileSize)
	if len(canonical.NestedSources) > 0 {
		// PR5 narrows nested identity to the articles full-file playback can
		// actually request. Keep ordinary v1 identities stable while fencing all
		// earlier nested fingerprints from this corrected dependency domain.
		writeFingerprintString(digest, nestedPlaybackFingerprintVersion)
	}

	writeFingerprintUint64(digest, uint64(len(canonical.SegmentData)))
	for i, segment := range canonical.SegmentData {
		if err := writeFingerprintSegment(digest, segment); err != nil {
			return "", fmt.Errorf("segment %d: %w", i, err)
		}
	}

	writeFingerprintUint64(digest, uint64(len(canonical.NestedSources)))
	for i, source := range canonical.NestedSources {
		if source == nil {
			return "", fmt.Errorf("nested source %d is nil", i)
		}
		if source.InnerOffset < 0 || source.InnerLength < 0 || source.InnerVolumeSize < 0 {
			return "", fmt.Errorf("nested source %d has negative layout bounds", i)
		}
		writeFingerprintInt64(digest, source.InnerOffset)
		writeFingerprintInt64(digest, source.InnerLength)
		writeFingerprintInt64(digest, source.InnerVolumeSize)
		writeFingerprintUint64(digest, uint64(len(source.Segments)))
		for j, segment := range source.Segments {
			if err := writeFingerprintSegment(digest, segment); err != nil {
				return "", fmt.Errorf("nested source %d segment %d: %w", i, j, err)
			}
		}
	}

	return "sha256:" + hex.EncodeToString(digest.Sum(nil)), nil
}

func validateResolvedFingerprintLayout(meta *metapb.FileMetadata) error {
	// Playback ignores retained main SegmentData whenever nested sources exist,
	// so only require the main representation to be resolved when it is active.
	if len(meta.NestedSources) == 0 {
		if err := validateResolvedSegmentStorage(meta.SegmentData, meta.SegmentRuns, meta.SegmentRefs); err != nil {
			return fmt.Errorf("main segment layout: %w", err)
		}
	}
	for i, shared := range meta.SharedOuterSources {
		if shared == nil {
			return fmt.Errorf("shared nested source %d is nil", i)
		}
		if err := validateResolvedSegmentStorage(shared.Segments, nil, shared.SegmentRefs); err != nil {
			return fmt.Errorf("shared nested source %d: %w", i, err)
		}
	}
	for i, source := range meta.NestedSources {
		if source == nil {
			return fmt.Errorf("nested source %d is nil", i)
		}
		if source.SharedOuterSourceIndex < 0 ||
			int(source.SharedOuterSourceIndex) > len(meta.SharedOuterSources) {
			return fmt.Errorf("nested source %d has invalid shared source reference", i)
		}
		if source.SharedOuterSourceIndex > 0 && meta.SharedOuterSources[source.SharedOuterSourceIndex-1] == nil {
			return fmt.Errorf("nested source %d references a nil shared source", i)
		}
		if source.SharedOuterSourceIndex == 0 {
			if err := validateResolvedSegmentStorage(source.Segments, nil, source.SegmentRefs); err != nil {
				return fmt.Errorf("nested source %d: %w", i, err)
			}
		}
	}
	return nil
}

func validateResolvedSegmentStorage(segments []*metapb.SegmentData, runs []*metapb.SegmentRun, refs []*metapb.SegmentRef) error {
	if len(runs) == 0 && len(refs) == 0 {
		return nil
	}
	expected := int64(len(refs))
	for _, run := range runs {
		if run == nil || run.Count <= 0 || expected > int64(^uint(0)>>1)-run.Count {
			return fmt.Errorf("invalid unresolved segment run")
		}
		expected += run.Count
	}
	if expected != int64(len(segments)) {
		return fmt.Errorf("store-backed segment layout is not fully resolved")
	}
	return nil
}

func writeFingerprintSegment(digest hash.Hash, segment *metapb.SegmentData) error {
	if err := validateFingerprintSegment(segment); err != nil {
		return err
	}
	writeFingerprintString(digest, segment.Id)
	writeFingerprintInt64(digest, segment.SegmentSize)
	writeFingerprintInt64(digest, segment.StartOffset)
	writeFingerprintInt64(digest, segment.EndOffset)
	return nil
}

func validateFingerprintSegment(segment *metapb.SegmentData) error {
	if segment == nil {
		return fmt.Errorf("segment is nil")
	}
	if segment.Id == "" {
		return fmt.Errorf("article identity is empty")
	}
	if segment.SegmentSize <= 0 {
		return fmt.Errorf("decoded segment size must be positive")
	}
	if segment.StartOffset < 0 || segment.EndOffset < segment.StartOffset || segment.EndOffset >= segment.SegmentSize {
		return fmt.Errorf("segment bounds are outside decoded size")
	}
	return nil
}

func writeFingerprintString(digest hash.Hash, value string) {
	writeFingerprintUint64(digest, uint64(len(value)))
	_, _ = digest.Write([]byte(value))
}

func writeFingerprintInt64(digest hash.Hash, value int64) {
	writeFingerprintUint64(digest, uint64(value))
}

func writeFingerprintUint64(digest hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = digest.Write(encoded[:])
}
