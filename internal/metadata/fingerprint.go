package metadata

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"

	metapb "github.com/javi11/altmount/internal/metadata/proto"
)

const segmentLayoutFingerprintVersion = "altmount-segment-layout-v2"

type fingerprintSegmentSliceKey struct {
	first  **metapb.SegmentData
	length int
}

// CanonicalSegmentLayoutFingerprint returns a representation-independent digest
// of the final ordered article layout and virtual file size. Mutable health,
// access, encryption, and storage-compaction fields are intentionally excluded.
// Article identifiers are fed only into SHA-256 and are never returned or logged.
func CanonicalSegmentLayoutFingerprint(meta *metapb.FileMetadata) (string, error) {
	if meta == nil {
		return "", fmt.Errorf("metadata is required")
	}
	if meta.FileSize < 0 {
		return "", fmt.Errorf("virtual file size must be non-negative")
	}

	digest := sha256.New()
	writeFingerprintString(digest, segmentLayoutFingerprintVersion)
	writeFingerprintInt64(digest, meta.FileSize)

	if len(meta.NestedSources) == 0 {
		writeFingerprintString(digest, "main")
		if err := validateResolvedSegmentStorage(meta.SegmentData, meta.SegmentRuns, meta.SegmentRefs); err != nil {
			return "", fmt.Errorf("main segment layout: %w", err)
		}
		segmentDigest, err := fingerprintSegmentList(meta.SegmentData)
		if err != nil {
			return "", fmt.Errorf("main segment layout: %w", err)
		}
		_, _ = digest.Write(segmentDigest[:])
		return "sha256:" + hex.EncodeToString(digest.Sum(nil)), nil
	}

	writeFingerprintString(digest, "nested")
	writeFingerprintUint64(digest, uint64(len(meta.NestedSources)))
	segmentDigests := make(map[fingerprintSegmentSliceKey][sha256.Size]byte)
	for i, source := range meta.NestedSources {
		if source == nil {
			return "", fmt.Errorf("nested source %d is nil", i)
		}

		segments := source.Segments
		refs := source.SegmentRefs
		innerVolumeSize := source.InnerVolumeSize
		if source.SharedOuterSourceIndex < 0 {
			return "", fmt.Errorf("nested source %d has invalid shared source reference", i)
		}
		if source.SharedOuterSourceIndex > 0 {
			sharedIndex := int(source.SharedOuterSourceIndex) - 1
			if sharedIndex < 0 || sharedIndex >= len(meta.SharedOuterSources) {
				return "", fmt.Errorf("nested source %d has invalid shared source reference", i)
			}
			shared := meta.SharedOuterSources[sharedIndex]
			if shared == nil {
				return "", fmt.Errorf("nested source %d references a nil shared source", i)
			}
			segments = shared.Segments
			refs = shared.SegmentRefs
			if innerVolumeSize == 0 {
				innerVolumeSize = shared.InnerVolumeSize
			}
		}
		if source.InnerOffset < 0 || source.InnerLength < 0 || innerVolumeSize < 0 {
			return "", fmt.Errorf("nested source %d has negative layout bounds", i)
		}
		if err := validateResolvedSegmentStorage(segments, nil, refs); err != nil {
			return "", fmt.Errorf("nested source %d: %w", i, err)
		}

		key := fingerprintSegmentListKey(segments)
		segmentDigest, ok := segmentDigests[key]
		if !ok {
			var err error
			segmentDigest, err = fingerprintSegmentList(segments)
			if err != nil {
				return "", fmt.Errorf("nested source %d: %w", i, err)
			}
			segmentDigests[key] = segmentDigest
		}
		writeFingerprintInt64(digest, source.InnerOffset)
		writeFingerprintInt64(digest, source.InnerLength)
		writeFingerprintInt64(digest, innerVolumeSize)
		_, _ = digest.Write(segmentDigest[:])
	}

	return "sha256:" + hex.EncodeToString(digest.Sum(nil)), nil
}

func fingerprintSegmentListKey(segments []*metapb.SegmentData) fingerprintSegmentSliceKey {
	key := fingerprintSegmentSliceKey{length: len(segments)}
	if len(segments) != 0 {
		key.first = &segments[0]
	}
	return key
}

func fingerprintSegmentList(segments []*metapb.SegmentData) ([sha256.Size]byte, error) {
	digest := sha256.New()
	writeFingerprintString(digest, segmentLayoutFingerprintVersion+"/segment-list")
	writeFingerprintUint64(digest, uint64(len(segments)))
	for i, segment := range segments {
		if err := writeFingerprintSegment(digest, segment); err != nil {
			return [sha256.Size]byte{}, fmt.Errorf("segment %d: %w", i, err)
		}
	}
	var result [sha256.Size]byte
	digest.Sum(result[:0])
	return result, nil
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
	writeFingerprintString(digest, segment.Id)
	writeFingerprintInt64(digest, segment.SegmentSize)
	writeFingerprintInt64(digest, segment.StartOffset)
	writeFingerprintInt64(digest, segment.EndOffset)
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
