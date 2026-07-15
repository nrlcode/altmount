package metadata

import (
	"fmt"
	"math"

	"github.com/javi11/altmount/internal/encryption/aes"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
)

// CanonicalSegment describes one positional article in the same order used by
// CanonicalSegmentLayoutFingerprint. Duplicate message identities deliberately
// remain distinct positions.
type CanonicalSegment struct {
	Position    int64
	MessageID   string
	UsableBytes int64
}

// CanonicalSegmentLayout is the immutable final article layout used by import
// and health scheduling. It excludes storage-compaction representation details.
type CanonicalSegmentLayout struct {
	Fingerprint string
	VirtualSize int64
	Segments    []CanonicalSegment
}

// retainNestedPlaybackDependencies applies the same physical range arithmetic
// as MetadataVirtualFile.createNestedSourceReader. Plain sources use the exact
// inner extent. AES-CBC sources also need the preceding ciphertext block for
// their IV and a block-rounded ciphertext end.
func retainNestedPlaybackDependencies(canonical *metapb.FileMetadata) error {
	for sourceIndex, source := range canonical.NestedSources {
		if source == nil {
			return fmt.Errorf("nested source %d is nil", sourceIndex)
		}

		dependencyStart, dependencyEnd, err := nestedPlaybackDependencyRange(source)
		if err != nil {
			return fmt.Errorf("nested source %d: %w", sourceIndex, err)
		}

		selected := make([]*metapb.SegmentData, 0, len(source.Segments))
		var backingOffset int64
		for segmentIndex, segment := range source.Segments {
			if err := validateFingerprintSegment(segment); err != nil {
				return fmt.Errorf("nested source %d segment %d: %w", sourceIndex, segmentIndex, err)
			}
			usableBytes := segment.EndOffset - segment.StartOffset + 1
			if usableBytes > math.MaxInt64-backingOffset {
				return fmt.Errorf("nested source %d backing byte total overflows", sourceIndex)
			}
			segmentEnd := backingOffset + usableBytes
			if segmentEnd > dependencyStart && backingOffset < dependencyEnd {
				selected = append(selected, segment)
			}
			backingOffset = segmentEnd
		}

		if backingOffset < dependencyEnd {
			return fmt.Errorf("nested source %d backing does not cover playback extent", sourceIndex)
		}
		if len(selected) == 0 {
			return fmt.Errorf("nested source %d has no playback article positions", sourceIndex)
		}
		source.Segments = selected
	}
	return nil
}

func nestedPlaybackDependencyRange(source *metapb.NestedSegmentSource) (int64, int64, error) {
	if source.InnerOffset < 0 {
		return 0, 0, fmt.Errorf("inner offset must not be negative")
	}
	if source.InnerLength <= 0 {
		return 0, 0, fmt.Errorf("inner length must be positive")
	}
	if source.InnerVolumeSize <= 0 {
		return 0, 0, fmt.Errorf("inner volume size must be positive")
	}
	if source.InnerLength > math.MaxInt64-source.InnerOffset {
		return 0, 0, fmt.Errorf("inner extent overflows")
	}
	extentEnd := source.InnerOffset + source.InnerLength
	if extentEnd > source.InnerVolumeSize {
		return 0, 0, fmt.Errorf("inner extent exceeds declared volume")
	}

	if len(source.AesKey) == 0 {
		return source.InnerOffset, extentEnd, nil
	}

	blockSize := int64(aes.BlockSize)
	blockNumber := source.InnerOffset / blockSize
	dependencyStart := int64(0)
	if blockNumber > 0 {
		dependencyStart = (blockNumber - 1) * blockSize
	}
	dependencyEnd := extentEnd
	if remainder := dependencyEnd % blockSize; remainder != 0 {
		padding := blockSize - remainder
		if dependencyEnd > math.MaxInt64-padding {
			return 0, 0, fmt.Errorf("encrypted playback extent overflows")
		}
		dependencyEnd += padding
	}
	return dependencyStart, dependencyEnd, nil
}

// ResolveCanonicalSegmentLayout expands compact nested-source references on a
// clone and returns the exact ordered article positions. Errors identify only
// structural positions and never include article identities.
func ResolveCanonicalSegmentLayout(meta *metapb.FileMetadata) (*CanonicalSegmentLayout, error) {
	canonical, err := canonicalPlaybackMetadata(meta)
	if err != nil {
		return nil, err
	}
	fingerprint, err := fingerprintCanonicalPlaybackMetadata(canonical)
	if err != nil {
		return nil, err
	}
	if err := validateCanonicalPlaybackCoverage(canonical); err != nil {
		return nil, err
	}

	segmentCount := len(canonical.SegmentData)
	for _, source := range canonical.NestedSources {
		segmentCount += len(source.Segments)
	}
	layout := &CanonicalSegmentLayout{
		Fingerprint: fingerprint,
		VirtualSize: canonical.FileSize,
		Segments:    make([]CanonicalSegment, 0, segmentCount),
	}
	appendSegment := func(segment *metapb.SegmentData) {
		layout.Segments = append(layout.Segments, CanonicalSegment{
			Position:    int64(len(layout.Segments)),
			MessageID:   segment.Id,
			UsableBytes: segment.EndOffset - segment.StartOffset + 1,
		})
	}
	for _, segment := range canonical.SegmentData {
		appendSegment(segment)
	}
	for _, source := range canonical.NestedSources {
		for _, segment := range source.Segments {
			appendSegment(segment)
		}
	}
	return layout, nil
}

func validateCanonicalPlaybackCoverage(canonical *metapb.FileMetadata) error {
	if canonical.FileSize <= 0 {
		return fmt.Errorf("virtual file size must be positive")
	}

	if len(canonical.NestedSources) > 0 {
		var virtualBytes int64
		for i, source := range canonical.NestedSources {
			if source.InnerLength <= 0 {
				return fmt.Errorf("nested source %d has no virtual byte extent", i)
			}
			if len(source.Segments) == 0 {
				return fmt.Errorf("nested source %d has no article positions", i)
			}
			if source.InnerLength > int64(^uint64(0)>>1)-virtualBytes {
				return fmt.Errorf("nested virtual byte total overflows")
			}
			virtualBytes += source.InnerLength
		}
		if virtualBytes != canonical.FileSize {
			return fmt.Errorf("nested virtual byte coverage does not match file size")
		}
		return nil
	}

	if len(canonical.SegmentData) == 0 {
		return fmt.Errorf("canonical segment layout must not be empty")
	}
	// Encrypted main representations occupy a different byte domain from the
	// decrypted virtual file. Their segment bounds remain structurally validated,
	// but only an unencrypted main representation has a directly comparable sum.
	if canonical.Encryption != metapb.Encryption_NONE {
		return nil
	}

	var usableBytes int64
	for _, segment := range canonical.SegmentData {
		span := segment.EndOffset - segment.StartOffset + 1
		if span > int64(^uint64(0)>>1)-usableBytes {
			return fmt.Errorf("canonical usable-byte total overflows")
		}
		usableBytes += span
	}
	if usableBytes != canonical.FileSize {
		return fmt.Errorf("canonical usable-byte coverage does not match file size")
	}
	return nil
}
