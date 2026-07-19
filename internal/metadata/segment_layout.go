package metadata

import (
	"fmt"
	"math"

	"github.com/javi11/altmount/internal/encryption/aes"
	"github.com/javi11/altmount/internal/encryption/rclone"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
)

// ExpectedSegmentLayoutSize translates a logical file size into the exact
// physical byte count its stored segments must cover.
func ExpectedSegmentLayoutSize(fileSize int64, encryption metapb.Encryption) (int64, error) {
	if fileSize < 0 {
		return 0, fmt.Errorf("file size must be non-negative: %d", fileSize)
	}

	expectedSize := fileSize
	switch encryption {
	case metapb.Encryption_NONE:
		// Plain layouts cover the logical file bytes directly.
	case metapb.Encryption_RCLONE:
		expectedSize = rclone.EncryptedSize(fileSize)
	case metapb.Encryption_AES:
		expectedSize = aes.EncryptedSize(fileSize)
	default:
		return 0, fmt.Errorf("unsupported encryption type %d", encryption)
	}
	if expectedSize < fileSize {
		return 0, fmt.Errorf("encrypted segment size overflows for file size %d", fileSize)
	}
	return expectedSize, nil
}

// InspectSegmentLayout validates each physical segment and returns its usable
// byte lengths plus their checked total, without imposing an expected total.
func InspectSegmentLayout(segments []*metapb.SegmentData) ([]int64, int64, error) {
	if len(segments) == 0 {
		return nil, 0, fmt.Errorf("segment layout is empty")
	}

	lengths := make([]int64, len(segments))
	var total int64
	for i, segment := range segments {
		if segment == nil {
			return nil, 0, fmt.Errorf("segment %d is nil", i)
		}
		if segment.Id == "" {
			return nil, 0, fmt.Errorf("segment %d has an empty message ID", i)
		}
		if segment.SegmentSize <= 0 {
			return nil, 0, fmt.Errorf("segment %d has invalid physical size %d", i, segment.SegmentSize)
		}
		if segment.StartOffset < 0 {
			return nil, 0, fmt.Errorf("segment %d has negative start offset %d", i, segment.StartOffset)
		}
		if segment.EndOffset < segment.StartOffset {
			return nil, 0, fmt.Errorf(
				"segment %d has end offset %d before start offset %d",
				i, segment.EndOffset, segment.StartOffset,
			)
		}
		if segment.EndOffset >= segment.SegmentSize {
			return nil, 0, fmt.Errorf(
				"segment %d end offset %d is outside physical size %d",
				i, segment.EndOffset, segment.SegmentSize,
			)
		}

		length := segment.EndOffset - segment.StartOffset + 1
		if length > math.MaxInt64-total {
			return nil, 0, fmt.Errorf("segment layout size overflows int64 at segment %d", i)
		}
		lengths[i] = length
		total += length
	}
	return lengths, total, nil
}

// ValidateSegmentLayout validates an ordered physical segment layout and returns
// the number of usable bytes contributed by each segment.
func ValidateSegmentLayout(expectedSize int64, segments []*metapb.SegmentData) ([]int64, error) {
	if expectedSize < 0 {
		return nil, fmt.Errorf("expected layout size must be non-negative: %d", expectedSize)
	}
	lengths, total, err := InspectSegmentLayout(segments)
	if err != nil {
		return nil, err
	}

	if total != expectedSize {
		return nil, fmt.Errorf("segment layout size mismatch: expected %d bytes, found %d", expectedSize, total)
	}

	return lengths, nil
}
