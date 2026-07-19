package validation

import (
	"fmt"

	"github.com/javi11/altmount/internal/metadata"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
)

// ValidateSegmentsForFile performs local structural validation of file segments including size
// verification. It validates that segments are structurally sound (valid offsets, non-empty IDs)
// and that their total size matches the expected file size (accounting for encryption overhead).
// Network reachability is handled solely by the fast-fail pass at import start.
func ValidateSegmentsForFile(
	filename string,
	fileSize int64,
	segments []*metapb.SegmentData,
	encryption metapb.Encryption,
) error {
	expectedSize, err := metadata.ExpectedSegmentLayoutSize(fileSize, encryption)
	if err != nil {
		return fmt.Errorf("invalid segment layout size for file %q: %w", filename, err)
	}

	if _, err = metadata.ValidateSegmentLayout(expectedSize, segments); err != nil {
		return fmt.Errorf("invalid segment layout for file %q: %w", filename, err)
	}

	return nil
}
