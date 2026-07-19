package validation

import (
	"testing"

	metapb "github.com/javi11/altmount/internal/metadata/proto"
)

// segmentsOfSize builds n physical segments whose usable ranges each cover
// that segment's full, segment-relative payload.
func segmentsOfSize(prefix string, n int, size int64) []*metapb.SegmentData {
	segs := make([]*metapb.SegmentData, n)
	for i := range n {
		segs[i] = &metapb.SegmentData{
			Id:          prefix + "-" + string(rune('0'+i)),
			StartOffset: 0,
			EndOffset:   size - 1,
			SegmentSize: size,
		}
	}
	return segs
}

func TestValidateSegmentsForFile_ValidSegmentsNoError(t *testing.T) {
	segs := segmentsOfSize("seg", 8, 1000) // total 8000 bytes
	err := ValidateSegmentsForFile("movie.mkv", 8000, segs, metapb.Encryption_NONE)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestValidateSegmentsForFile_DetectsIncompleteFile(t *testing.T) {
	segs := segmentsOfSize("seg", 8, 1000) // total 8000 bytes

	// Declared file size larger than segment sum → incomplete; must error
	err := ValidateSegmentsForFile("movie.mkv", 9000, segs, metapb.Encryption_NONE)
	if err == nil {
		t.Fatal("expected incomplete-file error, got nil")
	}
}

func TestValidateSegmentsForFile_EmptySegmentsError(t *testing.T) {
	err := ValidateSegmentsForFile("movie.mkv", 1000, nil, metapb.Encryption_NONE)
	if err == nil {
		t.Fatal("expected error for empty segments, got nil")
	}
}

func TestValidateSegmentsForFile_UsesEncryptionAwarePhysicalCoverage(t *testing.T) {
	tests := []struct {
		name         string
		fileSize     int64
		physicalSize int64
		encryption   metapb.Encryption
		wantError    bool
	}{
		{"AES accepts padded physical coverage", 17, 32, metapb.Encryption_AES, false},
		{"AES rejects logical-only coverage", 17, 17, metapb.Encryption_AES, true},
		{"rclone accepts header and block overhead", 1, 49, metapb.Encryption_RCLONE, false},
		{"rclone rejects logical-only coverage", 1, 1, metapb.Encryption_RCLONE, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSegmentsForFile(
				"encrypted.bin", tt.fileSize,
				segmentsOfSize("encrypted", 1, tt.physicalSize), tt.encryption,
			)
			if (err != nil) != tt.wantError {
				t.Fatalf("ValidateSegmentsForFile() error = %v, wantError %t", err, tt.wantError)
			}
		})
	}
}

func TestValidateSegmentsForFile_RejectsInvalidPhysicalBounds(t *testing.T) {
	tests := []struct {
		name     string
		fileSize int64
		segment  *metapb.SegmentData
	}{
		{
			name:     "zero physical segment size",
			fileSize: 1,
			segment:  &metapb.SegmentData{Id: "zero@test", StartOffset: 0, EndOffset: 0, SegmentSize: 0},
		},
		{
			name:     "negative physical segment size",
			fileSize: 1,
			segment:  &metapb.SegmentData{Id: "negative@test", StartOffset: 0, EndOffset: 0, SegmentSize: -1},
		},
		{
			name:     "end offset outside physical segment",
			fileSize: 1_001,
			segment:  &metapb.SegmentData{Id: "outside@test", StartOffset: 0, EndOffset: 1_000, SegmentSize: 1_000},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSegmentsForFile("movie.mkv", tt.fileSize, []*metapb.SegmentData{tt.segment}, metapb.Encryption_NONE)
			if err == nil {
				t.Fatal("expected invalid physical bounds error, got nil")
			}
		})
	}
}
