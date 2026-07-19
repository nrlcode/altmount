package metadata

import (
	"math"
	"reflect"
	"testing"

	metapb "github.com/javi11/altmount/internal/metadata/proto"
)

func TestValidateSegmentLayout(t *testing.T) {
	segment := func(id string, size, start, end int64) *metapb.SegmentData {
		return &metapb.SegmentData{Id: id, SegmentSize: size, StartOffset: start, EndOffset: end}
	}

	t.Run("valid nonuniform exact coverage", func(t *testing.T) {
		segments := []*metapb.SegmentData{
			segment("first@test", 5, 1, 4),
			segment("second@test", 10, 2, 7),
		}
		got, err := ValidateSegmentLayout(10, segments)
		if err != nil {
			t.Fatalf("ValidateSegmentLayout() error = %v", err)
		}
		want := []int64{4, 6}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("ValidateSegmentLayout() lengths = %v, want %v", got, want)
		}
	})

	tests := []struct {
		name         string
		expectedSize int64
		segments     []*metapb.SegmentData
	}{
		{"nil segment", 1, []*metapb.SegmentData{nil}},
		{"empty ID", 1, []*metapb.SegmentData{segment("", 1, 0, 0)}},
		{"zero segment size", 1, []*metapb.SegmentData{segment("zero@test", 0, 0, 0)}},
		{"negative segment size", 1, []*metapb.SegmentData{segment("negative-size@test", -1, 0, 0)}},
		{"negative start", 2, []*metapb.SegmentData{segment("negative-start@test", 2, -1, 0)}},
		{"end before start", 1, []*metapb.SegmentData{segment("reversed@test", 2, 1, 0)}},
		{"end outside segment", 2, []*metapb.SegmentData{segment("outside@test", 1, 0, 1)}},
		{"negative expected size", -1, []*metapb.SegmentData{segment("valid@test", 1, 0, 0)}},
		{"short total", 2, []*metapb.SegmentData{segment("short@test", 1, 0, 0)}},
		{"excess total", 1, []*metapb.SegmentData{segment("excess@test", 2, 0, 1)}},
		{"total overflow", math.MaxInt64, []*metapb.SegmentData{
			segment("max@test", math.MaxInt64, 0, math.MaxInt64-1),
			segment("overflow@test", 1, 0, 0),
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ValidateSegmentLayout(tt.expectedSize, tt.segments); err == nil {
				t.Fatal("ValidateSegmentLayout() error = nil, want structural error")
			}
		})
	}
}

func TestExpectedSegmentLayoutSize(t *testing.T) {
	valid := []struct {
		name       string
		fileSize   int64
		encryption metapb.Encryption
		want       int64
	}{
		{"plain", 17, metapb.Encryption_NONE, 17},
		{"AES aligned", 16, metapb.Encryption_AES, 16},
		{"AES padded", 17, metapb.Encryption_AES, 32},
		{"rclone empty payload", 0, metapb.Encryption_RCLONE, 32},
	}
	for _, tt := range valid {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExpectedSegmentLayoutSize(tt.fileSize, tt.encryption)
			if err != nil || got != tt.want {
				t.Fatalf("ExpectedSegmentLayoutSize() = (%d, %v), want (%d, nil)", got, err, tt.want)
			}
		})
	}

	invalid := []struct {
		name       string
		fileSize   int64
		encryption metapb.Encryption
	}{
		{"negative size", -1, metapb.Encryption_NONE},
		{"AES overflow", math.MaxInt64, metapb.Encryption_AES},
		{"rclone overflow", math.MaxInt64, metapb.Encryption_RCLONE},
		{"unsupported headers", 17, metapb.Encryption_HEADERS},
		{"unknown enum", 17, metapb.Encryption(99)},
	}
	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ExpectedSegmentLayoutSize(tt.fileSize, tt.encryption); err == nil {
				t.Fatal("ExpectedSegmentLayoutSize() error = nil, want rejection")
			}
		})
	}
}
