package nzbfilesystem

import (
	"context"
	"testing"

	"github.com/javi11/altmount/internal/holes"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
)

func TestOnHoleUsesExactMissingSegmentBytes(t *testing.T) {
	tests := []struct {
		name  string
		sizes []int64
		want  holes.Decision
	}{
		{
			name:  "uniform segment at exact two percent pads",
			sizes: makeUniformSegmentSizes(50, 200),
			want:  holes.DecisionPad,
		},
		{
			name: "nonuniform three percent segment fails",
			sizes: func() []int64 {
				sizes := make([]int64, 100)
				sizes[0] = 300
				for i := 1; i < 99; i++ {
					sizes[i] = 98
				}
				sizes[99] = 96
				return sizes
			}(),
			want: holes.DecisionFail,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			segments, fileSize := playbackSegments(tt.sizes)
			acc := &holes.Accumulator{}
			acc.Add(0) // replayed discovery: suppresses the async status-write path
			mvf := &MetadataVirtualFile{
				name: "movie.mkv",
				meta: &fileHandleMeta{
					FileSize:    fileSize,
					SegmentData: segments,
				},
				ctx:     context.Background(),
				holeAcc: acc,
			}

			if got := mvf.onHole(0, segments[0].Id); got != tt.want {
				t.Errorf("onHole() = %v, want %v", got, tt.want)
			}
			if got := acc.Total(); got != 1 {
				t.Errorf("accumulated holes = %d, want 1", got)
			}
		})
	}
}

func makeUniformSegmentSizes(count int, size int64) []int64 {
	sizes := make([]int64, count)
	for i := range sizes {
		sizes[i] = size
	}
	return sizes
}

func playbackSegments(sizes []int64) ([]*metapb.SegmentData, int64) {
	segments := make([]*metapb.SegmentData, len(sizes))
	var fileSize int64
	for i, size := range sizes {
		segments[i] = &metapb.SegmentData{
			Id:          "segment@test",
			StartOffset: 0,
			EndOffset:   size - 1,
			SegmentSize: size,
		}
		fileSize += size
	}
	return segments, fileSize
}
