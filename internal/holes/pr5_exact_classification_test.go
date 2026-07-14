package holes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPR5ClassifyPositionsUsesExactUsableBytes(t *testing.T) {
	t.Parallel()

	const fileBytes = int64(10_000)
	tests := []struct {
		name         string
		missing      []int
		segmentBytes []int64
		want         Verdict
		wantRatio    float64
	}{
		{
			name:         "exactly two percent remains degraded",
			missing:      []int{1},
			segmentBytes: []int64{4_900, 200, 4_900},
			want:         VerdictDegraded,
			wantRatio:    0.02,
		},
		{
			name:         "one byte beyond two percent fails",
			missing:      []int{1},
			segmentBytes: []int64{4_899, 201, 4_900},
			want:         VerdictFailed,
			wantRatio:    0.0201,
		},
		{
			name:         "variable segments sum their own spans not an average",
			missing:      []int{0, 3},
			segmentBytes: []int64{50, 4_900, 4_900, 50},
			want:         VerdictDegraded,
			wantRatio:    0.01,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			impact, err := ClassifyPositions(tt.missing, tt.segmentBytes, fileBytes)
			require.NoError(t, err)
			assert.Equal(t, tt.want, impact.Verdict)
			assert.Equal(t, len(tt.missing), impact.TotalMissing)
			assert.InDelta(t, tt.wantRatio, impact.PaddedRatio, 0.0000001)
		})
	}
}

func TestPR5ClassifyPositionsUsesPositionalRuns(t *testing.T) {
	t.Parallel()

	segmentBytes := make([]int64, 100)
	for i := range segmentBytes {
		segmentBytes[i] = 1
	}

	impact, err := ClassifyPositions([]int{12, 10, 14, 11, 13}, segmentBytes, 10_000)
	require.NoError(t, err)
	assert.Equal(t, VerdictFailed, impact.Verdict)
	assert.Equal(t, 5, impact.TotalMissing)
	assert.Equal(t, 5, impact.LongestRun)
}

func TestPR5ClassifyPositionsRejectsIncompleteOrAmbiguousLayout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		missing      []int
		segmentBytes []int64
	}{
		{name: "negative position", missing: []int{-1}, segmentBytes: []int64{100}},
		{name: "position outside layout", missing: []int{1}, segmentBytes: []int64{100}},
		{name: "duplicate position", missing: []int{0, 0}, segmentBytes: []int64{100}},
		{name: "unknown usable span", missing: []int{0}, segmentBytes: []int64{0}},
		{name: "negative usable span", missing: []int{0}, segmentBytes: []int64{-1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ClassifyPositions(tt.missing, tt.segmentBytes, 1_000)
			require.Error(t, err)
		})
	}
}
