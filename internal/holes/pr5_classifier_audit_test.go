package holes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPR5AuditClassifyPositionsRejectsIncompleteCanonicalDomain(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name         string
		missing      []int
		segmentBytes []int64
		fileBytes    int64
	}{
		{
			name:      "positive file has no canonical positions",
			fileBytes: 100,
		},
		{
			name:         "clean verdict cannot hide invalid canonical span",
			segmentBytes: []int64{0},
			fileBytes:    100,
		},
		{
			name:         "nonmissing canonical span is still validated",
			missing:      []int{0},
			segmentBytes: []int64{1, 0},
			fileBytes:    100,
		},
		{
			name:         "nonempty damage requires positive denominator",
			missing:      []int{0},
			segmentBytes: []int64{1},
			fileBytes:    0,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			impact, err := ClassifyPositions(tt.missing, tt.segmentBytes, tt.fileBytes)
			require.Error(t, err)
			assert.NotEqual(t, VerdictClean, impact.Verdict)
			assert.NotEqual(t, VerdictDegraded, impact.Verdict)
		})
	}
}

func TestPR5AuditClassifyPositionsEnforcesExactTotalBoundary(t *testing.T) {
	t.Parallel()

	segmentBytes := make([]int64, 129)
	for i := range segmentBytes {
		segmentBytes[i] = 1
	}
	missing64 := make([]int, 0, 64)
	for position := 0; position < 128; position += 2 {
		missing64 = append(missing64, position)
	}

	impact, err := ClassifyPositions(missing64, segmentBytes, 10_000)
	require.NoError(t, err)
	assert.Equal(t, VerdictDegraded, impact.Verdict)
	assert.Equal(t, 64, impact.TotalMissing)
	assert.Equal(t, 1, impact.LongestRun)

	missing65 := append(append([]int(nil), missing64...), 128)
	impact, err = ClassifyPositions(missing65, segmentBytes, 10_000)
	require.NoError(t, err)
	assert.Equal(t, VerdictFailed, impact.Verdict)
	assert.Equal(t, 65, impact.TotalMissing)
}

func TestPR5AuditClassifyPositionsEnforcesExactRunBoundary(t *testing.T) {
	t.Parallel()

	segmentBytes := make([]int64, 10)
	for i := range segmentBytes {
		segmentBytes[i] = 10
	}

	impact, err := ClassifyPositions([]int{0, 1, 2, 3}, segmentBytes, 10_000)
	require.NoError(t, err)
	assert.Equal(t, VerdictDegraded, impact.Verdict)
	assert.Equal(t, 4, impact.LongestRun)

	impact, err = ClassifyPositions([]int{0, 1, 2, 3, 4}, segmentBytes, 10_000)
	require.NoError(t, err)
	assert.Equal(t, VerdictFailed, impact.Verdict)
	assert.Equal(t, 5, impact.LongestRun)
}
