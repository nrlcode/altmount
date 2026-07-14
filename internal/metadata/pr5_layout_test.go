package metadata

import (
	"strings"
	"testing"

	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPR5ResolveCanonicalSegmentLayoutMatchesFingerprintOrder(t *testing.T) {
	t.Parallel()

	meta := &metapb.FileMetadata{
		FileSize: 85,
		SegmentData: []*metapb.SegmentData{
			{Id: "fixture-duplicate", SegmentSize: 100, StartOffset: 10, EndOffset: 59},
			{Id: "fixture-duplicate", SegmentSize: 100, StartOffset: 60, EndOffset: 99},
		},
		NestedSources: []*metapb.NestedSegmentSource{
			{
				InnerOffset:     0,
				InnerLength:     85,
				InnerVolumeSize: 100,
				Segments: []*metapb.SegmentData{
					{Id: "fixture-nested", SegmentSize: 100, StartOffset: 15, EndOffset: 99},
				},
			},
		},
	}

	wantFingerprint, err := CanonicalSegmentLayoutFingerprint(meta)
	require.NoError(t, err)

	layout, err := ResolveCanonicalSegmentLayout(meta)
	require.NoError(t, err)
	require.NotNil(t, layout)
	assert.Equal(t, wantFingerprint, layout.Fingerprint)
	assert.Equal(t, int64(85), layout.VirtualSize)
	require.Len(t, layout.Segments, 1)
	assert.Equal(t, "fixture-nested", layout.Segments[0].MessageID,
		"nested sources are the authoritative playback representation")
	assert.Equal(t, int64(85), layout.Segments[0].UsableBytes)
	for i, segment := range layout.Segments {
		assert.Equal(t, int64(i), segment.Position)
	}
}

func TestPR5ResolveCanonicalSegmentLayoutExpandsSharedSourcesWithoutMutation(t *testing.T) {
	t.Parallel()

	meta := &metapb.FileMetadata{
		FileSize: 25,
		SharedOuterSources: []*metapb.NestedSegmentSource{
			{
				InnerVolumeSize: 50,
				Segments: []*metapb.SegmentData{
					{Id: "fixture-shared", SegmentSize: 50, StartOffset: 0, EndOffset: 24},
				},
			},
		},
		NestedSources: []*metapb.NestedSegmentSource{
			{SharedOuterSourceIndex: 1, InnerOffset: 0, InnerLength: 25},
		},
	}

	layout, err := ResolveCanonicalSegmentLayout(meta)
	require.NoError(t, err)
	require.Len(t, layout.Segments, 1)
	assert.Equal(t, "fixture-shared", layout.Segments[0].MessageID)
	assert.Equal(t, int32(1), meta.NestedSources[0].SharedOuterSourceIndex,
		"canonicalization must not mutate the caller's compact metadata")
	assert.Empty(t, meta.NestedSources[0].Segments)
}

func TestPR5ResolveCanonicalSegmentLayoutDoesNotEchoArticleIdentityInErrors(t *testing.T) {
	t.Parallel()

	const syntheticIdentity = "fixture-sensitive-identity"
	meta := &metapb.FileMetadata{
		FileSize: 10,
		SegmentData: []*metapb.SegmentData{
			{Id: syntheticIdentity, SegmentSize: 10, StartOffset: 9, EndOffset: 8},
		},
	}

	_, err := ResolveCanonicalSegmentLayout(meta)
	require.Error(t, err)
	assert.False(t, strings.Contains(err.Error(), syntheticIdentity),
		"layout validation errors must identify positions, never article identities")
}
