package metadata

import (
	"testing"

	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func pr5AuditSegment(id string, size int64) *metapb.SegmentData {
	return &metapb.SegmentData{
		Id:          id,
		SegmentSize: size,
		StartOffset: 0,
		EndOffset:   size - 1,
	}
}

func TestPR5AuditCanonicalLayoutRejectsStructurallyUnusableFiles(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		meta *metapb.FileMetadata
	}{
		{
			name: "positive file with no article positions",
			meta: &metapb.FileMetadata{FileSize: 100},
		},
		{
			name: "plain layout does not cover virtual size",
			meta: &metapb.FileMetadata{
				FileSize:    100,
				SegmentData: []*metapb.SegmentData{pr5AuditSegment("fixture-main", 99)},
			},
		},
		{
			name: "nested extents do not cover virtual size",
			meta: &metapb.FileMetadata{
				FileSize: 100,
				NestedSources: []*metapb.NestedSegmentSource{
					{
						Segments:        []*metapb.SegmentData{pr5AuditSegment("fixture-nested", 50)},
						InnerLength:     50,
						InnerVolumeSize: 50,
					},
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			layout, err := ResolveCanonicalSegmentLayout(tt.meta)
			require.Error(t, err)
			require.Nil(t, layout)
		})
	}
}

func TestPR5AuditCanonicalLayoutFollowsNestedPlaybackRepresentation(t *testing.T) {
	t.Parallel()

	meta := &metapb.FileMetadata{
		FileSize:    100,
		SegmentData: []*metapb.SegmentData{pr5AuditSegment("fixture-unused-main", 100)},
		NestedSources: []*metapb.NestedSegmentSource{
			{
				Segments:        []*metapb.SegmentData{pr5AuditSegment("fixture-playback-nested", 100)},
				InnerLength:     100,
				InnerVolumeSize: 100,
			},
		},
	}

	layout, err := ResolveCanonicalSegmentLayout(meta)
	require.NoError(t, err)
	require.Len(t, layout.Segments, 1,
		"health positions must follow the nested source selected by playback")
	require.Equal(t, "fixture-playback-nested", layout.Segments[0].MessageID)

	baseFingerprint := layout.Fingerprint
	unusedMainChanged := proto.Clone(meta).(*metapb.FileMetadata)
	unusedMainChanged.SegmentData[0].Id = "fixture-other-unused-main"
	unusedLayout, err := ResolveCanonicalSegmentLayout(unusedMainChanged)
	require.NoError(t, err)
	assert.Equal(t, baseFingerprint, unusedLayout.Fingerprint,
		"an unused main representation cannot churn nested playback health identity")

	playbackChanged := proto.Clone(meta).(*metapb.FileMetadata)
	playbackChanged.NestedSources[0].Segments[0].Id = "fixture-other-playback-nested"
	changedLayout, err := ResolveCanonicalSegmentLayout(playbackChanged)
	require.NoError(t, err)
	assert.NotEqual(t, baseFingerprint, changedLayout.Fingerprint,
		"a changed playback article must create a new health revision")
}
