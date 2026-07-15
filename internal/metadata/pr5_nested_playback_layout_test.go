package metadata

import (
	"testing"

	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func pr5NestedBackingSegments(count int, size int64) []*metapb.SegmentData {
	segments := make([]*metapb.SegmentData, count)
	for i := range segments {
		segments[i] = &metapb.SegmentData{
			Id:          "fixture-nested-" + string(rune('a'+i)),
			SegmentSize: size,
			StartOffset: 0,
			EndOffset:   size - 1,
		}
	}
	return segments
}

func pr5CanonicalMessageIDs(layout *CanonicalSegmentLayout) []string {
	ids := make([]string, len(layout.Segments))
	for i, segment := range layout.Segments {
		ids[i] = segment.MessageID
	}
	return ids
}

func TestPR5CanonicalNestedLayoutUsesOnlyPlaybackDependencies(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		aesKey  []byte
		aesIV   []byte
		wantIDs []string
	}{
		{
			name:    "plain exact extent",
			wantIDs: []string{"fixture-nested-c"},
		},
		{
			name:    "AES includes preceding CBC block",
			aesKey:  []byte("0123456789abcdef"),
			aesIV:   []byte("abcdef0123456789"),
			wantIDs: []string{"fixture-nested-b", "fixture-nested-c"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			meta := &metapb.FileMetadata{
				FileSize: 4,
				NestedSources: []*metapb.NestedSegmentSource{{
					Segments:        pr5NestedBackingSegments(6, 16),
					AesKey:          tt.aesKey,
					AesIv:           tt.aesIV,
					InnerOffset:     34,
					InnerLength:     4,
					InnerVolumeSize: 96,
				}},
			}

			layout, err := ResolveCanonicalSegmentLayout(meta)
			require.NoError(t, err)
			assert.Equal(t, tt.wantIDs, pr5CanonicalMessageIDs(layout))

			unrelatedTailChanged := proto.Clone(meta).(*metapb.FileMetadata)
			unrelatedTailChanged.NestedSources[0].Segments[5].Id = "fixture-unrelated-tail-changed"
			unrelatedLayout, err := ResolveCanonicalSegmentLayout(unrelatedTailChanged)
			require.NoError(t, err)
			assert.Equal(t, layout.Fingerprint, unrelatedLayout.Fingerprint,
				"an article playback cannot read must not churn health identity")

			requiredChanged := proto.Clone(meta).(*metapb.FileMetadata)
			requiredChanged.NestedSources[0].Segments[2].Id = "fixture-required-changed"
			requiredLayout, err := ResolveCanonicalSegmentLayout(requiredChanged)
			require.NoError(t, err)
			assert.NotEqual(t, layout.Fingerprint, requiredLayout.Fingerprint,
				"a changed playback dependency must create a new health identity")
		})
	}
}

func TestPR5CanonicalNestedLayoutRejectsInvalidExtentsAndBackingCoverage(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		source *metapb.NestedSegmentSource
	}{
		{
			name: "extent exceeds declared inner volume",
			source: &metapb.NestedSegmentSource{
				Segments:        pr5NestedBackingSegments(7, 16),
				InnerOffset:     80,
				InnerLength:     17,
				InnerVolumeSize: 96,
			},
		},
		{
			name: "plain backing does not reach extent end",
			source: &metapb.NestedSegmentSource{
				Segments:        pr5NestedBackingSegments(3, 16),
				InnerOffset:     32,
				InnerLength:     17,
				InnerVolumeSize: 64,
			},
		},
		{
			name: "encrypted backing does not reach rounded ciphertext end",
			source: &metapb.NestedSegmentSource{
				Segments:        pr5NestedBackingSegments(3, 16),
				AesKey:          []byte("0123456789abcdef"),
				AesIv:           []byte("abcdef0123456789"),
				InnerOffset:     32,
				InnerLength:     17,
				InnerVolumeSize: 64,
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ResolveCanonicalSegmentLayout(&metapb.FileMetadata{
				FileSize:      tt.source.InnerLength,
				NestedSources: []*metapb.NestedSegmentSource{tt.source},
			})
			require.Error(t, err)
		})
	}
}

func TestPR5CanonicalNestedLayoutResolvesDistinctEncryptedSharedExtents(t *testing.T) {
	t.Parallel()

	meta := &metapb.FileMetadata{
		FileSize: 32,
		SharedOuterSources: []*metapb.NestedSegmentSource{{
			Segments:        pr5NestedBackingSegments(6, 16),
			AesKey:          []byte("0123456789abcdef"),
			AesIv:           []byte("abcdef0123456789"),
			InnerVolumeSize: 96,
		}},
		NestedSources: []*metapb.NestedSegmentSource{
			{SharedOuterSourceIndex: 1, InnerOffset: 32, InnerLength: 16},
			{SharedOuterSourceIndex: 1, InnerOffset: 64, InnerLength: 16},
		},
	}

	layout, err := ResolveCanonicalSegmentLayout(meta)
	require.NoError(t, err)
	assert.Equal(t,
		[]string{
			"fixture-nested-b", "fixture-nested-c",
			"fixture-nested-d", "fixture-nested-e",
		},
		pr5CanonicalMessageIDs(layout),
		"each compact reference must contribute only its own CBC playback dependencies")

	expanded := proto.Clone(meta).(*metapb.FileMetadata)
	require.NoError(t, ExpandSharedOuterSources(expanded))
	expanded.SharedOuterSources = nil
	for _, source := range expanded.NestedSources {
		source.SharedOuterSourceIndex = 0
	}
	expandedLayout, err := ResolveCanonicalSegmentLayout(expanded)
	require.NoError(t, err)
	assert.Equal(t, layout.Fingerprint, expandedLayout.Fingerprint,
		"shared-source storage compaction must remain outside health identity")
}
