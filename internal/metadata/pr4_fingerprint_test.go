package metadata

import (
	"strings"
	"testing"

	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func pr4FingerprintMetadata() *metapb.FileMetadata {
	return &metapb.FileMetadata{
		FileSize: 300,
		Status:   metapb.FileStatus_FILE_STATUS_HEALTHY,
		SegmentData: []*metapb.SegmentData{
			{Id: "segment-a.invalid", SegmentSize: 200, StartOffset: 0, EndOffset: 199},
			{Id: "segment-b.invalid", SegmentSize: 200, StartOffset: 20, EndOffset: 119},
		},
	}
}

func TestPR4CanonicalSegmentLayoutFingerprint(t *testing.T) {
	base := pr4FingerprintMetadata()
	want, err := CanonicalSegmentLayoutFingerprint(base)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(want, "sha256:"))
	require.Len(t, want, len("sha256:")+64)
	assert.NotContains(t, want, "segment-a.invalid", "fingerprint must not expose article identifiers")

	clone := proto.Clone(base).(*metapb.FileMetadata)
	clone.Status = metapb.FileStatus_FILE_STATUS_CORRUPTED
	clone.ModifiedAt = 123456
	clone.Password = "unrelated-layout-secret"
	got, err := CanonicalSegmentLayoutFingerprint(clone)
	require.NoError(t, err)
	assert.Equal(t, want, got, "mutable health and access metadata must not change revision identity")

	tests := []struct {
		name   string
		mutate func(*metapb.FileMetadata)
	}{
		{name: "virtual size", mutate: func(m *metapb.FileMetadata) { m.FileSize++ }},
		{name: "message identity", mutate: func(m *metapb.FileMetadata) { m.SegmentData[0].Id = "replacement.invalid" }},
		{name: "ordered layout", mutate: func(m *metapb.FileMetadata) { m.SegmentData[0], m.SegmentData[1] = m.SegmentData[1], m.SegmentData[0] }},
		{name: "decoded size", mutate: func(m *metapb.FileMetadata) { m.SegmentData[0].SegmentSize++ }},
		{name: "start offset", mutate: func(m *metapb.FileMetadata) { m.SegmentData[1].StartOffset++ }},
		{name: "end offset", mutate: func(m *metapb.FileMetadata) { m.SegmentData[1].EndOffset++ }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changed := proto.Clone(base).(*metapb.FileMetadata)
			tt.mutate(changed)
			got, err := CanonicalSegmentLayoutFingerprint(changed)
			require.NoError(t, err)
			assert.NotEqual(t, want, got)
		})
	}
}

func TestPR4CanonicalFingerprintNormalizesSharedNestedSources(t *testing.T) {
	shared := &metapb.NestedSegmentSource{
		Segments: []*metapb.SegmentData{
			{Id: "outer-a.invalid", SegmentSize: 500, StartOffset: 5, EndOffset: 404},
			{Id: "outer-b.invalid", SegmentSize: 500, StartOffset: 0, EndOffset: 499},
		},
		InnerVolumeSize: 900,
	}
	compact := &metapb.FileMetadata{
		FileSize:           250,
		SharedOuterSources: []*metapb.NestedSegmentSource{shared},
		NestedSources: []*metapb.NestedSegmentSource{
			{SharedOuterSourceIndex: 1, InnerOffset: 10, InnerLength: 100},
			{SharedOuterSourceIndex: 1, InnerOffset: 300, InnerLength: 150},
		},
	}
	expanded := proto.Clone(compact).(*metapb.FileMetadata)
	require.NoError(t, ExpandSharedOuterSources(expanded))
	expanded.SharedOuterSources = nil
	for _, source := range expanded.NestedSources {
		source.SharedOuterSourceIndex = 0
	}

	compactFingerprint, err := CanonicalSegmentLayoutFingerprint(compact)
	require.NoError(t, err)
	expandedFingerprint, err := CanonicalSegmentLayoutFingerprint(expanded)
	require.NoError(t, err)
	assert.Equal(t, compactFingerprint, expandedFingerprint,
		"storage compaction must not create a different structural revision")

	changed := proto.Clone(expanded).(*metapb.FileMetadata)
	changed.NestedSources[1].InnerOffset++
	changedFingerprint, err := CanonicalSegmentLayoutFingerprint(changed)
	require.NoError(t, err)
	assert.NotEqual(t, expandedFingerprint, changedFingerprint)
}

func TestPR4CanonicalFingerprintRejectsInvalidLayout(t *testing.T) {
	_, err := CanonicalSegmentLayoutFingerprint(nil)
	require.Error(t, err)

	invalid := pr4FingerprintMetadata()
	invalid.SegmentData[0] = nil
	_, err = CanonicalSegmentLayoutFingerprint(invalid)
	require.Error(t, err)

	invalid = pr4FingerprintMetadata()
	invalid.SegmentData[0].EndOffset = -1
	_, err = CanonicalSegmentLayoutFingerprint(invalid)
	require.Error(t, err)
}
