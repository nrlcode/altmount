package metadata

import (
	"fmt"
	"testing"

	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

const (
	chg008SegmentSize  = int64(64)
	chg008ExtentLength = int64(4)
)

var chg008FingerprintResult string

func chg008Segments(count int) []*metapb.SegmentData {
	segments := make([]*metapb.SegmentData, count)
	for i := range segments {
		segments[i] = &metapb.SegmentData{
			Id:          fmt.Sprintf("chg008-segment-%06d@example.invalid", i),
			SegmentSize: chg008SegmentSize,
			StartOffset: 0,
			EndOffset:   chg008SegmentSize - 1,
		}
	}
	return segments
}

func chg008CompactNestedMetadata(extentCount, segmentCount int) *metapb.FileMetadata {
	segments := chg008Segments(segmentCount)
	meta := &metapb.FileMetadata{
		FileSize: int64(extentCount) * chg008ExtentLength,
		SharedOuterSources: []*metapb.NestedSegmentSource{{
			Segments:        segments,
			InnerVolumeSize: int64(segmentCount) * chg008SegmentSize,
		}},
		NestedSources: make([]*metapb.NestedSegmentSource, extentCount),
	}
	for i := range meta.NestedSources {
		meta.NestedSources[i] = &metapb.NestedSegmentSource{
			SharedOuterSourceIndex: 1,
			InnerOffset:            int64(i) * chg008ExtentLength,
			InnerLength:            chg008ExtentLength,
		}
	}
	return meta
}

func chg008ReadExpandedMetadata(extentCount, segmentCount int) *metapb.FileMetadata {
	meta := chg008CompactNestedMetadata(extentCount, segmentCount)
	if err := ExpandSharedOuterSources(meta); err != nil {
		panic(err)
	}
	return meta
}

func chg008SharedAliasMetadata(extentCount, segmentCount int) *metapb.FileMetadata {
	meta := chg008ReadExpandedMetadata(extentCount, segmentCount)
	meta.SharedOuterSources = nil
	for _, source := range meta.NestedSources {
		source.SharedOuterSourceIndex = 0
	}
	return meta
}

func chg008LegacyExpandedMetadata(extentCount, segmentCount int) *metapb.FileMetadata {
	meta := chg008SharedAliasMetadata(extentCount, segmentCount)
	encoded, err := proto.Marshal(meta)
	if err != nil {
		panic(err)
	}
	decoded := &metapb.FileMetadata{}
	if err := proto.Unmarshal(encoded, decoded); err != nil {
		panic(err)
	}
	return decoded
}

func chg008AssertSharedSegmentBacking(t testing.TB, meta *metapb.FileMetadata) {
	t.Helper()
	require.NotEmpty(t, meta.NestedSources)
	require.NotEmpty(t, meta.NestedSources[0].Segments)
	first := &meta.NestedSources[0].Segments[0]
	for i, source := range meta.NestedSources[1:] {
		require.NotNil(t, source)
		require.NotEmpty(t, source.Segments)
		assert.Same(t, first, &source.Segments[0], "nested source %d lost shared segment backing", i+1)
	}
}

func chg008AssertDistinctSegmentBacking(t testing.TB, meta *metapb.FileMetadata) {
	t.Helper()
	require.NotEmpty(t, meta.NestedSources)
	for i, source := range meta.NestedSources {
		require.NotNil(t, source)
		require.NotEmpty(t, source.Segments)
		for j := 0; j < i; j++ {
			assert.NotSame(t, &meta.NestedSources[j].Segments[0], &source.Segments[0],
				"legacy sources %d and %d unexpectedly share segment backing", j, i)
		}
	}
}

func TestFACORECHG008FingerprintEncodingUsesV2(t *testing.T) {
	assert.Equal(t, "altmount-segment-layout-v2", segmentLayoutFingerprintVersion)
}

func TestFACORECHG008NestedFingerprintIgnoresValidMainLayout(t *testing.T) {
	base := chg008CompactNestedMetadata(3, 4)
	base.SegmentData = []*metapb.SegmentData{{
		Id:          "ignored-main@example.invalid",
		SegmentSize: base.FileSize,
		StartOffset: 0,
		EndOffset:   base.FileSize - 1,
	}}
	want, err := CanonicalSegmentLayoutFingerprint(base)
	require.NoError(t, err)

	changed := proto.Clone(base).(*metapb.FileMetadata)
	changed.SegmentData[0].Id = "different-ignored-main@example.invalid"
	got, err := CanonicalSegmentLayoutFingerprint(changed)
	require.NoError(t, err)
	assert.Equal(t, want, got, "runtime-ignored main segments must not change nested revision identity")
}

func TestFACORECHG008NestedFingerprintIgnoresMalformedMainLayout(t *testing.T) {
	base := chg008CompactNestedMetadata(3, 4)
	want, err := CanonicalSegmentLayoutFingerprint(base)
	require.NoError(t, err)

	base.SegmentData = []*metapb.SegmentData{nil}
	got, err := CanonicalSegmentLayoutFingerprint(base)
	require.NoError(t, err, "runtime-ignored malformed main segments must not reject a nested layout")
	assert.Equal(t, want, got)
}

func TestFACORECHG008NestedFingerprintIgnoresUnresolvedMainLayout(t *testing.T) {
	base := chg008CompactNestedMetadata(3, 4)
	want, err := CanonicalSegmentLayoutFingerprint(base)
	require.NoError(t, err)

	base.SegmentRuns = []*metapb.SegmentRun{{
		BaseStoreIndex: 9,
		Count:          1,
		DecodedBytes:   base.FileSize,
	}}
	got, err := CanonicalSegmentLayoutFingerprint(base)
	require.NoError(t, err, "runtime-ignored unresolved main storage must not reject a nested layout")
	assert.Equal(t, want, got)
}

func TestFACORECHG008NestedFingerprintIgnoresMalformedUnreferencedSharedStorage(t *testing.T) {
	base := chg008CompactNestedMetadata(3, 4)
	want, err := CanonicalSegmentLayoutFingerprint(base)
	require.NoError(t, err)

	tests := []struct {
		name   string
		unused *metapb.NestedSegmentSource
	}{
		{name: "nil", unused: nil},
		{name: "unresolved", unused: &metapb.NestedSegmentSource{
			SegmentRefs: []*metapb.SegmentRef{{StoreIndex: 99, StartOffset: 0, EndOffset: 63, DecodedBytes: 64}},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidate := proto.Clone(base).(*metapb.FileMetadata)
			candidate.SharedOuterSources = append(candidate.SharedOuterSources, tt.unused)
			got, err := CanonicalSegmentLayoutFingerprint(candidate)
			require.NoError(t, err, "unreferenced storage is not part of the effective served layout")
			assert.Equal(t, want, got)
		})
	}
}

func TestFACORECHG008NestedFingerprintRejectsMalformedReferencedSharedStorage(t *testing.T) {
	base := chg008CompactNestedMetadata(3, 4)
	_, err := CanonicalSegmentLayoutFingerprint(base)
	require.NoError(t, err, "valid referenced shared storage is the positive control")

	tests := []struct {
		name   string
		mutate func(*metapb.FileMetadata)
	}{
		{name: "nil", mutate: func(meta *metapb.FileMetadata) {
			meta.SharedOuterSources = append(meta.SharedOuterSources, nil)
			meta.NestedSources[0].SharedOuterSourceIndex = 2
		}},
		{name: "unresolved", mutate: func(meta *metapb.FileMetadata) {
			meta.SharedOuterSources = append(meta.SharedOuterSources, &metapb.NestedSegmentSource{
				SegmentRefs: []*metapb.SegmentRef{{StoreIndex: 99, StartOffset: 0, EndOffset: 63, DecodedBytes: 64}},
			})
			meta.NestedSources[0].SharedOuterSourceIndex = 2
		}},
		{name: "out of range", mutate: func(meta *metapb.FileMetadata) {
			meta.NestedSources[0].SharedOuterSourceIndex = 2
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidate := proto.Clone(base).(*metapb.FileMetadata)
			tt.mutate(candidate)
			_, err := CanonicalSegmentLayoutFingerprint(candidate)
			require.Error(t, err, "malformed storage referenced by an effective extent must fail closed")
		})
	}
}

func TestFACORECHG008NestedFingerprintDiscriminatesEffectiveLayout(t *testing.T) {
	base := chg008CompactNestedMetadata(3, 4)
	want, err := CanonicalSegmentLayoutFingerprint(base)
	require.NoError(t, err)

	tests := []struct {
		name   string
		mutate func(*metapb.FileMetadata)
	}{
		{name: "source order", mutate: func(meta *metapb.FileMetadata) {
			meta.NestedSources[0], meta.NestedSources[1] = meta.NestedSources[1], meta.NestedSources[0]
		}},
		{name: "inner offset", mutate: func(meta *metapb.FileMetadata) {
			meta.NestedSources[1].InnerOffset++
		}},
		{name: "inner lengths", mutate: func(meta *metapb.FileMetadata) {
			meta.NestedSources[0].InnerLength++
			meta.NestedSources[1].InnerLength--
		}},
		{name: "inherited inner volume size", mutate: func(meta *metapb.FileMetadata) {
			meta.SharedOuterSources[0].InnerVolumeSize++
		}},
		{name: "extent inner volume override", mutate: func(meta *metapb.FileMetadata) {
			meta.NestedSources[1].InnerVolumeSize = meta.SharedOuterSources[0].InnerVolumeSize - 1
		}},
		{name: "segment identity", mutate: func(meta *metapb.FileMetadata) {
			meta.SharedOuterSources[0].Segments[0].Id = "changed-effective-segment@example.invalid"
		}},
		{name: "file size", mutate: func(meta *metapb.FileMetadata) {
			meta.FileSize++
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidate := proto.Clone(base).(*metapb.FileMetadata)
			tt.mutate(candidate)
			got, err := CanonicalSegmentLayoutFingerprint(candidate)
			require.NoError(t, err)
			assert.NotEqual(t, want, got, "effective nested layout changes require a new revision identity")
		})
	}
}

func TestFACORECHG008NestedFingerprintNormalizesStorageShapesWithoutMutation(t *testing.T) {
	compact := chg008CompactNestedMetadata(3, 4)
	readExpanded := chg008ReadExpandedMetadata(3, 4)
	sharedAlias := chg008SharedAliasMetadata(3, 4)
	legacyExpanded := chg008LegacyExpandedMetadata(3, 4)

	chg008AssertSharedSegmentBacking(t, readExpanded)
	chg008AssertSharedSegmentBacking(t, sharedAlias)
	chg008AssertDistinctSegmentBacking(t, legacyExpanded)
	snapshots := []*metapb.FileMetadata{
		proto.Clone(compact).(*metapb.FileMetadata),
		proto.Clone(readExpanded).(*metapb.FileMetadata),
		proto.Clone(sharedAlias).(*metapb.FileMetadata),
		proto.Clone(legacyExpanded).(*metapb.FileMetadata),
	}

	compactFingerprint, err := CanonicalSegmentLayoutFingerprint(compact)
	require.NoError(t, err)
	readFingerprint, err := CanonicalSegmentLayoutFingerprint(readExpanded)
	require.NoError(t, err)
	aliasFingerprint, err := CanonicalSegmentLayoutFingerprint(sharedAlias)
	require.NoError(t, err)
	legacyFingerprint, err := CanonicalSegmentLayoutFingerprint(legacyExpanded)
	require.NoError(t, err)
	assert.Equal(t, compactFingerprint, readFingerprint)
	assert.Equal(t, compactFingerprint, aliasFingerprint)
	assert.Equal(t, compactFingerprint, legacyFingerprint)

	for i, candidate := range []*metapb.FileMetadata{compact, readExpanded, sharedAlias, legacyExpanded} {
		assert.True(t, proto.Equal(snapshots[i], candidate), "storage shape %d was mutated", i)
	}
	chg008AssertSharedSegmentBacking(t, readExpanded)
	chg008AssertSharedSegmentBacking(t, sharedAlias)
	chg008AssertDistinctSegmentBacking(t, legacyExpanded)
}

func TestFACORECHG008ReadExpandedFingerprintAllocationsDoNotMultiplyByExtents(t *testing.T) {
	const (
		segmentCount            = 64
		manyExtentCount         = 64
		measurementRuns         = 3
		maxAllocsPerAddedExtent = 8
	)
	shapes := []struct {
		name  string
		build func(int, int) *metapb.FileMetadata
	}{
		{name: "read-expanded-index", build: chg008ReadExpandedMetadata},
		{name: "expanded-shared-alias", build: chg008SharedAliasMetadata},
	}
	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			measure := func(meta *metapb.FileMetadata) float64 {
				return testing.AllocsPerRun(measurementRuns, func() {
					var err error
					chg008FingerprintResult, err = CanonicalSegmentLayoutFingerprint(meta)
					if err != nil {
						panic(err)
					}
				})
			}
			oneExtentAllocs := measure(shape.build(1, segmentCount))
			manyExtentAllocs := measure(shape.build(manyExtentCount, segmentCount))
			maxAllocs := oneExtentAllocs + float64((manyExtentCount-1)*maxAllocsPerAddedExtent)
			if manyExtentAllocs > maxAllocs {
				t.Fatalf(
					"fingerprint allocations multiplied with shared extents: one extent %.0f, %d extents %.0f, want <= %.0f (one-extent baseline plus %d bookkeeping allocations per added extent)",
					oneExtentAllocs, manyExtentCount, manyExtentAllocs, maxAllocs, maxAllocsPerAddedExtent,
				)
			}
		})
	}
}

var chg008BenchmarkCases = []struct {
	name         string
	segmentCount int
	extentCount  int
}{
	{name: "segments_1000/extents_1", segmentCount: 1_000, extentCount: 1},
	{name: "segments_1000/extents_100", segmentCount: 1_000, extentCount: 100},
	{name: "segments_1000/extents_500", segmentCount: 1_000, extentCount: 500},
	{name: "segments_10000/extents_1", segmentCount: 10_000, extentCount: 1},
	{name: "segments_10000/extents_50", segmentCount: 10_000, extentCount: 50},
}

func BenchmarkFACORECHG008CanonicalFingerprintCompactShared(b *testing.B) {
	benchmarkFACORECHG008CanonicalFingerprint(b, chg008CompactNestedMetadata)
}

func BenchmarkFACORECHG008CanonicalFingerprintReadExpandedShared(b *testing.B) {
	benchmarkFACORECHG008CanonicalFingerprint(b, chg008ReadExpandedMetadata)
}

func BenchmarkFACORECHG008CanonicalFingerprintExpandedSharedAlias(b *testing.B) {
	benchmarkFACORECHG008CanonicalFingerprint(b, chg008SharedAliasMetadata)
}

func benchmarkFACORECHG008CanonicalFingerprint(
	b *testing.B,
	build func(extentCount, segmentCount int) *metapb.FileMetadata,
) {
	for _, tc := range chg008BenchmarkCases {
		b.Run(tc.name, func(b *testing.B) {
			b.StopTimer()
			meta := build(tc.extentCount, tc.segmentCount)
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for b.Loop() {
				fingerprint, err := CanonicalSegmentLayoutFingerprint(meta)
				if err != nil {
					b.Fatal(err)
				}
				chg008FingerprintResult = fingerprint
			}
		})
	}
}
