package nzbfilesystem

import (
	"errors"
	"testing"

	metapb "github.com/javi11/altmount/internal/metadata/proto"
)

func nestedTestSegments(id string, size int64) []*metapb.SegmentData {
	return []*metapb.SegmentData{{
		Id: id, SegmentSize: size, StartOffset: 0, EndOffset: size - 1,
	}}
}

func TestNestedLayoutPreflightReusesSharedIndex(t *testing.T) {
	segments := nestedTestSegments("shared@test", 10)
	first := &metapb.NestedSegmentSource{
		Segments: segments, InnerOffset: 0, InnerLength: 5, InnerVolumeSize: 10,
	}
	second := &metapb.NestedSegmentSource{
		Segments: segments, InnerOffset: 5, InnerLength: 5, InnerVolumeSize: 10,
	}
	mvf := &MetadataVirtualFile{meta: &fileHandleMeta{
		FileSize: 10, NestedSources: []*metapb.NestedSegmentSource{first, second},
	}}

	indexes := mvf.initNestedIndexes()
	if indexes == nil {
		t.Fatal("initNestedIndexes() = nil for valid shared layout")
	}
	if indexes[first] == nil || indexes[first] != indexes[second] {
		t.Fatal("shared segment slice did not reuse one offset index")
	}
}

func TestNestedLayoutPreflightAcceptsLegacyUnknownPlainVolumeSize(t *testing.T) {
	source := &metapb.NestedSegmentSource{
		Segments: nestedTestSegments("legacy@test", 10), InnerOffset: 2, InnerLength: 4,
	}
	mvf := &MetadataVirtualFile{meta: &fileHandleMeta{
		FileSize: 4, NestedSources: []*metapb.NestedSegmentSource{source},
	}}
	if indexes := mvf.initNestedIndexes(); indexes == nil || indexes[source] == nil {
		t.Fatal("legacy plain source with derived volume size was rejected")
	}
}

func TestDirectLayoutPreflightUsesEncryptionAwarePhysicalSize(t *testing.T) {
	tests := []struct {
		name         string
		fileSize     int64
		physicalSize int64
		encryption   metapb.Encryption
		wantValid    bool
	}{
		{"AES padded coverage", 17, 32, metapb.Encryption_AES, true},
		{"AES logical-only coverage", 17, 17, metapb.Encryption_AES, false},
		{"rclone overhead coverage", 1, 49, metapb.Encryption_RCLONE, true},
		{"rclone logical-only coverage", 1, 1, metapb.Encryption_RCLONE, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mvf := &MetadataVirtualFile{meta: &fileHandleMeta{
				FileSize: tt.fileSize, Encryption: tt.encryption,
				SegmentData: nestedTestSegments("direct@test", tt.physicalSize),
			}}
			if got := mvf.initSegmentIndex() != nil; got != tt.wantValid {
				t.Fatalf("initSegmentIndex() valid = %t, want %t", got, tt.wantValid)
			}
		})
	}
}

func TestNestedLayoutPreflightUsesAESPhysicalSize(t *testing.T) {
	source := func(physicalSize int64) *metapb.NestedSegmentSource {
		return &metapb.NestedSegmentSource{
			Segments: nestedTestSegments("nested-aes@test", physicalSize),
			AesKey:   make([]byte, 16), AesIv: make([]byte, 16),
			InnerLength: 17, InnerVolumeSize: 17,
		}
	}

	valid := source(32)
	mvf := &MetadataVirtualFile{meta: &fileHandleMeta{
		FileSize: 17, NestedSources: []*metapb.NestedSegmentSource{valid},
	}}
	if indexes := mvf.initNestedIndexes(); indexes == nil || indexes[valid] == nil {
		t.Fatal("AES nested source with padded physical coverage was rejected")
	}

	invalid := source(17)
	mvf = &MetadataVirtualFile{meta: &fileHandleMeta{
		FileSize: 17, NestedSources: []*metapb.NestedSegmentSource{invalid},
	}}
	if indexes := mvf.initNestedIndexes(); indexes != nil {
		t.Fatal("AES nested source with logical-only physical coverage was accepted")
	}
}

func TestCreateNestedReaderRejectsInvalidWholeTopology(t *testing.T) {
	valid := &metapb.NestedSegmentSource{
		Segments: nestedTestSegments("valid@test", 5), InnerLength: 5, InnerVolumeSize: 5,
	}
	tests := []struct {
		name   string
		second *metapb.NestedSegmentSource
	}{
		{"nil later source", nil},
		{"nil later segment", &metapb.NestedSegmentSource{Segments: []*metapb.SegmentData{nil}, InnerLength: 5, InnerVolumeSize: 5}},
		{"range outside volume", &metapb.NestedSegmentSource{Segments: nestedTestSegments("range@test", 5), InnerOffset: 4, InnerLength: 2, InnerVolumeSize: 5}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mvf := &MetadataVirtualFile{meta: &fileHandleMeta{
				FileSize: 10, NestedSources: []*metapb.NestedSegmentSource{valid, tt.second},
			}}
			reader, err := mvf.createNestedReader(0, 4)
			if reader != nil {
				_ = reader.Close()
				t.Fatal("createNestedReader() returned a reader for invalid later source")
			}
			if !errors.Is(err, ErrMissmatchedSegments) {
				t.Fatalf("createNestedReader() error = %v, want ErrMissmatchedSegments", err)
			}
		})
	}
}
