package metadata

import (
	"context"
	"errors"
	"testing"

	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pr5WriteValidatorFunc func(context.Context, string, *metapb.FileMetadata) error

func (fn pr5WriteValidatorFunc) ValidateMetadataWrite(
	ctx context.Context,
	virtualPath string,
	meta *metapb.FileMetadata,
) error {
	return fn(ctx, virtualPath, meta)
}

func TestPR5MetadataWriteValidationRunsBeforeFileBecomesVisible(t *testing.T) {
	service := NewMetadataService(t.TempDir())
	blocked := errors.New("synthetic validation hold")
	called := false
	service.SetWriteValidator(pr5WriteValidatorFunc(func(
		_ context.Context,
		virtualPath string,
		meta *metapb.FileMetadata,
	) error {
		called = true
		assert.Equal(t, "library/movie.mkv", virtualPath)
		assert.Equal(t, int64(100), meta.FileSize)
		assert.False(t, service.FileExists(virtualPath),
			"provisional metadata became visible before admission completed")
		return blocked
	}))

	err := service.WriteFileMetadataAuto(context.Background(), "library/movie.mkv", &metapb.FileMetadata{
		FileSize: 100,
		SegmentData: []*metapb.SegmentData{{
			Id: "synthetic-article", SegmentSize: 100, StartOffset: 0, EndOffset: 99,
		}},
	}, nil, "")
	require.ErrorIs(t, err, blocked)
	assert.True(t, called)
	assert.False(t, service.FileExists("library/movie.mkv"))
}

func TestPR5MetadataWriteWithoutImportValidationPreservesExistingCallers(t *testing.T) {
	service := NewMetadataService(t.TempDir())
	err := service.WriteFileMetadataAuto(context.Background(), "library/ordinary.mkv", &metapb.FileMetadata{
		FileSize: 50,
		SegmentData: []*metapb.SegmentData{{
			Id: "synthetic-ordinary", SegmentSize: 50, StartOffset: 0, EndOffset: 49,
		}},
	}, nil, "")
	require.NoError(t, err)
	assert.True(t, service.FileExists("library/ordinary.mkv"))
}
