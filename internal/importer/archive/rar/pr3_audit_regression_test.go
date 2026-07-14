package rar

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/importer/parser"
	"github.com/javi11/altmount/internal/metadata"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/javi11/nntppool/v4"
)

func TestPR3TypedArchiveFailuresAreNotSilentlyIsolated(t *testing.T) {
	tests := []struct {
		name  string
		kind  nntppool.OutcomeKind
		cause error
	}{
		{name: "temporary", kind: nntppool.OutcomeTemporaryFailure, cause: errors.New("temporary provider failure")},
		{name: "unavailable", kind: nntppool.OutcomeProviderUnavailable, cause: nntppool.ErrServiceUnavailable},
		{name: "corrupt", kind: nntppool.OutcomeCorruptBody, cause: nntppool.ErrBodyCorrupt},
		{name: "transport", kind: nntppool.OutcomeTransportFailure, cause: errors.New("transport failure")},
		{name: "inconclusive", kind: nntppool.OutcomeInconclusive, cause: errors.New("inconclusive provider result")},
		{name: "hard absence", kind: nntppool.OutcomeHardArticleAbsence, cause: nntppool.ErrArticleNotFound},
		{name: "cancellation", kind: nntppool.OutcomeCancellation, cause: context.Canceled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			typed := &nntppool.TransportError{Kind: tt.kind, Cause: tt.cause}
			proc := &scriptedRarProcessor{behavior: map[string]groupBehavior{
				"seta": {err: typed},
				"setb": {contents: []Content{{
					InternalPath: "videoB.mkv",
					Filename:     "videoB.mkv",
					Size:         1000,
					Segments: []*metapb.SegmentData{{
						Id:          "synthetic-segment",
						StartOffset: 0,
						EndOffset:   999,
					}},
				}}},
			}}

			err := ProcessArchive(context.Background(), ProcessArchiveOptions{
				VirtualDir: "movies/Release",
				ArchiveFiles: []parser.ParsedFile{
					{Filename: "setA.part01.rar"}, {Filename: "setA.part02.rar"},
					{Filename: "setB.part01.rar"}, {Filename: "setB.part02.rar"},
				},
				NzbPath:         "movies/Release.nzb",
				Processor:       proc,
				MetadataService: metadata.NewMetadataService(t.TempDir()),
				ExtractedFiles:  []parser.ExtractedFileInfo{{Name: "videoB.mkv", Size: 1000}},
				MaxPrefetch:     1,
				ReadTimeout:     30 * time.Second,
			})
			if err == nil {
				t.Fatal("typed provider failure was isolated as a bad archive group; want incomplete import")
			}
			if !errors.Is(err, typed) {
				t.Fatalf("error %v does not preserve typed provider outcome", err)
			}
		})
	}
}
