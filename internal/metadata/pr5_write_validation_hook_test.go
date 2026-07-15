package metadata

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pr5WritePermitFunc func(context.Context) error

func (fn pr5WritePermitFunc) FinalizeMetadataWrite(ctx context.Context) error { return fn(ctx) }

type pr5WriteValidatorFunc func(context.Context, string, *metapb.FileMetadata) (MetadataWritePermit, error)

func (fn pr5WriteValidatorFunc) PrepareMetadataWrite(
	ctx context.Context,
	virtualPath string,
	meta *metapb.FileMetadata,
) (MetadataWritePermit, error) {
	return fn(ctx, virtualPath, meta)
}

type pr5RollbackJournalPermit struct{}

func (pr5RollbackJournalPermit) FinalizeMetadataWrite(context.Context) error { return nil }

func (pr5RollbackJournalPermit) JournalPriorMetadata(
	context.Context,
	string,
	[]byte,
	bool,
	string,
	string,
) error {
	return nil
}

func (pr5RollbackJournalPermit) ValidateMetadataRollbackJournal(context.Context, string) error {
	return nil
}

type pr5DurableWritePermit struct {
	finalize func(context.Context) error
}

func (p pr5DurableWritePermit) FinalizeMetadataWrite(ctx context.Context) error {
	if p.finalize == nil {
		return nil
	}
	return p.finalize(ctx)
}

func (pr5DurableWritePermit) JournalPriorMetadata(
	context.Context, string, []byte, bool, string, string,
) error {
	return nil
}

func (pr5DurableWritePermit) ValidateMetadataRollbackJournal(context.Context, string) error {
	return nil
}

func (pr5DurableWritePermit) DurableCandidateMetadata() bool { return true }

func (pr5DurableWritePermit) PrepareCandidateMetadata(context.Context, string, string) error {
	return nil
}

type pr5StoreRefCounter struct {
	increments int
	decrements int
}

func (c *pr5StoreRefCounter) IncStoreRef(context.Context, string) error {
	c.increments++
	return nil
}

func (c *pr5StoreRefCounter) DecStoreRef(context.Context, string) (int64, error) {
	c.decrements++
	return 0, nil
}

func TestPR5MetadataWriteValidationRunsBeforeFileBecomesVisible(t *testing.T) {
	service := NewMetadataService(t.TempDir())
	blocked := errors.New("synthetic validation hold")
	called := false
	service.SetWriteValidator(pr5WriteValidatorFunc(func(
		_ context.Context,
		virtualPath string,
		meta *metapb.FileMetadata,
	) (MetadataWritePermit, error) {
		called = true
		assert.Equal(t, "library/movie.mkv", virtualPath)
		assert.Equal(t, int64(100), meta.FileSize)
		assert.False(t, service.FileExists(virtualPath),
			"provisional metadata became visible before admission completed")
		return nil, blocked
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

func TestPR5MetadataWriteFailureDoesNotFinalizeAdmissionPermit(t *testing.T) {
	root := filepath.Join(t.TempDir(), "metadata-root-is-a-file")
	require.NoError(t, os.WriteFile(root, []byte("fixture"), 0o600))
	service := NewMetadataService(root)
	finalized := false
	service.SetWriteValidator(pr5WriteValidatorFunc(func(
		context.Context, string, *metapb.FileMetadata,
	) (MetadataWritePermit, error) {
		return pr5WritePermitFunc(func(context.Context) error {
			finalized = true
			return nil
		}), nil
	}))

	err := service.WriteFileMetadataAuto(context.Background(), "library/movie.mkv", &metapb.FileMetadata{
		FileSize: 100,
		SegmentData: []*metapb.SegmentData{{
			Id: "synthetic-article", SegmentSize: 100, StartOffset: 0, EndOffset: 99,
		}},
	}, nil, "")
	require.Error(t, err)
	assert.False(t, finalized, "a failed atomic metadata write must not activate its candidate revision")
}

func TestPR5MetadataWriteFinalizerFailureRemovesNewlyVisibleMetadata(t *testing.T) {
	service := NewMetadataService(t.TempDir())
	activationFailed := errors.New("synthetic activation failure")
	finalized := false
	service.SetWriteValidator(pr5WriteValidatorFunc(func(
		context.Context, string, *metapb.FileMetadata,
	) (MetadataWritePermit, error) {
		return pr5WritePermitFunc(func(context.Context) error {
			finalized = true
			assert.True(t, service.FileExists("library/movie.mkv"),
				"candidate activation must run only after the atomic metadata rename")
			return activationFailed
		}), nil
	}))

	err := service.WriteFileMetadataAuto(context.Background(), "library/movie.mkv", &metapb.FileMetadata{
		FileSize: 100,
		SegmentData: []*metapb.SegmentData{{
			Id: "synthetic-article", SegmentSize: 100, StartOffset: 0, EndOffset: 99,
		}},
	}, nil, "")
	require.ErrorIs(t, err, activationFailed)
	assert.True(t, finalized)
	assert.False(t, service.FileExists("library/movie.mkv"),
		"an unactivated new path must not remain visible after finalizer failure")
}

func TestPR5MetadataWriteFinalizerFailureRestoresPriorVisibleMetadata(t *testing.T) {
	service := NewMetadataService(t.TempDir())
	oldMeta := &metapb.FileMetadata{
		FileSize: 80, Status: metapb.FileStatus_FILE_STATUS_HEALTHY,
		SourceNzbPath: "prior-visible-marker",
		SegmentData: []*metapb.SegmentData{{
			Id: "synthetic-prior", SegmentSize: 80, StartOffset: 0, EndOffset: 79,
		}},
	}
	require.NoError(t, service.WriteFileMetadata("library/movie.mkv", oldMeta))
	activationFailed := errors.New("synthetic activation failure")
	service.SetWriteValidator(pr5WriteValidatorFunc(func(
		context.Context, string, *metapb.FileMetadata,
	) (MetadataWritePermit, error) {
		return pr5WritePermitFunc(func(context.Context) error {
			visible, err := service.ReadFileMetadata("library/movie.mkv")
			require.NoError(t, err)
			require.NotNil(t, visible)
			assert.Equal(t, int64(100), visible.FileSize,
				"finalization still runs only after the replacement is atomically visible")
			return activationFailed
		}), nil
	}))
	newMeta := &metapb.FileMetadata{
		FileSize: 100, Status: metapb.FileStatus_FILE_STATUS_HEALTHY,
		SegmentData: []*metapb.SegmentData{{
			Id: "synthetic-new", SegmentSize: 100, StartOffset: 0, EndOffset: 99,
		}},
	}

	err := service.WriteFileMetadataAuto(context.Background(), "library/movie.mkv", newMeta, nil, "")
	require.ErrorIs(t, err, activationFailed)
	restored, err := service.ReadFileMetadata("library/movie.mkv")
	require.NoError(t, err)
	require.NotNil(t, restored)
	assert.Equal(t, "prior-visible-marker", restored.SourceNzbPath)
	assert.Equal(t, int64(80), restored.FileSize)
}

func TestPR5MetadataPathSerializationKeepsCommittedWriterVisibleAfterLaterFailure(t *testing.T) {
	service := NewMetadataService(t.TempDir())
	firstFinalizing := make(chan struct{})
	releaseFirst := make(chan struct{})
	var validationCalls atomic.Int32
	service.SetWriteValidator(pr5WriteValidatorFunc(func(
		_ context.Context, _ string, meta *metapb.FileMetadata,
	) (MetadataWritePermit, error) {
		validationCalls.Add(1)
		if meta.FileSize == 100 {
			return pr5WritePermitFunc(func(context.Context) error {
				close(firstFinalizing)
				<-releaseFirst
				return nil
			}), nil
		}
		return pr5WritePermitFunc(func(context.Context) error {
			return errors.New("later activation failed")
		}), nil
	}))
	path := "library/serialized.mkv"
	first := &metapb.FileMetadata{
		FileSize: 100, Status: metapb.FileStatus_FILE_STATUS_HEALTHY,
		SourceNzbPath: "first-visible",
		SegmentData: []*metapb.SegmentData{{
			Id: "first-segment", SegmentSize: 100, EndOffset: 99,
		}},
	}
	second := &metapb.FileMetadata{
		FileSize: 200, Status: metapb.FileStatus_FILE_STATUS_HEALTHY,
		SourceNzbPath: "second-visible",
		SegmentData: []*metapb.SegmentData{{
			Id: "second-segment", SegmentSize: 200, EndOffset: 199,
		}},
	}
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- service.WriteFileMetadataAuto(context.Background(), path, first, nil, "")
	}()
	<-firstFinalizing
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- service.WriteFileMetadataAuto(context.Background(), path, second, nil, "")
	}()
	select {
	case <-secondDone:
		t.Fatal("later writer interleaved with the first writer's unresolved finalization")
	case <-time.After(25 * time.Millisecond):
	}
	assert.Equal(t, int32(1), validationCalls.Load())
	close(releaseFirst)
	require.NoError(t, <-firstDone)
	require.Error(t, <-secondDone)
	visible, err := service.ReadFileMetadata(path)
	require.NoError(t, err)
	require.NotNil(t, visible)
	assert.Equal(t, "first-visible", visible.SourceNzbPath)
}

func TestPR5DurableLongFilenamePublicationUsesCanonicalMetadataPath(t *testing.T) {
	service := NewMetadataService(t.TempDir())
	path := filepath.Join("library", strings.Repeat("l", 280)+".mkv")
	finalized := false
	service.SetWriteValidator(pr5WriteValidatorFunc(func(
		context.Context, string, *metapb.FileMetadata,
	) (MetadataWritePermit, error) {
		return pr5DurableWritePermit{finalize: func(context.Context) error {
			visible, err := service.ReadFileMetadata(path)
			require.NoError(t, err)
			require.NotNil(t, visible)
			assert.Equal(t, int64(123), visible.FileSize)
			finalized = true
			return nil
		}}, nil
	}))

	err := service.WriteFileMetadataAuto(context.Background(), path, &metapb.FileMetadata{
		FileSize: 123, Status: metapb.FileStatus_FILE_STATUS_HEALTHY,
		SegmentData: []*metapb.SegmentData{{
			Id: "long-name-segment", SegmentSize: 123, EndOffset: 122,
		}},
	}, nil, "")
	require.NoError(t, err)
	assert.True(t, finalized)
	visible, err := service.ReadFileMetadata(path)
	require.NoError(t, err)
	require.NotNil(t, visible)
	assert.Equal(t, int64(123), visible.FileSize)
	lite, err := service.ReadFileMetadataLite(path)
	require.NoError(t, err)
	require.NotNil(t, lite)
	assert.Equal(t, int64(123), lite.FileSize)
	_, state, err := service.CaptureMetadataVisibilitySnapshot(path)
	require.NoError(t, err)
	assert.True(t, state.Exists)
}

func TestPR5TruncationCollisionSerializesDurablePublicationAndRestore(t *testing.T) {
	service := NewMetadataService(t.TempDir())
	prefix := strings.Repeat("c", 260)
	firstPath := filepath.Join("library", prefix+"-first.mkv")
	secondPath := filepath.Join("library", prefix+"-second.mkv")
	require.Equal(t, service.metadataPath(firstPath), service.metadataPath(secondPath))

	firstFinalizing := make(chan struct{})
	releaseFirst := make(chan struct{})
	var calls atomic.Int32
	service.SetWriteValidator(pr5WriteValidatorFunc(func(
		_ context.Context, _ string, meta *metapb.FileMetadata,
	) (MetadataWritePermit, error) {
		calls.Add(1)
		if meta.FileSize == 100 {
			return pr5DurableWritePermit{finalize: func(context.Context) error {
				close(firstFinalizing)
				<-releaseFirst
				return nil
			}}, nil
		}
		return pr5DurableWritePermit{finalize: func(context.Context) error {
			return errors.New("later colliding activation failed")
		}}, nil
	}))
	first := &metapb.FileMetadata{
		FileSize: 100, Status: metapb.FileStatus_FILE_STATUS_HEALTHY,
		SourceNzbPath: "first-collision-visible",
		SegmentData:   []*metapb.SegmentData{{Id: "first-collision", SegmentSize: 100, EndOffset: 99}},
	}
	second := &metapb.FileMetadata{
		FileSize: 200, Status: metapb.FileStatus_FILE_STATUS_HEALTHY,
		SourceNzbPath: "second-collision-visible",
		SegmentData:   []*metapb.SegmentData{{Id: "second-collision", SegmentSize: 200, EndOffset: 199}},
	}
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- service.WriteFileMetadataAuto(context.Background(), firstPath, first, nil, "")
	}()
	<-firstFinalizing
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- service.WriteFileMetadataAuto(context.Background(), secondPath, second, nil, "")
	}()
	select {
	case <-secondDone:
		t.Fatal("colliding truncated path interleaved with unresolved finalization")
	case <-time.After(25 * time.Millisecond):
	}
	assert.Equal(t, int32(1), calls.Load())
	close(releaseFirst)
	require.NoError(t, <-firstDone)
	require.Error(t, <-secondDone)
	visible, err := service.ReadFileMetadata(firstPath)
	require.NoError(t, err)
	require.NotNil(t, visible)
	assert.Equal(t, "first-collision-visible", visible.SourceNzbPath)
}

func TestPR5MetadataWriteFinalizerFailureBalancesV3StoreReference(t *testing.T) {
	service := NewMetadataService(t.TempDir())
	counter := &pr5StoreRefCounter{}
	service.SetStoreRefCounter(counter)
	activationFailed := errors.New("synthetic activation failure")
	service.SetWriteValidator(pr5WriteValidatorFunc(func(
		context.Context, string, *metapb.FileMetadata,
	) (MetadataWritePermit, error) {
		return pr5WritePermitFunc(func(context.Context) error { return activationFailed }), nil
	}))
	meta := &metapb.FileMetadata{
		FileSize: 100, Status: metapb.FileStatus_FILE_STATUS_HEALTHY,
		SegmentData: []*metapb.SegmentData{{
			Id: "synthetic-v3", SegmentSize: 100, StartOffset: 0, EndOffset: 99,
		}},
	}

	err := service.WriteFileMetadataAuto(
		context.Background(), "library/movie.mkv", meta,
		map[string]int64{"synthetic-v3": 0}, "fixture-store.nzbz",
	)
	require.ErrorIs(t, err, activationFailed)
	assert.False(t, service.FileExists("library/movie.mkv"))
	assert.Equal(t, 1, counter.increments)
	assert.Equal(t, 1, counter.decrements,
		"rolling back a newly visible v3 file must undo its store reference")
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

func TestPR5DurableV3FallbackDoesNotLogPrivatePathOrSegmentIdentity(t *testing.T) {
	var logs bytes.Buffer
	priorLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(priorLogger) })

	service := NewMetadataService(t.TempDir())
	service.SetWriteValidator(pr5WriteValidatorFunc(func(
		context.Context, string, *metapb.FileMetadata,
	) (MetadataWritePermit, error) {
		return pr5RollbackJournalPermit{}, nil
	}))
	const (
		privatePath = "library/private-provider-user-marker.mkv"
		privateID   = "private-article-identity-marker"
	)
	err := service.WriteFileMetadataAuto(
		context.Background(),
		privatePath,
		&metapb.FileMetadata{
			FileSize: 100,
			SegmentData: []*metapb.SegmentData{{
				Id: privateID, SegmentSize: 100, StartOffset: 0, EndOffset: 99,
			}},
		},
		map[string]int64{},
		"private-store-marker.nzbz",
	)
	require.NoError(t, err, "a missing v3 index entry should retain the established v1 fallback")
	assert.True(t, service.FileExists(privatePath))
	assert.NotContains(t, logs.String(), privatePath)
	assert.NotContains(t, logs.String(), privateID)
}
