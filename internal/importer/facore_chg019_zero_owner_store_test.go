package importer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/javi11/altmount/internal/importer/archive/rar"
	"github.com/javi11/altmount/internal/importer/parser"
	"github.com/javi11/altmount/internal/metadata"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/javi11/altmount/internal/progress"
	"github.com/javi11/altmount/internal/testsupport/nzbbuild"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type chg019StoreRefCounter struct {
	mu sync.Mutex

	counts map[string]int64

	incCalls    int
	failIncAt   int
	incErr      error
	incHook     func()
	getErr      error
	getHook     func(string)
	getCalls    []string
	getCanceled []bool
}

func newCHG019StoreRefCounter() *chg019StoreRefCounter {
	return &chg019StoreRefCounter{counts: make(map[string]int64)}
}

func (c *chg019StoreRefCounter) IncStoreRef(ctx context.Context, path string) error {
	c.mu.Lock()
	c.incCalls++
	call := c.incCalls
	hook := c.incHook
	fail := c.failIncAt == call
	incErr := c.incErr
	c.mu.Unlock()

	if hook != nil {
		hook()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if fail {
		return incErr
	}

	c.mu.Lock()
	c.counts[path]++
	c.mu.Unlock()
	return nil
}

func (c *chg019StoreRefCounter) DecStoreRef(ctx context.Context, path string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.counts[path] <= 1 {
		delete(c.counts, path)
		return 0, nil
	}
	c.counts[path]--
	return c.counts[path], nil
}

func (c *chg019StoreRefCounter) GetStoreRefCount(ctx context.Context, path string) (int64, error) {
	c.mu.Lock()
	c.getCalls = append(c.getCalls, path)
	c.getCanceled = append(c.getCanceled, ctx.Err() != nil)
	hook := c.getHook
	getErr := c.getErr
	c.mu.Unlock()

	if hook != nil {
		hook(path)
	}
	if getErr != nil {
		return 0, getErr
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[path], nil
}

func (c *chg019StoreRefCounter) count(path string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[path]
}

func (c *chg019StoreRefCounter) getObservations() ([]string, []bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.getCalls...), append([]bool(nil), c.getCanceled...)
}

func newCHG019BatteryEnv(t *testing.T) (*batteryEnv, *chg019StoreRefCounter) {
	t.Helper()
	env := newBatteryEnv(t)
	counter := newCHG019StoreRefCounter()
	env.svc.SetStoreRefCounter(counter)
	return env, counter
}

func chg019SingleNzb(t *testing.T, env *batteryEnv, id, subject, name string) string {
	t.Helper()
	content := bytes.Repeat([]byte("store-release"), 64)
	segments := env.registerContent(id, content, len(content), 1, nil)
	nzb := nzbbuild.Build(nzbbuild.File{Subject: subject, Segments: segments})
	return nzbbuild.WriteTemp(t, nzb, name)
}

func chg019Process(env *batteryEnv, ctx context.Context, source string, queueID int) (string, []string, error) {
	return env.proc.ProcessNzbFile(
		ctx,
		source,
		filepath.Dir(source),
		queueID,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
}

func chg019StorePath(env *batteryEnv, queueID int, source string) string {
	base := filepath.Base(source)
	base = base[:len(base)-len(filepath.Ext(base))]
	return filepath.Join(env.configDir, ".nzbs", fmt.Sprintf("%d-%s.nzbz", queueID, base))
}

func TestCHG019RemovesRejectedZeroOwnerStore(t *testing.T) {
	env, counter := newCHG019BatteryEnv(t)
	source := chg019SingleNzb(t, env, "chg019-reject", "payload.not-allowed", "zero-owner-reject")
	storePath := chg019StorePath(env, 101, source)

	_, written, err := chg019Process(env, context.Background(), source, 101)

	require.Error(t, err)
	assert.Empty(t, written)
	assert.Zero(t, counter.count(storePath))
	assert.NoFileExists(t, storePath)
	getCalls, _ := counter.getObservations()
	assert.Equal(t, []string{storePath}, getCalls)
}

func TestCHG019RemovesZeroOwnerStoreAfterCancellation(t *testing.T) {
	env, counter := newCHG019BatteryEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	counter.incHook = cancel
	source := chg019SingleNzb(t, env, "chg019-cancel", "movie.mkv", "zero-owner-cancel")
	storePath := chg019StorePath(env, 102, source)

	_, _, err := chg019Process(env, ctx, source, 102)

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, counter.count(storePath))
	assert.NoFileExists(t, storePath)
	getCalls, getCanceled := counter.getObservations()
	assert.Equal(t, []string{storePath}, getCalls)
	assert.Equal(t, []bool{false}, getCanceled, "release lookup must outlive caller cancellation")
}

func TestCHG019RetainsSuccessfulOwnedStore(t *testing.T) {
	env, counter := newCHG019BatteryEnv(t)
	source := chg019SingleNzb(t, env, "chg019-owned", "movie.mkv", "owned-success")
	storePath := chg019StorePath(env, 103, source)

	_, written, err := chg019Process(env, context.Background(), source, 103)

	require.NoError(t, err)
	require.Len(t, written, 1)
	assert.Equal(t, int64(1), counter.count(storePath))
	assert.FileExists(t, storePath)
	getCalls, _ := counter.getObservations()
	assert.Equal(t, []string{storePath}, getCalls)
}

type chg019RarProcessor struct {
	contents []rar.Content
}

func (p chg019RarProcessor) AnalyzeRarContentFromNzb(context.Context, []parser.ParsedFile, string, *progress.Tracker) ([]rar.Content, error) {
	return p.contents, nil
}

func (chg019RarProcessor) CreateFileMetadataFromRarContent(content rar.Content, _ string, _ int64, _ string) *metapb.FileMetadata {
	return &metapb.FileMetadata{
		FileSize:    content.Size,
		SegmentData: content.Segments,
		Status:      metapb.FileStatus_FILE_STATUS_HEALTHY,
	}
}

func TestCHG019RemovesSuccessfulArchiveStoreWithNoNewOwner(t *testing.T) {
	env, counter := newCHG019BatteryEnv(t)
	content := bytes.Repeat([]byte("archive"), 64)
	segments := env.registerContent("chg019-archive", content, len(content), 1, nil)
	nzb := nzbbuild.Build(nzbbuild.File{Subject: "archive.rar", Segments: segments})
	source := nzbbuild.WriteTemp(t, nzb, "archive-reuse")
	segmentID := segments[0].ID
	env.proc.rarProcessor = chg019RarProcessor{contents: []rar.Content{{
		InternalPath: "movie.mkv",
		Filename:     "movie.mkv",
		Size:         int64(len(content)),
		PackedSize:   int64(len(content)),
		Segments: []*metapb.SegmentData{{
			Id:          segmentID,
			SegmentSize: int64(len(content)),
			StartOffset: 0,
			EndOffset:   int64(len(content) - 1),
		}},
	}}}
	firstStore := chg019StorePath(env, 201, source)
	secondStore := chg019StorePath(env, 202, source)

	_, firstWritten, err := chg019Process(env, context.Background(), source, 201)
	require.NoError(t, err)
	require.NotEmpty(t, firstWritten)
	require.Equal(t, int64(1), counter.count(firstStore))
	require.FileExists(t, firstStore)

	_, secondWritten, err := chg019Process(env, context.Background(), source, 202)
	require.NoError(t, err)
	require.Equal(t, []string{"DIR:/archive-reuse"}, secondWritten)
	assert.Zero(t, counter.count(secondStore))
	assert.NoFileExists(t, secondStore)
	assert.Equal(t, int64(1), counter.count(firstStore))
	assert.FileExists(t, firstStore)
}

func TestCHG019RetainsStoreWithPartialOwner(t *testing.T) {
	env, counter := newCHG019BatteryEnv(t)
	partialErr := errors.New("second metadata owner rejected")
	counter.failIncAt = 2
	counter.incErr = partialErr
	first := env.registerContent("chg019-partial-first", bytes.Repeat([]byte("a"), 32), 32, 1, nil)
	second := env.registerContent("chg019-partial-second", bytes.Repeat([]byte("b"), 32), 32, 1, nil)
	nzb := nzbbuild.Build(
		nzbbuild.File{Subject: "first.mkv", Segments: first},
		nzbbuild.File{Subject: "second.mkv", Segments: second},
	)
	source := nzbbuild.WriteTemp(t, nzb, "partial-owner")
	storePath := chg019StorePath(env, 301, source)

	_, written, err := chg019Process(env, context.Background(), source, 301)

	require.Error(t, err)
	assert.ErrorIs(t, err, partialErr)
	require.Len(t, written, 1)
	assert.Equal(t, int64(1), counter.count(storePath))
	assert.FileExists(t, storePath)
	getCalls, _ := counter.getObservations()
	assert.Equal(t, []string{storePath}, getCalls)
}

func TestCHG019JoinsPrimaryAndReleaseErrors(t *testing.T) {
	env, counter := newCHG019BatteryEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	counter.incHook = cancel
	releaseErr := errors.New("store reference lookup unavailable")
	counter.getErr = releaseErr
	source := chg019SingleNzb(t, env, "chg019-joined", "joined.mkv", "joined-errors")
	storePath := chg019StorePath(env, 302, source)

	_, _, err := chg019Process(env, ctx, source, 302)

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.ErrorIs(t, err, releaseErr)
	assert.FileExists(t, storePath, "unknown ownership must preserve the store")
}

type chg019StoreReleaser interface {
	RemoveStoreIfUnreferenced(context.Context, string) error
}

func requireCHG019StoreReleaser(t *testing.T, service *metadata.MetadataService) chg019StoreReleaser {
	t.Helper()
	releaser, ok := any(service).(chg019StoreReleaser)
	require.True(t, ok, "MetadataService must expose the bounded rooted zero-owner release boundary")
	return releaser
}

func newCHG019MetadataService(t *testing.T) (*metadata.MetadataService, *chg019StoreRefCounter, string) {
	t.Helper()
	metadataRoot := t.TempDir()
	storeRoot := filepath.Join(t.TempDir(), ".nzbs")
	service := metadata.NewMetadataService(metadataRoot)
	counter := newCHG019StoreRefCounter()
	service.SetStoreRefCounter(counter)
	require.NoError(t, service.ConfigureCleanupRoots(storeRoot))
	return service, counter, storeRoot
}

func TestCHG019RootedStoreReleaseBoundary(t *testing.T) {
	t.Run("zero removes and evicts cache", func(t *testing.T) {
		service, _, storeRoot := newCHG019MetadataService(t)
		storePath := filepath.Join(storeRoot, "zero.nzbz")
		require.NoError(t, service.Store().WriteStore(storePath, &metapb.NzbStore{}))
		_, err := service.Store().ReadStore(storePath)
		require.NoError(t, err)

		require.NoError(t, requireCHG019StoreReleaser(t, service).RemoveStoreIfUnreferenced(context.Background(), storePath))

		assert.NoFileExists(t, storePath)
		_, err = service.Store().ReadStore(storePath)
		assert.ErrorIs(t, err, os.ErrNotExist, "removed store must not remain readable through cache")
	})

	t.Run("positive retains", func(t *testing.T) {
		service, counter, storeRoot := newCHG019MetadataService(t)
		storePath := filepath.Join(storeRoot, "owned.nzbz")
		require.NoError(t, service.Store().WriteStore(storePath, &metapb.NzbStore{}))
		require.NoError(t, counter.IncStoreRef(context.Background(), storePath))

		require.NoError(t, requireCHG019StoreReleaser(t, service).RemoveStoreIfUnreferenced(context.Background(), storePath))

		assert.FileExists(t, storePath)
		assert.Equal(t, int64(1), counter.count(storePath))
	})

	t.Run("outside root rejects before lookup", func(t *testing.T) {
		service, counter, _ := newCHG019MetadataService(t)
		outside := filepath.Join(t.TempDir(), "outside.nzbz")
		require.NoError(t, os.WriteFile(outside, []byte("keep"), 0o600))

		err := requireCHG019StoreReleaser(t, service).RemoveStoreIfUnreferenced(context.Background(), outside)

		require.Error(t, err)
		assert.ErrorContains(t, err, "outside root")
		assert.FileExists(t, outside)
		getCalls, _ := counter.getObservations()
		assert.Empty(t, getCalls)
	})

	t.Run("symlink removes link only", func(t *testing.T) {
		service, _, storeRoot := newCHG019MetadataService(t)
		require.NoError(t, os.MkdirAll(storeRoot, 0o755))
		victim := filepath.Join(t.TempDir(), "victim.nzbz")
		require.NoError(t, os.WriteFile(victim, []byte("keep"), 0o600))
		link := filepath.Join(storeRoot, "link.nzbz")
		require.NoError(t, os.Symlink(victim, link))

		require.NoError(t, requireCHG019StoreReleaser(t, service).RemoveStoreIfUnreferenced(context.Background(), link))

		assert.NoFileExists(t, link)
		assert.FileExists(t, victim)
	})

	t.Run("lookup error retains", func(t *testing.T) {
		service, counter, storeRoot := newCHG019MetadataService(t)
		storePath := filepath.Join(storeRoot, "unknown.nzbz")
		require.NoError(t, service.Store().WriteStore(storePath, &metapb.NzbStore{}))
		lookupErr := errors.New("lookup failed")
		counter.getErr = lookupErr

		err := requireCHG019StoreReleaser(t, service).RemoveStoreIfUnreferenced(context.Background(), storePath)

		assert.ErrorIs(t, err, lookupErr)
		assert.FileExists(t, storePath)
	})

	t.Run("removal error retains replacement", func(t *testing.T) {
		service, counter, storeRoot := newCHG019MetadataService(t)
		storePath := filepath.Join(storeRoot, "replace.nzbz")
		require.NoError(t, service.Store().WriteStore(storePath, &metapb.NzbStore{}))
		counter.getHook = func(string) {
			require.NoError(t, os.Remove(storePath))
			require.NoError(t, os.Mkdir(storePath, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(storePath, "keep"), []byte("keep"), 0o600))
		}

		err := requireCHG019StoreReleaser(t, service).RemoveStoreIfUnreferenced(context.Background(), storePath)

		require.Error(t, err)
		assert.FileExists(t, filepath.Join(storePath, "keep"))
	})
}
