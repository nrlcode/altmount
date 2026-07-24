package metadata

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/require"
)

type acquisitionRefCounter struct {
	mu sync.Mutex

	count    int64
	incErr   error
	incCalls int
	decCalls int

	incEntered chan struct{}
	incRelease <-chan struct{}
	incOnce    sync.Once
	decEntered chan struct{}
	decRelease <-chan struct{}
	decOnce    sync.Once
}

func (c *acquisitionRefCounter) IncStoreRef(ctx context.Context, _ string) error {
	c.incOnce.Do(func() {
		if c.incEntered != nil {
			close(c.incEntered)
		}
	})
	if c.incRelease != nil {
		select {
		case <-c.incRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.incCalls++
	if c.incErr != nil {
		return c.incErr
	}
	c.count++
	return nil
}

func (c *acquisitionRefCounter) DecStoreRef(ctx context.Context, _ string) (int64, error) {
	c.decOnce.Do(func() {
		if c.decEntered != nil {
			close(c.decEntered)
		}
	})
	if c.decRelease != nil {
		select {
		case <-c.decRelease:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.decCalls++
	if c.count > 0 {
		c.count--
	}
	return c.count, nil
}

func (c *acquisitionRefCounter) snapshot() (count int64, incCalls, decCalls int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count, c.incCalls, c.decCalls
}

func writeAcquisitionStore(t *testing.T, ms *MetadataService, path string) {
	t.Helper()
	require.NoError(t, ms.Store().WriteStore(path, sampleStore()))
	_, err := ms.Store().ReadStore(path)
	require.NoError(t, err, "seed the cache so validation must bypass it")
}

func acquisitionMetadata() *metapb.FileMetadata {
	return &metapb.FileMetadata{
		FileSize: 1,
		Status:   metapb.FileStatus_FILE_STATUS_HEALTHY,
	}
}

func requireMetadataAbsent(t *testing.T, ms *MetadataService, virtualPath string) {
	t.Helper()
	_, err := os.Stat(ms.GetMetadataFilePath(virtualPath))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestWriteFileMetadataV3_RequiresReferenceBeforePublication(t *testing.T) {
	t.Run("counter unavailable", func(t *testing.T) {
		root := t.TempDir()
		ms := NewMetadataService(filepath.Join(root, "metadata"))
		storePath := filepath.Join(root, "store", "release.nzbz")
		writeAcquisitionStore(t, ms, storePath)

		err := ms.WriteFileMetadataV3(context.Background(), "movie.mkv", acquisitionMetadata(), nil, storePath)
		require.Error(t, err)
		requireMetadataAbsent(t, ms, "movie.mkv")
	})

	t.Run("increment fails", func(t *testing.T) {
		root := t.TempDir()
		ms := NewMetadataService(filepath.Join(root, "metadata"))
		storePath := filepath.Join(root, "store", "release.nzbz")
		writeAcquisitionStore(t, ms, storePath)
		counter := &acquisitionRefCounter{incErr: errors.New("database unavailable")}
		ms.SetStoreRefCounter(counter)

		err := ms.WriteFileMetadataV3(context.Background(), "movie.mkv", acquisitionMetadata(), nil, storePath)
		require.ErrorContains(t, err, "database unavailable")
		requireMetadataAbsent(t, ms, "movie.mkv")
		count, incCalls, decCalls := counter.snapshot()
		require.Equal(t, int64(0), count)
		require.Equal(t, 1, incCalls)
		require.Zero(t, decCalls)
	})
}

func TestWriteFileMetadataV3_ValidatesFreshStoreAndRollsBack(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, string)
	}{
		{
			name: "missing",
			mutate: func(t *testing.T, path string) {
				require.NoError(t, os.Remove(path))
			},
		},
		{
			name: "corrupt",
			mutate: func(t *testing.T, path string) {
				require.NoError(t, os.WriteFile(path, []byte("not a zstd store"), 0o600))
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			ms := NewMetadataService(filepath.Join(root, "metadata"))
			storePath := filepath.Join(root, "store", "release.nzbz")
			writeAcquisitionStore(t, ms, storePath)
			test.mutate(t, storePath)
			counter := &acquisitionRefCounter{}
			ms.SetStoreRefCounter(counter)

			err := ms.WriteFileMetadataV3(context.Background(), "movie.mkv", acquisitionMetadata(), nil, storePath)
			require.Error(t, err)
			requireMetadataAbsent(t, ms, "movie.mkv")
			count, incCalls, decCalls := counter.snapshot()
			require.Equal(t, int64(0), count)
			require.Equal(t, 1, incCalls)
			require.Equal(t, 1, decCalls)
		})
	}
}

func TestWriteFileMetadataV3_PublicationFailureRestoresReference(t *testing.T) {
	root := t.TempDir()
	metadataRoot := filepath.Join(root, "metadata-is-a-file")
	require.NoError(t, os.WriteFile(metadataRoot, []byte("obstruction"), 0o600))
	ms := NewMetadataService(metadataRoot)
	storePath := filepath.Join(root, "store", "release.nzbz")
	writeAcquisitionStore(t, ms, storePath)
	counter := &acquisitionRefCounter{count: 1}
	ms.SetStoreRefCounter(counter)

	err := ms.WriteFileMetadataV3(context.Background(), "movie.mkv", acquisitionMetadata(), nil, storePath)
	require.Error(t, err)
	count, incCalls, decCalls := counter.snapshot()
	require.Equal(t, int64(1), count, "the existing owner must survive rollback")
	require.Equal(t, 1, incCalls)
	require.Equal(t, 1, decCalls)
	_, statErr := os.Stat(storePath)
	require.NoError(t, statErr, "writer rollback must not remove a shared store")
}

func TestWriteFileMetadataV3_SerializesWriterBeforeCleanup(t *testing.T) {
	root := t.TempDir()
	metadataRoot := filepath.Join(root, "metadata")
	storeRoot := filepath.Join(root, "store")
	storePath := filepath.Join(storeRoot, "release.nzbz")
	writer := NewMetadataService(metadataRoot)
	cleanup := NewMetadataService(metadataRoot)
	writeAcquisitionStore(t, writer, storePath)
	require.NoError(t, cleanup.ConfigureCleanupRoots(storeRoot))
	require.NoError(t, cleanup.WriteFileMetadata("old.mkv", &metapb.FileMetadata{StoreRef: storePath}))

	incEntered := make(chan struct{})
	incRelease := make(chan struct{})
	decEntered := make(chan struct{})
	counter := &acquisitionRefCounter{
		count:      1,
		incEntered: incEntered,
		incRelease: incRelease,
		decEntered: decEntered,
	}
	writer.SetStoreRefCounter(counter)
	cleanup.SetStoreRefCounter(counter)

	writerDone := make(chan error, 1)
	go func() {
		writerDone <- writer.WriteFileMetadataV3(context.Background(), "new.mkv", acquisitionMetadata(), nil, storePath)
	}()
	<-incEntered

	cleanupStarted := make(chan struct{})
	cleanupDone := make(chan error, 1)
	go func() {
		close(cleanupStarted)
		cleanupDone <- cleanup.DeleteFileMetadataWithSourceNzb(context.Background(), "old.mkv", false)
	}()
	<-cleanupStarted

	cleanupEnteredEarly := false
	select {
	case <-decEntered:
		cleanupEnteredEarly = true
	case <-time.After(100 * time.Millisecond):
	}
	close(incRelease)

	require.NoError(t, <-writerDone)
	require.NoError(t, <-cleanupDone)
	require.False(t, cleanupEnteredEarly, "cleanup must wait for provisional acquisition and validation")
	count, incCalls, decCalls := counter.snapshot()
	require.Equal(t, int64(1), count)
	require.Equal(t, 1, incCalls)
	require.Equal(t, 1, decCalls)
	_, err := os.Stat(storePath)
	require.NoError(t, err)
	_, err = os.Stat(writer.GetMetadataFilePath("new.mkv"))
	require.NoError(t, err)
}

func TestWriteFileMetadataV3_SerializesCleanupBeforeWriter(t *testing.T) {
	root := t.TempDir()
	metadataRoot := filepath.Join(root, "metadata")
	storeRoot := filepath.Join(root, "store")
	storePath := filepath.Join(storeRoot, "release.nzbz")
	writer := NewMetadataService(metadataRoot)
	cleanup := NewMetadataService(metadataRoot)
	writeAcquisitionStore(t, writer, storePath)
	require.NoError(t, cleanup.ConfigureCleanupRoots(storeRoot))
	require.NoError(t, cleanup.WriteFileMetadata("old.mkv", &metapb.FileMetadata{StoreRef: storePath}))

	decEntered := make(chan struct{})
	decRelease := make(chan struct{})
	incEntered := make(chan struct{})
	counter := &acquisitionRefCounter{
		count:      1,
		incEntered: incEntered,
		decEntered: decEntered,
		decRelease: decRelease,
	}
	writer.SetStoreRefCounter(counter)
	cleanup.SetStoreRefCounter(counter)

	cleanupDone := make(chan error, 1)
	go func() {
		cleanupDone <- cleanup.DeleteFileMetadataWithSourceNzb(context.Background(), "old.mkv", false)
	}()
	<-decEntered

	writerStarted := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		close(writerStarted)
		writerDone <- writer.WriteFileMetadataV3(context.Background(), "new.mkv", acquisitionMetadata(), nil, storePath)
	}()
	<-writerStarted

	writerEnteredEarly := false
	select {
	case <-incEntered:
		writerEnteredEarly = true
	case <-time.After(100 * time.Millisecond):
	}
	close(decRelease)

	require.NoError(t, <-cleanupDone)
	require.Error(t, <-writerDone)
	require.False(t, writerEnteredEarly, "writer must wait until cleanup releases its authority")
	count, incCalls, decCalls := counter.snapshot()
	require.Equal(t, int64(0), count)
	require.Equal(t, 1, incCalls)
	require.Equal(t, 2, decCalls, "failed fresh validation must roll back its provisional reference")
	requireMetadataAbsent(t, writer, "new.mkv")
	_, err := os.Stat(storePath)
	require.ErrorIs(t, err, os.ErrNotExist)
}
