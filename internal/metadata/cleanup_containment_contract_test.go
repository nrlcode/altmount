package metadata

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type cleanupRefCounter struct {
	count int64
	err   error
	calls []string
}

func (c *cleanupRefCounter) IncStoreRef(context.Context, string) error {
	return nil
}

func (c *cleanupRefCounter) DecStoreRef(_ context.Context, path string) (int64, error) {
	c.calls = append(c.calls, path)
	return c.count, c.err
}

type cleanupRootConfigurer interface {
	configureCleanupRoots(storeRoot string, sourceRoots []string) error
}

func configureCleanupRootsForTest(t *testing.T, ms *MetadataService, storeRoot string, sourceRoots ...string) error {
	t.Helper()
	configurer, ok := any(ms).(cleanupRootConfigurer)
	if !ok {
		t.Skip("cleanup root options are introduced by the tests-first implementation")
	}
	return configurer.configureCleanupRoots(storeRoot, sourceRoots)
}

func writeCleanupMetadata(t *testing.T, ms *MetadataService, virtualPath, sourcePath, storePath string) string {
	t.Helper()

	meta := &metapb.FileMetadata{
		FileSize:      1,
		SourceNzbPath: sourcePath,
		StoreRef:      storePath,
		Status:        metapb.FileStatus_FILE_STATUS_HEALTHY,
	}
	raw, err := proto.Marshal(meta)
	require.NoError(t, err)
	if storePath != "" {
		raw = append(append([]byte(nil), metaMagicV3...), raw...)
	}

	metaPath := ms.GetMetadataFilePath(virtualPath)
	require.NoError(t, os.MkdirAll(filepath.Dir(metaPath), 0o755))
	require.NoError(t, os.WriteFile(metaPath, raw, 0o600))
	return metaPath
}

func writeCleanupFile(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte("keep"), 0o600))
}

func TestCleanupContainment_RejectsUntrustedTargetsBeforeMutation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("containment contract uses Unix symlink semantics")
	}

	tests := []struct {
		name  string
		setup func(t *testing.T) (delete func() error, protected []string, refCounter *cleanupRefCounter)
	}{
		{
			name: "metadata lexical traversal",
			setup: func(t *testing.T) (func() error, []string, *cleanupRefCounter) {
				base := t.TempDir()
				ms := NewMetadataService(filepath.Join(base, "metadata"))
				virtualPath := filepath.Join("..", "outside", "victim")
				metaPath := writeCleanupMetadata(t, ms, virtualPath, "", "")
				return func() error {
					return ms.DeleteFileMetadataWithSourceNzb(context.Background(), virtualPath, false)
				}, []string{metaPath}, nil
			},
		},
		{
			name: "metadata parent symlink",
			setup: func(t *testing.T) (func() error, []string, *cleanupRefCounter) {
				base := t.TempDir()
				root := filepath.Join(base, "metadata")
				outside := filepath.Join(base, "outside")
				require.NoError(t, os.MkdirAll(root, 0o755))
				require.NoError(t, os.MkdirAll(outside, 0o755))
				require.NoError(t, os.Symlink(outside, filepath.Join(root, "linked")))
				ms := NewMetadataService(root)
				metaPath := filepath.Join(outside, "victim.meta")
				writeCleanupFile(t, metaPath)
				return func() error {
					return ms.DeleteFileMetadataWithSourceNzb(context.Background(), filepath.Join("linked", "victim"), false)
				}, []string{metaPath}, nil
			},
		},
		{
			name: "source path without authority",
			setup: func(t *testing.T) (func() error, []string, *cleanupRefCounter) {
				base := t.TempDir()
				ms := NewMetadataService(filepath.Join(base, "metadata"))
				sourcePath := filepath.Join(base, "source", "victim.nzb")
				writeCleanupFile(t, sourcePath)
				metaPath := writeCleanupMetadata(t, ms, "movie.mkv", sourcePath, "")
				return func() error {
					return ms.DeleteFileMetadataWithSourceNzb(context.Background(), "movie.mkv", true)
				}, []string{metaPath, sourcePath}, nil
			},
		},
		{
			name: "store path without authority",
			setup: func(t *testing.T) (func() error, []string, *cleanupRefCounter) {
				base := t.TempDir()
				ms := NewMetadataService(filepath.Join(base, "metadata"))
				storePath := filepath.Join(base, "store", "victim.nzbz")
				writeCleanupFile(t, storePath)
				metaPath := writeCleanupMetadata(t, ms, "movie.mkv", "", storePath)
				counter := &cleanupRefCounter{count: 0}
				ms.SetStoreRefCounter(counter)
				return func() error {
					return ms.DeleteFileMetadataWithSourceNzb(context.Background(), "movie.mkv", false)
				}, []string{metaPath, storePath}, counter
			},
		},
		{
			name: "physical lexical escape",
			setup: func(t *testing.T) (func() error, []string, *cleanupRefCounter) {
				base := t.TempDir()
				ms := NewMetadataService(filepath.Join(base, "metadata"))
				metaPath := writeCleanupMetadata(t, ms, "movie.mkv", "", "")
				physicalRoot := filepath.Join(base, "library")
				physicalPath := filepath.Join(base, "library-sibling", "victim.mkv")
				writeCleanupFile(t, physicalPath)
				return func() error {
					return ms.DeleteCorruptedFile(context.Background(), "movie.mkv", false, physicalPath, physicalRoot)
				}, []string{metaPath, physicalPath}, nil
			},
		},
		{
			name: "physical parent symlink",
			setup: func(t *testing.T) (func() error, []string, *cleanupRefCounter) {
				base := t.TempDir()
				ms := NewMetadataService(filepath.Join(base, "metadata"))
				metaPath := writeCleanupMetadata(t, ms, "movie.mkv", "", "")
				physicalRoot := filepath.Join(base, "library")
				outside := filepath.Join(base, "outside")
				require.NoError(t, os.MkdirAll(physicalRoot, 0o755))
				require.NoError(t, os.MkdirAll(outside, 0o755))
				require.NoError(t, os.Symlink(outside, filepath.Join(physicalRoot, "linked")))
				physicalPath := filepath.Join(physicalRoot, "linked", "victim.mkv")
				writeCleanupFile(t, filepath.Join(outside, "victim.mkv"))
				return func() error {
					return ms.DeleteCorruptedFile(context.Background(), "movie.mkv", false, physicalPath, physicalRoot)
				}, []string{metaPath, filepath.Join(outside, "victim.mkv")}, nil
			},
		},
		{
			name: "physical path without root authority",
			setup: func(t *testing.T) (func() error, []string, *cleanupRefCounter) {
				base := t.TempDir()
				ms := NewMetadataService(filepath.Join(base, "metadata"))
				metaPath := writeCleanupMetadata(t, ms, "movie.mkv", "", "")
				physicalPath := filepath.Join(base, "victim.mkv")
				writeCleanupFile(t, physicalPath)
				return func() error {
					return ms.DeleteCorruptedFile(context.Background(), "movie.mkv", false, physicalPath, "")
				}, []string{metaPath, physicalPath}, nil
			},
		},
		{
			name: "physical root is not a directory",
			setup: func(t *testing.T) (func() error, []string, *cleanupRefCounter) {
				base := t.TempDir()
				ms := NewMetadataService(filepath.Join(base, "metadata"))
				metaPath := writeCleanupMetadata(t, ms, "movie.mkv", "", "")
				physicalRoot := filepath.Join(base, "not-a-directory")
				writeCleanupFile(t, physicalRoot)
				physicalPath := filepath.Join(physicalRoot, "victim.mkv")
				return func() error {
					return ms.DeleteCorruptedFile(context.Background(), "movie.mkv", false, physicalPath, physicalRoot)
				}, []string{metaPath, physicalRoot}, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deleteTarget, protected, counter := tt.setup(t)
			err := deleteTarget()
			assert.Error(t, err)
			for _, path := range protected {
				require.FileExists(t, path, "preflight failure must not mutate %s", path)
			}
			if counter != nil {
				require.Empty(t, counter.calls, "preflight failure must not mutate reference counts")
			}
		})
	}
}

func TestCleanupContainment_ContainedMetadataAndMissingFileRemainCompatible(t *testing.T) {
	root := t.TempDir()
	ms := NewMetadataService(root)

	metaPath := writeCleanupMetadata(t, ms, filepath.Join("movies", "movie.mkv"), "", "")
	require.NoError(t, ms.DeleteFileMetadataWithSourceNzb(context.Background(), filepath.Join("movies", "movie.mkv"), false))
	require.NoFileExists(t, metaPath)

	require.NoError(t, ms.DeleteFileMetadataWithSourceNzb(context.Background(), filepath.Join("movies", "already-gone.mkv"), false))
}

func TestCleanupContainment_PropagatesMutationErrors(t *testing.T) {
	t.Run("physical target is nonempty directory", func(t *testing.T) {
		base := t.TempDir()
		ms := NewMetadataService(filepath.Join(base, "metadata"))
		metaPath := writeCleanupMetadata(t, ms, "movie.mkv", "", "")
		physicalRoot := filepath.Join(base, "library")
		physicalPath := filepath.Join(physicalRoot, "movie.mkv")
		writeCleanupFile(t, filepath.Join(physicalPath, "child"))

		err := ms.DeleteCorruptedFile(context.Background(), "movie.mkv", false, physicalPath, physicalRoot)
		assert.Error(t, err)
		require.FileExists(t, metaPath, "invalid target type must fail during preflight")
		require.DirExists(t, physicalPath)
	})

	t.Run("metadata sidecar cannot be removed", func(t *testing.T) {
		ms := NewMetadataService(t.TempDir())
		metaPath := writeCleanupMetadata(t, ms, "movie.mkv", "", "")
		writeCleanupFile(t, filepath.Join(metaPath+".id", "child"))

		err := ms.DeleteFileMetadataWithSourceNzb(context.Background(), "movie.mkv", false)
		assert.Error(t, err)
		require.FileExists(t, metaPath, "invalid sidecar type must fail during preflight")
		require.DirExists(t, metaPath+".id")
	})
}

func TestCleanupContainment_ConfiguredRootsRejectEscapesBeforeMutation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("containment contract uses Unix symlink semantics")
	}

	tests := []struct {
		name    string
		isStore bool
		target  func(t *testing.T, base, trustedRoot, outsideRoot string) string
	}{
		{
			name: "source lexical escape",
			target: func(t *testing.T, _, _, outsideRoot string) string {
				path := filepath.Join(outsideRoot, "victim.nzb")
				writeCleanupFile(t, path)
				return path
			},
		},
		{
			name: "source parent symlink escape",
			target: func(t *testing.T, _, trustedRoot, outsideRoot string) string {
				require.NoError(t, os.Symlink(outsideRoot, filepath.Join(trustedRoot, "linked")))
				path := filepath.Join(trustedRoot, "linked", "victim.nzb")
				writeCleanupFile(t, filepath.Join(outsideRoot, "victim.nzb"))
				return path
			},
		},
		{
			name:    "store lexical escape",
			isStore: true,
			target: func(t *testing.T, _, _, outsideRoot string) string {
				path := filepath.Join(outsideRoot, "victim.nzbz")
				writeCleanupFile(t, path)
				return path
			},
		},
		{
			name:    "store parent symlink escape",
			isStore: true,
			target: func(t *testing.T, _, trustedRoot, outsideRoot string) string {
				require.NoError(t, os.Symlink(outsideRoot, filepath.Join(trustedRoot, "linked")))
				path := filepath.Join(trustedRoot, "linked", "victim.nzbz")
				writeCleanupFile(t, filepath.Join(outsideRoot, "victim.nzbz"))
				return path
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := t.TempDir()
			metadataRoot := filepath.Join(base, "metadata")
			sourceRoot := filepath.Join(base, "sources")
			storeRoot := filepath.Join(base, "stores")
			outsideRoot := filepath.Join(base, "outside")
			require.NoError(t, os.MkdirAll(sourceRoot, 0o755))
			require.NoError(t, os.MkdirAll(storeRoot, 0o755))
			require.NoError(t, os.MkdirAll(outsideRoot, 0o755))

			ms := NewMetadataService(metadataRoot)
			require.NoError(t, configureCleanupRootsForTest(t, ms, storeRoot, sourceRoot))
			counter := &cleanupRefCounter{count: 0}
			ms.SetStoreRefCounter(counter)

			trustedRoot := sourceRoot
			if tt.isStore {
				trustedRoot = storeRoot
			}
			target := tt.target(t, base, trustedRoot, outsideRoot)
			sourcePath, storePath := target, ""
			deleteSource := true
			if tt.isStore {
				sourcePath, storePath = "", target
				deleteSource = false
			}
			metaPath := writeCleanupMetadata(t, ms, "movie.mkv", sourcePath, storePath)

			err := ms.DeleteFileMetadataWithSourceNzb(context.Background(), "movie.mkv", deleteSource)
			assert.Error(t, err)
			require.FileExists(t, metaPath, "all cleanup targets must pass preflight before metadata changes")
			require.FileExists(t, target)
			require.Empty(t, counter.calls, "preflight failure must not mutate reference counts")
		})
	}
}

func TestCleanupContainment_ConfiguredContainedTargetsDelete(t *testing.T) {
	base := t.TempDir()
	metadataRoot := filepath.Join(base, "metadata")
	sourceRoot := filepath.Join(base, "sources")
	storeRoot := filepath.Join(base, "stores")
	physicalRoot := filepath.Join(base, "library")
	for _, root := range []string{sourceRoot, storeRoot, physicalRoot} {
		require.NoError(t, os.MkdirAll(root, 0o755))
	}

	ms := NewMetadataService(metadataRoot)
	require.NoError(t, configureCleanupRootsForTest(t, ms, storeRoot, sourceRoot))
	counter := &cleanupRefCounter{count: 0}
	ms.SetStoreRefCounter(counter)

	sourcePath := filepath.Join(sourceRoot, "incoming", "movie.nzb")
	storePath := filepath.Join(storeRoot, "releases", "movie.nzbz")
	physicalPath := filepath.Join(physicalRoot, "movies", "movie.mkv")
	for _, path := range []string{sourcePath, storePath, physicalPath} {
		writeCleanupFile(t, path)
	}
	metaPath := writeCleanupMetadata(t, ms, filepath.Join("movies", "movie.mkv"), sourcePath, storePath)

	err := ms.DeleteCorruptedFile(
		context.Background(),
		filepath.Join("movies", "movie.mkv"),
		true,
		physicalPath,
		physicalRoot,
	)
	require.NoError(t, err)
	for _, path := range []string{metaPath, sourcePath, storePath, physicalPath} {
		require.NoFileExists(t, path)
	}
	require.Equal(t, []string{storePath}, counter.calls)
	require.DirExists(t, metadataRoot)
	require.DirExists(t, sourceRoot)
	require.DirExists(t, storeRoot)
	require.DirExists(t, physicalRoot, "pruning must stop at the trusted root")
}

func TestCleanupContainment_MissingContainedTargetsAreIdempotent(t *testing.T) {
	base := t.TempDir()
	metadataRoot := filepath.Join(base, "metadata")
	sourceRoot := filepath.Join(base, "sources")
	storeRoot := filepath.Join(base, "stores")
	physicalRoot := filepath.Join(base, "library")
	for _, root := range []string{sourceRoot, storeRoot, physicalRoot} {
		require.NoError(t, os.MkdirAll(root, 0o755))
	}

	ms := NewMetadataService(metadataRoot)
	require.NoError(t, configureCleanupRootsForTest(t, ms, storeRoot, sourceRoot))
	counter := &cleanupRefCounter{count: 0}
	ms.SetStoreRefCounter(counter)
	sourcePath := filepath.Join(sourceRoot, "missing.nzb")
	storePath := filepath.Join(storeRoot, "missing.nzbz")
	physicalPath := filepath.Join(physicalRoot, "missing.mkv")
	writeCleanupMetadata(t, ms, "movie.mkv", sourcePath, storePath)

	require.NoError(t, ms.DeleteCorruptedFile(
		context.Background(), "movie.mkv", true, physicalPath, physicalRoot,
	))
	require.Equal(t, []string{storePath}, counter.calls)
	require.NoError(t, ms.DeleteCorruptedFile(
		context.Background(), "movie.mkv", true, physicalPath, physicalRoot,
	))
	require.Equal(t, []string{storePath}, counter.calls, "missing metadata has no reference to decrement")
}

func TestCleanupContainment_StoreReferenceOwnership(t *testing.T) {
	counterErr := errors.New("reference database unavailable")
	tests := []struct {
		name          string
		counter       *cleanupRefCounter
		sourceIsStore bool
		wantErr       error
		wantStore     bool
		wantCalls     int
	}{
		{name: "nil counter retains store", wantStore: true},
		{name: "counter error retains store", counter: &cleanupRefCounter{err: counterErr}, wantErr: counterErr, wantStore: true, wantCalls: 1},
		{name: "nonzero count retains store", counter: &cleanupRefCounter{count: 2}, wantStore: true, wantCalls: 1},
		{name: "zero count deletes store", counter: &cleanupRefCounter{count: 0}, wantCalls: 1},
		{name: "deduplicated nil counter retains store", sourceIsStore: true, wantStore: true},
		{name: "deduplicated nonzero count retains store", counter: &cleanupRefCounter{count: 1}, sourceIsStore: true, wantStore: true, wantCalls: 1},
		{name: "deduplicated zero count deletes store", counter: &cleanupRefCounter{count: 0}, sourceIsStore: true, wantCalls: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := t.TempDir()
			metadataRoot := filepath.Join(base, "metadata")
			sourceRoot := filepath.Join(base, "sources")
			storeRoot := filepath.Join(base, "stores")
			require.NoError(t, os.MkdirAll(sourceRoot, 0o755))
			require.NoError(t, os.MkdirAll(storeRoot, 0o755))
			ms := NewMetadataService(metadataRoot)
			require.NoError(t, configureCleanupRootsForTest(t, ms, storeRoot, sourceRoot, storeRoot))
			if tt.counter != nil {
				ms.SetStoreRefCounter(tt.counter)
			}

			storePath := filepath.Join(storeRoot, "movie.nzbz")
			writeCleanupFile(t, storePath)
			sourcePath := ""
			deleteSource := false
			if tt.sourceIsStore {
				sourcePath = storePath
				deleteSource = true
			}
			metaPath := writeCleanupMetadata(t, ms, "movie.mkv", sourcePath, storePath)

			err := ms.DeleteFileMetadataWithSourceNzb(context.Background(), "movie.mkv", deleteSource)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			if tt.wantStore {
				require.FileExists(t, storePath)
			} else {
				require.NoFileExists(t, storePath)
			}
			if tt.counter != nil {
				require.Len(t, tt.counter.calls, tt.wantCalls)
			}
			if tt.wantErr == nil {
				require.NoFileExists(t, metaPath)
			}
		})
	}
}

func TestCleanupContainment_ConfiguredRootTypeAmbiguityFailsClosed(t *testing.T) {
	tests := []struct {
		name      string
		storeRoot bool
	}{
		{name: "source root is a file"},
		{name: "store root is a file", storeRoot: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := t.TempDir()
			metadataRoot := filepath.Join(base, "metadata")
			sourceRoot := filepath.Join(base, "sources")
			storeRoot := filepath.Join(base, "stores")
			require.NoError(t, os.MkdirAll(sourceRoot, 0o755))
			require.NoError(t, os.MkdirAll(storeRoot, 0o755))

			badRoot := filepath.Join(base, "not-a-directory")
			writeCleanupFile(t, badRoot)
			sourcePath, storePath := filepath.Join(badRoot, "victim.nzb"), ""
			if tt.storeRoot {
				sourcePath, storePath = "", filepath.Join(badRoot, "victim.nzbz")
				storeRoot = badRoot
			} else {
				sourceRoot = badRoot
			}

			ms := NewMetadataService(metadataRoot)
			counter := &cleanupRefCounter{count: 0}
			ms.SetStoreRefCounter(counter)
			metaPath := writeCleanupMetadata(t, ms, "movie.mkv", sourcePath, storePath)

			err := configureCleanupRootsForTest(t, ms, storeRoot, sourceRoot)
			if err == nil {
				err = ms.DeleteFileMetadataWithSourceNzb(context.Background(), "movie.mkv", sourcePath != "")
			}
			assert.Error(t, err)
			require.FileExists(t, metaPath)
			require.FileExists(t, badRoot)
			require.Empty(t, counter.calls)
		})
	}
}

func TestCleanupContainment_DeleteDirectoryRejectsEscapes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("containment contract uses Unix symlink semantics")
	}

	tests := []struct {
		name        string
		virtualPath func(t *testing.T, metadataRoot, outsideRoot string) string
	}{
		{
			name: "lexical traversal",
			virtualPath: func(_ *testing.T, _, _ string) string {
				return filepath.Join("..", "outside", "victim")
			},
		},
		{
			name: "parent symlink",
			virtualPath: func(t *testing.T, metadataRoot, outsideRoot string) string {
				require.NoError(t, os.Symlink(outsideRoot, filepath.Join(metadataRoot, "linked")))
				return filepath.Join("linked", "victim")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := t.TempDir()
			metadataRoot := filepath.Join(base, "metadata")
			outsideRoot := filepath.Join(base, "outside")
			require.NoError(t, os.MkdirAll(metadataRoot, 0o755))
			protected := filepath.Join(outsideRoot, "victim", "keep.meta")
			writeCleanupFile(t, protected)
			ms := NewMetadataService(metadataRoot)

			err := ms.DeleteDirectory(tt.virtualPath(t, metadataRoot, outsideRoot))
			assert.Error(t, err)
			require.FileExists(t, protected)
		})
	}
}

func TestCleanupContainment_DeleteDirectoryPreflightsStoreRefs(t *testing.T) {
	base := t.TempDir()
	metadataRoot := filepath.Join(base, "metadata")
	ms := NewMetadataService(metadataRoot)
	storePath := filepath.Join(base, "outside", "victim.nzbz")
	writeCleanupFile(t, storePath)
	metaPath := writeCleanupMetadata(t, ms, filepath.Join("movies", "movie.mkv"), "", storePath)
	counter := &cleanupRefCounter{count: 0}
	ms.SetStoreRefCounter(counter)

	err := ms.DeleteDirectory("movies")
	assert.Error(t, err)
	assert.FileExists(t, metaPath, "an unsafe child must preserve the complete directory")
	assert.FileExists(t, storePath)
	assert.Empty(t, counter.calls, "directory preflight must finish before reference-count mutation")
}

func TestCleanupContainment_DeleteContainedDirectoryRemainsCompatible(t *testing.T) {
	ms := NewMetadataService(t.TempDir())
	metaPath := writeCleanupMetadata(t, ms, filepath.Join("movies", "movie.mkv"), "", "")

	require.NoError(t, ms.DeleteDirectory("movies"))
	require.NoFileExists(t, metaPath)
}
