package importer

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/importer/parser"
	"github.com/javi11/altmount/internal/importer/singlefile"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/javi11/altmount/internal/testsupport/fakepool"
	"github.com/javi11/altmount/internal/testsupport/nzbbuild"
)

func chg017MetadataFiles(t *testing.T, root string) []string {
	t.Helper()
	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !info.IsDir() && strings.HasSuffix(path, ".meta") {
			files = append(files, path)
		}
		return nil
	})
	if os.IsNotExist(err) {
		return nil
	}
	require.NoError(t, err)
	return files
}

func TestOrdinaryQueueImportFailsClosedWhenStorePreparationFails(t *testing.T) {
	tests := []struct {
		name     string
		obstruct func(*testing.T, *fbaseFinalizationEnv, int64)
	}{
		{
			name: "store directory creation",
			obstruct: func(t *testing.T, env *fbaseFinalizationEnv, _ int64) {
				require.NoError(t, os.WriteFile(env.storeRoot, []byte("not a directory"), 0o600))
			},
		},
		{
			name: "store atomic write",
			obstruct: func(t *testing.T, env *fbaseFinalizationEnv, itemID int64) {
				require.NoError(t, os.MkdirAll(env.storeRoot, 0o755))
				storeTarget := filepath.Join(env.storeRoot, fmt.Sprintf("%d-ordinary-store-required.nzbz", itemID))
				require.NoError(t, os.Mkdir(storeTarget, 0o755))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			env := newFbaseFinalizationEnv(t)
			client := fakepool.New()
			payload := bytes.Repeat([]byte("A"), 1024)
			client.SetBehavior("chg017-store@example", fakepool.SegmentBehavior{Bytes: payload})
			nzb := nzbbuild.Build(nzbbuild.File{
				Subject:  "Ordinary.Store.Required.mkv",
				Segments: []nzbbuild.Segment{{ID: "chg017-store@example", Bytes: len(payload)}},
			})
			source := nzbbuild.WriteTemp(t, nzb, "ordinary-store-required")
			item := &database.ImportQueueItem{
				NzbPath:             source,
				Priority:            database.QueuePriorityNormal,
				Status:              database.QueueStatusProcessing,
				MaxRetries:          3,
				SkipArrNotification: true,
				SkipPostImportLinks: true,
				CreatedAt:           time.Now(),
			}
			require.NoError(t, env.service.AddQueueItem(ctx, item))
			require.NotZero(t, item.ID)
			require.NoError(t, env.database.Repository.UpdateQueueItemStatus(ctx, item.ID, database.QueueStatusProcessing, nil))
			env.service.processor = NewProcessor(
				env.service.metadataService,
				processorTestPoolManager{client: client},
				nil,
				env.service.configGetter,
				nil,
			)
			tt.obstruct(t, env, item.ID)

			result, processErr := env.service.ProcessItem(ctx, item)
			finalized := false
			var finalizationErr error
			if processErr == nil {
				finalized = true
				finalizationErr = env.service.HandleSuccess(ctx, item, result)
			}

			require.Error(t, processErr, "ordinary imports must not fall back to source-dependent v1 metadata")
			assert.Contains(t, strings.ToLower(processErr.Error()), "store")
			assert.False(t, finalized, "store admission failure must not enter success finalization")
			if finalized {
				assert.NoError(t, finalizationErr)
			}
			assert.Empty(t, chg017MetadataFiles(t, env.config.Metadata.RootPath))
			assert.FileExists(t, item.NzbPath, "the rooted queue source must remain available after admission failure")

			stored, err := env.database.Repository.GetQueueItem(ctx, item.ID)
			require.NoError(t, err)
			require.NotNil(t, stored)
			assert.Equal(t, database.QueueStatusProcessing, stored.Status)
		})
	}
}

func TestV3ConversionFailureCannotPublishV1OrFinalizeQueueItem(t *testing.T) {
	ctx := context.Background()
	env := newFbaseFinalizationEnv(t)
	require.NoError(t, os.MkdirAll(env.queueRoot, 0o755))
	source := filepath.Join(env.queueRoot, "v3-mismatch.nzb")
	require.NoError(t, os.WriteFile(source, []byte("<nzb/>"), 0o600))
	item := env.addProcessingItem(t, source)

	storeRef := filepath.Join(env.storeRoot, "v3-mismatch.nzbz")
	require.NoError(t, env.service.metadataService.Store().WriteStore(storeRef, &metapb.NzbStore{
		Files: []*metapb.NzbFileEntry{{
			Segments: []*metapb.NzbSeg{{Id: "stored@example", Number: 1, Bytes: 100}},
		}},
	}))
	file := parser.ParsedFile{
		Filename: "movie.mkv",
		Size:     100,
		Segments: []*metapb.SegmentData{{
			Id:          "missing@example",
			SegmentSize: 100,
			StartOffset: 0,
			EndOffset:   99,
		}},
		ReleaseDate: time.Unix(1, 0),
	}

	result, writtenPath, processErr := singlefile.ProcessSingleFile(
		ctx,
		"/complete",
		file,
		nil,
		item.NzbPath,
		env.service.metadataService,
		[]string{".mkv"},
		false,
		map[string]int64{"stored@example": 0},
		storeRef,
	)
	finalized := false
	var finalizationErr error
	if processErr == nil {
		finalized = true
		env.service.writtenPathsCache.Store(item.ID, []string{writtenPath})
		finalizationErr = env.service.HandleSuccess(ctx, item, result)
	}

	require.Error(t, processErr, "a nonempty store reference must not fall back to v1 metadata")
	assert.Contains(t, strings.ToLower(processErr.Error()), "main segments")
	assert.False(t, finalized, "conversion failure must not enter success finalization")
	if finalized {
		assert.NoError(t, finalizationErr)
	}
	assert.NoFileExists(t, env.service.metadataService.GetMetadataFilePath("/complete/movie.mkv"))
	assert.FileExists(t, item.NzbPath, "the rooted queue source must remain available after conversion failure")

	stored, err := env.database.Repository.GetQueueItem(ctx, item.ID)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, database.QueueStatusProcessing, stored.Status)
}

func TestSTRMImportStillWritesInlineMetadataWithoutNzbStore(t *testing.T) {
	env := newBatteryEnv(t)
	require.NoError(t, os.WriteFile(filepath.Join(env.configDir, ".nzbs"), []byte("not a directory"), 0o600))
	strmPath := filepath.Join(t.TempDir(), "compat.strm")
	const link = "nxglnk://?h=TzlIY1lxNVFNQ0MyOXE6NjYyMzow&chunk_size=1048576&file_size=6944718848&name=test_file.mkv"
	require.NoError(t, os.WriteFile(strmPath, []byte(link), 0o600))

	_, written, err := env.proc.ProcessNzbFile(
		context.Background(),
		strmPath,
		filepath.Dir(strmPath),
		1,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)
	paths := filePaths(written)
	require.Len(t, paths, 1)
	meta := env.readMeta(paths[0])
	assert.Empty(t, meta.StoreRef)
	assert.NotEmpty(t, meta.SegmentData)
}
