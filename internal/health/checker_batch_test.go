package health

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/javi11/altmount/internal/database"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/javi11/altmount/internal/pool"
	"github.com/javi11/altmount/internal/testsupport/fakepool"
	"github.com/javi11/altmount/internal/usenet"
	"github.com/javi11/nntppool/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeClientPoolManager is a pool.Manager backed by a fakepool client, for
// batch checks that need STATs to actually succeed or miss deterministically.
type fakeClientPoolManager struct {
	mockPoolManager
	client pool.NntpClient
}

func (m *fakeClientPoolManager) GetPool() (pool.NntpClient, error) { return m.client, nil }
func (m *fakeClientPoolManager) HasPool() bool                     { return true }

// newBatchTestEnv builds a repair test env whose checker and worker use a
// fakepool-backed pool manager instead of the always-failing mock.
func newBatchTestEnv(t *testing.T, tempDir string, client pool.NntpClient) *repairTestEnv {
	t.Helper()
	env := newRepairTestEnv(t, tempDir, nil)

	pm := &fakeClientPoolManager{client: client}
	env.healthChecker = NewHealthChecker(
		env.healthRepo,
		env.metadataService,
		pm,
		env.hw.configGetter,
		&MockRcloneClient{},
	)
	env.hw = NewHealthWorker(
		env.healthChecker,
		env.healthRepo,
		env.metadataService,
		env.mockARRs,
		&mockImportService{},
		env.hw.configGetter,
		nil,
	)
	return env
}

// writeHealthyFile writes valid single-segment metadata for filePath with a
// segment ID unique to that path (validSegmentMeta hardcodes one shared ID,
// which would alias behaviors across files) and returns the segment's ID.
func writeHealthyFile(t *testing.T, env *repairTestEnv, filePath string) string {
	t.Helper()
	const fileSize = int64(1024)
	seg := &metapb.SegmentData{
		Id:          fmt.Sprintf("seg-%s@test.example.com", filePath),
		SegmentSize: fileSize,
		StartOffset: 0,
		EndOffset:   fileSize - 1,
	}
	meta := env.metadataService.CreateFileMetadata(
		fileSize, "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		[]*metapb.SegmentData{seg},
		metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
	)
	require.NoError(t, env.metadataService.WriteFileMetadata(filePath, meta))
	return seg.Id
}

func writeSegmentMetadata(t *testing.T, env *repairTestEnv, filePath string, fileSize int64, segments []*metapb.SegmentData) {
	t.Helper()
	meta := env.metadataService.CreateFileMetadata(
		fileSize, "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		segments, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
	)
	require.NoError(t, env.metadataService.WriteFileMetadata(filePath, meta))
}

func TestCheckFilesBatch(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}

	t.Run("all healthy", func(t *testing.T) {
		client := fakepool.New()
		env := newBatchTestEnv(t, t.TempDir(), client)

		paths := []string{"complete/a.mkv", "complete/b.mkv", "complete/c.mkv"}
		for _, p := range paths {
			writeHealthyFile(t, env, p)
		}

		events := env.healthChecker.CheckFilesBatch(context.Background(), paths)
		require.Len(t, events, 3)
		for i, ev := range events {
			assert.Equal(t, EventTypeFileHealthy, ev.Type, "file %d", i)
			assert.Equal(t, paths[i], ev.FilePath, "file %d", i)
		}
		assert.Equal(t, int64(3), client.StatCalls())
	})

	t.Run("one broken file reports missing segments", func(t *testing.T) {
		client := fakepool.New()
		env := newBatchTestEnv(t, t.TempDir(), client)

		paths := []string{"complete/good.mkv", "complete/broken.mkv"}
		writeHealthyFile(t, env, paths[0])
		brokenID := writeHealthyFile(t, env, paths[1])
		client.SetBehavior(brokenID, fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

		events := env.healthChecker.CheckFilesBatch(context.Background(), paths)
		require.Len(t, events, 2)
		assert.Equal(t, EventTypeFileHealthy, events[0].Type)
		assert.Equal(t, EventTypeFileCorrupted, events[1].Type)
		require.Error(t, events[1].Error)
		assert.Contains(t, events[1].Error.Error(), "1 of 1 checked segments")
	})

	t.Run("metadata-missing file removed, siblings still checked", func(t *testing.T) {
		client := fakepool.New()
		env := newBatchTestEnv(t, t.TempDir(), client)

		paths := []string{"complete/a.mkv", "complete/gone.mkv", "complete/c.mkv"}
		writeHealthyFile(t, env, paths[0])
		writeHealthyFile(t, env, paths[2])
		insertFileHealth(t, env.db, paths[1], "", 0, 3)

		events := env.healthChecker.CheckFilesBatch(context.Background(), paths)
		require.Len(t, events, 3)
		assert.Equal(t, EventTypeFileHealthy, events[0].Type)
		assert.Equal(t, EventTypeFileRemoved, events[1].Type)
		assert.Equal(t, EventTypeFileHealthy, events[2].Type)
		assert.Equal(t, int64(2), client.StatCalls(), "removed file must not be statted")

		// Missing metadata is evidence only; the checker cannot delete database authority.
		fh, err := env.healthRepo.GetFileHealth(context.Background(), paths[1])
		require.NoError(t, err)
		require.NotNil(t, fh)
		assert.Equal(t, database.HealthStatusPending, fh.Status)
	})

	t.Run("pool down fails all non-early files", func(t *testing.T) {
		env := newRepairTestEnv(t, t.TempDir(), nil) // mockPoolManager: GetPool errors
		env.healthChecker = NewHealthChecker(
			env.healthRepo,
			env.metadataService,
			&mockPoolManager{},
			env.hw.configGetter,
			&MockRcloneClient{},
		)

		paths := []string{"complete/a.mkv", "complete/b.mkv"}
		for _, p := range paths {
			writeHealthyFile(t, env, p)
		}

		events := env.healthChecker.CheckFilesBatch(context.Background(), paths)
		require.Len(t, events, 2)
		for i, ev := range events {
			assert.Equal(t, EventTypeCheckFailed, ev.Type, "file %d", i)
			require.Error(t, ev.Error, "file %d", i)
			assert.Contains(t, ev.Error.Error(), "failed to validate segments", "file %d", i)
		}
	})

	t.Run("empty input", func(t *testing.T) {
		client := fakepool.New()
		env := newBatchTestEnv(t, t.TempDir(), client)
		assert.Nil(t, env.healthChecker.CheckFilesBatch(context.Background(), nil))
	})
}

func TestCheckFileRejectsMalformedSegmentsBeforeSTAT(t *testing.T) {
	tests := []struct {
		name     string
		fileSize int64
		segment  *metapb.SegmentData
	}{
		{
			name:     "nil segment",
			fileSize: 1,
			segment:  nil,
		},
		{
			name:     "empty message ID",
			fileSize: 1,
			segment:  &metapb.SegmentData{StartOffset: 0, EndOffset: 0, SegmentSize: 1},
		},
		{
			name:     "negative start offset",
			fileSize: 2,
			segment:  &metapb.SegmentData{Id: "negative@test", StartOffset: -1, EndOffset: 0, SegmentSize: 2},
		},
		{
			name:     "start after end",
			fileSize: 1,
			segment:  &metapb.SegmentData{Id: "reversed@test", StartOffset: 1, EndOffset: 0, SegmentSize: 2},
		},
		{
			name:     "zero physical size",
			fileSize: 1,
			segment:  &metapb.SegmentData{Id: "zero@test", StartOffset: 0, EndOffset: 0, SegmentSize: 0},
		},
		{
			name:     "negative physical size",
			fileSize: 1,
			segment:  &metapb.SegmentData{Id: "negative-size@test", StartOffset: 0, EndOffset: 0, SegmentSize: -1},
		},
		{
			name:     "end outside physical segment",
			fileSize: 2,
			segment:  &metapb.SegmentData{Id: "outside@test", StartOffset: 0, EndOffset: 1, SegmentSize: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := fakepool.New()
			env := newBatchTestEnv(t, t.TempDir(), client)
			path := "complete/invalid.mkv"
			writeSegmentMetadata(t, env, path, tt.fileSize, []*metapb.SegmentData{tt.segment})

			event := env.healthChecker.CheckFile(context.Background(), path)
			assert.Equal(t, EventTypeFileCorrupted, event.Type)
			require.Error(t, event.Error)
			assert.Contains(t, event.Error.Error(), "metadata corruption")
			assert.Zero(t, client.StatCalls(), "malformed metadata must fail before network I/O")
		})
	}
}

func TestCheckFilesBatchMalformedFileDoesNotPoisonHealthySibling(t *testing.T) {
	client := fakepool.New()
	env := newBatchTestEnv(t, t.TempDir(), client)
	paths := []string{"complete/invalid.mkv", "complete/healthy.mkv"}
	writeSegmentMetadata(t, env, paths[0], 2, []*metapb.SegmentData{{
		Id: "outside@test", StartOffset: 0, EndOffset: 1, SegmentSize: 1,
	}})
	writeHealthyFile(t, env, paths[1])

	events := env.healthChecker.CheckFilesBatch(context.Background(), paths)
	require.Len(t, events, 2)
	assert.Equal(t, EventTypeFileCorrupted, events[0].Type)
	assert.Equal(t, EventTypeFileHealthy, events[1].Type)
	assert.Equal(t, int64(1), client.StatCalls(), "only the structurally valid sibling may reach STAT")
}

// TestCheckFile_ParityWithBatch guards the manual-check path: the single-file
// entry point must produce the same verdicts as the batch path.
func TestCheckFile_ParityWithBatch(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}

	t.Run("healthy", func(t *testing.T) {
		client := fakepool.New()
		env := newBatchTestEnv(t, t.TempDir(), client)
		writeHealthyFile(t, env, "complete/solo.mkv")

		event := env.healthChecker.CheckFile(context.Background(), "complete/solo.mkv")
		assert.Equal(t, EventTypeFileHealthy, event.Type)
	})

	t.Run("missing segment", func(t *testing.T) {
		client := fakepool.New()
		env := newBatchTestEnv(t, t.TempDir(), client)
		id := writeHealthyFile(t, env, "complete/solo.mkv")
		client.SetBehavior(id, fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

		event := env.healthChecker.CheckFile(context.Background(), "complete/solo.mkv")
		assert.Equal(t, EventTypeFileCorrupted, event.Type)
		require.Error(t, event.Error)
		assert.Contains(t, event.Error.Error(), "1 of 1 checked segments")
	})

	t.Run("metadata missing", func(t *testing.T) {
		client := fakepool.New()
		env := newBatchTestEnv(t, t.TempDir(), client)

		event := env.healthChecker.CheckFile(context.Background(), "complete/never-written.mkv")
		assert.Equal(t, EventTypeFileRemoved, event.Type)
	})
}

func TestPR3BatchCancellationDoesNotPublishCompletedSiblingVerdict(t *testing.T) {
	hc := &HealthChecker{}
	prep := preparedCheck{filePath: "complete/sibling.mkv", totalSegments: 1}
	result := usenet.ValidationResult{TotalExpected: 1, TotalChecked: 1}
	aggregate := &usenet.IncompleteError{
		Expected:  2,
		Completed: 1,
		Cause:     context.Canceled,
	}

	event := hc.judgeValidation(context.Background(), prep, result, aggregate)
	assert.Equal(t, EventTypeCheckFailed, event.Type,
		"a canceled batch cannot publish a healthy sibling verdict")
	assert.Equal(t, database.HealthStatusPending, event.Status)
}

func TestPR3PerFileTemporaryOutcomeDoesNotPoisonCompletedSibling(t *testing.T) {
	hc := &HealthChecker{}
	prep := preparedCheck{filePath: "complete/sibling.mkv", totalSegments: 1}
	result := usenet.ValidationResult{TotalExpected: 1, TotalChecked: 1}
	aggregate := &usenet.IncompleteError{
		Expected:  2,
		Completed: 1,
		Cause: &nntppool.TransportError{
			Kind:  nntppool.OutcomeTemporaryFailure,
			Cause: errors.New("synthetic sibling failure"),
		},
	}

	event := hc.judgeValidation(context.Background(), prep, result, aggregate)
	assert.Equal(t, EventTypeFileHealthy, event.Type,
		"one file's attributed temporary result must not erase a completed sibling")
}

// TestRunHealthCheckCycle_BatchExceedsMaxJobs verifies one cycle processes far
// more due files than max_concurrent_jobs: the batch fetch is decoupled from
// job concurrency, so 10 due files complete in a single cycle at maxJobs=1.
func TestRunHealthCheckCycle_BatchExceedsMaxJobs(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on Windows")
	}

	client := fakepool.New()
	env := newBatchTestEnv(t, t.TempDir(), client)

	const files = 10
	for i := range files {
		path := fmt.Sprintf("complete/file-%02d.mkv", i)
		writeHealthyFile(t, env, path)
		insertFileHealth(t, env.db, path, "", 0, 3)
	}

	require.NoError(t, env.hw.runHealthCheckCycle(context.Background()))

	// Every file's segment was statted in this single cycle (maxJobs is 1).
	assert.Equal(t, int64(files), client.StatCalls())

	// No file is left due: healthy records are resolved, none stuck 'checking'.
	var stuck int
	require.NoError(t, env.db.QueryRow(
		`SELECT COUNT(*) FROM file_health WHERE status IN ('checking', 'pending')`,
	).Scan(&stuck))
	assert.Equal(t, 0, stuck, "no files should remain due after one cycle")
}
