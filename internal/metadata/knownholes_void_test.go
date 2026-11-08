package metadata

import (
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/holes"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A Void layout is valid only for the exact run it was computed for: a merge
// that changes a run's extent must drop it (it would point into the middle of
// the merged hole), while untouched runs and a re-stamp of the same extent
// keep theirs.
func TestMergeStampedDropsVoidLayoutWhenTheRunChanges(t *testing.T) {
	now := time.Now()
	old := now.Add(-2 * time.Hour).Unix()
	stored := []*metapb.HoleRun{
		{StartSegment: 5, Count: 1, RecordedAt: old, VoidOffset: 5000, VoidSize: 700},
		{StartSegment: 20, Count: 2, RecordedAt: old, VoidOffset: 21000, VoidSize: 900},
		{StartSegment: 40, Count: 1, RecordedAt: old, VoidOffset: -1}, // computed: zeros
		{StartSegment: 60, Count: 1, RecordedAt: old, VoidOffset: 61000, VoidSize: 10},
	}
	fresh := []holes.Run{
		{Start: 6, Count: 1},  // grows 5 into (5,2)
		{Start: 20, Count: 2}, // re-confirms (20,2) exactly
		{Start: 59, Count: 1}, // grows 60 into (59,2) from the left
	}
	out := mergeStamped(stored, fresh, now)
	byStart := map[int64]*metapb.HoleRun{}
	for _, r := range out {
		byStart[r.StartSegment] = r
	}

	require.Contains(t, byStart, int64(5))
	assert.EqualValues(t, 2, byStart[5].Count)
	assert.Zero(t, byStart[5].VoidOffset, "merged run (5,2) must not inherit the layout of (5,1)")
	assert.Zero(t, byStart[5].VoidSize)

	require.Contains(t, byStart, int64(59))
	assert.Zero(t, byStart[59].VoidOffset, "a run grown on the left loses its layout too")

	assert.EqualValues(t, 21000, byStart[20].VoidOffset, "same extent, same bytes: the layout stays")
	assert.EqualValues(t, 900, byStart[20].VoidSize)
	assert.Equal(t, now.Unix(), byStart[20].RecordedAt, "the re-confirmed run is re-stamped")

	assert.EqualValues(t, -1, byStart[40].VoidOffset, "an untouched run keeps its computed answer")
	assert.EqualValues(t, old, byStart[40].RecordedAt)
}

func TestAddKnownHolesUnderAnotherProviderSetDropsLayouts(t *testing.T) {
	ms := NewMetadataService(t.TempDir())
	path := "movies/a.mkv"
	writeHoleFile(t, ms, path, []*metapb.HoleRun{{StartSegment: 5, Count: 1, RecordedAt: time.Now().Unix(), VoidOffset: 5000, VoidSize: 10}}, "fp-a")
	require.NoError(t, ms.AddKnownHoles(path, []holes.Run{{Start: 5, Count: 1}}, "fp-b"))
	m, err := ms.ReadFileMetadata(path)
	require.NoError(t, err)
	require.Len(t, m.KnownHoles, 1)
	assert.Zero(t, m.KnownHoles[0].VoidOffset)
}

// SetHoleLayout must write only onto a run that still has exactly the
// extent the layout was computed for.
func TestSetHoleLayoutOnlyOntoTheExactRun(t *testing.T) {
	ms := NewMetadataService(t.TempDir())
	path := "movies/b.mkv"
	writeHoleFile(t, ms, path, []*metapb.HoleRun{
		{StartSegment: 5, Count: 2, RecordedAt: time.Now().Unix()},
		{StartSegment: 9, Count: 1, RecordedAt: time.Now().Unix()},
	}, "")

	// Computed for (5,1), but the run has since merged into (5,2).
	require.NoError(t, ms.SetHoleLayout(path, holes.Run{Start: 5, Count: 1}, 5100, 77))
	require.NoError(t, ms.SetHoleLayout(path, holes.Run{Start: 9, Count: 1}, 9100, 88))

	m, err := ms.ReadFileMetadata(path)
	require.NoError(t, err)
	byStart := map[int64]*metapb.HoleRun{}
	for _, r := range m.KnownHoles {
		byStart[r.StartSegment] = r
	}
	assert.Zero(t, byStart[5].VoidOffset, "a layout for a stale extent must be discarded")
	assert.EqualValues(t, 9100, byStart[9].VoidOffset)
	assert.EqualValues(t, 88, byStart[9].VoidSize)
}

// Layout writes, hole merges and status writes are read-modify-writes of the
// same file: run concurrently, none may undo another (a layout write that read
// the run (5,1) before a merge grew it to (5,2) used to write (5,1) back).
func TestHoleWritesDoNotLoseConcurrentUpdates(t *testing.T) {
	ms := NewMetadataService(t.TempDir())
	path := "movies/c.mkv"
	for i := range 200 {
		writeHoleFile(t, ms, path, []*metapb.HoleRun{{StartSegment: 5, Count: 1, RecordedAt: time.Now().Unix()}}, "fp")
		start := make(chan struct{})
		errs := make(chan error, 3)
		go func() { <-start; errs <- ms.SetHoleLayout(path, holes.Run{Start: 5, Count: 1}, 5100, 600) }()
		go func() { <-start; errs <- ms.AddKnownHoles(path, []holes.Run{{Start: 6, Count: 1}}, "fp") }()
		go func() { <-start; errs <- ms.UpdateFileStatus(path, metapb.FileStatus_FILE_STATUS_CORRUPTED) }()
		close(start)
		for range 3 {
			require.NoError(t, <-errs)
		}
		m, err := ms.ReadFileMetadata(path)
		require.NoError(t, err)
		require.Len(t, m.KnownHoles, 1, "iteration %d", i)
		require.EqualValues(t, 2, m.KnownHoles[0].Count, "iteration %d: the merge was lost", i)
		require.Zero(t, m.KnownHoles[0].VoidOffset, "iteration %d: a layout for (5,1) landed on (5,2)", i)
		require.Equal(t, metapb.FileStatus_FILE_STATUS_CORRUPTED, m.Status, "iteration %d: the status write was lost", i)
	}
}

func writeHoleFile(t *testing.T, ms *MetadataService, path string, rows []*metapb.HoleRun, fp string) {
	t.Helper()
	segs := make([]*metapb.SegmentData, 20)
	for i := range segs {
		segs[i] = &metapb.SegmentData{Id: "seg", SegmentSize: 1000, StartOffset: 0, EndOffset: 999}
	}
	m := ms.CreateFileMetadata(20000, "x.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY, segs,
		metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "")
	m.KnownHoles = rows
	m.HoleProviderFingerprint = fp
	require.NoError(t, ms.WriteFileMetadata(path, m))
}
