package nzbfilesystem

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/javi11/nntppool/v5"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/holes"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/par2repair"
	"github.com/kipsilabs/altmount/internal/testsupport/fakepool"
	"github.com/kipsilabs/altmount/internal/testsupport/segments"
	"github.com/kipsilabs/altmount/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// acceptable_missing_test.go pins that playback honours
// health.acceptable_missing_segments_percentage, the ceiling import and health
// checks already apply: a miss the fixed padding caps would zero-fill still
// fails the stream, and the file goes to repair, once it takes the file past
// the configured percentage.

// acceptableMissingConfig is a health-enabled config with the given ceiling.
func acceptableMissingConfig(pct float64) *config.Config {
	enabled := true
	cfg := config.DefaultConfig()
	cfg.Health.Enabled = &enabled
	cfg.Health.AcceptableMissingSegmentsPercentage = pct
	cfg.MountPath = ""
	return cfg
}

// newImportedCeilingFile wires a hole-eligible, already imported file of 100
// segments to real health and metadata persistence, with the given segments
// gone on every provider. The streaming-failure handler runs for real, so a
// repair moves the metadata to the corrupted safety folder.
func newImportedCeilingFile(
	t *testing.T, filePath string, cfg *config.Config, missing ...int,
) (*MetadataVirtualFile, *database.HealthRepository, *metadata.MetadataService) {
	t.Helper()
	const (
		segCount = 100
		segSize  = 1024
	)

	repo, db, ms := setupStreamHealthEnv(t)
	_, err := db.Exec(
		`INSERT INTO file_health (file_path, library_path, status, scheduled_check_at, streaming_failure_count)
		 VALUES (?, ?, 'healthy', datetime('now'), 0)`,
		filePath, "/media/library/"+filePath,
	)
	require.NoError(t, err)
	writeMetaForTestFile(t, ms, filePath, segCount, segSize)

	fp := fakepool.New()
	configurePoolForFile(fp, segCount, segSize, fakepool.SegmentBehavior{})
	for _, i := range missing {
		fp.SetBehavior(segments.MessageID(i), fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
	}

	mvf := withConfig(newTestMVF(t, context.Background(), fp, segCount, segSize, 4), cfg)
	mvf.name = filePath
	mvf.healthRepository = repo
	mvf.metadataService = ms
	return mvf, repo, ms
}

// assertRepairTriggered checks the streaming-failure handler took the repair
// path: health row repair_triggered and metadata moved away.
func assertRepairTriggered(t *testing.T, repo *database.HealthRepository, ms *metadata.MetadataService, filePath string) {
	t.Helper()
	fh, err := repo.GetFileHealth(context.Background(), filePath)
	require.NoError(t, err)
	require.NotNil(t, fh)
	assert.Equal(t, database.HealthStatusRepairTriggered, fh.Status,
		"the streaming-failure handler must repair the file, not keep it as degraded")

	meta, err := ms.ReadFileMetadata(filePath)
	require.NoError(t, err)
	assert.Nil(t, meta, "repair must move the metadata to the corrupted safety folder")
}

// With a 0 % ceiling one confirmed miss fails the read instead of being
// zero-filled, and the streaming-failure handler repairs the file rather than
// keeping it as degraded.
func TestAcceptableMissingZero_FailsReadAndTriggersRepair(t *testing.T) {
	filePath := "movies/strict.mkv"
	// One article in a hundred is gone: well inside every fixed padding cap.
	mvf, repo, ms := newImportedCeilingFile(t, filePath, acceptableMissingConfig(0), 50)
	metaStore := &fakePadMetadataStore{}
	recorder := newPadRecorder(metaStore, &fakePadHealthStore{}, nil)
	mvf.padRecorder = recorder

	buf := make([]byte, mvf.meta.FileSize)
	_, readErr := mvf.ReadAtContext(context.Background(), buf, 0)
	require.Error(t, readErr, "a miss over the configured ceiling must fail the read, not be zero-filled")

	recorder.Close() // drains, so any write it was handed has happened
	metaStore.mu.Lock()
	assert.Empty(t, metaStore.knownHoles, "a failed miss must not be recorded as a padded hole")
	assert.Empty(t, metaStore.statusUpdates, "a failed miss must not mark the file degraded")
	metaStore.mu.Unlock()

	assertRepairTriggered(t, repo, ms, filePath)
}

// The handler judges a failed read by the ceiling that failed it. A config
// reload after the handle opened must not turn it into a degraded verdict
// that skips repair.
func TestAcceptableMissing_ConfigReloadKeepsTheCeilingThatFailedTheRead(t *testing.T) {
	filePath := "movies/reloaded.mkv"
	cfg := acceptableMissingConfig(0)
	mvf, repo, ms := newImportedCeilingFile(t, filePath, cfg, 50)
	mvf.holeHooks() // the handle opens at 0 %
	cfg.Health.AcceptableMissingSegmentsPercentage = 2

	buf := make([]byte, mvf.meta.FileSize)
	_, readErr := mvf.ReadAtContext(context.Background(), buf, 0)
	require.Error(t, readErr, "the miss is judged by the 0 % ceiling the handle opened with")

	assertRepairTriggered(t, repo, ms, filePath)
}

// gatedPadMetadata passes the pad recorder's writes through to the real
// metadata service, but holds its status write until released: the recorder
// has taken the pad and is about to mark the file degraded.
type gatedPadMetadata struct {
	*metadata.MetadataService
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *gatedPadMetadata) hold() {
	g.once.Do(func() { close(g.entered) })
	<-g.release
}

func (g *gatedPadMetadata) UpdateFileStatusUnlessCorrupted(virtualPath string, status metapb.FileStatus) error {
	g.hold()
	return g.MetadataService.UpdateFileStatusUnlessCorrupted(virtualPath, status)
}

// A pad that is recorded while a later miss fails the stream must not undo
// the failure: the health row stays repair_triggered, and metadata that was
// marked corrupted (a file not yet imported, so not moved) stays corrupted.
func TestAcceptableMissing_PadRecordedDuringRepairDoesNotUndoIt(t *testing.T) {
	for _, imported := range []bool{true, false} {
		name := "imported"
		if !imported {
			name = "not imported"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			filePath := "movies/late-pad.mkv"
			// At 1 % segment 10 pads (1 of 100) and segment 30 fails (2 of 100).
			mvf, repo, ms := newImportedCeilingFile(t, filePath, acceptableMissingConfig(1), 10, 30)
			if !imported {
				require.NoError(t, repo.UpdateLibraryPath(ctx, filePath, ""))
			}
			gate := &gatedPadMetadata{MetadataService: ms, entered: make(chan struct{}), release: make(chan struct{})}
			recorder := newPadRecorder(gate, repo, nil)
			mvf.padRecorder = recorder
			drain := sync.OnceFunc(func() {
				close(gate.release)
				recorder.Close()
			})
			t.Cleanup(drain)

			buf := make([]byte, 1024)
			_, err := mvf.ReadAtContext(ctx, buf, 10*1024)
			require.NoError(t, err, "segment 10 is within the 1 % ceiling")
			select {
			case <-gate.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("the pad was never recorded")
			}
			_, err = mvf.ReadAtContext(ctx, buf, 30*1024)
			require.Error(t, err, "segment 30 takes the file past the 1 % ceiling")
			drain() // the recorder now finishes its degraded writes

			fh, err := repo.GetFileHealth(ctx, filePath)
			require.NoError(t, err)
			require.NotNil(t, fh)
			assert.Equal(t, database.HealthStatusRepairTriggered, fh.Status,
				"a pad recorded during the failure must not mark the file degraded")
			meta, err := ms.ReadFileMetadata(filePath)
			require.NoError(t, err)
			if imported {
				assert.Nil(t, meta, "repair moves the metadata to the corrupted safety folder")
			} else {
				require.NotNil(t, meta)
				assert.Equal(t, metapb.FileStatus_FILE_STATUS_CORRUPTED, meta.Status,
					"a pad recorded during the failure must not reopen corrupted metadata")
			}
		})
	}
}

// Within the ceiling (and the caps) misses pad as before; one miss past it
// fails the read.
func TestAcceptableMissing_PadsWithinAndFailsPastTheCeiling(t *testing.T) {
	const (
		segCount = 100
		segSize  = 1024
	)

	tests := []struct {
		name     string
		pct      float64
		missing  []int
		wantFail bool
	}{
		{name: "2 percent, 2 of 100 missing pads", pct: 2, missing: []int{10, 30}},
		{name: "2 percent, 3 of 100 missing fails", pct: 2, missing: []int{10, 30, 50}, wantFail: true},
		{name: "1 percent, 2 of 100 missing fails", pct: 1, missing: []int{10, 30}, wantFail: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			fp := fakepool.New()
			configurePoolForFile(fp, segCount, segSize, fakepool.SegmentBehavior{})
			for _, i := range tt.missing {
				fp.SetBehavior(segments.MessageID(i), fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
			}

			mvf := withConfig(newTestMVF(t, ctx, fp, segCount, segSize, 4), acceptableMissingConfig(tt.pct))
			mvf.name = "movies/ceiling.mkv"

			buf := make([]byte, segCount*segSize)
			n, readErr := mvf.ReadAtContext(ctx, buf, 0)
			if tt.wantFail {
				require.Error(t, readErr, "a miss past the configured ceiling must fail the read")
				return
			}
			require.NoError(t, readErr)
			require.Equal(t, len(buf), n)
			for _, i := range tt.missing {
				assert.True(t, bytes.Equal(make([]byte, segSize), buf[i*segSize:(i+1)*segSize]),
					"segment %d must be zero-filled", i)
			}
		})
	}
}

// A stored hole is replayed without a fetch only while the file is within the
// ceiling. Past it the article is asked for again: a recovered article serves
// its real bytes, a still-missing one fails the read.
func TestAcceptableMissing_KnownHoleReplay(t *testing.T) {
	const segSize = 4096 // holeFile's segment size

	t.Run("0 percent, still missing, is fetched and fails", func(t *testing.T) {
		fp := fakepool.New()
		mvf := holeFile(t, fp, time.Now().Add(-time.Hour), "")
		cfg := acceptableMissingConfig(0)
		withConfig(mvf, cfg)
		mvf.meta.HoleProviderFingerprint = cfg.ProviderFingerprint()

		buf := make([]byte, mvf.meta.FileSize)
		_, readErr := mvf.ReadAtContext(context.Background(), buf, 0)
		require.Error(t, readErr, "a stored hole over the ceiling must not be zero-filled")
		assert.NotZero(t, fp.PerMessageCalls(segments.MessageID(2)),
			"a stored hole over the ceiling must be fetched again")
	})

	t.Run("0 percent, recovered, serves real bytes", func(t *testing.T) {
		fp := fakepool.New()
		mvf := holeFile(t, fp, time.Now().Add(-time.Hour), "")
		fp.SetBehavior(segments.MessageID(2), fakepool.SegmentBehavior{Bytes: segments.Payload(2, segSize)})
		cfg := acceptableMissingConfig(0)
		withConfig(mvf, cfg)
		mvf.meta.HoleProviderFingerprint = cfg.ProviderFingerprint()

		buf := make([]byte, mvf.meta.FileSize)
		_, readErr := mvf.ReadAtContext(context.Background(), buf, 0)
		require.NoError(t, readErr)
		assert.Equal(t, segments.Payload(2, segSize), buf[2*segSize:3*segSize],
			"a recovered article must serve its real bytes, not zeros")
	})

	t.Run("2 percent, within the ceiling, is replayed without a fetch", func(t *testing.T) {
		fp := fakepool.New()
		mvf := holeFile(t, fp, time.Now().Add(-time.Hour), "")
		cfg := acceptableMissingConfig(2)
		withConfig(mvf, cfg)
		mvf.meta.HoleProviderFingerprint = cfg.ProviderFingerprint()

		buf := make([]byte, mvf.meta.FileSize)
		_, readErr := mvf.ReadAtContext(context.Background(), buf, 0)
		require.NoError(t, readErr)
		assert.Equal(t, make([]byte, segSize), buf[2*segSize:3*segSize], "known hole must be zero-filled")
		assert.Zero(t, fp.PerMessageCalls(segments.MessageID(2)), "a hole within the ceiling must not be fetched")
	})
}

// Gap placeholders were judged at import and keep zero-filling whatever the
// ceiling.
func TestAcceptableMissingZero_PlaceholdersStillZeroFill(t *testing.T) {
	const (
		segCount = 100
		segSize  = 1024
	)
	ctx := context.Background()
	fp := fakepool.New()
	configurePoolForFile(fp, segCount, segSize, fakepool.SegmentBehavior{})

	mvf := withConfig(newTestMVF(t, ctx, fp, segCount, segSize, 4), acceptableMissingConfig(0))
	mvf.name = "movies/gappy.mkv"
	mvf.meta.SegmentData[5].Id = holes.PlaceholderID(6, segments.MessageID(0))

	buf := make([]byte, segCount*segSize)
	n, readErr := mvf.ReadAtContext(ctx, buf, 0)
	require.NoError(t, readErr, "a placeholder must not fail the read")
	require.Equal(t, len(buf), n)
	assert.Equal(t, make([]byte, segSize), buf[5*segSize:6*segSize], "a placeholder must be zero-filled")
}

// patchedHoleFile builds a 100-segment plain video handle at the given
// ceiling whose stored holes are fresh on the current provider set, are gone
// on the provider, and, for the patched ones, served by a PAR2 patch.
func patchedHoleFile(t *testing.T, fp *fakepool.Client, pct float64, stored, patched []int) *MetadataVirtualFile {
	t.Helper()
	const n, segSize = 100, 1024
	configurePoolForFile(fp, n, segSize, fakepool.SegmentBehavior{})
	cfg := acceptableMissingConfig(pct)
	mvf := withConfig(newTestMVF(t, context.Background(), fp, n, segSize, 4), cfg)
	mvf.name = "movies/patched.mkv"
	mvf.meta.HoleProviderFingerprint = cfg.ProviderFingerprint()
	for _, i := range stored {
		fp.SetBehavior(segments.MessageID(i), fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
		mvf.meta.KnownHoles = append(mvf.meta.KnownHoles,
			&metapb.HoleRun{StartSegment: int64(i), Count: 1, RecordedAt: time.Now().Add(-time.Hour).Unix()})
	}
	ps := &fakePatchSource{patches: map[string][]byte{}}
	for _, i := range patched {
		ps.patches[strings.Trim(segments.MessageID(i), "<>")] = segments.Payload(i, segSize)
	}
	mvf.patchSource = ps
	return mvf
}

// PAR2-patched stored holes are real data and do not count against the
// ceiling, as in health's classifyHoles.
func TestAcceptableMissing_PatchedHolesDoNotCount(t *testing.T) {
	const segSize = 1024

	t.Run("1 percent, one patched stored hole plus one new miss pads", func(t *testing.T) {
		fp := fakepool.New()
		mvf := patchedHoleFile(t, fp, 1, []int{20}, []int{20})
		fp.SetBehavior(segments.MessageID(50), fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})

		buf := make([]byte, mvf.meta.FileSize)
		_, readErr := mvf.ReadAtContext(context.Background(), buf, 0)
		require.NoError(t, readErr, "only the new miss counts: 1 of 100 is within 1 %")
		assert.Equal(t, segments.Payload(20, segSize), buf[20*segSize:21*segSize], "the patch serves the stored hole")
		assert.Equal(t, make([]byte, segSize), buf[50*segSize:51*segSize], "the new miss is zero-filled")
	})

	t.Run("2 percent, three stored holes with one patched still replays", func(t *testing.T) {
		fp := fakepool.New()
		mvf := patchedHoleFile(t, fp, 2, []int{10, 30, 50}, []int{30})

		buf := make([]byte, mvf.meta.FileSize)
		_, readErr := mvf.ReadAtContext(context.Background(), buf, 0)
		require.NoError(t, readErr, "2 unpatched of 100 is within 2 %")
		for _, i := range []int{10, 50} {
			assert.Equal(t, make([]byte, segSize), buf[i*segSize:(i+1)*segSize], "stored hole %d is zero-filled", i)
			assert.Zero(t, fp.PerMessageCalls(segments.MessageID(i)), "stored hole %d is replayed without a fetch", i)
		}
		assert.Equal(t, segments.Payload(30, segSize), buf[30*segSize:31*segSize], "the patch serves the patched hole")
	})
}

// A stored hole fetched because the file is past the ceiling, and served by
// the provider after all, is no longer missing: it must not push the file
// over the ceiling or the caps and into repair.
func TestAcceptableMissing_RecoveredStoredHoleNoLongerCounts(t *testing.T) {
	const segSize = 1024
	ctx := context.Background()
	filePath := "movies/recovered.mkv"
	cfg := acceptableMissingConfig(2)
	// Three stored holes in 100 segments; 30 and 50 are still gone.
	mvf, repo, ms := newImportedCeilingFile(t, filePath, cfg, 30, 50)
	stamp := time.Now().Add(-time.Hour).Unix()
	for _, i := range []int{20, 30, 50} {
		mvf.meta.KnownHoles = append(mvf.meta.KnownHoles, &metapb.HoleRun{StartSegment: int64(i), Count: 1, RecordedAt: stamp})
	}
	mvf.meta.HoleProviderFingerprint = cfg.ProviderFingerprint()
	fp := mvf.poolManager.(*fakePoolManager).client.(*fakepool.Client)

	buf := make([]byte, mvf.meta.FileSize)
	_, readErr := mvf.ReadAtContext(ctx, buf, 0)
	require.NoError(t, readErr, "2 of 100 still missing is within 2 %")
	assert.Equal(t, segments.Payload(20, segSize), buf[20*segSize:21*segSize], "the recovered segment serves its real bytes")
	assert.NotZero(t, fp.PerMessageCalls(segments.MessageID(20)), "the stored hole past the ceiling was fetched")

	fh, err := repo.GetFileHealth(ctx, filePath)
	require.NoError(t, err)
	require.NotNil(t, fh)
	assert.Equal(t, database.HealthStatusHealthy, fh.Status, "the file must not go to repair")
	meta, err := ms.ReadFileMetadata(filePath)
	require.NoError(t, err)
	assert.NotNil(t, meta, "the metadata must stay in place")
}

// isolateSegments renames the handle's articles after the test, served as
// usual except for the missing ones. Readers share in-flight articles
// process-wide by message ID, so a handle an earlier test left behind could
// otherwise serve an article this test needs missing, or the reverse.
func isolateSegments(t *testing.T, mvf *MetadataVirtualFile, fp *fakepool.Client, segSize int, missing ...int) {
	t.Helper()
	gone := map[int]bool{}
	for _, i := range missing {
		gone[i] = true
	}
	for i, seg := range mvf.meta.SegmentData {
		seg.Id = t.Name() + "-" + seg.Id
		if gone[i] {
			fp.SetBehavior(seg.Id, fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
		} else {
			fp.SetBehavior(seg.Id, fakepool.SegmentBehavior{Bytes: segments.Payload(i, segSize)})
		}
	}
}

// A stored hole read before a recovered one must not send the file to repair.
// The stored holes are past the ceiling, so each is asked about once when the
// handle's hooks are built: 30 is back and stops counting, 20 and 50 are 2 of
// 100, within 2 %.
func TestAcceptableMissing_StoredMissReadBeforeARecoveredOne(t *testing.T) {
	const segSize = 1024
	ctx := context.Background()
	filePath := "movies/recovered-later.mkv"
	cfg := acceptableMissingConfig(2)
	mvf, repo, ms := newImportedCeilingFile(t, filePath, cfg)
	isolateSegments(t, mvf, mvf.poolManager.(*fakePoolManager).client.(*fakepool.Client), segSize, 20, 50)
	stamp := time.Now().Add(-time.Hour).Unix()
	for _, i := range []int{20, 30, 50} {
		mvf.meta.KnownHoles = append(mvf.meta.KnownHoles, &metapb.HoleRun{StartSegment: int64(i), Count: 1, RecordedAt: stamp})
	}
	mvf.meta.HoleProviderFingerprint = cfg.ProviderFingerprint()

	seg := make([]byte, segSize)
	_, readErr := mvf.ReadAtContext(ctx, seg, 20*segSize)
	require.NoError(t, readErr, "segment 20 is the only confirmed miss: 1 of 100")
	assert.Equal(t, make([]byte, segSize), seg, "segment 20 is zero-filled")

	buf := make([]byte, mvf.meta.FileSize)
	_, readErr = mvf.ReadAtContext(ctx, buf, 0)
	require.NoError(t, readErr, "2 of 100 still missing is within 2 %")
	assert.Equal(t, segments.Payload(30, segSize), buf[30*segSize:31*segSize], "the recovered segment serves its real bytes")
	assert.Equal(t, int64(3), mvf.poolManager.(*fakePoolManager).client.(*fakepool.Client).StatPriorityCalls(),
		"each stored hole is asked about once per handle, and nothing else is")

	fh, err := repo.GetFileHealth(ctx, filePath)
	require.NoError(t, err)
	require.NotNil(t, fh)
	assert.Equal(t, database.HealthStatusHealthy, fh.Status, "the file must not go to repair")
	meta, err := ms.ReadFileMetadata(filePath)
	require.NoError(t, err)
	assert.NotNil(t, meta, "the metadata must stay in place")
}

// Stored holes past the ceiling that are all still missing all count: at 2 %,
// three of them fail the file on the first one read.
func TestAcceptableMissing_ThreeStoredMissesStillFail(t *testing.T) {
	const segSize = 1024
	fp := fakepool.New()
	mvf := patchedHoleFile(t, fp, 2, []int{20, 30, 50}, nil)
	isolateSegments(t, mvf, fp, segSize, 20, 30, 50)

	buf := make([]byte, mvf.meta.FileSize)
	_, readErr := mvf.ReadAtContext(context.Background(), buf, 0)
	require.Error(t, readErr, "3 of 100 confirmed missing is past 2 %")
	for _, i := range []int{20, 30, 50} {
		assert.NotZero(t, fp.PerMessageCalls(mvf.meta.SegmentData[i].Id), "stored hole %d is asked about, not replayed", i)
	}
}

// A new miss read before any stored hole is judged with the stored holes that
// are still missing, as before: at 2 %, three stored misses and the new one
// fail the read.
func TestAcceptableMissing_StoredMissesPastTheCeilingCountWithANewMiss(t *testing.T) {
	const segSize = 1024
	fp := fakepool.New()
	mvf := patchedHoleFile(t, fp, 2, []int{20, 30, 50}, nil)
	isolateSegments(t, mvf, fp, segSize, 20, 30, 50, 70)

	buf := make([]byte, 2*segSize)
	_, readErr := mvf.ReadAtContext(context.Background(), buf, 70*segSize)
	require.Error(t, readErr, "three stored misses plus the new miss are 4 of 100, past 2 %")
}

// A stored hole whose check fails (a provider error, not an answer) still
// counts as missing.
func TestAcceptableMissing_StoredHoleCheckErrorCounts(t *testing.T) {
	const segSize = 1024
	fp := fakepool.New()
	mvf := patchedHoleFile(t, fp, 2, []int{20, 30, 50}, nil)
	isolateSegments(t, mvf, fp, segSize, 20, 50)
	fp.SetBehavior(mvf.meta.SegmentData[30].Id, fakepool.SegmentBehavior{Err: errors.New("provider unreachable")})

	buf := make([]byte, segSize)
	_, readErr := mvf.ReadAtContext(context.Background(), buf, 20*segSize)
	require.Error(t, readErr, "20 and 50 missing and 30 unknown are 3 of 100, past 2 %")
}

// Stored holes within the ceiling are trusted without a check: they count from
// the start, so a new miss read before any of them is judged with them, as
// before.
func TestAcceptableMissing_TrustedStoredHolesCountWithANewMiss(t *testing.T) {
	const segSize = 1024
	fp := fakepool.New()
	mvf := patchedHoleFile(t, fp, 2, []int{20, 30}, nil)
	isolateSegments(t, mvf, fp, segSize, 20, 30, 70)

	buf := make([]byte, segSize)
	_, readErr := mvf.ReadAtContext(context.Background(), buf, 70*segSize)
	require.Error(t, readErr, "two trusted stored holes plus the new miss are 3 of 100, past 2 %")
	for _, i := range []int{20, 30} {
		assert.Zero(t, fp.PerMessageCalls(mvf.meta.SegmentData[i].Id), "stored hole %d within the ceiling is not asked about", i)
	}
}

// A patch serves a bounded read that ends inside the patched segment, so the
// ceiling may count that hole as repaired.
func TestAcceptableMissingZero_PatchServesABoundedRead(t *testing.T) {
	const segSize = 1024
	for _, realStore := range []bool{false, true} {
		name := "patch read by Get"
		if realStore {
			name = "PatchStore checked by Size"
		}
		t.Run(name, func(t *testing.T) {
			fp := fakepool.New()
			mvf := patchedHoleFile(t, fp, 0, []int{20}, []int{20})
			if realStore {
				ps := par2repair.NewPatchStore(t.TempDir())
				require.NoError(t, ps.Put(strings.Trim(segments.MessageID(20), "<>"), segments.Payload(20, segSize)))
				mvf.patchSource = ps
			}
			// An HTTP range ending halfway through the patched segment.
			mvf.ctx = context.WithValue(context.Background(), utils.RangeKey, "bytes=0-20991")
			mvf.originalRangeEnd = 0

			buf := make([]byte, 20*segSize+segSize/2)
			n, readErr := mvf.ReadAtContext(context.Background(), buf, 0)
			require.NoError(t, readErr, "the patched hole does not count")
			require.Equal(t, len(buf), n)
			assert.Equal(t, segments.Payload(20, segSize)[:segSize/2], buf[20*segSize:], "the patch serves the bounded read")
		})
	}
}

// The reader ignores a patch whose length differs from the segment's, so the
// ceiling must not count that hole as repaired either.
func TestAcceptableMissingZero_UnusablePatchStillCounts(t *testing.T) {
	fp := fakepool.New()
	mvf := patchedHoleFile(t, fp, 0, []int{20}, []int{20})
	mvf.patchSource.(*fakePatchSource).patches[strings.Trim(segments.MessageID(20), "<>")] = []byte{1}

	buf := make([]byte, mvf.meta.FileSize)
	_, readErr := mvf.ReadAtContext(context.Background(), buf, 0)
	require.Error(t, readErr, "the stored hole has no usable patch and is past the 0 % ceiling")
	assert.NotZero(t, fp.PerMessageCalls(segments.MessageID(20)), "the stored hole must be fetched again")
}

// countingPatchSource counts lookups; with sized set it also reports patch
// lengths without reading them, like par2repair.PatchStore.
type countingPatchSource struct {
	mu      sync.Mutex
	patches map[string][]byte
	gets    int
	sizes   int
}

func (c *countingPatchSource) Get(messageID string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gets++
	p, ok := c.patches[messageID]
	return p, ok
}

func (c *countingPatchSource) Size(messageID string) (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sizes++
	p, ok := c.patches[messageID]
	return int64(len(p)), ok
}

func (c *countingPatchSource) counts() (gets, sizes int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gets, c.sizes
}

// Patches are only looked up for a ceiling decision that depends on them,
// once per handle and by size, never by reading every payload at open.
func TestAcceptableMissing_PatchesLookedUpOnlyWhenNeeded(t *testing.T) {
	const segSize = 1024

	t.Run("opening the handle reads no patch", func(t *testing.T) {
		fp := fakepool.New()
		mvf := patchedHoleFile(t, fp, 2, []int{20, 30}, nil)
		ps := &countingPatchSource{patches: map[string][]byte{
			strings.Trim(segments.MessageID(20), "<>"): segments.Payload(20, segSize),
			strings.Trim(segments.MessageID(30), "<>"): segments.Payload(30, segSize),
		}}
		mvf.patchSource = ps
		require.NotNil(t, mvf.holeHooks())
		gets, sizes := ps.counts()
		assert.Zero(t, gets+sizes, "no patch is looked up before a ceiling decision needs one")
	})

	t.Run("a ceiling decision checks each stored hole once, by size", func(t *testing.T) {
		fp := fakepool.New()
		mvf := patchedHoleFile(t, fp, 1, []int{20}, nil)
		fp.SetBehavior(segments.MessageID(50), fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
		ps := &countingPatchSource{patches: map[string][]byte{
			strings.Trim(segments.MessageID(20), "<>"): segments.Payload(20, segSize),
		}}
		mvf.patchSource = ps

		buf := make([]byte, mvf.meta.FileSize)
		_, readErr := mvf.ReadAtContext(context.Background(), buf, 0)
		require.NoError(t, readErr, "the patched hole does not count: 1 of 100 is within 1 %")
		_, sizes := ps.counts()
		assert.Equal(t, 1, sizes, "the one stored hole is checked once, by size")
	})
}

// The stored-hole check runs while the first read's reader is built. It must
// leave that read's watchdog a reader to interrupt, or a slow fetch after the
// check would run past the configured read timeout.
func TestAcceptableMissing_StoredHoleCheckKeepsTheReadTimeout(t *testing.T) {
	const segSize = 1024
	fp := fakepool.New()
	mvf := patchedHoleFile(t, fp, 0, []int{20}, nil)
	isolateSegments(t, mvf, fp, segSize)
	for _, i := range []int{0, 20} {
		fp.SetBehavior(mvf.meta.SegmentData[i].Id, fakepool.SegmentBehavior{Bytes: segments.Payload(i, segSize), Latency: 5 * time.Second})
	}
	mvf.configGetter().Streaming.ReadTimeoutSeconds = 1

	started := time.Now()
	_, readErr := mvf.ReadAtContext(context.Background(), make([]byte, segSize), 0)
	require.ErrorIs(t, readErr, ErrReadTimeout, "the stalled check and the slow first segment must not outlast the read timeout")
	assert.Less(t, time.Since(started), 1500*time.Millisecond)
}

// statErrorPool fails the existence check for one message ID.
type statErrorPool struct {
	*fakepool.Client
	id string
}

func (p *statErrorPool) ExistsMany(ctx context.Context, ids []string, opts nntppool.ManyOptions) <-chan nntppool.ExistsResult {
	out := make(chan nntppool.ExistsResult, len(ids))
	go func() {
		defer close(out)
		for r := range p.Client.ExistsMany(ctx, ids, opts) {
			if r.MessageID == p.id {
				r.Result, r.Err = nil, errors.New("provider unreachable")
			}
			out <- r
		}
	}()
	return out
}

// gatedPutStore holds the segment cache's Put for one message ID.
type gatedPutStore struct {
	id      string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *gatedPutStore) Get(string) ([]byte, bool) { return nil, false }
func (s *gatedPutStore) Put(id string, _ []byte) error {
	if id == s.id {
		s.once.Do(func() { close(s.entered) })
		<-s.release
	}
	return nil
}

// A stored hole whose body has arrived leaves the count before the segment
// cache stores it: a reader may already have the bytes. At 2 %, 20's check
// errored so it counts, then its body is read; 30 and 50 are still missing.
func TestAcceptableMissing_RecoveredBodyLeavesTheCountBeforeCaching(t *testing.T) {
	const segSize = 1024
	fp := fakepool.New()
	mvf := patchedHoleFile(t, fp, 2, []int{20, 30, 50}, nil)
	isolateSegments(t, mvf, fp, segSize, 30, 50)
	recovered := mvf.meta.SegmentData[20].Id
	mvf.poolManager = newFakePoolManager(&statErrorPool{Client: fp, id: recovered})
	store := &gatedPutStore{id: recovered, entered: make(chan struct{}), release: make(chan struct{})}
	mvf.segmentStore = store
	defer close(store.release)

	buf := make([]byte, segSize)
	_, readErr := mvf.ReadAtContext(context.Background(), buf, 20*segSize)
	require.NoError(t, readErr)
	require.Equal(t, segments.Payload(20, segSize), buf)
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("segment 20 was not cached")
	}
	_, readErr = mvf.ReadAtContext(context.Background(), buf, 30*segSize)
	require.NoError(t, readErr, "only 30 and 50 are still missing, 2 of 100")
}
