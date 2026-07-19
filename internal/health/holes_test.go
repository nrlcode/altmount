package health

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/holes"
	"github.com/javi11/altmount/internal/metadata"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/javi11/altmount/internal/pool"
	"github.com/javi11/altmount/internal/testsupport/fakepool"
	"github.com/javi11/altmount/internal/usenet"
	"github.com/javi11/nntppool/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakePoolManager is a pool.Manager backed by a fakepool.Client, so health
// checks can stat segments without a network.
type fakePoolManager struct {
	mockPoolManager
	client *fakepool.Client
}

func (m *fakePoolManager) GetPool() (pool.NntpClient, error) { return m.client, nil }
func (m *fakePoolManager) HasPool() bool                     { return true }

// holeTestEnv wires a HealthChecker over a fake pool that stats a video file
// split into fixed-size segments.
type holeTestEnv struct {
	checker  *HealthChecker
	ms       *metadata.MetadataService
	fp       *fakepool.Client
	filePath string
	segIDs   []string
	cfg      *config.Config
}

func newHoleTestEnv(t *testing.T, fileName string, fileSize, segSize int64) *holeTestEnv {
	t.Helper()
	var sizes []int64
	for off := int64(0); off < fileSize; off += segSize {
		sizes = append(sizes, min(segSize, fileSize-off))
	}
	return newHoleTestEnvWithSegmentSizes(t, fileName, sizes)
}

func newHoleTestEnvWithSegmentSizes(t *testing.T, fileName string, sizes []int64) *holeTestEnv {
	t.Helper()
	tempDir := t.TempDir()
	ms := metadata.NewMetadataService(tempDir)
	fp := fakepool.New()

	var fileSize int64
	var segs []*metapb.SegmentData
	var ids []string
	for _, size := range sizes {
		id := fmt.Sprintf("hole-seg-%d@test", len(segs))
		fp.SetBehavior(id, fakepool.SegmentBehavior{Bytes: make([]byte, size)})
		segs = append(segs, &metapb.SegmentData{
			Id:          id,
			SegmentSize: size,
			StartOffset: 0,
			EndOffset:   size - 1,
		})
		ids = append(ids, id)
		fileSize += size
	}

	filePath := "/movies/" + fileName
	meta := ms.CreateFileMetadata(
		fileSize, "test.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY,
		segs, metapb.Encryption_NONE, "", "", nil, nil, 0, nil, "",
	)
	require.NoError(t, ms.WriteFileMetadata(filePath, meta))

	healthEnabled := true
	cfg := config.DefaultConfig()
	cfg.Health.Enabled = &healthEnabled
	cfg.Metadata.RootPath = tempDir
	cfg.Health.MaxConnectionsForHealthChecks = 2
	checkAll := true
	cfg.Health.CheckAllSegments = &checkAll // deterministic: stat every segment

	checker := NewHealthChecker(
		nil, // healthRepo unused by CheckFile happy paths (no deletes)
		ms,
		&fakePoolManager{client: fp},
		func() *config.Config { return cfg },
		nil,
	)

	return &holeTestEnv{
		checker:  checker,
		ms:       ms,
		fp:       fp,
		filePath: filePath,
		segIDs:   ids,
		cfg:      cfg,
	}
}

// markSegmentMissing makes Stat fail for the segment.
func (e *holeTestEnv) markSegmentMissing(index int) {
	e.fp.SetBehavior(e.segIDs[index], fakepool.SegmentBehavior{Err: nntppool.ErrArticleNotFound})
}

func TestHealthCheckCleanFileIsHealthy(t *testing.T) {
	env := newHoleTestEnv(t, "movie.mp4", 4*1024*1024, 1024)

	event := env.checker.CheckFile(context.Background(), env.filePath)
	require.Equal(t, EventTypeFileHealthy, event.Type, "err: %v", event.Error)
	assert.Nil(t, event.Classification)
}

func TestHealthCheckClassifiesSmallHoleAsDegraded(t *testing.T) {
	env := newHoleTestEnv(t, "movie.mp4", 4*1024*1024, 1024)
	// Two isolated missing segments — well within the pad caps.
	env.markSegmentMissing(10)
	env.markSegmentMissing(30)

	event := env.checker.CheckFile(context.Background(), env.filePath)
	require.Equal(t, EventTypeFileCorrupted, event.Type)
	require.NotNil(t, event.Classification)
	assert.Equal(t, holes.VerdictDegraded, event.Classification.Verdict)
	assert.Equal(t, 2, event.Classification.TotalMissing)
	assert.Equal(t, 1, event.Classification.LongestRun)

	// Details envelope round-trips with playback impact.
	require.NotNil(t, event.Details)
	var details database.HealthErrorDetails
	require.NoError(t, json.Unmarshal([]byte(*event.Details), &details))
	assert.Equal(t, "missing_segments", details.ErrorType)
	require.NotNil(t, details.PlaybackImpact)
	assert.Equal(t, holes.VerdictDegraded, details.PlaybackImpact.Verdict)

	// PR3 quarantines the legacy .meta hole field. A single health sweep must
	// not populate it and thereby authorize replay pre-padding.
	meta, err := env.ms.ReadFileMetadata(env.filePath)
	require.NoError(t, err)
	assert.Empty(t, meta.KnownHoles, "health results must not create .meta padding authority")
}

func TestHealthCheckClassifiesLongRunAsFailed(t *testing.T) {
	env := newHoleTestEnv(t, "movie.mp4", 4*1024*1024, 1024)
	// A run of 5 consecutive missing segments exceeds MaxPadRunSegments (4).
	for i := 10; i < 15; i++ {
		env.markSegmentMissing(i)
	}

	event := env.checker.CheckFile(context.Background(), env.filePath)
	require.Equal(t, EventTypeFileCorrupted, event.Type)
	require.NotNil(t, event.Classification)
	assert.Equal(t, holes.VerdictFailed, event.Classification.Verdict)

	// Failed files are not persisted as known holes (they head to repair).
	meta, err := env.ms.ReadFileMetadata(env.filePath)
	require.NoError(t, err)
	assert.Empty(t, meta.KnownHoles)
}

func TestHealthCheckExactTwoPercentIsDegraded(t *testing.T) {
	sizes := make([]int64, 50)
	for i := range sizes {
		sizes[i] = 200
	}
	env := newHoleTestEnvWithSegmentSizes(t, "movie.mp4", sizes)
	env.markSegmentMissing(0)

	event := env.checker.CheckFile(context.Background(), env.filePath)
	require.Equal(t, EventTypeFileCorrupted, event.Type)
	require.NotNil(t, event.Classification)
	assert.Equal(t, holes.VerdictDegraded, event.Classification.Verdict)
	assert.InDelta(t, 0.02, event.Classification.PaddedRatio, 1e-12)
}

func TestHealthCheckUsesExactMissingSegmentBytes(t *testing.T) {
	// One 300-byte segment plus 98 98-byte segments and one 96-byte segment:
	// exactly 10,000 bytes across 100 non-uniform segments. The missing first
	// segment is 3% of the file even though the average segment is only 1%.
	sizes := make([]int64, 100)
	sizes[0] = 300
	for i := 1; i < 99; i++ {
		sizes[i] = 98
	}
	sizes[99] = 96
	env := newHoleTestEnvWithSegmentSizes(t, "movie.mp4", sizes)
	env.markSegmentMissing(0)

	event := env.checker.CheckFile(context.Background(), env.filePath)
	require.Equal(t, EventTypeFileCorrupted, event.Type)
	require.NotNil(t, event.Classification)
	assert.Equal(t, holes.VerdictFailed, event.Classification.Verdict)
	assert.InDelta(t, 0.03, event.Classification.PaddedRatio, 1e-12)
}

func TestHealthCheckSkipsClassificationForNonVideo(t *testing.T) {
	env := newHoleTestEnv(t, "archive.rar", 4*1024*1024, 1024)
	env.markSegmentMissing(10)

	event := env.checker.CheckFile(context.Background(), env.filePath)
	require.Equal(t, EventTypeFileCorrupted, event.Type)
	assert.Nil(t, event.Classification, "non-video files are not hole-classified")
}

func TestHealthCheckSkipsClassificationForEncrypted(t *testing.T) {
	env := newHoleTestEnv(t, "movie.mp4", 4*1024*1024, 1024)
	require.NoError(t, env.ms.UpdateFileMetadata(env.filePath, func(m *metapb.FileMetadata) {
		m.Encryption = metapb.Encryption_RCLONE
	}))
	env.markSegmentMissing(10)

	event := env.checker.CheckFile(context.Background(), env.filePath)
	require.Equal(t, EventTypeFileCorrupted, event.Type)
	assert.Nil(t, event.Classification, "encrypted files are not hole-classified")
}

func TestHealthCheckIgnoresLegacyPersistedHoles(t *testing.T) {
	env := newHoleTestEnv(t, "movie.mp4", 4*1024*1024, 1024)
	// Seed a persisted hole (as if playback padded it earlier).
	require.NoError(t, env.ms.AddKnownHoles(env.filePath, []holes.Run{{Start: 5, Count: 1}}))
	// A fresh check finds a different missing segment.
	env.markSegmentMissing(20)

	event := env.checker.CheckFile(context.Background(), env.filePath)
	require.Equal(t, EventTypeFileCorrupted, event.Type)
	require.NotNil(t, event.Classification)
	assert.Equal(t, holes.VerdictDegraded, event.Classification.Verdict)
	// The legacy .meta hole is unverified and must not influence the verdict.
	assert.Equal(t, 1, event.Classification.TotalMissing)
}

func TestHealthClassificationUsesCompletePositionalMissingSet(t *testing.T) {
	env := newHoleTestEnv(t, "movie.mp4", 4*1024*1024, 1024)

	// Seventy isolated misses exceed the total-segment cap. Only the first 50
	// are retained as display examples; those 50 alone remain within every cap.
	missing := make([]usenet.MissingSegment, 0, 70)
	examples := make([]string, 0, 50)
	for i := range 70 {
		idx := i * 10
		missing = append(missing, usenet.MissingSegment{Index: idx, ID: env.segIDs[idx]})
		if i < 50 {
			examples = append(examples, env.segIDs[idx])
		}
	}

	impact := env.checker.classifyHoles(context.Background(), env.filePath, usenet.ValidationResult{
		TotalChecked:    len(env.segIDs),
		MissingCount:    len(missing),
		MissingIDs:      examples,
		MissingSegments: missing,
	})
	require.NotNil(t, impact)
	assert.Equal(t, holes.VerdictFailed, impact.Verdict)
	assert.Equal(t, 70, impact.TotalMissing)
}
