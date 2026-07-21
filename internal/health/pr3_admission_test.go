package health

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/testsupport/fakepool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fixedPlaybackActivity struct {
	active int
}

func (s fixedPlaybackActivity) ActiveStreams() int { return s.active }

func TestPR3OrdinaryHealthCyclePausesDuringPlayback(t *testing.T) {
	enabled := true
	env := newRepairTestEnv(t, t.TempDir(), nil, func(cfg *config.Config) {
		cfg.Health.PauseDuringPlayback = &enabled
	})
	filePath := "movies/pause-during-playback.mkv"
	require.NoError(t, env.metadataService.WriteFileMetadata(filePath, validSegmentMeta(env.metadataService, 1024)))
	insertFileHealth(t, env.db, filePath, "", 0, 3)
	env.hw.SetPlaybackActivitySource(fixedPlaybackActivity{active: 1})

	require.NoError(t, env.hw.runHealthCheckCycle(context.Background()))
	fh, err := env.healthRepo.GetFileHealth(context.Background(), filePath)
	require.NoError(t, err)
	require.NotNil(t, fh)
	require.Equal(t, database.HealthStatusPending, fh.Status)
	require.Zero(t, fh.RetryCount, "paused work must not consume a health retry")
}

func TestPR3HealthPlaybackPauseIsFeatureGated(t *testing.T) {
	disabled := false
	env := newRepairTestEnv(t, t.TempDir(), nil, func(cfg *config.Config) {
		cfg.Health.PauseDuringPlayback = &disabled
	})
	env.hw.SetPlaybackActivitySource(fixedPlaybackActivity{active: 1})

	if env.hw.shouldPauseForPlayback() {
		t.Fatal("disabled pause feature gate still blocked ordinary health work")
	}
}

// boundaryPlaybackActivity models the shared linearization point required by
// automatic health admission. A legacy worker that samples ActiveStreams twice
// can observe a stale zero: playback starts immediately after the second
// snapshot. The admission method represents the corrected protocol and makes
// the playback transition win atomically at that boundary.
type boundaryPlaybackActivity struct {
	boundary       sync.RWMutex
	active         atomic.Int32
	snapshotCalls  atomic.Int32
	admissionCalls atomic.Int32
}

func (s *boundaryPlaybackActivity) ActiveStreams() int {
	snapshot := s.active.Load()
	if s.snapshotCalls.Add(1) == 2 {
		s.startPlayback()
	}
	// Return the value sampled before the transition. This is the race the
	// shared admission method must close.
	return int(snapshot)
}

func (s *boundaryPlaybackActivity) startPlayback() {
	s.boundary.Lock()
	s.active.Store(1)
	s.boundary.Unlock()
}

// AcquireHealthAdmission is intentionally an optional extension to the
// current PlaybackActivitySource test seam. Production code should discover
// this method and use it for the claim boundary; older code ignores it.
func (s *boundaryPlaybackActivity) AcquireHealthAdmission() (func(), bool) {
	s.admissionCalls.Add(1)
	// Arrange for playback to win exactly as a concurrent stream start would.
	s.startPlayback()
	s.boundary.RLock()
	if s.active.Load() > 0 {
		s.boundary.RUnlock()
		return func() {}, false
	}
	return s.boundary.RUnlock, true
}

func TestCHG005PlaybackWinsAutomaticHealthAdmissionBoundary(t *testing.T) {
	client := fakepool.New()
	env := newBatchTestEnv(t, t.TempDir(), client)
	filePath := "complete/admission-boundary.bin"
	writeHealthyFile(t, env, filePath)
	insertFileHealth(t, env.db, filePath, "", 0, 3)

	source := &boundaryPlaybackActivity{}
	env.hw.SetPlaybackActivitySource(source)
	require.NoError(t, env.hw.runHealthCheckCycle(context.Background()))

	assert.Equal(t, int32(1), source.admissionCalls.Load(),
		"automatic health work must use the shared admission protocol")
	assert.Zero(t, client.StatCalls(),
		"playback winning the admission boundary must prevent any health STAT")
	row, err := env.healthRepo.GetFileHealth(context.Background(), filePath)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, database.HealthStatusPending, row.Status)
	assert.Zero(t, row.RetryCount,
		"work rejected at admission must remain pending without consuming a retry")
}
