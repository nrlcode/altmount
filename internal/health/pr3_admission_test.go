package health

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/testsupport/fakepool"
	"github.com/javi11/nntppool/v4"
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

type admissionReleaseObservation struct {
	status database.HealthStatus
	err    error
}

// retainedPlaybackAdmission mirrors StreamTracker's read/write admission
// boundary while exposing the exact status at release. A playback start takes
// the write side, so it can only complete after the worker releases admission.
type retainedPlaybackAdmission struct {
	boundary sync.RWMutex
	active   atomic.Int32

	admissionCalls atomic.Int32
	held           atomic.Bool
	acquired       chan struct{}
	acquiredOnce   sync.Once
	released       chan admissionReleaseObservation
	releaseOnce    sync.Once

	repo     *database.HealthRepository
	filePath string
}

func newRetainedPlaybackAdmission(
	repo *database.HealthRepository,
	filePath string,
) *retainedPlaybackAdmission {
	return &retainedPlaybackAdmission{
		acquired: make(chan struct{}),
		released: make(chan admissionReleaseObservation, 1),
		repo:     repo,
		filePath: filePath,
	}
}

func (s *retainedPlaybackAdmission) ActiveStreams() int {
	return int(s.active.Load())
}

func (s *retainedPlaybackAdmission) AcquireHealthAdmission() (func(), bool) {
	s.admissionCalls.Add(1)
	s.boundary.RLock()
	if s.active.Load() > 0 {
		s.boundary.RUnlock()
		return func() {}, false
	}

	s.held.Store(true)
	s.acquiredOnce.Do(func() { close(s.acquired) })
	return s.release, true
}

func (s *retainedPlaybackAdmission) release() {
	s.releaseOnce.Do(func() {
		observation := admissionReleaseObservation{}
		row, err := s.repo.GetFileHealth(context.Background(), s.filePath)
		observation.err = err
		if row != nil {
			observation.status = row.Status
		}

		s.held.Store(false)
		s.boundary.RUnlock()
		s.released <- observation
	})
}

func (s *retainedPlaybackAdmission) forceRelease() {
	if s.held.Load() {
		s.release()
	}
}

func (s *retainedPlaybackAdmission) startPlayback() {
	s.boundary.Lock()
	s.active.Store(1)
	s.boundary.Unlock()
}

type retainedAdmissionStatClient struct {
	*fakepool.Client
	source <-chan nntppool.StatManyResult
	called chan struct{}
	once   sync.Once
}

func (c *retainedAdmissionStatClient) StatMany(
	context.Context,
	[]string,
	nntppool.StatManyOptions,
) <-chan nntppool.StatManyResult {
	c.once.Do(func() { close(c.called) })
	return c.source
}

func TestCHG005SuccessfulAdmissionHeldUntilCyclePublication(t *testing.T) {
	statSource := make(chan nntppool.StatManyResult, 1)
	var closeStatOnce sync.Once
	closeStatSource := func() { closeStatOnce.Do(func() { close(statSource) }) }
	defer closeStatSource()

	client := &retainedAdmissionStatClient{
		Client: fakepool.New(),
		source: statSource,
		called: make(chan struct{}),
	}
	env := newBatchTestEnv(t, t.TempDir(), client)
	const filePath = "complete/retained-admission.bin"
	segmentID := writeHealthyFile(t, env, filePath)
	insertFileHealth(t, env.db, filePath, "", 0, 3)

	admission := newRetainedPlaybackAdmission(env.healthRepo, filePath)
	t.Cleanup(admission.forceRelease)
	env.hw.SetPlaybackActivitySource(admission)

	cycleDone := make(chan error, 1)
	go func() {
		cycleDone <- env.hw.runHealthCheckCycle(context.Background())
	}()

	select {
	case <-client.called:
	case <-time.After(time.Second):
		t.Fatal("health cycle did not reach the blocking STAT sweep")
	}
	select {
	case <-admission.acquired:
	default:
		t.Error("automatic health cycle reached STAT without acquiring shared admission")
	}

	claimed, err := env.healthRepo.GetFileHealth(context.Background(), filePath)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, database.HealthStatusChecking, claimed.Status,
		"blocking STAT must begin only after the due row is claimed")

	playbackStarted := make(chan struct{})
	go func() {
		admission.startPlayback()
		close(playbackStarted)
	}()
	select {
	case <-playbackStarted:
		t.Error("playback crossed admission while claimed health evidence was still blocked")
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case observation := <-admission.released:
		t.Errorf("worker released admission before evidence publication: %+v", observation)
	default:
	}

	statSource <- nntppool.StatManyResult{
		MessageID: segmentID,
		Result:    &nntppool.StatResult{MessageID: segmentID},
	}
	closeStatSource()

	select {
	case err := <-cycleDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("health cycle did not complete after the STAT sweep was released")
	}

	var observation admissionReleaseObservation
	select {
	case observation = <-admission.released:
	case <-time.After(time.Second):
		t.Fatal("successful health cycle did not release shared admission")
	}
	require.NoError(t, observation.err)
	assert.Equal(t, database.HealthStatusHealthy, observation.status,
		"admission was released before checked evidence was published")
	assert.Equal(t, int32(1), admission.admissionCalls.Load())

	select {
	case <-playbackStarted:
	case <-time.After(time.Second):
		t.Fatal("playback did not resume after the bounded health cycle released admission")
	}
}
