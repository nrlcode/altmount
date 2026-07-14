package health

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mutablePlaybackActivity struct {
	active atomic.Int32
}

func (s *mutablePlaybackActivity) ActiveStreams() int { return int(s.active.Load()) }
func (s *mutablePlaybackActivity) SetActive(n int32)  { s.active.Store(n) }

func TestPR5ObservationAdmissionBoundsGlobalAndProviderOccupancy(t *testing.T) {
	admission := newObservationAdmission(2, 1)
	releaseA, err := admission.Acquire(context.Background(), "provider-a")
	require.NoError(t, err)
	defer releaseA()

	// The per-provider cap is one, while a different provider can still use
	// the second global slot.
	blockedProviderCtx, cancelProvider := context.WithCancel(context.Background())
	providerResult := make(chan error, 1)
	go func() {
		release, acquireErr := admission.Acquire(blockedProviderCtx, "provider-a")
		if acquireErr == nil {
			release()
		}
		providerResult <- acquireErr
	}()

	releaseB, err := admission.Acquire(context.Background(), "provider-b")
	require.NoError(t, err)
	defer releaseB()

	blockedGlobalCtx, cancelGlobal := context.WithCancel(context.Background())
	globalResult := make(chan error, 1)
	go func() {
		release, acquireErr := admission.Acquire(blockedGlobalCtx, "provider-c")
		if acquireErr == nil {
			release()
		}
		globalResult <- acquireErr
	}()

	select {
	case err := <-providerResult:
		t.Fatalf("same-provider admission escaped its cap: %v", err)
	case err := <-globalResult:
		t.Fatalf("global admission escaped its cap: %v", err)
	default:
	}

	cancelProvider()
	cancelGlobal()
	require.ErrorIs(t, receiveAdmissionResult(t, providerResult), context.Canceled)
	require.ErrorIs(t, receiveAdmissionResult(t, globalResult), context.Canceled)
}

func TestPR5ObservationAdmissionWaitIsInterruptible(t *testing.T) {
	admission := newObservationAdmission(1, 1)
	release, err := admission.Acquire(context.Background(), "provider-a")
	require.NoError(t, err)
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		queuedRelease, acquireErr := admission.Acquire(ctx, "provider-b")
		if acquireErr == nil {
			queuedRelease()
		}
		result <- acquireErr
	}()
	cancel()
	assert.ErrorIs(t, receiveAdmissionResult(t, result), context.Canceled)
}

func TestPR5PlaybackGateChecksAtActualDispatchBoundary(t *testing.T) {
	admission := newObservationAdmission(1, 1)
	playback := &mutablePlaybackActivity{}
	gate := newObservationDispatchGate(admission, playback, true)

	occupied, err := admission.Acquire(context.Background(), "provider-a")
	require.NoError(t, err)

	result := make(chan error, 1)
	go func() {
		release, acquireErr := gate.Acquire(context.Background(), "provider-b")
		if acquireErr == nil {
			release()
		}
		result <- acquireErr
	}()

	// Playback begins while this chunk is waiting for admission. Releasing the
	// capacity must not let it dispatch based on the stale pre-wait state.
	playback.SetActive(1)
	occupied()
	require.ErrorIs(t, receiveAdmissionResult(t, result), ErrObservationPausedForPlayback)

	playback.SetActive(0)
	release, err := gate.Acquire(context.Background(), "provider-b")
	require.NoError(t, err)
	release()
}

func TestPR5PlaybackPauseIsFeatureGatedForObservationChunks(t *testing.T) {
	playback := &mutablePlaybackActivity{}
	playback.SetActive(1)
	gate := newObservationDispatchGate(newObservationAdmission(1, 1), playback, false)

	release, err := gate.Acquire(context.Background(), "provider-a")
	require.NoError(t, err)
	release()
}

func receiveAdmissionResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for cancelable observation admission")
		return errors.New("unreachable")
	}
}
