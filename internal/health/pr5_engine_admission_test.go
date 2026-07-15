package health

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mutablePlaybackActivity struct {
	active atomic.Int32
}

type blockingSharedWireAdmission struct {
	slots   chan int
	permits chan struct{}
	granted atomic.Int32
}

func (a *blockingSharedWireAdmission) AcquireImportConnections(
	ctx context.Context,
	slots int,
) (func(), int, error) {
	a.slots <- slots
	select {
	case <-a.permits:
		granted := int(a.granted.Load())
		if granted <= 0 {
			granted = slots
		}
		return func() {}, granted, nil
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
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

func TestPR5ObservationAdmissionWeightsWireOccupancy(t *testing.T) {
	admission := newObservationAdmission(3, 2)
	releaseProviderA, err := admission.AcquireSlots(context.Background(), "provider-a", 2)
	require.NoError(t, err)
	defer releaseProviderA()

	providerCtx, cancelProvider := context.WithCancel(context.Background())
	providerResult := make(chan error, 1)
	go func() {
		release, acquireErr := admission.AcquireSlots(providerCtx, "provider-a", 1)
		if acquireErr == nil {
			release()
		}
		providerResult <- acquireErr
	}()

	releaseProviderB, err := admission.AcquireSlots(context.Background(), "provider-b", 1)
	require.NoError(t, err)
	defer releaseProviderB()

	globalCtx, cancelGlobal := context.WithCancel(context.Background())
	globalResult := make(chan error, 1)
	go func() {
		release, acquireErr := admission.AcquireSlots(globalCtx, "provider-c", 1)
		if acquireErr == nil {
			release()
		}
		globalResult <- acquireErr
	}()

	select {
	case err := <-providerResult:
		t.Fatalf("weighted provider admission escaped its wire cap: %v", err)
	case err := <-globalResult:
		t.Fatalf("weighted global admission escaped its wire cap: %v", err)
	default:
	}

	cancelProvider()
	cancelGlobal()
	require.ErrorIs(t, receiveAdmissionResult(t, providerResult), context.Canceled)
	require.ErrorIs(t, receiveAdmissionResult(t, globalResult), context.Canceled)
}

func TestPR5ObservationRequestReservesActualWireSlots(t *testing.T) {
	targets := make([]observationSegmentTarget, 256)
	stat := observationTransportRequest{
		ObservationKind: database.HealthObservationSTAT,
		Targets:         targets,
	}
	body := observationTransportRequest{
		ObservationKind: database.HealthObservationValidatedBody,
		Targets:         targets,
	}

	require.Equal(t, 17, observationRequestWireSlots(stat, 17))
	require.Equal(t, 1, observationRequestWireSlots(body, 17))
	require.Equal(t, 1, observationRequestWireSlots(stat, 0))
}

func TestPR5ObservationGateUsesSharedPlaybackAwareWireBudget(t *testing.T) {
	shared := &blockingSharedWireAdmission{
		slots: make(chan int, 1), permits: make(chan struct{}, 1),
	}
	gate := newObservationDispatchGateWithSharedBudget(
		newObservationAdmission(3, 3), shared, nil, true,
	)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		release, err := gate.AcquireSlots(ctx, "provider-a", 2)
		if err == nil {
			release()
		}
		result <- err
	}()
	require.Equal(t, 2, <-shared.slots)
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)

	// Cancellation while waiting on the shared pool budget must release the
	// local provider/global reservation rather than leaking capacity.
	shared.permits <- struct{}{}
	release, err := gate.AcquireSlots(context.Background(), "provider-a", 3)
	require.NoError(t, err)
	require.Equal(t, 3, <-shared.slots)
	release()
}

func TestPR5ObservationGateUsesAtomicDynamicGrant(t *testing.T) {
	shared := &blockingSharedWireAdmission{
		slots: make(chan int, 1), permits: make(chan struct{}, 1),
	}
	shared.granted.Store(2)
	shared.permits <- struct{}{}
	gate := newObservationDispatchGateWithSharedBudget(
		newObservationAdmission(17, 17), shared, nil, true,
	)
	release, granted, err := gate.AcquireWireSlots(context.Background(), "provider-a", 17)
	require.NoError(t, err)
	require.Equal(t, 17, <-shared.slots)
	require.Equal(t, 2, granted)
	release()
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
