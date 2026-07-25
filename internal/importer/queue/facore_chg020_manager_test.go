package queue

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const facoreCHG020BarrierTimeout = 2 * time.Second

type facoreCHG020Processor struct {
	process func(context.Context, *database.ImportQueueItem) (string, error)
	success func(context.Context, *database.ImportQueueItem, string) error
	failure func(context.Context, *database.ImportQueueItem, error)
}

func (p *facoreCHG020Processor) ProcessItem(ctx context.Context, item *database.ImportQueueItem) (string, error) {
	if p.process == nil {
		return "", nil
	}
	return p.process(ctx, item)
}

func (p *facoreCHG020Processor) HandleSuccess(ctx context.Context, item *database.ImportQueueItem, path string) error {
	if p.success == nil {
		return nil
	}
	return p.success(ctx, item, path)
}

func (p *facoreCHG020Processor) HandleFailure(ctx context.Context, item *database.ImportQueueItem, err error) {
	if p.failure != nil {
		p.failure(ctx, item, err)
	}
}

type facoreCHG020Listener struct {
	claimed chan *database.ImportQueueItem
	release chan struct{}
}

func (l *facoreCHG020Listener) OnItemClaimed(_ context.Context, item *database.ImportQueueItem) {
	l.claimed <- item
	<-l.release
}

func facoreCHG020ConfigGetter() *config.Config {
	return &config.Config{
		Import: config.ImportConfig{QueueProcessingIntervalSeconds: 60},
	}
}

func facoreCHG020Manager(
	t *testing.T,
	processor ItemProcessor,
	listener QueueEventListener,
) (*Manager, *database.QueueRepository) {
	t.Helper()

	db, err := database.NewDB(database.Config{
		Type:         "sqlite",
		DatabasePath: filepath.Join(t.TempDir(), "queue.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	manager := NewManager(ManagerConfig{
		Workers:      1,
		ConfigGetter: facoreCHG020ConfigGetter,
	}, db.Repository, processor, listener)
	t.Cleanup(manager.cancel)
	return manager, db.Repository
}

func facoreCHG020AddPendingItem(t *testing.T, repo *database.QueueRepository, path string) *database.ImportQueueItem {
	t.Helper()

	item := &database.ImportQueueItem{
		NzbPath:    path,
		Priority:   database.QueuePriorityNormal,
		Status:     database.QueueStatusPending,
		MaxRetries: 3,
	}
	require.NoError(t, repo.AddToQueue(context.Background(), item))
	require.NotZero(t, item.ID)
	return item
}

func facoreCHG020Receive[T any](t *testing.T, ch <-chan T, message string) T {
	t.Helper()

	select {
	case value := <-ch:
		return value
	case <-time.After(facoreCHG020BarrierTimeout):
		t.Fatal(message)
		var zero T
		return zero
	}
}

func facoreCHG020Wait(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()
	facoreCHG020Receive(t, ch, message)
}

func TestFACORECHG020CancelProcessingRejectsMissingRuntimeOwner(t *testing.T) {
	manager := NewManager(ManagerConfig{
		Workers:      1,
		ConfigGetter: facoreCHG020ConfigGetter,
	}, nil, &facoreCHG020Processor{}, nil)
	t.Cleanup(manager.cancel)

	err := manager.CancelProcessing(404)
	require.ErrorIs(t, err, ErrQueueItemNotProcessing,
		"cancellation must distinguish a missing runtime owner from a delivered cancellation")
}

func TestFACORECHG020OwnerReleaseIsIdentityChecked(t *testing.T) {
	manager := NewManager(ManagerConfig{
		Workers:      1,
		ConfigGetter: facoreCHG020ConfigGetter,
	}, nil, &facoreCHG020Processor{}, nil)
	t.Cleanup(manager.cancel)

	oldCtx, cancelOld := context.WithCancel(context.Background())
	replacementCtx, cancelReplacement := context.WithCancel(context.Background())
	t.Cleanup(cancelReplacement)
	oldOwner := &processingOwner{cancel: cancelOld}
	replacement := &processingOwner{cancel: cancelReplacement}
	manager.cancelFuncs[42] = replacement

	manager.releaseProcessingOwner(42, oldOwner)

	manager.cancelMu.RLock()
	current := manager.cancelFuncs[42]
	manager.cancelMu.RUnlock()
	assert.Same(t, replacement, current)
	assert.ErrorIs(t, oldCtx.Err(), context.Canceled,
		"the retired owner's context must still be released")
	assert.NoError(t, replacementCtx.Err(),
		"retiring an old owner must not cancel its replacement")
}

func TestFACORECHG020ExecuteItemRegistersCancellationBeforeReturning(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	processor := &facoreCHG020Processor{process: func(context.Context, *database.ImportQueueItem) (string, error) {
		close(started)
		<-release
		close(finished)
		return "result", nil
	}}
	manager, repo := facoreCHG020Manager(t, processor, nil)
	item := facoreCHG020AddPendingItem(t, repo, "synchronous-registration.nzb")

	manager.cancelMu.Lock()
	registryLocked := true
	defer func() {
		if registryLocked {
			manager.cancelMu.Unlock()
		}
	}()
	executeResult := make(chan error, 1)
	go func() {
		executeResult <- manager.ExecuteItem(context.Background(), item.ID)
	}()

	var (
		returnedWhileRegistryLocked bool
		earlyResult                 error
	)
	deadline := time.NewTimer(facoreCHG020BarrierTimeout)
	ticker := time.NewTicker(time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	barrierReached := false
	for !barrierReached && !returnedWhileRegistryLocked {
		if !manager.claimMu.TryLock() {
			barrierReached = true
			break
		}
		manager.claimMu.Unlock()
		select {
		case earlyResult = <-executeResult:
			returnedWhileRegistryLocked = true
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("ExecuteItem did not reach the manager admission boundary")
		}
	}
	require.True(t, barrierReached || returnedWhileRegistryLocked)
	manager.cancelMu.Unlock()
	registryLocked = false

	if returnedWhileRegistryLocked {
		require.NoError(t, earlyResult)
	} else {
		require.NoError(t, facoreCHG020Receive(t, executeResult,
			"ExecuteItem did not return after the cancellation registry was unlocked"))
	}
	facoreCHG020Wait(t, started, "manual processor did not start")
	close(release)
	facoreCHG020Wait(t, finished, "manual processor did not finish")

	assert.False(t, returnedWhileRegistryLocked,
		"ExecuteItem returned before its cancellation owner could be registered")
}

func TestFACORECHG020DuplicateManualExecutionHasOneAdmission(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	finished := make(chan struct{}, 2)
	processor := &facoreCHG020Processor{process: func(context.Context, *database.ImportQueueItem) (string, error) {
		calls.Add(1)
		started <- struct{}{}
		<-release
		finished <- struct{}{}
		return "result", nil
	}}
	manager, repo := facoreCHG020Manager(t, processor, nil)
	item := facoreCHG020AddPendingItem(t, repo, "duplicate-manual.nzb")

	require.NoError(t, manager.ExecuteItem(context.Background(), item.ID))
	facoreCHG020Wait(t, started, "first manual execution did not start")

	secondErr := manager.ExecuteItem(context.Background(), item.ID)
	if secondErr == nil {
		facoreCHG020Wait(t, started, "second admitted manual execution did not start")
	}
	close(release)
	facoreCHG020Wait(t, finished, "first manual execution did not finish")
	if secondErr == nil {
		facoreCHG020Wait(t, finished, "second admitted manual execution did not finish")
	}

	assert.ErrorIs(t, secondErr, database.ErrQueueItemClaimConflict,
		"an already-processing row must not receive a second manual owner")
	assert.EqualValues(t, 1, calls.Load(), "one durable queue row must have one active processor")
}

func TestFACORECHG020ActiveManualCancellationReachesProcessor(t *testing.T) {
	started := make(chan struct{})
	cancelled := make(chan struct{})
	stopped := make(chan struct{})
	finished := make(chan struct{})
	var stopOnce sync.Once
	t.Cleanup(func() { stopOnce.Do(func() { close(stopped) }) })

	processor := &facoreCHG020Processor{process: func(ctx context.Context, _ *database.ImportQueueItem) (string, error) {
		close(started)
		defer close(finished)
		select {
		case <-ctx.Done():
			close(cancelled)
			return "", ctx.Err()
		case <-stopped:
			return "", nil
		}
	}}
	manager, repo := facoreCHG020Manager(t, processor, nil)
	item := facoreCHG020AddPendingItem(t, repo, "active-manual-cancel.nzb")

	require.NoError(t, manager.ExecuteItem(context.Background(), item.ID))
	facoreCHG020Wait(t, started, "manual execution did not start")
	require.NoError(t, manager.CancelProcessing(item.ID))
	facoreCHG020Wait(t, cancelled, "manual cancellation did not reach the active processor context")
	facoreCHG020Wait(t, finished, "cancelled manual processor did not finish")
}

func TestFACORECHG020ManualAndAutomaticExecutionHaveOneAdmission(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{}, 2)
	releaseProcessing := make(chan struct{})
	finished := make(chan struct{}, 2)
	listener := &facoreCHG020Listener{
		claimed: make(chan *database.ImportQueueItem, 1),
		release: make(chan struct{}),
	}
	processor := &facoreCHG020Processor{process: func(context.Context, *database.ImportQueueItem) (string, error) {
		calls.Add(1)
		started <- struct{}{}
		<-releaseProcessing
		finished <- struct{}{}
		return "result", nil
	}}
	manager, repo := facoreCHG020Manager(t, processor, listener)
	item := facoreCHG020AddPendingItem(t, repo, "manual-automatic.nzb")

	automaticDone := make(chan struct{})
	go func() {
		manager.processNextItem(context.Background(), 1)
		close(automaticDone)
	}()
	claimed := facoreCHG020Receive(t, listener.claimed, "automatic worker did not claim the item")
	require.Equal(t, item.ID, claimed.ID)

	manualResult := make(chan error, 1)
	go func() {
		manualResult <- manager.ExecuteItem(context.Background(), item.ID)
	}()
	var manualErr error
	manualReturned := false
	select {
	case manualErr = <-manualResult:
		manualReturned = true
	case <-time.After(100 * time.Millisecond):
	}
	close(listener.release)
	if !manualReturned {
		manualErr = facoreCHG020Receive(t, manualResult,
			"manual admission did not return after the automatic claim barrier was released")
	}
	if manualErr == nil {
		facoreCHG020Wait(t, started, "first admitted processor did not start")
		facoreCHG020Wait(t, started, "second admitted processor did not start")
	} else {
		facoreCHG020Wait(t, started, "automatic processor did not start")
	}
	close(releaseProcessing)
	facoreCHG020Wait(t, finished, "automatic processor did not finish")
	if manualErr == nil {
		facoreCHG020Wait(t, finished, "manual processor did not finish")
	}
	facoreCHG020Wait(t, automaticDone, "automatic execution did not return")

	assert.ErrorIs(t, manualErr, database.ErrQueueItemClaimConflict,
		"manual admission must lose after the worker has claimed the row")
	assert.EqualValues(t, 1, calls.Load(), "manual and automatic paths must share one admission authority")
}

func TestFACORECHG020AutomaticClaimLosesAfterManualAdmission(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	processor := &facoreCHG020Processor{process: func(context.Context, *database.ImportQueueItem) (string, error) {
		close(started)
		<-release
		close(finished)
		return "result", nil
	}}
	manager, repo := facoreCHG020Manager(t, processor, nil)
	item := facoreCHG020AddPendingItem(t, repo, "manual-before-automatic.nzb")

	require.NoError(t, manager.ExecuteItem(context.Background(), item.ID))
	facoreCHG020Wait(t, started, "manual processor did not start")

	automaticDone := make(chan struct{})
	go func() {
		manager.processNextItem(context.Background(), 1)
		close(automaticDone)
	}()
	facoreCHG020Wait(t, automaticDone, "automatic loser did not return")

	close(release)
	facoreCHG020Wait(t, finished, "manual processor did not finish")
}

func TestFACORECHG020FinalizationRetainsAdmissionOwner(t *testing.T) {
	for _, failure := range []bool{false, true} {
		name := "success"
		if failure {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			testFACORECHG020FinalizationRetainsAdmissionOwner(t, failure)
		})
	}
}

func testFACORECHG020FinalizationRetainsAdmissionOwner(t *testing.T, failure bool) {
	var calls atomic.Int32
	var finalizerCalls atomic.Int32
	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	finalizerStarted := make(chan struct{})
	finalizerRelease := make(chan struct{})
	finalizerErr := make(chan error, 1)
	replacementStarted := make(chan struct{})
	replacementFinished := make(chan struct{})
	replacementStop := make(chan struct{})
	var firstReleaseOnce, finalizerReleaseOnce, replacementStopOnce sync.Once
	t.Cleanup(func() {
		firstReleaseOnce.Do(func() { close(firstRelease) })
		finalizerReleaseOnce.Do(func() { close(finalizerRelease) })
	})

	var repo *database.QueueRepository
	var itemID int64
	finalize := func(status database.QueueStatus) {
		err := repo.UpdateQueueItemStatus(context.Background(), itemID, status, nil)
		finalizerErr <- err
		close(finalizerStarted)
		if err == nil {
			<-finalizerRelease
		}
	}
	processor := &facoreCHG020Processor{
		process: func(ctx context.Context, _ *database.ImportQueueItem) (string, error) {
			switch calls.Add(1) {
			case 1:
				close(firstStarted)
				<-firstRelease
				if failure {
					return "", assert.AnError
				}
				return "first", nil
			case 2:
				close(replacementStarted)
				defer close(replacementFinished)
				select {
				case <-ctx.Done():
					return "", ctx.Err()
				case <-replacementStop:
					return "stopped", nil
				}
			default:
				return "", assert.AnError
			}
		},
		success: func(context.Context, *database.ImportQueueItem, string) error {
			if !failure && finalizerCalls.Add(1) == 1 {
				finalize(database.QueueStatusCompleted)
			}
			return nil
		},
		failure: func(context.Context, *database.ImportQueueItem, error) {
			if failure && finalizerCalls.Add(1) == 1 {
				finalize(database.QueueStatusFailed)
			}
		},
	}
	manager, queueRepo := facoreCHG020Manager(t, processor, nil)
	t.Cleanup(func() {
		replacementStopOnce.Do(func() { close(replacementStop) })
		select {
		case <-replacementStarted:
			select {
			case <-replacementFinished:
			case <-time.After(facoreCHG020BarrierTimeout):
			}
		default:
		}
	})
	repo = queueRepo
	item := facoreCHG020AddPendingItem(t, repo, fmt.Sprintf("finalization-owner-%t.nzb", failure))
	itemID = item.ID

	require.NoError(t, manager.ExecuteItem(context.Background(), item.ID))
	facoreCHG020Wait(t, firstStarted, "first processing owner did not start")
	firstReleaseOnce.Do(func() { close(firstRelease) })
	facoreCHG020Wait(t, finalizerStarted, "first finalizer did not publish an eligible status")
	require.NoError(t, <-finalizerErr)

	manager.cancelMu.RLock()
	_, ownerPresent := manager.cancelFuncs[item.ID]
	manager.cancelMu.RUnlock()
	assert.True(t, ownerPresent, "admission ownership must span finalization")
	assert.ErrorIs(t, manager.CancelProcessing(item.ID), ErrQueueItemNotProcessing,
		"an admission-only finalization owner must not report a delivered cancellation")
	overlapErr := manager.ExecuteItem(context.Background(), item.ID)
	if overlapErr == nil {
		facoreCHG020Wait(t, replacementStarted, "overlapping replacement did not start")
	}

	finalizerReleaseOnce.Do(func() { close(finalizerRelease) })
	if overlapErr == nil {
		require.NoError(t, manager.CancelProcessing(item.ID))
		facoreCHG020Wait(t, replacementFinished, "overlapping replacement did not finish")
	} else {
		require.Eventually(t, func() bool {
			manager.cancelMu.RLock()
			defer manager.cancelMu.RUnlock()
			_, present := manager.cancelFuncs[item.ID]
			return !present
		}, facoreCHG020BarrierTimeout, time.Millisecond)
		require.NoError(t, manager.ExecuteItem(context.Background(), item.ID))
		facoreCHG020Wait(t, replacementStarted, "post-finalization replacement did not start")
		require.NoError(t, manager.CancelProcessing(item.ID))
		facoreCHG020Wait(t, replacementFinished, "post-finalization replacement did not finish")
	}
	require.Eventually(t, func() bool {
		manager.cancelMu.RLock()
		defer manager.cancelMu.RUnlock()
		_, present := manager.cancelFuncs[item.ID]
		return !present
	}, facoreCHG020BarrierTimeout, time.Millisecond)

	assert.ErrorIs(t, overlapErr, database.ErrQueueItemClaimConflict,
		"retry admission must wait until the prior finalizer releases ownership")
	assert.EqualValues(t, 2, calls.Load())
}
