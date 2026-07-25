package queue

import (
	"context"
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
	require.Error(t, err,
		"cancellation must distinguish a missing runtime owner from a delivered cancellation")
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

	deadline := time.NewTimer(facoreCHG020BarrierTimeout)
	ticker := time.NewTicker(time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	for {
		stored, err := repo.GetQueueItem(context.Background(), item.ID)
		require.NoError(t, err)
		if stored.Status == database.QueueStatusProcessing {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			manager.cancelMu.Unlock()
			registryLocked = false
			t.Fatal("ExecuteItem did not reach durable admission")
		}
	}

	returnedWhileRegistryLocked := false
	select {
	case err := <-executeResult:
		returnedWhileRegistryLocked = true
		require.NoError(t, err)
	case <-time.After(100 * time.Millisecond):
	}
	manager.cancelMu.Unlock()
	registryLocked = false

	if !returnedWhileRegistryLocked {
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

	assert.Error(t, secondErr, "an already-processing row must not receive a second manual owner")
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

	assert.Error(t, manualErr, "manual admission must lose after the worker has claimed the row")
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

func TestFACORECHG020StaleTeardownDoesNotEraseReplacementOwner(t *testing.T) {
	var calls atomic.Int32
	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	firstHandled := make(chan struct{})
	firstFinalizeErr := make(chan error, 1)
	firstFinalizeRelease := make(chan struct{})
	secondStarted := make(chan struct{})
	secondCancelled := make(chan struct{})
	secondStop := make(chan struct{})
	secondFinished := make(chan struct{})
	var secondStopOnce sync.Once
	t.Cleanup(func() { secondStopOnce.Do(func() { close(secondStop) }) })

	var repo *database.QueueRepository
	var itemID int64
	processor := &facoreCHG020Processor{
		process: func(ctx context.Context, _ *database.ImportQueueItem) (string, error) {
			switch calls.Add(1) {
			case 1:
				close(firstStarted)
				<-firstRelease
				return "first", nil
			case 2:
				close(secondStarted)
				defer close(secondFinished)
				select {
				case <-ctx.Done():
					close(secondCancelled)
					return "", ctx.Err()
				case <-secondStop:
					return "second", nil
				}
			default:
				return "", assert.AnError
			}
		},
		success: func(_ context.Context, _ *database.ImportQueueItem, path string) error {
			if path == "first" {
				err := repo.UpdateQueueItemStatus(
					context.Background(), itemID, database.QueueStatusCompleted, nil,
				)
				firstFinalizeErr <- err
				close(firstHandled)
				if err != nil {
					return err
				}
				<-firstFinalizeRelease
			}
			return nil
		},
	}
	manager, queueRepo := facoreCHG020Manager(t, processor, nil)
	repo = queueRepo
	item := facoreCHG020AddPendingItem(t, repo, "replacement-owner.nzb")
	itemID = item.ID

	require.NoError(t, manager.ExecuteItem(context.Background(), item.ID))
	facoreCHG020Wait(t, firstStarted, "first processing owner did not start")

	close(firstRelease)
	facoreCHG020Wait(t, firstHandled, "first owner did not publish its completed state")
	require.NoError(t, <-firstFinalizeErr)
	manager.cancelMu.RLock()
	_, staleOwnerStillRegistered := manager.cancelFuncs[item.ID]
	manager.cancelMu.RUnlock()
	assert.False(t, staleOwnerStillRegistered,
		"the completed row must not become retry-eligible while its old runtime owner remains registered")

	// The completed row can now be retried while the old finalizer is still
	// unwinding. Its deferred teardown must not erase the replacement identity.
	require.NoError(t, manager.ExecuteItem(context.Background(), item.ID))
	facoreCHG020Wait(t, secondStarted, "replacement processing owner did not start")
	close(firstFinalizeRelease)

	// On the defective base, the stale owner's identity-blind defer deletes the
	// replacement entry. Polling for that transition also provides a barrier that
	// the stale teardown has run; a corrected registry deliberately stays present.
	registryDeadline := time.NewTimer(100 * time.Millisecond)
	registryTicker := time.NewTicker(time.Millisecond)
	for registryPresent := true; registryPresent; {
		manager.cancelMu.RLock()
		_, registryPresent = manager.cancelFuncs[item.ID]
		manager.cancelMu.RUnlock()
		if !registryPresent {
			break
		}
		select {
		case <-registryTicker.C:
		case <-registryDeadline.C:
			registryPresent = false
		}
	}
	registryTicker.Stop()
	registryDeadline.Stop()

	cancelErr := manager.CancelProcessing(item.ID)
	assert.NoError(t, cancelErr, "replacement owner must remain cancellable")
	select {
	case <-secondCancelled:
	case <-time.After(100 * time.Millisecond):
		t.Error("stale teardown erased the replacement cancellation owner")
	}
	secondStopOnce.Do(func() { close(secondStop) })
	facoreCHG020Wait(t, secondFinished, "replacement processor did not finish")
	assert.EqualValues(t, 2, calls.Load(), "the stale-owner scenario must create exactly one replacement")
}
