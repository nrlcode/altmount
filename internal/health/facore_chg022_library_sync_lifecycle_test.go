package health

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func chg022LibrarySyncConfig(intervalMinutes int) *config.Config {
	enabled := true
	cfg := config.DefaultConfig()
	cfg.Health.Enabled = &enabled
	cfg.Health.LibrarySyncIntervalMinutes = intervalMinutes
	return cfg
}

func newCHG022LibrarySyncWorker(
	t *testing.T,
	configGetter config.ConfigGetter,
) *LibrarySyncWorker {
	t.Helper()
	env := newRepairTestEnv(t, t.TempDir(), nil)
	return NewLibrarySyncWorker(
		env.metadataService,
		env.healthRepo,
		configGetter,
		nil,
		&MockRcloneClient{},
	)
}

func waitForCHG022Signal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal(message)
	}
}

func assertNoCHG022Signal(t *testing.T, signal <-chan struct{}, message string) bool {
	t.Helper()
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-signal:
		t.Error(message)
		return false
	case <-timer.C:
		return true
	}
}

func TestFACORECHG022LibrarySyncStopJoinsAndExcludesRestart(t *testing.T) {
	firstEntered := make(chan struct{})
	secondEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	releaseSecond := make(chan struct{})
	var releaseFirstOnce sync.Once
	var releaseSecondOnce sync.Once
	releaseFirstRun := func() { releaseFirstOnce.Do(func() { close(releaseFirst) }) }
	releaseSecondRun := func() { releaseSecondOnce.Do(func() { close(releaseSecond) }) }
	liveConfig := chg022LibrarySyncConfig(360)
	exitConfig := chg022LibrarySyncConfig(0)
	var configCalls atomic.Int32

	worker := newCHG022LibrarySyncWorker(t, func() *config.Config {
		switch configCalls.Add(1) {
		case 1:
			close(firstEntered)
			<-releaseFirst
			return liveConfig
		case 2:
			close(secondEntered)
			<-releaseSecond
			return exitConfig
		default:
			return exitConfig
		}
	})

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	t.Cleanup(func() {
		releaseFirstRun()
		releaseSecondRun()
		cancelWorker()
		if worker.IsRunning() {
			worker.Stop(context.Background())
		}
	})

	worker.StartLibrarySync(workerCtx)
	waitForCHG022Signal(t, firstEntered, "first library-sync generation did not enter its gate")
	require.True(t, worker.IsRunning())

	firstStopDone := make(chan struct{})
	go func() {
		worker.Stop(context.Background())
		close(firstStopDone)
	}()

	stopIsJoining := assertNoCHG022Signal(t, firstStopDone,
		"Stop returned while its current library-sync generation was still active")
	assert.True(t, worker.IsRunning(),
		"a generation must remain observable while Stop is joining it")

	worker.StartLibrarySync(workerCtx)
	noOverlap := assertNoCHG022Signal(t, secondEntered,
		"Start admitted a replacement before the stopping generation exited")

	releaseFirstRun()
	waitForCHG022Signal(t, firstStopDone, "Stop did not return after its generation exited")
	if !stopIsJoining || !noOverlap {
		releaseSecondRun()
		return
	}

	require.False(t, worker.IsRunning())
	require.Error(t, worker.TriggerManualSync(context.Background()))

	worker.StartLibrarySync(workerCtx)
	waitForCHG022Signal(t, secondEntered, "replacement library-sync generation did not start")
	require.True(t, worker.IsRunning())
	require.NoError(t, worker.TriggerManualSync(context.Background()),
		"the current non-stopping generation must accept its own trigger")

	secondStopDone := make(chan struct{})
	go func() {
		worker.Stop(context.Background())
		close(secondStopDone)
	}()
	assertNoCHG022Signal(t, secondStopDone,
		"replacement Stop returned before the replacement generation exited")
	releaseSecondRun()
	waitForCHG022Signal(t, secondStopDone,
		"replacement Stop did not return after the replacement generation exited")
	require.False(t, worker.IsRunning())
	require.Error(t, worker.TriggerManualSync(context.Background()))
}

func TestFACORECHG022ManualTriggerDoesNotCrossGenerations(t *testing.T) {
	firstEntered := make(chan struct{})
	secondEntered := make(chan struct{})
	syncEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	releaseSync := make(chan struct{})
	var releaseFirstOnce sync.Once
	var releaseSyncOnce sync.Once
	releaseFirstRun := func() { releaseFirstOnce.Do(func() { close(releaseFirst) }) }
	releaseSyncRun := func() { releaseSyncOnce.Do(func() { close(releaseSync) }) }
	exitConfig := chg022LibrarySyncConfig(0)
	liveConfig := chg022LibrarySyncConfig(360)
	var configCalls atomic.Int32

	worker := newCHG022LibrarySyncWorker(t, func() *config.Config {
		switch configCalls.Add(1) {
		case 1:
			close(firstEntered)
			<-releaseFirst
			return exitConfig
		case 2:
			close(secondEntered)
			return liveConfig
		case 3:
			close(syncEntered)
			<-releaseSync
			return liveConfig
		default:
			return liveConfig
		}
	})

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	t.Cleanup(func() {
		releaseFirstRun()
		releaseSyncRun()
		cancelWorker()
		if worker.IsRunning() {
			worker.Stop(context.Background())
		}
	})

	worker.StartLibrarySync(workerCtx)
	waitForCHG022Signal(t, firstEntered, "first library-sync generation did not enter its gate")
	require.NoError(t, worker.TriggerManualSync(context.Background()))
	releaseFirstRun()
	waitForHealthContract(t, func() bool { return !worker.IsRunning() },
		"first library-sync generation did not retire")

	worker.StartLibrarySync(workerCtx)
	waitForCHG022Signal(t, secondEntered, "replacement library-sync generation did not start")
	if !assertNoCHG022Signal(t, syncEntered,
		"a trigger queued for the retired generation was consumed by its replacement") {
		releaseSyncRun()
		return
	}

	require.NoError(t, worker.TriggerManualSync(context.Background()))
	waitForCHG022Signal(t, syncEntered,
		"the replacement generation did not consume its own manual trigger")
	releaseSyncRun()
	worker.Stop(context.Background())
	require.False(t, worker.IsRunning())
}
