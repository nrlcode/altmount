package health

import (
	"context"
	"io"
	"log/slog"
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

type chg022LogGateState struct {
	message string
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

type chg022LogGateHandler struct {
	next  slog.Handler
	state *chg022LogGateState
}

func (h *chg022LogGateHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *chg022LogGateHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Message == h.state.message {
		h.state.once.Do(func() { close(h.state.entered) })
		<-h.state.release
	}
	return h.next.Handle(ctx, record)
}

func (h *chg022LogGateHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &chg022LogGateHandler{next: h.next.WithAttrs(attrs), state: h.state}
}

func (h *chg022LogGateHandler) WithGroup(name string) slog.Handler {
	return &chg022LogGateHandler{next: h.next.WithGroup(name), state: h.state}
}

func newCHG022SyncWorker(
	t *testing.T,
	cfg *config.Config,
	configGetter config.ConfigGetter,
) (*LibrarySyncWorker, *repairTestEnv) {
	t.Helper()
	env := newRepairTestEnv(t, cfg.Metadata.RootPath, nil)
	for _, path := range []string{
		"complete/chg022-a.mkv",
		"complete/chg022-b.mkv",
		"complete/chg022-c.mkv",
		"complete/chg022-d.mkv",
	} {
		writeHealthyFile(t, env, path)
	}
	return NewLibrarySyncWorker(
		env.metadataService,
		env.healthRepo,
		configGetter,
		nil,
		&MockRcloneClient{},
	), env
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
	assert.Error(t, worker.TriggerManualSync(context.Background()),
		"a stopping generation must not accept work it cannot guarantee to consume")

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

func TestFACORECHG022MetadataOnlyCancellationJoinsWorkerPool(t *testing.T) {
	root := t.TempDir()
	cfg := chg022LibrarySyncConfig(360)
	cfg.Metadata.RootPath = root
	cfg.Import.ImportStrategy = config.ImportStrategyNone
	cfg.Health.LibraryDir = nil
	cfg.Health.LibrarySyncConcurrency = 1

	secondChildEntered := make(chan struct{})
	thirdChildEntered := make(chan struct{})
	releaseSecondChild := make(chan struct{})
	releaseThirdChild := make(chan struct{})
	var releaseSecondOnce sync.Once
	var releaseThirdOnce sync.Once
	releaseSecond := func() { releaseSecondOnce.Do(func() { close(releaseSecondChild) }) }
	releaseThird := func() { releaseThirdOnce.Do(func() { close(releaseThirdChild) }) }
	var configCalls atomic.Int32

	worker, _ := newCHG022SyncWorker(t, cfg, func() *config.Config {
		switch configCalls.Add(1) {
		case 10:
			close(secondChildEntered)
			<-releaseSecondChild
		case 11:
			close(thirdChildEntered)
			<-releaseThirdChild
		}
		return cfg
	})

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	t.Cleanup(func() {
		releaseSecond()
		releaseThird()
		cancelWorker()
		if worker.IsRunning() {
			worker.Stop(context.Background())
		}
	})

	worker.StartLibrarySync(workerCtx)
	require.NoError(t, worker.TriggerManualSync(context.Background()))
	waitForCHG022Signal(t, secondChildEntered,
		"metadata-only sync did not enter its second worker task")
	// The single pool worker is now held by child two. Give the submitting
	// goroutine one bounded scheduling window to block while admitting child
	// three before cancellation is published.
	time.Sleep(50 * time.Millisecond)

	stopDone := make(chan struct{})
	go func() {
		worker.Stop(context.Background())
		close(stopDone)
	}()
	releaseSecond()
	waitForCHG022Signal(t, thirdChildEntered,
		"metadata-only sync did not admit its third worker task after cancellation")
	assertNoCHG022Signal(t, stopDone,
		"Stop returned while a metadata-only generation worker remained active")

	releaseThird()
	waitForCHG022Signal(t, stopDone,
		"Stop did not return after the metadata-only worker pool completed")
}

func TestFACORECHG022FullSyncCancellationJoinsResultConsumer(t *testing.T) {
	root := t.TempDir()
	libraryDir := t.TempDir()
	cfg := chg022LibrarySyncConfig(360)
	cfg.Metadata.RootPath = root
	cfg.Import.ImportStrategy = config.ImportStrategyNone
	cfg.Health.LibraryDir = &libraryDir
	cfg.Health.LibrarySyncConcurrency = 1

	secondChildEntered := make(chan struct{})
	thirdChildEntered := make(chan struct{})
	consumerEntered := make(chan struct{})
	releaseSecondChild := make(chan struct{})
	releaseThirdChild := make(chan struct{})
	releaseConsumer := make(chan struct{})
	var releaseSecondOnce sync.Once
	var releaseThirdOnce sync.Once
	var releaseConsumerOnce sync.Once
	releaseSecond := func() { releaseSecondOnce.Do(func() { close(releaseSecondChild) }) }
	releaseThird := func() { releaseThirdOnce.Do(func() { close(releaseThirdChild) }) }
	releaseResultConsumer := func() { releaseConsumerOnce.Do(func() { close(releaseConsumer) }) }

	previousLogger := slog.Default()
	gate := &chg022LogGateState{
		message: "Failed to batch add automatic health checks",
		entered: consumerEntered,
		release: releaseConsumer,
	}
	slog.SetDefault(slog.New(&chg022LogGateHandler{
		next:  slog.NewTextHandler(io.Discard, nil),
		state: gate,
	}))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	var configCalls atomic.Int32
	worker, _ := newCHG022SyncWorker(t, cfg, func() *config.Config {
		switch configCalls.Add(1) {
		case 12:
			close(secondChildEntered)
			<-releaseSecondChild
		case 14:
			close(thirdChildEntered)
			<-releaseThirdChild
		}
		return cfg
	})

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	t.Cleanup(func() {
		releaseSecond()
		releaseThird()
		releaseResultConsumer()
		cancelWorker()
		if worker.IsRunning() {
			worker.Stop(context.Background())
		}
	})

	worker.StartLibrarySync(workerCtx)
	require.NoError(t, worker.TriggerManualSync(context.Background()))
	waitForCHG022Signal(t, secondChildEntered,
		"full sync did not enter its second metadata worker task")
	// As above, make the p.Go admission boundary deterministic before Stop can
	// make the next loop-level cancellation check ready.
	time.Sleep(50 * time.Millisecond)

	stopDone := make(chan struct{})
	go func() {
		worker.Stop(context.Background())
		close(stopDone)
	}()
	releaseSecond()
	waitForCHG022Signal(t, thirdChildEntered,
		"full sync did not admit its third metadata worker task after cancellation")
	releaseThird()
	waitForCHG022Signal(t, consumerEntered,
		"full sync result consumer did not enter its cancellation flush")
	assertNoCHG022Signal(t, stopDone,
		"Stop returned while the full-sync result consumer remained active")

	releaseResultConsumer()
	waitForCHG022Signal(t, stopDone,
		"Stop did not return after the full-sync result consumer completed")
}

func TestFACORECHG022FullSyncPanicJoinsResultConsumer(t *testing.T) {
	root := t.TempDir()
	libraryDir := t.TempDir()
	cfg := chg022LibrarySyncConfig(360)
	cfg.Metadata.RootPath = root
	cfg.Import.ImportStrategy = config.ImportStrategyNone
	cfg.Health.LibraryDir = &libraryDir
	cfg.Health.LibrarySyncConcurrency = 1

	panicReported := make(chan struct{})
	logRelease := make(chan struct{})
	close(logRelease)
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(&chg022LogGateHandler{
		next: slog.NewTextHandler(io.Discard, nil),
		state: &chg022LogGateState{
			message: "Panic in library sync",
			entered: panicReported,
			release: logRelease,
		},
	}))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	var configCalls atomic.Int32
	worker, env := newCHG022SyncWorker(t, cfg, func() *config.Config {
		if configCalls.Add(1) == 12 {
			panic("FACORE CHG-022 worker panic")
		}
		return cfg
	})

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancelWorker()
		if worker.IsRunning() {
			worker.Stop(context.Background())
		}
	})

	worker.StartLibrarySync(workerCtx)
	require.NoError(t, worker.TriggerManualSync(context.Background()))
	waitForCHG022Signal(t, panicReported,
		"full sync did not report its metadata-worker panic")
	worker.Stop(context.Background())

	records, err := env.healthRepo.GetAllHealthCheckRecords(context.Background())
	require.NoError(t, err)
	assert.Len(t, records, 3,
		"the result consumer must drain and exit before a worker panic is recovered")
}
