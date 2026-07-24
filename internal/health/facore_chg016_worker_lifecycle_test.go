package health

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/testsupport/fakepool"
	"github.com/stretchr/testify/require"
)

type healthLifecycleResult struct {
	err        error
	panicValue any
}

func callHealthLifecycle(fn func() error) (result healthLifecycleResult) {
	defer func() {
		result.panicValue = recover()
	}()
	result.err = fn()
	return result
}

func TestFACORECHG016HealthWorkerStopReturnsAfterInflightCycleCompletes(t *testing.T) {
	client := fakepool.New()
	releaseCheck := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseCheck) }) }
	t.Cleanup(release)
	client.BlockUntil(releaseCheck)

	env := newBatchTestEnv(t, t.TempDir(), client)
	cfg := env.hw.configGetter()
	cfg.Health.CheckIntervalSeconds = 1
	env.hw.configGetter = func() *config.Config { return cfg }

	const filePath = "movies/stop-inflight.mkv"
	writeHealthyFile(t, env, filePath)
	insertFileHealth(t, env.db, filePath, "/library/stop-inflight.mkv", 0, 3)

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	t.Cleanup(cancelWorker)
	require.NoError(t, env.hw.Start(workerCtx))
	env.hw.mu.RLock()
	firstStopSignal := env.hw.stopChan
	env.hw.mu.RUnlock()

	waitForHealthContract(t, func() bool { return client.InFlight() == 1 },
		"timed out waiting for an in-flight health cycle")

	stopResult := make(chan error, 1)
	go func() {
		stopResult <- env.hw.Stop(context.Background())
	}()

	waitForHealthContract(t, func() bool {
		return env.hw.GetStats().Status == WorkerStatusStopping
	}, "timed out waiting for the worker to enter stopping state")

	select {
	case err := <-stopResult:
		t.Fatalf("Stop returned before the in-flight cycle completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	startResult := make(chan error, 1)
	attemptCtx, cancelAttempt := context.WithCancel(context.Background())
	t.Cleanup(cancelAttempt)
	go func() {
		startResult <- env.hw.Start(attemptCtx)
	}()
	select {
	case err := <-startResult:
		require.Error(t, err, "Start must reject a generation that is still stopping")
		env.hw.mu.RLock()
		currentStopSignal := env.hw.stopChan
		env.hw.mu.RUnlock()
		require.Equal(t, firstStopSignal, currentStopSignal,
			"a rejected Start cannot replace the stopping generation")
	case <-time.After(500 * time.Millisecond):
		t.Error("Start did not promptly reject the stopping generation")
	}

	release()
	select {
	case err := <-stopResult:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return after the in-flight cycle completed")
	}

	restartCtx, cancelRestart := context.WithCancel(context.Background())
	t.Cleanup(cancelRestart)
	require.NoError(t, env.hw.Start(restartCtx))
	env.hw.mu.RLock()
	restartStopSignal := env.hw.stopChan
	env.hw.mu.RUnlock()
	require.NotEqual(t, firstStopSignal, restartStopSignal)
	require.NoError(t, env.hw.Stop(context.Background()))
}

func TestFACORECHG016HealthWorkerCanStartStopStartStop(t *testing.T) {
	env := newRepairTestEnv(t, t.TempDir(), nil)

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	t.Cleanup(cancelFirst)
	require.NoError(t, env.hw.Start(firstCtx))
	env.hw.mu.RLock()
	firstStop := env.hw.stopChan
	env.hw.mu.RUnlock()
	require.NoError(t, env.hw.Stop(context.Background()))
	cancelFirst()

	secondCtx, cancelSecond := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancelSecond()
		if env.hw.IsRunning() {
			_ = callHealthLifecycle(func() error {
				return env.hw.Stop(context.Background())
			})
		}
	})
	require.NoError(t, env.hw.Start(secondCtx))
	env.hw.mu.RLock()
	secondStop := env.hw.stopChan
	env.hw.mu.RUnlock()
	require.NotEqual(t, firstStop, secondStop, "each worker generation needs its own stop signal")
	select {
	case <-secondStop:
		t.Fatal("the restarted worker reused an already-closed stop signal")
	default:
	}

	result := callHealthLifecycle(func() error {
		return env.hw.Stop(context.Background())
	})
	require.Nil(t, result.panicValue)
	require.NoError(t, result.err)
}

func TestFACORECHG016HealthWorkerConcurrentAndRepeatedStopDoNotCloseTwice(t *testing.T) {
	env := newRepairTestEnv(t, t.TempDir(), nil)
	workerCtx, cancelWorker := context.WithCancel(context.Background())
	t.Cleanup(cancelWorker)
	require.NoError(t, env.hw.Start(workerCtx))

	const callers = 8
	start := make(chan struct{})
	results := make(chan healthLifecycleResult, callers)
	for range callers {
		go func() {
			<-start
			results <- callHealthLifecycle(func() error {
				return env.hw.Stop(context.Background())
			})
		}()
	}
	close(start)

	successes := 0
	for range callers {
		select {
		case result := <-results:
			require.Nil(t, result.panicValue)
			if result.err == nil {
				successes++
			}
		case <-time.After(2 * time.Second):
			t.Fatal("concurrent Stop call did not return")
		}
	}
	require.Equal(t, 1, successes, "exactly one caller owns the running generation")

	repeated := callHealthLifecycle(func() error {
		return env.hw.Stop(context.Background())
	})
	require.Nil(t, repeated.panicValue)
	require.Error(t, repeated.err)
}

func TestFACORECHG016HealthSystemControllerCanDisableReenableDisable(t *testing.T) {
	env := newRepairTestEnv(t, t.TempDir(), nil)
	cfg := env.hw.configGetter()
	disabled := false
	cfg.Health.Enabled = &disabled
	cfg.Health.LibrarySyncIntervalMinutes = 0
	configManager := config.NewManager(cfg, "")
	env.hw.configGetter = configManager.GetConfig
	env.healthChecker.configGetter = configManager.GetConfig

	librarySync := NewLibrarySyncWorker(
		env.metadataService,
		env.healthRepo,
		configManager.GetConfig,
		configManager,
		&MockRcloneClient{},
	)
	controller := NewHealthSystemController(env.hw, librarySync)
	controllerCtx, cancelController := context.WithCancel(context.Background())
	controller.RegisterConfigChangeHandler(controllerCtx, configManager)
	t.Cleanup(func() {
		cancelController()
		if librarySync.IsRunning() {
			librarySync.Stop(context.Background())
		}
		if env.hw.IsRunning() {
			_ = callHealthLifecycle(func() error {
				return env.hw.Stop(context.Background())
			})
		}
	})

	setEnabled := func(enabled bool) healthLifecycleResult {
		candidate := configManager.GetConfig()
		candidate.Health.Enabled = &enabled
		return callHealthLifecycle(func() error {
			return configManager.UpdateConfig(candidate)
		})
	}

	for _, enabled := range []bool{true, false, true, false} {
		result := setEnabled(enabled)
		require.Nil(t, result.panicValue, "health toggle to %t panicked", enabled)
		require.NoError(t, result.err)
		require.Equal(t, enabled, env.hw.IsRunning())
	}
}
