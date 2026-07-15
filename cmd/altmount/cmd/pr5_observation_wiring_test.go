package cmd

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/stretchr/testify/require"
)

type recordingObservationService struct {
	events   *[]string
	running  bool
	startErr error
	stopErr  error
	starts   int
	stops    int
}

var errRecordingObservationServiceRunning = errors.New("synthetic observation service already running")

func (s *recordingObservationService) Start(context.Context) error {
	if s.running {
		return errRecordingObservationServiceRunning
	}
	s.starts++
	*s.events = append(*s.events, "observation-start")
	if s.startErr != nil {
		return s.startErr
	}
	s.running = true
	return nil
}

func (s *recordingObservationService) Stop(context.Context) error {
	return s.StopAndWait(context.Background())
}

func (s *recordingObservationService) StopAndWait(context.Context) error {
	if !s.running {
		return nil
	}
	s.stops++
	*s.events = append(*s.events, "observation-stop")
	if s.stopErr != nil {
		return s.stopErr
	}
	s.running = false
	return nil
}

type recordingLibrarySync struct {
	events   *[]string
	running  bool
	starts   int
	stops    int
	startErr error
	stopErr  error
}

func (s *recordingLibrarySync) StartLibrarySync(context.Context) {
	_ = s.StartLibrarySyncChecked(context.Background())
}

func (s *recordingLibrarySync) StartLibrarySyncChecked(context.Context) error {
	s.starts++
	*s.events = append(*s.events, "library-start")
	if s.startErr != nil {
		return s.startErr
	}
	s.running = true
	return nil
}

func (s *recordingLibrarySync) StopAndWait(context.Context) error {
	if !s.running {
		return nil
	}
	s.stops++
	*s.events = append(*s.events, "library-stop")
	if s.stopErr != nil {
		return s.stopErr
	}
	s.running = false
	return nil
}

func (s *recordingLibrarySync) IsRunning() bool { return s.running }

func TestPR5ObservationRuntimeTransitionsOnlyObservationAndDiscovery(t *testing.T) {
	events := []string{}
	service := &recordingObservationService{events: &events}
	library := &recordingLibrarySync{events: &events}
	runtime := &healthObservationRuntime{service: service, library: library}

	require.NoError(t, runtime.setEnabled(context.Background(), true))
	require.NoError(t, runtime.setEnabled(context.Background(), true), "enable must be idempotent")
	require.Equal(t, []string{"observation-start", "library-start"}, events)

	require.NoError(t, runtime.setEnabled(context.Background(), false))
	require.NoError(t, runtime.setEnabled(context.Background(), false), "disable must be idempotent")
	require.Equal(t, []string{
		"observation-start", "library-start", "library-stop", "observation-stop",
	}, events, "discovery must stop before the observation worker")
	require.Equal(t, 1, service.starts)
	require.Equal(t, 1, service.stops)
	require.Equal(t, 1, library.starts)
	require.Equal(t, 1, library.stops)
}

func TestPR5ObservationRuntimeDoesNotStartDiscoveryAfterServiceFailure(t *testing.T) {
	events := []string{}
	service := &recordingObservationService{events: &events, startErr: errors.New("synthetic start failure")}
	library := &recordingLibrarySync{events: &events}
	runtime := &healthObservationRuntime{service: service, library: library}

	require.Error(t, runtime.setEnabled(context.Background(), true))
	require.Equal(t, []string{"observation-start"}, events)
	require.Zero(t, library.starts)
	require.False(t, runtime.enabled)
}

func TestPR5ObservationRuntimeRejectsCanceledEnableContext(t *testing.T) {
	events := []string{}
	service := &recordingObservationService{events: &events}
	library := &recordingLibrarySync{events: &events}
	runtime := &healthObservationRuntime{service: service, library: library}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, runtime.setEnabled(ctx, true), context.Canceled)
	require.Empty(t, events)
	require.False(t, runtime.enabled)
}

func TestPR5ObservationRuntimeStillStopsServiceWhenDiscoveryJoinFails(t *testing.T) {
	events := []string{}
	service := &recordingObservationService{events: &events, running: true}
	library := &recordingLibrarySync{
		events: &events, running: true, stopErr: errors.New("synthetic discovery timeout"),
	}
	runtime := &healthObservationRuntime{service: service, library: library, enabled: true}

	require.Error(t, runtime.setEnabled(context.Background(), false))
	require.Equal(t, []string{"library-stop", "observation-stop"}, events)
	require.False(t, runtime.enabled)
}

func TestPR5ObservationRuntimeJoinsTimedOutGenerationBeforeReenable(t *testing.T) {
	events := []string{}
	service := &recordingObservationService{events: &events, running: true}
	library := &recordingLibrarySync{
		events: &events, running: true, stopErr: context.DeadlineExceeded,
	}
	runtime := &healthObservationRuntime{service: service, library: library, enabled: true}

	require.ErrorIs(t, runtime.setEnabled(context.Background(), false), context.DeadlineExceeded)
	require.True(t, library.running,
		"a timed-out join must keep ownership of the residual generation")
	require.False(t, runtime.enabled)

	// Model the canceled generation becoming joinable after the timeout. A
	// re-enable must join it before starting the service and exactly one fresh
	// discovery generation.
	library.stopErr = nil
	require.NoError(t, runtime.setEnabled(context.Background(), true))
	require.Equal(t, []string{
		"library-stop", "observation-stop",
		"library-stop", "observation-start", "library-start",
	}, events)
	require.Equal(t, 2, library.stops)
	require.Equal(t, 1, library.starts)
	require.Equal(t, 1, service.starts)
	require.True(t, runtime.enabled)
}

func TestPR5ObservationRuntimeRollsBackServiceWhenDiscoveryStartFails(t *testing.T) {
	events := []string{}
	service := &recordingObservationService{events: &events}
	library := &recordingLibrarySync{
		events: &events, startErr: errors.New("synthetic discovery start failure"),
	}
	runtime := &healthObservationRuntime{service: service, library: library}

	err := runtime.setEnabled(context.Background(), true)
	require.ErrorContains(t, err, "synthetic discovery start failure")
	require.Equal(t, []string{
		"observation-start", "library-start", "observation-stop",
	}, events)
	require.Equal(t, 1, service.starts)
	require.Equal(t, 1, service.stops)
	require.False(t, runtime.enabled)
}

func TestPR5ObservationRuntimeJoinsTimedOutServiceBeforeReenable(t *testing.T) {
	events := []string{}
	service := &recordingObservationService{
		events: &events, running: true, stopErr: context.DeadlineExceeded,
	}
	library := &recordingLibrarySync{events: &events, running: true}
	runtime := &healthObservationRuntime{service: service, library: library, enabled: true}

	require.ErrorIs(t, runtime.setEnabled(context.Background(), false), context.DeadlineExceeded)
	require.True(t, service.running,
		"a timed-out join must keep ownership of the residual service generation")
	require.False(t, runtime.enabled)

	service.stopErr = nil
	require.NoError(t, runtime.setEnabled(context.Background(), true))
	require.Equal(t, []string{
		"library-stop", "observation-stop",
		"observation-stop", "observation-start", "library-start",
	}, events)
	require.Equal(t, 2, service.stops)
	require.Equal(t, 1, service.starts)
	require.Equal(t, 1, library.starts)
	require.True(t, runtime.enabled)
}

func TestPR5ObservationRuntimeShutdownJoinsResidualDisabledGenerations(t *testing.T) {
	events := []string{}
	service := &recordingObservationService{
		events: &events, running: true, stopErr: context.DeadlineExceeded,
	}
	library := &recordingLibrarySync{
		events: &events, running: true, stopErr: context.DeadlineExceeded,
	}
	runtime := &healthObservationRuntime{service: service, library: library, enabled: true}

	require.ErrorIs(t, runtime.setEnabled(context.Background(), false), context.DeadlineExceeded)
	require.False(t, runtime.enabled)
	require.True(t, service.running)
	require.True(t, library.running)

	service.stopErr = nil
	library.stopErr = nil
	require.NoError(t, runtime.stop(context.Background()),
		"shutdown must join residual generations even though the logical flag is already disabled")
	require.False(t, service.running)
	require.False(t, library.running)
	require.Equal(t, []string{
		"library-stop", "observation-stop",
		"library-stop", "observation-stop",
	}, events)
	require.Equal(t, 2, service.stops)
	require.Equal(t, 2, library.stops)

	require.NoError(t, runtime.stop(context.Background()), "fully stopped shutdown remains idempotent")
	require.Equal(t, 2, service.stops)
	require.Equal(t, 2, library.stops)
}

func TestPR5ObservationConfigHandlerTransitionsDurableRuntime(t *testing.T) {
	events := []string{}
	service := &recordingObservationService{events: &events}
	library := &recordingLibrarySync{events: &events}
	runtime := &healthObservationRuntime{service: service, library: library}
	initial := config.DefaultConfig()
	manager := config.NewManager(initial, filepath.Join(t.TempDir(), "config.yaml"))
	runtime.registerConfigChangeHandler(context.Background(), manager)

	enabled := initial.DeepCopy()
	on := true
	enabled.Health.Enabled = &on
	require.NoError(t, manager.UpdateConfig(enabled))

	disabled := enabled.DeepCopy()
	off := false
	disabled.Health.Enabled = &off
	require.NoError(t, manager.UpdateConfig(disabled))
	require.Equal(t, []string{
		"observation-start", "library-start", "library-stop", "observation-stop",
	}, events)
}

func TestPR5ObservationConfigUsesBoundedOperatorSettings(t *testing.T) {
	cfg := config.DefaultConfig()
	libraryDir := "/library"
	cfg.Health.LibraryDir = &libraryDir
	cfg.Health.MaxConcurrentJobs = 3
	cfg.Health.MaxConnectionsForHealthChecks = 17
	cfg.Health.CheckBatchSize = 19
	cfg.Health.CheckIntervalSeconds = 7
	cfg.Health.GapConfirmationDelayMinutes = 23
	cfg.Health.MaxRetries = 5
	cfg.Import.ImportStrategy = config.ImportStrategySYMLINK

	settings := observationServiceConfig(cfg)
	require.Equal(t, observationWorkerOwner, settings.Owner)
	require.True(t, strings.HasPrefix(settings.Owner, "altmount-health-observation-"))
	require.Equal(t, 3, settings.WorkerCount)
	require.Equal(t, 17, settings.GlobalConcurrency)
	require.Equal(t, 17, settings.PerProviderConcurrency)
	require.Equal(t, 17, settings.STATConcurrency)
	require.Equal(t, 19, settings.ScheduleBatchSize)
	require.Equal(t, 7*time.Second, settings.ScheduleInterval)
	require.Equal(t, 23*time.Minute, settings.ConfirmationDelay)
	require.Equal(t, string(config.ImportStrategySYMLINK), settings.HealthStrategy)
	require.Equal(t, libraryDir, settings.LibraryDir)
	require.Equal(t, 5, settings.MaxRetries)
	require.True(t, settings.PauseDuringPlayback)
}

func TestPR5ProviderRegistryBackfillsEmptyConfigIDsBeforePoolConstruction(t *testing.T) {
	enabled := true
	disabled := false
	cfg := config.DefaultConfig()
	cfg.Providers = []config.ProviderConfig{
		{
			Name: "Generated", Host: "generated.invalid", Port: 119,
			Username: "synthetic-account", MaxConnections: 1, Enabled: &enabled,
		},
		{
			Name: "Disabled", Host: "disabled.invalid", Port: 119,
			Username: "synthetic-disabled", MaxConnections: 1, Enabled: &disabled,
		},
		{
			ID: "legacy-provider", Name: "Legacy", Host: "legacy.invalid", Port: 119,
			Username: "synthetic-legacy", MaxConnections: 1, Enabled: &enabled,
		},
	}
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	manager := config.NewManager(cfg, configPath)
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "registry.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	repository := database.NewHealthStateRepository(db.Connection(), database.DialectSQLite)

	backfilled, err := reconcileHealthProviderConfig(
		context.Background(), cfg, repository, manager,
	)
	require.NoError(t, err)
	require.NoError(t, uuid.Validate(backfilled.Providers[0].ID))
	require.NoError(t, uuid.Validate(backfilled.Providers[1].ID))
	require.Equal(t, "legacy-provider", backfilled.Providers[2].ID)
	require.Equal(t, backfilled.Providers[0].ID, backfilled.ToNNTPProviders()[0].ID,
		"the pool must be built with the durable provider ID")
	require.NotContains(t, backfilled.Providers[0].ID, "synthetic-account")

	reloaded, err := config.LoadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, backfilled.Providers[0].ID, reloaded.Providers[0].ID,
		"the generated durable ID must survive process restart")
	require.Equal(t, backfilled.Providers[1].ID, reloaded.Providers[1].ID)

	providerID := backfilled.Providers[0].ID
	changed := backfilled.DeepCopy()
	changed.Providers[0].Host = "generated-new.invalid"
	require.NoError(t, manager.UpdateConfig(changed))
	reconciled, err := reconcileHealthProviderConfig(
		context.Background(), changed, repository, manager,
	)
	require.NoError(t, err)
	require.Equal(t, providerID, reconciled.Providers[0].ID)
	generations, err := repository.ListProviderGenerations(context.Background(), providerID)
	require.NoError(t, err)
	require.Len(t, generations, 2)
}

func TestPR5ObservationLibraryGetterClampsDestructiveLegacySettings(t *testing.T) {
	cfg := config.DefaultConfig()
	enabled := true
	cfg.Health.Enabled = &enabled
	cfg.Health.CleanupOrphanedMetadata = &enabled
	cfg.Health.Repair.Enabled = &enabled
	cfg.Health.CorruptionAction = "delete"

	safe := observationLibraryConfigGetter(func() *config.Config { return cfg })()
	require.NotSame(t, cfg, safe)
	require.True(t, cfg.GetHealthDeleteOnCorruption(), "source config must remain unchanged")
	require.True(t, *cfg.Health.CleanupOrphanedMetadata)
	require.True(t, *cfg.Health.Repair.Enabled)
	require.True(t, safe.GetHealthEnabled(), "discovery follows the health enable switch")
	require.False(t, *safe.Health.CleanupOrphanedMetadata)
	require.False(t, *safe.Health.Repair.Enabled)
	require.False(t, safe.GetHealthDeleteOnCorruption())
}

func TestPR5ObservationRestartBoundaryExcludesLiveProviderChanges(t *testing.T) {
	oldConfig := config.DefaultConfig()
	newConfig := oldConfig.DeepCopy()
	newConfig.Providers = append(newConfig.Providers, config.ProviderConfig{ID: "provider-a"})
	require.False(t, observationRuntimeRestartRequired(oldConfig, newConfig),
		"provider reconciliation reads the live config getter")

	newConfig.Health.GapConfirmationDelayMinutes++
	require.True(t, observationRuntimeRestartRequired(oldConfig, newConfig),
		"captured bounded worker settings require restart")
}

func TestPR5SetupRepositoriesIncludesDurableHealthState(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type:         "sqlite",
		DatabasePath: filepath.Join(t.TempDir(), "wiring.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	repositories := setupRepositories(context.Background(), db)
	require.NotNil(t, repositories.MainRepo)
	require.NotNil(t, repositories.HealthRepo)
	require.NotNil(t, repositories.StateRepo)
	require.NotNil(t, repositories.UserRepo)
	require.NoError(t, repositories.StateRepo.SetGapConfirmationMinimumDelay(11*time.Minute))
}

func TestPR5SetupAPIServerInjectsDurableRunsBeforeRequests(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type:         "sqlite",
		DatabasePath: filepath.Join(t.TempDir(), "api-wiring.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	repositories := setupRepositories(context.Background(), db)
	cfg := config.DefaultConfig()
	cfg.API.AllowedOrigins = []string{"http://localhost"}
	manager := config.NewManager(cfg, filepath.Join(t.TempDir(), "config.yaml"))
	metadataService, metadataReader := initializeMetadata(cfg)
	app, _ := createFiberApp(context.Background(), cfg)
	t.Cleanup(func() { require.NoError(t, app.Shutdown()) })

	setupAPIServer(
		app, repositories, nil, manager, metadataReader, metadataService,
		nil, nil, nil, nil, nil, nil, nil, nil,
	)
	response, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/health/runs", nil))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode,
		"a missing application injection would return service unavailable")
	require.NoError(t, response.Body.Close())
}
