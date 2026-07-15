package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-pkgz/auth/v2/token"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	fLogger "github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/google/uuid"
	"github.com/javi11/altmount/internal/api"
	"github.com/javi11/altmount/internal/arrs"
	"github.com/javi11/altmount/internal/auth"
	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/health"
	"github.com/javi11/altmount/internal/httpclient"
	"github.com/javi11/altmount/internal/importer"
	"github.com/javi11/altmount/internal/metadata"
	"github.com/javi11/altmount/internal/nzbfilesystem"
	"github.com/javi11/altmount/internal/nzbfilesystem/segcache"
	"github.com/javi11/altmount/internal/pool"
	"github.com/javi11/altmount/internal/progress"
	"github.com/javi11/altmount/internal/rclone"
	"github.com/javi11/altmount/internal/webdav"
	"github.com/javi11/altmount/pkg/rclonecli"
)

// repositorySet holds all database repositories
type repositorySet struct {
	MainRepo   *database.Repository
	HealthRepo *database.HealthRepository
	StateRepo  *database.HealthStateRepository
	UserRepo   *database.UserRepository
}

// initializeDatabase creates and initializes the database
func initializeDatabase(ctx context.Context, cfg *config.Config) (*database.DB, error) {
	dbConfig := database.Config{
		Type:         cfg.Database.Type,
		DatabasePath: cfg.Database.Path,
		DSN:          cfg.Database.DSN,
	}

	db, err := database.NewDB(dbConfig)
	if err != nil {
		slog.ErrorContext(ctx, "failed to initialize database", "err", err)
		return nil, err
	}

	return db, nil
}

// initializeMetadata creates metadata service and reader
func initializeMetadata(cfg *config.Config) (*metadata.MetadataService, *metadata.MetadataReader) {
	metadataService := metadata.NewMetadataService(cfg.Metadata.RootPath)
	metadataReader := metadata.NewMetadataReader(metadataService)
	return metadataService, metadataReader
}

// initializeImporter creates and starts the importer service
func initializeImporter(
	ctx context.Context,
	cfg *config.Config,
	metadataService *metadata.MetadataService,
	db *database.DB,
	poolManager pool.Manager,
	rcloneClient rclonecli.RcloneRcClient,
	configGetter config.ConfigGetter,
	broadcaster *progress.ProgressBroadcaster,
	userRepo *database.UserRepository,
	healthRepo *database.HealthRepository,
) (*importer.Service, error) {
	// Set defaults for workers if not configured
	maxProcessorWorkers := cfg.Import.MaxProcessorWorkers
	if maxProcessorWorkers <= 0 {
		maxProcessorWorkers = 2 // Default: 2 parallel workers
	}

	serviceConfig := importer.ServiceConfig{
		Workers: maxProcessorWorkers,
	}

	importerService, err := importer.NewService(serviceConfig, metadataService, db, poolManager, rcloneClient, configGetter, healthRepo, broadcaster, userRepo)
	if err != nil {
		slog.ErrorContext(ctx, "failed to create importer service", "err", err)
		return nil, err
	}

	// Start importer service
	if err := importerService.Start(ctx); err != nil {
		slog.ErrorContext(ctx, "failed to start importer service", "err", err)
		return nil, err
	}

	return importerService, nil
}

// initializeFilesystem creates the NZB filesystem with health tracking
func initializeFilesystem(
	ctx context.Context,
	metadataService *metadata.MetadataService,
	healthRepo *database.HealthRepository,
	arrsService *arrs.Service,
	rcloneClient rclonecli.RcloneRcClient,
	poolManager pool.Manager,
	configGetter config.ConfigGetter,
	streamTracker nzbfilesystem.StreamTracker,
	cacheSource *segcache.Source,
) *nzbfilesystem.NzbFilesystem {
	// Reset all in-progress file health checks on start up
	if err := healthRepo.ResetFileAllChecking(ctx); err != nil {
		slog.ErrorContext(ctx, "failed to reset in progress file health", "err", err)
	}

	// Create metadata-based remote file handler
	metadataRemoteFile := nzbfilesystem.NewMetadataRemoteFile(
		metadataService,
		healthRepo,
		arrsService,
		rcloneClient,
		poolManager,
		configGetter,
		streamTracker,
		cacheSource,
	)

	// Create filesystem backed by metadata
	return nzbfilesystem.NewNzbFilesystem(metadataRemoteFile)
}

// setupNNTPPool initializes the NNTP connection pool
func setupNNTPPool(ctx context.Context, cfg *config.Config, poolManager pool.Manager) error {
	if len(cfg.Providers) > 0 {
		providers := cfg.ToNNTPProviders()
		if err := poolManager.SetProviders(providers); err != nil {
			slog.ErrorContext(ctx, "failed to create initial NNTP pool", "err", err)
			return err
		}
		slog.InfoContext(ctx, "NNTP connection pool initialized", "provider_count", len(cfg.Providers))
	} else {
		slog.InfoContext(ctx, "Starting server without NNTP providers - configure via API to enable downloads")
	}

	return nil
}

// setupRCloneClient creates an RClone client if enabled
func setupRCloneClient(ctx context.Context, cfg *config.Config, configManager *config.Manager) rclonecli.RcloneRcClient {
	if cfg.RClone.RCEnabled != nil && *cfg.RClone.RCEnabled {
		httpClient := httpclient.NewDefault()
		rcloneClient := rclonecli.NewRcloneRcClient(configManager, httpClient)

		if cfg.RClone.RCUrl != "" {
			slog.InfoContext(ctx, "RClone RC client initialized for external server",
				"rc_url", cfg.RClone.RCUrl)
		} else {
			slog.InfoContext(ctx, "RClone RC client initialized for internal server",
				"rc_port", cfg.RClone.RCPort)
		}
		return rcloneClient
	}

	slog.InfoContext(ctx, "RClone RC notifications disabled")
	return nil
}

// createFiberApp creates and configures the Fiber application
func createFiberApp(ctx context.Context, cfg *config.Config) (*fiber.App, *bool) {
	app := fiber.New(fiber.Config{
		RequestMethods: append(
			fiber.DefaultMethods, "PROPFIND", "PROPPATCH", "MKCOL", "COPY", "MOVE", "LOCK", "UNLOCK",
		),
		BodyLimit: 100 * 1024 * 1024, // 100MB limit for uploads (e.g. nzbdav DBs)
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError
			if e, ok := err.(*fiber.Error); ok {
				code = e.Code
			}
			slog.ErrorContext(ctx, "Fiber error", "path", c.Path(), "method", c.Method(), "error", err)
			return c.Status(code).JSON(fiber.Map{
				"error": err.Error(),
			})
		},
	})

	// Conditional Fiber request logging - only in debug mode
	debugMode := cfg.Log.Level == "debug"

	// Create the logger middleware but wrap it to check debug mode
	fiberLogger := fLogger.New()
	app.Use(func(c *fiber.Ctx) error {
		if debugMode {
			return fiberLogger(c)
		}
		return c.Next()
	})

	return app, &debugMode
}

// setupRepositories creates all database repositories
func setupRepositories(ctx context.Context, db *database.DB) *repositorySet {
	dbConn := db.Connection()
	d := db.Dialect()

	return &repositorySet{
		MainRepo:   database.NewRepository(dbConn, d),
		HealthRepo: database.NewHealthRepository(dbConn, d),
		StateRepo:  database.NewHealthStateRepository(dbConn, d),
		UserRepo:   database.NewUserRepository(dbConn, d),
	}
}

// reconcileHealthProviderConfig gives legacy providers without a configured
// identity the UUID issued (or unambiguously relinked) by the durable registry.
// It must run before the NNTP pool is built so transport evidence itself uses
// the durable ID rather than an endpoint/account-derived fallback.
func reconcileHealthProviderConfig(
	ctx context.Context,
	cfg *config.Config,
	stateRepo *database.HealthStateRepository,
	configManager *config.Manager,
) (*config.Config, error) {
	if cfg == nil || stateRepo == nil || configManager == nil {
		return nil, fmt.Errorf("provider registry dependencies are incomplete")
	}

	next := cfg.DeepCopy()
	activeIndexes := make([]int, 0, len(next.Providers))
	specs := make([]database.ProviderSpec, 0, len(next.Providers))
	changed := false
	for index := range next.Providers {
		provider := &next.Providers[index]
		trimmedID := strings.TrimSpace(provider.ID)
		if provider.ID != trimmedID {
			provider.ID = trimmedID
			changed = true
		}
		if provider.Enabled == nil || !*provider.Enabled {
			continue
		}
		role := database.ProviderRolePrimary
		if provider.IsBackupProvider != nil && *provider.IsBackupProvider {
			role = database.ProviderRoleBackup
		}
		displayName := strings.TrimSpace(provider.Name)
		if displayName == "" {
			displayName = strings.TrimSpace(provider.Host)
		}
		activeIndexes = append(activeIndexes, index)
		specs = append(specs, database.ProviderSpec{
			StableID: provider.ID, DisplayName: displayName,
			Endpoint: provider.Host, Port: provider.Port, Account: provider.Username,
			Role: role, Order: len(specs),
		})
	}

	registered, err := stateRepo.ReconcileProviders(ctx, specs)
	if err != nil {
		return nil, fmt.Errorf("reconcile durable provider registry: %w", err)
	}
	if len(registered) != len(activeIndexes) {
		return nil, fmt.Errorf("durable provider registry returned an incomplete active set")
	}
	for offset, index := range activeIndexes {
		if next.Providers[index].ID != "" {
			continue
		}
		if registered[offset].ID == "" {
			return nil, fmt.Errorf("durable provider registry returned an empty identity")
		}
		next.Providers[index].ID = registered[offset].ID
		changed = true
	}
	for index := range next.Providers {
		if next.Providers[index].ID == "" {
			// Disabled providers have no active registry row to relink. Assign the
			// same UUID form now so enabling them cannot create endpoint-derived
			// transport evidence.
			next.Providers[index].ID = uuid.NewString()
			changed = true
		}
	}
	if !changed {
		return cfg, nil
	}
	if err := configManager.UpdateConfig(next); err != nil {
		return nil, fmt.Errorf("install durable provider identities: %w", err)
	}
	if err := configManager.SaveConfig(); err != nil {
		_ = configManager.UpdateConfig(cfg)
		return nil, fmt.Errorf("persist durable provider identities: %w", err)
	}
	return next, nil
}

// setupAuthService creates and initializes the authentication service.
// When loginRequired is true, JWT_SECRET must be set or an error is returned.
// When loginRequired is false, a missing JWT_SECRET is logged as a warning and nil is returned.
func setupAuthService(ctx context.Context, cfg *config.Config, userRepo *database.UserRepository, loginRequired bool) (*auth.Service, error) {
	authConfig, err := auth.LoadConfigFromEnv()
	if err != nil {
		if loginRequired {
			return nil, fmt.Errorf("failed to load auth configuration: %w", err)
		}
		slog.WarnContext(ctx, "Auth configuration not loaded (login is disabled)", "err", err)
		return nil, nil
	}

	// Override with values from config file
	authConfig.Host = cfg.WebDAV.Host
	authConfig.Port = cfg.WebDAV.Port

	authService, err := auth.NewService(authConfig, userRepo)
	if err != nil {
		return nil, fmt.Errorf("failed to create authentication service: %w", err)
	}

	// Setup OAuth providers
	if err := authService.SetupProviders(authConfig); err != nil {
		return nil, fmt.Errorf("failed to setup auth providers: %w", err)
	}

	slog.InfoContext(ctx, "Authentication service initialized")
	return authService, nil
}

// setupStreamHandler creates the HTTP stream handler for file streaming
func setupStreamHandler(
	nzbFilesystem *nzbfilesystem.NzbFilesystem,
	userRepo *database.UserRepository,
	streamTracker *api.StreamTracker,
) *api.StreamHandler {
	return api.NewStreamHandler(nzbFilesystem, userRepo, streamTracker)
}

// setupAPIServer creates and configures the API server
func setupAPIServer(
	app *fiber.App,
	repos *repositorySet,
	authService *auth.Service,
	configManager *config.Manager,
	metadataReader *metadata.MetadataReader,
	metadataService *metadata.MetadataService,
	nzbFilesystem *nzbfilesystem.NzbFilesystem,
	poolManager pool.Manager,
	importerService *importer.Service,
	arrsService *arrs.Service,
	mountService *rclone.MountService,
	progressBroadcaster *progress.ProgressBroadcaster,
	streamTracker *api.StreamTracker,
	cacheSource *segcache.Source,
) *api.Server {
	apiConfig := &api.Config{
		Prefix: "/api",
	}

	apiServer := api.NewServer(
		apiConfig,
		repos.MainRepo,
		repos.HealthRepo,
		authService,
		repos.UserRepo,
		configManager,
		metadataReader,
		metadataService,
		nzbFilesystem,
		poolManager,
		importerService,
		arrsService,
		mountService,
		progressBroadcaster,
		streamTracker,
		cacheSource,
	)

	// The durable repository must be installed before the server can receive
	// requests. Routes retain the Server pointer, but wiring it here also keeps
	// setup order explicit and independently testable.
	apiServer.SetHealthRunRepository(repos.StateRepo)
	apiServer.SetupRoutes(app)

	// Register RClone handlers
	rcloneHandlers := api.NewRCloneHandlers(mountService, configManager.GetConfigGetter())
	api.RegisterRCloneRoutes(app.Group("/api"), rcloneHandlers)

	// Add simple liveness endpoint for Docker health checks
	app.Get("/live", handleFiberHealth)

	return apiServer
}

// initializeSegmentCache creates and starts the segment cache manager, loading it into
// source. Returns the manager so the caller can defer Stop(). Returns nil if CachePath
// is not configured (enabled/disabled is checked at read-time via source.Store()).
func initializeSegmentCache(ctx context.Context, cfg *config.Config, source *segcache.Source) *segcache.Manager {
	if cfg.SegmentCache.CachePath == "" {
		slog.InfoContext(ctx, "Segment cache not configured (no cache_path set)")
		return nil
	}

	mgrCfg := segcache.ManagerConfig{
		CachePath:      cfg.SegmentCache.CachePath,
		MaxSizeBytes:   int64(cfg.SegmentCache.MaxSizeGB) * 1024 * 1024 * 1024,
		ExpiryDuration: time.Duration(cfg.SegmentCache.ExpiryHours) * time.Hour,
	}.WithDefaults()

	mgr, err := segcache.NewManager(mgrCfg, slog.Default().With("component", "segcache"))
	if err != nil {
		slog.WarnContext(ctx, "Failed to create segment cache manager, running without segment cache", "error", err)
		return nil
	}

	mgr.Start(ctx)
	source.Swap(mgr)
	slog.InfoContext(ctx, "Segment cache started (catalog loads in background)",
		"cache_path", mgrCfg.CachePath,
		"max_size_bytes", mgrCfg.MaxSizeBytes,
		"expiry_duration", mgrCfg.ExpiryDuration)

	return mgr
}

// setupWebDAV creates and configures the WebDAV handler
func setupWebDAV(
	cfg *config.Config,
	fs *nzbfilesystem.NzbFilesystem,
	authService *auth.Service,
	userRepo *database.UserRepository,
	configManager *config.Manager,
	streamTracker *api.StreamTracker,
) (*webdav.Handler, error) {
	var tokenService *token.Service
	var webdavUserRepo *database.UserRepository

	// Pass authentication services if available
	if authService != nil {
		tokenService = authService.TokenService()
		webdavUserRepo = userRepo
	}

	webdavHandler, err := webdav.NewHandler(&webdav.Config{
		Port:   cfg.WebDAV.Port,
		User:   cfg.WebDAV.User,
		Pass:   cfg.WebDAV.Password,
		Prefix: "/webdav",
	}, fs, tokenService, webdavUserRepo, configManager.GetConfigGetter(), streamTracker)

	if err != nil {
		return nil, err
	}

	return webdavHandler, nil
}

// observationWorkerOwner is stable for this process but unique across
// processes sharing a database. AcquireRunLease permits an existing owner to
// renew an unexpired lease, so a compile-time owner would let two replicas
// unnecessarily fence each other.
var observationWorkerOwner = "altmount-health-observation-" + uuid.NewString()

type observationServiceLifecycle interface {
	Start(context.Context) error
	StopAndWait(context.Context) error
}

type librarySyncLifecycle interface {
	StartLibrarySyncChecked(context.Context) error
	StopAndWait(context.Context) error
	IsRunning() bool
}

// healthObservationRuntime owns the PR5 observation-mode lifecycle. The
// legacy HealthWorker is intentionally absent: it can repair or delete after
// a check and therefore must not be started or dynamically re-enabled while
// observation mode is active.
type healthObservationRuntime struct {
	service observationServiceLifecycle
	library librarySyncLifecycle

	mu      sync.Mutex
	enabled bool
}

func (r *healthObservationRuntime) setEnabled(ctx context.Context, enabled bool) error {
	if r == nil || r.service == nil {
		return fmt.Errorf("health observation runtime is incomplete")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// Enabled idempotence needs no work. Disabled idempotence still runs the
	// join path: a prior timed-out disable deliberately cleared the logical
	// flag while the canceled service/discovery generation retained ownership.
	if r.enabled == enabled && enabled {
		return nil
	}
	if enabled {
		if err := ctx.Err(); err != nil {
			return err
		}
		// A previous disable can time out while the canceled discovery
		// generation still owns its goroutine. Join that residual generation
		// before starting any part of the replacement runtime.
		if r.library != nil {
			joinCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			joinErr := r.library.StopAndWait(joinCtx)
			cancel()
			if joinErr != nil {
				return fmt.Errorf("join residual observation discovery: %w", joinErr)
			}
		}
		// The service has the same generation ownership rule as discovery.
		// StopAndWait is a no-op for the initial stopped state, and joins a
		// residual stopping generation after an earlier disable timed out.
		joinCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		joinErr := r.service.StopAndWait(joinCtx)
		cancel()
		if joinErr != nil {
			return fmt.Errorf("join residual observation service: %w", joinErr)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := r.service.Start(ctx); err != nil {
			return err
		}
		if r.library != nil {
			if err := r.library.StartLibrarySyncChecked(ctx); err != nil {
				rollbackCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				rollbackErr := r.service.StopAndWait(rollbackCtx)
				cancel()
				if rollbackErr != nil {
					rollbackErr = fmt.Errorf("rollback observation service: %w", rollbackErr)
				}
				return errors.Join(
					fmt.Errorf("start observation discovery: %w", err),
					rollbackErr,
				)
			}
		}
		r.enabled = true
		return nil
	}

	stopBase := ctx
	if stopBase.Err() != nil {
		stopBase = context.Background()
	}
	stopCtx, cancel := context.WithTimeout(stopBase, 30*time.Second)
	defer cancel()
	var stopErrors []error
	if r.library != nil {
		if err := r.library.StopAndWait(stopCtx); err != nil {
			stopErrors = append(stopErrors, fmt.Errorf("stop observation discovery: %w", err))
		}
	}
	if err := r.service.StopAndWait(stopCtx); err != nil {
		stopErrors = append(stopErrors, fmt.Errorf("stop observation service: %w", err))
	}
	r.enabled = false
	return errors.Join(stopErrors...)
}

func (r *healthObservationRuntime) stop(ctx context.Context) error {
	if r == nil {
		return nil
	}
	return r.setEnabled(ctx, false)
}

func (r *healthObservationRuntime) librarySyncWorker() *health.LibrarySyncWorker {
	if r == nil {
		return nil
	}
	worker, _ := r.library.(*health.LibrarySyncWorker)
	return worker
}

func (r *healthObservationRuntime) observationService() *health.ObservationService {
	if r == nil {
		return nil
	}
	service, _ := r.service.(*health.ObservationService)
	return service
}

// observationServiceConfig maps the existing bounded health settings onto
// the PR5 observation engine. Provider configuration remains live through the
// ConfigGetter; the values captured here require a process restart to change.
func observationServiceConfig(cfg *config.Config) health.ObservationServiceConfig {
	libraryDir := ""
	if cfg != nil && cfg.Health.LibraryDir != nil {
		libraryDir = *cfg.Health.LibraryDir
	}
	if cfg == nil {
		cfg = config.DefaultConfig()
	}
	wireCapacity := cfg.GetMaxConnectionsForHealthChecks()
	return health.ObservationServiceConfig{
		Owner:                   observationWorkerOwner,
		WorkerCount:             cfg.GetMaxConcurrentJobs(),
		GlobalConcurrency:       wireCapacity,
		PerProviderConcurrency:  wireCapacity,
		LeaseTTL:                time.Minute,
		ChunkSize:               256,
		ConfirmationDelay:       cfg.GetGapConfirmationMinimumDelay(),
		PlaybackRetryDelay:      time.Second,
		PauseDuringPlayback:     cfg.GetPauseHealthDuringPlayback(),
		STATConcurrency:         wireCapacity,
		PollInterval:            250 * time.Millisecond,
		ScheduleInterval:        cfg.GetCheckInterval(),
		ScheduleBatchSize:       cfg.GetCheckBatchSize(),
		HealthStrategy:          string(cfg.Import.ImportStrategy),
		LibraryDir:              libraryDir,
		MaxRetries:              cfg.GetMaxRetries(),
		OrdinaryRecheckInterval: 24 * time.Hour,
	}
}

// observationLibraryConfigGetter preserves discovery while preventing the
// legacy library worker from deleting metadata/library content or enabling
// repair side effects during PR5 observation mode. Passing no ConfigManager to
// the worker separately prevents mount-change symlink rewrites.
func observationLibraryConfigGetter(getter config.ConfigGetter) config.ConfigGetter {
	return func() *config.Config {
		if getter == nil {
			return config.DefaultConfig()
		}
		current := getter()
		if current == nil {
			return config.DefaultConfig()
		}
		safe := current.DeepCopy()
		disabled := false
		safe.Health.CleanupOrphanedMetadata = &disabled
		safe.Health.Repair.Enabled = &disabled
		safe.Health.CorruptionAction = "repair"
		return safe
	}
}

func observationRuntimeRestartRequired(oldConfig, newConfig *config.Config) bool {
	if oldConfig == nil || newConfig == nil {
		return false
	}
	oldSettings := observationServiceConfig(oldConfig)
	newSettings := observationServiceConfig(newConfig)
	return oldSettings.WorkerCount != newSettings.WorkerCount ||
		oldSettings.GlobalConcurrency != newSettings.GlobalConcurrency ||
		oldSettings.ConfirmationDelay != newSettings.ConfirmationDelay ||
		oldSettings.PauseDuringPlayback != newSettings.PauseDuringPlayback ||
		oldSettings.STATConcurrency != newSettings.STATConcurrency ||
		oldSettings.ScheduleInterval != newSettings.ScheduleInterval ||
		oldSettings.ScheduleBatchSize != newSettings.ScheduleBatchSize ||
		oldSettings.HealthStrategy != newSettings.HealthStrategy ||
		oldSettings.LibraryDir != newSettings.LibraryDir ||
		oldSettings.MaxRetries != newSettings.MaxRetries ||
		oldConfig.Health.LibrarySyncIntervalMinutes != newConfig.Health.LibrarySyncIntervalMinutes
}

func (r *healthObservationRuntime) registerConfigChangeHandler(
	ctx context.Context,
	configManager *config.Manager,
) {
	if r == nil || configManager == nil {
		return
	}
	configManager.OnConfigChange(func(oldConfig, newConfig *config.Config) {
		if observationRuntimeRestartRequired(oldConfig, newConfig) {
			slog.WarnContext(ctx,
				"Health observation runtime settings changed; restart required to apply bounded worker settings")
		}
		oldEnabled := oldConfig != nil && oldConfig.GetHealthEnabled()
		newEnabled := newConfig != nil && newConfig.GetHealthEnabled()
		if oldEnabled == newEnabled {
			return
		}
		if err := r.setEnabled(ctx, newEnabled); err != nil {
			slog.ErrorContext(ctx, "Failed to transition health observation runtime", "error", err)
		}
	})
}

// initializeHealthObservationRuntime creates and conditionally starts the
// resumable PR5 observation engine. It has no repair, padding, or deletion
// dependency.
func initializeHealthObservationRuntime(
	ctx context.Context,
	cfg *config.Config,
	stateRepo *database.HealthStateRepository,
	healthRepo *database.HealthRepository,
	metadataService *metadata.MetadataService,
	poolManager pool.Manager,
	configManager *config.Manager,
	broadcaster *progress.ProgressBroadcaster,
	playbackSource health.PlaybackActivitySource,
) (*healthObservationRuntime, error) {
	if cfg == nil || stateRepo == nil || healthRepo == nil || metadataService == nil ||
		poolManager == nil || configManager == nil {
		return nil, fmt.Errorf("health observation dependencies are incomplete")
	}
	if err := stateRepo.SetGapConfirmationMinimumDelay(cfg.GetGapConfirmationMinimumDelay()); err != nil {
		return nil, fmt.Errorf("configure health gap confirmation delay: %w", err)
	}
	service, err := health.NewObservationService(
		stateRepo,
		healthRepo,
		metadataService,
		poolManager,
		configManager.GetConfigGetter(),
		playbackSource,
		func(health.ObservationProgress) {
			if broadcaster != nil {
				broadcaster.BroadcastHealthChanged()
			}
		},
		observationServiceConfig(cfg),
	)
	if err != nil {
		return nil, fmt.Errorf("create health observation service: %w", err)
	}

	// Retain metadata/library discovery, but clamp destructive legacy options
	// off and omit ConfigManager so mount-path changes cannot rewrite symlinks.
	librarySyncWorker := health.NewObservationLibrarySyncWorker(
		metadataService,
		healthRepo,
		observationLibraryConfigGetter(configManager.GetConfigGetter()),
	)
	runtime := &healthObservationRuntime{service: service, library: librarySyncWorker}
	if err := runtime.setEnabled(ctx, cfg.GetHealthEnabled()); err != nil {
		return nil, fmt.Errorf("start health observation service: %w", err)
	}
	if cfg.GetHealthEnabled() {
		slog.InfoContext(ctx, "Resumable health observation system started")
	} else {
		slog.InfoContext(ctx, "Health observation system disabled")
	}
	return runtime, nil
}

// startMountService starts the RClone mount service if enabled
func startMountService(ctx context.Context, cfg *config.Config, mountService *rclone.MountService, logger *slog.Logger) error {
	if cfg.RClone.MountEnabled == nil || !*cfg.RClone.MountEnabled {
		slog.InfoContext(ctx, "RClone mount service is disabled in configuration")
		return nil
	}

	if err := mountService.Start(ctx); err != nil {
		slog.ErrorContext(ctx, "Failed to start mount service", "error", err)
		return err
	}

	slog.InfoContext(ctx, "RClone mount service started", "mount_point", cfg.MountPath)
	return nil
}

// createHTTPServer creates the HTTP server with routing
func createHTTPServer(apiServer *api.Server, app *fiber.App, webdavHandler *webdav.Handler, streamHandler *api.StreamHandler, port int, configGetter config.ConfigGetter) *http.Server {
	// Mount WebDAV handler directly (no Fiber adapter needed)
	webdavHTTPHandler := webdavHandler.GetHTTPHandler()

	// Mount stream handler directly (no Fiber adapter needed)
	streamHTTPHandler := streamHandler.GetHTTPHandler()

	// Convert Fiber app to HTTP handler for all other routes
	fiberHTTPHandler := adaptor.FiberApp(app)

	// Create a handler that routes between WebDAV, Stream, and Fiber
	mainHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		// Check if server is ready, but allow /live and /api/system/health
		if !apiServer.IsReady() && path != "/live" && path != "/api/system/health" && !strings.HasPrefix(path, "/api/auth/config") {
			w.Header().Set("Retry-After", "10")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("503 Service Unavailable: Server is initializing"))
			return
		}

		// Route profiler requests if enabled
		if configGetter().ProfilerEnabled && strings.HasPrefix(path, "/debug/pprof") {
			http.DefaultServeMux.ServeHTTP(w, r)
			return
		}

		// Route stream requests directly to stream handler
		if strings.HasPrefix(path, "/api/files/stream") {
			streamHTTPHandler.ServeHTTP(w, r)
			return
		}

		// Route SSE log stream directly — bypasses adaptor.FiberApp which
		// blocks forever on streaming responses (calls Response.Body() which
		// reads the SSE pipe until EOF that never comes).
		if path == "/api/logs/stream" {
			apiServer.ServeLogsSSE(w, r)
			return
		}
		if path == "/api/queue/stream" {
			apiServer.ServeQueueSSE(w, r)
			return
		}
		if path == "/api/health/stream" {
			apiServer.ServeHealthSSE(w, r)
			return
		}

		// Route WebDAV requests directly to WebDAV handler
		if len(path) >= 7 && path[:7] == "/webdav" {
			webdavHTTPHandler.ServeHTTP(w, r)
			return
		}

		// Route all other requests to Fiber handler
		fiberHTTPHandler.ServeHTTP(w, r)
	})

	// Create and configure the HTTP server
	return &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      mainHandler,
		IdleTimeout:  time.Minute * 5,
		WriteTimeout: time.Minute * 30,
		ReadTimeout:  time.Minute * 5,
	}
}
