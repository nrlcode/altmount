package health

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/metadata"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	altpool "github.com/javi11/altmount/internal/pool"
	"github.com/javi11/nntppool/v4"
)

type observationFileLookup interface {
	GetFileHealthByID(context.Context, int64) (*database.FileHealth, error)
}

type observationMetadataReader interface {
	ReadFileMetadata(string) (*metapb.FileMetadata, error)
}

// metadataObservationTargetSource reconstructs message identities only in
// memory and fences them against the immutable revision dimensions before any
// network work can begin. Errors deliberately contain no article identities.
type metadataObservationTargetSource struct {
	files    observationFileLookup
	metadata observationMetadataReader
}

func newMetadataObservationTargetSource(
	files observationFileLookup,
	metadataReader observationMetadataReader,
) *metadataObservationTargetSource {
	return &metadataObservationTargetSource{files: files, metadata: metadataReader}
}

func (s *metadataObservationTargetSource) ObservationTargets(
	ctx context.Context,
	revision *database.HealthFileRevision,
) ([]observationSegmentTarget, error) {
	if revision == nil || revision.ID == "" || revision.FileHealthID <= 0 || !revision.Active {
		return nil, fmt.Errorf("active observation revision is required")
	}
	if s == nil || s.files == nil || s.metadata == nil {
		return nil, fmt.Errorf("observation metadata source is incomplete")
	}
	file, err := s.files.GetFileHealthByID(ctx, revision.FileHealthID)
	if err != nil {
		return nil, fmt.Errorf("load observation file record: %w", err)
	}
	if file == nil || strings.TrimSpace(file.FilePath) == "" {
		return nil, fmt.Errorf("observation file record is missing")
	}
	layout, err := readCanonicalObservationLayout(s.metadata, file.FilePath)
	if err != nil {
		return nil, err
	}
	if layout.Fingerprint != revision.LayoutFingerprint ||
		layout.VirtualSize != revision.VirtualSize ||
		int64(len(layout.Segments)) != revision.SegmentCount {
		return nil, fmt.Errorf("canonical observation layout no longer matches revision")
	}
	targets := make([]observationSegmentTarget, len(layout.Segments))
	for i, segment := range layout.Segments {
		targets[i] = observationSegmentTarget{
			Position: segment.Position, MessageID: segment.MessageID, UsableBytes: segment.UsableBytes,
		}
	}
	return targets, nil
}

func readCanonicalObservationLayout(
	reader observationMetadataReader,
	filePath string,
) (*metadata.CanonicalSegmentLayout, error) {
	if reader == nil || strings.TrimSpace(filePath) == "" {
		return nil, fmt.Errorf("canonical observation metadata is unavailable")
	}
	fileMetadata, err := reader.ReadFileMetadata(filePath)
	if err != nil {
		return nil, fmt.Errorf("read canonical observation metadata: %w", err)
	}
	layout, err := metadata.ResolveCanonicalSegmentLayout(fileMetadata)
	if err != nil {
		return nil, fmt.Errorf("resolve canonical observation layout: %w", err)
	}
	if layout == nil || layout.Fingerprint == "" || layout.VirtualSize <= 0 || len(layout.Segments) == 0 {
		return nil, fmt.Errorf("canonical observation layout is empty")
	}
	return layout, nil
}

type observationProviderRegistry interface {
	ListProviders(context.Context, bool) ([]database.HealthProvider, error)
}

type observationProviderResolver interface {
	ResolveObservationProvider(context.Context, observationDispatchProvider) (string, error)
}

type currentObservationProviderResolver struct {
	registry observationProviderRegistry
	config   config.ConfigGetter
}

func newCurrentObservationProviderResolver(
	registry observationProviderRegistry,
	configGetter config.ConfigGetter,
) *currentObservationProviderResolver {
	return &currentObservationProviderResolver{registry: registry, config: configGetter}
}

func (r *currentObservationProviderResolver) ResolveObservationProvider(
	ctx context.Context,
	target observationDispatchProvider,
) (string, error) {
	if r == nil || r.registry == nil || r.config == nil || target.ID == "" ||
		target.Generation <= 0 || target.ActivationEpoch <= 0 {
		return "", fmt.Errorf("observation provider target is invalid")
	}
	registered, err := r.registry.ListProviders(ctx, true)
	if err != nil {
		return "", fmt.Errorf("load observation provider registry: %w", err)
	}
	current := false
	for _, provider := range registered {
		if provider.ID == target.ID && provider.Active &&
			provider.CurrentGeneration == target.Generation &&
			provider.ActivationEpoch == target.ActivationEpoch {
			current = true
			break
		}
	}
	if !current {
		return "", fmt.Errorf("observation provider activation is no longer current")
	}
	cfg := r.config()
	if cfg == nil {
		return "", fmt.Errorf("observation provider configuration is unavailable")
	}
	for i := range cfg.Providers {
		provider := &cfg.Providers[i]
		if provider.ID != target.ID {
			continue
		}
		if provider.Enabled != nil && !*provider.Enabled {
			return "", fmt.Errorf("observation provider is disabled")
		}
		if strings.TrimSpace(provider.Host) == "" || provider.Port <= 0 || provider.Port > 65535 {
			return "", fmt.Errorf("observation provider transport is invalid")
		}
		return provider.NNTPPoolName(), nil
	}
	return "", fmt.Errorf("observation provider configuration is missing")
}

type observationPoolGetter interface {
	GetPool() (altpool.NntpClient, error)
}

type targetedObservationBodyClient interface {
	BodyTargeted(
		context.Context,
		string,
		nntppool.TargetedBodyOptions,
		...func(nntppool.YEncMeta),
	) (*nntppool.ArticleBody, error)
}

type nntpObservationTransport struct {
	pool        observationPoolGetter
	providers   observationProviderResolver
	concurrency int
}

func newNNTPObservationTransport(
	poolGetter observationPoolGetter,
	providers observationProviderResolver,
	concurrency int,
) *nntpObservationTransport {
	if concurrency <= 0 {
		concurrency = 1
	}
	return &nntpObservationTransport{pool: poolGetter, providers: providers, concurrency: concurrency}
}

func (t *nntpObservationTransport) Observe(
	ctx context.Context,
	request observationTransportRequest,
) ([]observationTransportResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if t == nil || t.pool == nil || t.providers == nil || len(request.Targets) == 0 {
		return nil, fmt.Errorf("observation transport request is incomplete")
	}
	providerName, err := t.providers.ResolveObservationProvider(ctx, request.Provider)
	if err != nil {
		return nil, err
	}
	client, err := t.pool.GetPool()
	if err != nil {
		return nil, err
	}
	if client == nil {
		return nil, fmt.Errorf("observation NNTP pool is unavailable")
	}
	switch request.ObservationKind {
	case database.HealthObservationSTAT:
		return t.observeSTAT(ctx, client, providerName, request.Targets)
	case database.HealthObservationValidatedBody:
		bodyClient, ok := client.(targetedObservationBodyClient)
		if !ok {
			return nil, fmt.Errorf("observation NNTP pool does not support targeted validated BODY")
		}
		return t.observeValidatedBody(ctx, bodyClient, providerName, request)
	default:
		return nil, fmt.Errorf("observation kind is invalid")
	}
}

func (t *nntpObservationTransport) observeSTAT(
	ctx context.Context,
	client altpool.NntpClient,
	providerName string,
	targets []observationSegmentTarget,
) ([]observationTransportResult, error) {
	ids := make([]string, len(targets))
	for i := range targets {
		ids[i] = targets[i].MessageID
	}
	concurrency := min(max(t.concurrency, 1), len(ids))
	results := make([]observationTransportResult, 0, len(ids))
	for result := range client.StatMany(ctx, ids, nntppool.StatManyOptions{
		Concurrency: concurrency, Provider: providerName,
	}) {
		observation := observationTransportResultFromNNTP(result.MessageID, result.Result, result.Err)
		observation.Outcome = normalizeObservationOutcome(observation)
		results = append(results, observation)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func (t *nntpObservationTransport) observeValidatedBody(
	ctx context.Context,
	client targetedObservationBodyClient,
	providerName string,
	request observationTransportRequest,
) ([]observationTransportResult, error) {
	results := make([]observationTransportResult, 0, len(request.Targets))
	for _, target := range request.Targets {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		body, err := client.BodyTargeted(ctx, target.MessageID, nntppool.TargetedBodyOptions{
			Provider: providerName, FreshTransport: request.FreshTransport,
		})
		results = append(results, observationTransportResultFromBody(target.MessageID, body, err))
	}
	return results, nil
}

func observationTransportResultFromBody(
	messageID string,
	body *nntppool.ArticleBody,
	err error,
) observationTransportResult {
	result := observationTransportResult{MessageID: messageID, Err: err}
	var attempts []nntppool.AttemptEvidence
	if body != nil {
		attempts = body.Attempts
	}
	var transportError *nntppool.TransportError
	if errors.As(err, &transportError) {
		attempts = transportError.Attempts
	}
	for _, attempt := range attempts {
		outcome := observationOutcomeFromNNTP(attempt.Outcome)
		result.Attempts = append(result.Attempts, observationTransportAttempt{
			Operation: string(attempt.Operation), Outcome: outcome,
			ResponseCode: attempt.ResponseCode, BodyValidation: string(attempt.BodyValidation),
			CauseClass: observationCauseClass(outcome), PoolQueue: attempt.PoolQueueDuration,
			PipelineWait:    attempt.PipelineHeadWaitDuration,
			ResponseService: attempt.ResponseServiceDuration,
		})
	}
	if err == nil && body != nil {
		result.Outcome = observationOutcomePresent
	} else if err == nil {
		result.Err = fmt.Errorf("validated BODY returned no result")
	}
	result.Outcome = normalizeObservationOutcome(result)
	return result
}

type ordinaryObservationHealthRepository interface {
	observationFileLookup
	GetUnhealthyFiles(context.Context, int, string, string, int) ([]*database.FileHealth, error)
	DeferScheduledHealthCheck(context.Context, string, time.Time) error
}

type ordinaryObservationStateRepository interface {
	ReconcileProviders(context.Context, []database.ProviderSpec) ([]database.HealthProvider, error)
	EnsureFileRevision(context.Context, database.FileRevisionSpec) (*database.HealthFileRevision, error)
	HasReusableCompletedImportSTATCoverage(context.Context, string, int64) (bool, error)
	CaptureActiveProviderSnapshot(context.Context, time.Time) (*database.ProviderSnapshot, error)
	EnsureScheduledHealthRun(
		context.Context,
		database.ScheduledHealthRunSpec,
	) (*database.HealthRun, bool, error)
}

type observationSchedulerConfig struct {
	BatchSize       int
	HealthStrategy  string
	LibraryDir      string
	MaxRetries      int
	RecheckInterval time.Duration
}

func (c observationSchedulerConfig) normalized() observationSchedulerConfig {
	if c.BatchSize <= 0 {
		c.BatchSize = 32
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = 3
	}
	if c.RecheckInterval <= 0 {
		c.RecheckInterval = 24 * time.Hour
	}
	if strings.TrimSpace(c.HealthStrategy) == "" {
		c.HealthStrategy = "NONE"
	}
	return c
}

// ObservationScheduleResult summarizes one bounded discovery pass. It
// contains only counts; file paths and article identities are never surfaced.
type ObservationScheduleResult struct {
	Examined       int `json:"examined"`
	Created        int `json:"created"`
	Existing       int `json:"existing"`
	CoverageReused int `json:"coverage_reused"`
	Failed         int `json:"failed"`
}

type observationDueScheduler interface {
	ScheduleDue(context.Context) (ObservationScheduleResult, error)
}

type ordinaryObservationScheduler struct {
	health   ordinaryObservationHealthRepository
	state    ordinaryObservationStateRepository
	metadata observationMetadataReader
	config   config.ConfigGetter
	settings observationSchedulerConfig
	clock    observationClock
}

func newOrdinaryObservationScheduler(
	health ordinaryObservationHealthRepository,
	state ordinaryObservationStateRepository,
	metadataReader observationMetadataReader,
	configGetter config.ConfigGetter,
	settings observationSchedulerConfig,
	clock observationClock,
) *ordinaryObservationScheduler {
	settings = settings.normalized()
	if clock == nil {
		clock = wallObservationClock{}
	}
	return &ordinaryObservationScheduler{
		health: health, state: state, metadata: metadataReader, config: configGetter,
		settings: settings, clock: clock,
	}
}

func (s *ordinaryObservationScheduler) ScheduleDue(ctx context.Context) (ObservationScheduleResult, error) {
	var result ObservationScheduleResult
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil || s.health == nil || s.state == nil || s.metadata == nil || s.config == nil {
		return result, fmt.Errorf("ordinary observation scheduler is incomplete")
	}
	cfg := s.config()
	if cfg == nil {
		return result, fmt.Errorf("ordinary observation configuration is unavailable")
	}
	if _, err := s.state.ReconcileProviders(ctx, observationProviderSpecs(cfg.Providers)); err != nil {
		return result, fmt.Errorf("reconcile observation providers: %w", err)
	}
	files, err := s.health.GetUnhealthyFiles(
		ctx, s.settings.BatchSize, s.settings.HealthStrategy,
		s.settings.LibraryDir, s.settings.MaxRetries,
	)
	if err != nil {
		return result, err
	}
	now := s.clock.Now().UTC()
	var snapshot *database.ProviderSnapshot
	var failures []error
	for _, file := range files {
		result.Examined++
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if file == nil || file.ID <= 0 || strings.TrimSpace(file.FilePath) == "" {
			result.Failed++
			failures = append(failures, fmt.Errorf("due observation file record is invalid"))
			continue
		}
		layout, err := readCanonicalObservationLayout(s.metadata, file.FilePath)
		if err != nil {
			result.Failed++
			failures = append(failures, err)
			continue
		}
		revision, err := s.state.EnsureFileRevision(ctx, database.FileRevisionSpec{
			FilePath: file.FilePath, LayoutFingerprint: layout.Fingerprint,
			VirtualSize: layout.VirtualSize, SegmentCount: int64(len(layout.Segments)),
		})
		if err != nil || revision == nil || revision.ID == "" {
			result.Failed++
			if err == nil {
				err = fmt.Errorf("ordinary observation revision is missing")
			}
			failures = append(failures, err)
			continue
		}
		reusable, err := s.state.HasReusableCompletedImportSTATCoverage(
			ctx, revision.ID, revision.SegmentCount,
		)
		if err != nil {
			result.Failed++
			failures = append(failures, err)
			continue
		}
		if reusable {
			if err := s.health.DeferScheduledHealthCheck(
				ctx, file.FilePath, now.Add(s.settings.RecheckInterval),
			); err != nil {
				result.Failed++
				failures = append(failures, err)
				continue
			}
			result.CoverageReused++
			continue
		}
		if snapshot == nil {
			snapshot, err = s.state.CaptureActiveProviderSnapshot(ctx, now)
			if err != nil || snapshot == nil || snapshot.ID == "" || len(snapshot.Entries) == 0 {
				result.Failed++
				if err == nil {
					err = fmt.Errorf("active observation provider snapshot is empty")
				}
				failures = append(failures, err)
				continue
			}
		}
		priority := database.HealthRunPriorityNormal
		if file.Priority > database.HealthPriorityNormal || file.Status == database.HealthStatusPending {
			priority = database.HealthRunPriorityHigh
		}
		_, created, err := s.state.EnsureScheduledHealthRun(ctx, database.ScheduledHealthRunSpec{
			Run: database.HealthRunSpec{
				FileRevisionID: revision.ID, ProviderSnapshotID: snapshot.ID,
				Trigger: "ordinary", Mode: "observation",
				TotalSegments: revision.SegmentCount, CreatedAt: now,
			},
			DedupeKey: "ordinary:" + revision.ID, Priority: priority, NotBefore: now,
		})
		if err != nil {
			result.Failed++
			failures = append(failures, err)
			continue
		}
		if err := s.health.DeferScheduledHealthCheck(
			ctx, file.FilePath, now.Add(s.settings.RecheckInterval),
		); err != nil {
			result.Failed++
			failures = append(failures, err)
			continue
		}
		if created {
			result.Created++
		} else {
			result.Existing++
		}
	}
	return result, errors.Join(failures...)
}

func observationProviderSpecs(providers []config.ProviderConfig) []database.ProviderSpec {
	specs := make([]database.ProviderSpec, 0, len(providers))
	for index, provider := range providers {
		if provider.Enabled != nil && !*provider.Enabled {
			continue
		}
		role := database.ProviderRolePrimary
		if provider.IsBackupProvider != nil && *provider.IsBackupProvider {
			role = database.ProviderRoleBackup
		}
		displayName := strings.TrimSpace(provider.Name)
		if displayName == "" {
			displayName = strings.TrimSpace(provider.ID)
		}
		specs = append(specs, database.ProviderSpec{
			StableID: provider.ID, DisplayName: displayName, Endpoint: provider.Host,
			Port: provider.Port, Account: provider.Username, Role: role, Order: index,
		})
	}
	sort.SliceStable(specs, func(i, j int) bool { return specs[i].Order < specs[j].Order })
	return specs
}

// ObservationProgress is the provider-credential-free progress projection
// suitable for API storage or SSE invalidation broadcasts.
type ObservationProgress struct {
	RunID                    string                   `json:"run_id"`
	FileRevisionID           string                   `json:"file_revision_id"`
	Status                   database.HealthRunStatus `json:"status"`
	ResolvedSegments         int64                    `json:"resolved_segments"`
	TotalSegments            int64                    `json:"total_segments"`
	ProviderChecks           int64                    `json:"provider_checks"`
	MissingCandidates        int64                    `json:"missing_candidates"`
	InconclusiveCount        int64                    `json:"inconclusive_count"`
	Stage                    string                   `json:"stage"`
	CurrentProviderID        string                   `json:"current_provider_id,omitempty"`
	ChecksPerSecond          float64                  `json:"checks_per_second"`
	EstimatedCompletionDelay time.Duration            `json:"estimated_completion_delay"`
	ObservedAt               time.Time                `json:"observed_at"`
}

type observationProgressCallback func(ObservationProgress)

func (callback observationProgressCallback) PublishObservationProgress(event observationProgressEvent) {
	if callback == nil {
		return
	}
	progress := ObservationProgress(event)
	// A consumer callback must not be able to kill a durable health worker.
	defer func() { _ = recover() }()
	callback(progress)
}

// ObservationServiceConfig controls bounded runtime orchestration. Network
// dispatch remains background priority; playback can pause actual admission.
type ObservationServiceConfig struct {
	Owner                   string
	WorkerCount             int
	GlobalConcurrency       int
	PerProviderConcurrency  int
	LeaseTTL                time.Duration
	ChunkSize               int64
	ConfirmationDelay       time.Duration
	PlaybackRetryDelay      time.Duration
	PauseDuringPlayback     bool
	STATConcurrency         int
	PollInterval            time.Duration
	ScheduleInterval        time.Duration
	ScheduleBatchSize       int
	HealthStrategy          string
	LibraryDir              string
	MaxRetries              int
	OrdinaryRecheckInterval time.Duration
}

func (c ObservationServiceConfig) normalized() ObservationServiceConfig {
	if c.WorkerCount <= 0 {
		c.WorkerCount = 1
	}
	if c.GlobalConcurrency <= 0 {
		c.GlobalConcurrency = c.WorkerCount
	}
	if c.PerProviderConcurrency <= 0 {
		c.PerProviderConcurrency = 1
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 250 * time.Millisecond
	}
	if c.ScheduleInterval <= 0 {
		c.ScheduleInterval = time.Minute
	}
	return c
}

type ObservationServiceStatus string

const (
	ObservationServiceStopped  ObservationServiceStatus = "stopped"
	ObservationServiceRunning  ObservationServiceStatus = "running"
	ObservationServiceStopping ObservationServiceStatus = "stopping"
)

var (
	ErrObservationServiceRunning = errors.New("observation service is already running")
	ErrObservationServiceStopped = errors.New("observation service is stopped")
)

type observationProcessor interface {
	ProcessNext(context.Context) (observationWorkerStep, error)
}

// ObservationService is the application-facing lifecycle around the durable
// worker and scheduler. It performs no padding, deletion, or destructive repair.
type ObservationService struct {
	processor observationProcessor
	scheduler observationDueScheduler
	config    ObservationServiceConfig

	mu     sync.Mutex
	status ObservationServiceStatus
	cancel context.CancelFunc
	done   chan struct{}
}

func NewObservationService(
	stateRepository *database.HealthStateRepository,
	healthRepository *database.HealthRepository,
	metadataService *metadata.MetadataService,
	poolManager altpool.Manager,
	configGetter config.ConfigGetter,
	playback PlaybackActivitySource,
	progress func(ObservationProgress),
	serviceConfig ObservationServiceConfig,
) (*ObservationService, error) {
	if stateRepository == nil || healthRepository == nil || metadataService == nil ||
		poolManager == nil || configGetter == nil {
		return nil, fmt.Errorf("observation service dependencies are incomplete")
	}
	scheduleRepository, ok := any(stateRepository).(ordinaryObservationStateRepository)
	if !ok {
		return nil, fmt.Errorf("health state repository does not support import coverage reuse")
	}
	observationHealthRepository, ok := any(healthRepository).(ordinaryObservationHealthRepository)
	if !ok {
		return nil, fmt.Errorf("health repository does not support non-authoritative scheduling")
	}
	serviceConfig = serviceConfig.normalized()
	targets := newMetadataObservationTargetSource(healthRepository, metadataService)
	resolver := newCurrentObservationProviderResolver(stateRepository, configGetter)
	transport := newNNTPObservationTransport(poolManager, resolver, serviceConfig.STATConcurrency)
	gate := newObservationDispatchGate(
		newObservationAdmission(serviceConfig.GlobalConcurrency, serviceConfig.PerProviderConcurrency),
		playback,
		serviceConfig.PauseDuringPlayback,
	)
	worker := newObservationWorker(
		stateRepository, targets, transport, gate, observationProgressCallback(progress),
		observationWorkerConfig{
			Owner: serviceConfig.Owner, LeaseTTL: serviceConfig.LeaseTTL,
			ChunkSize: serviceConfig.ChunkSize, ConfirmationDelay: serviceConfig.ConfirmationDelay,
			PlaybackRetryDelay: serviceConfig.PlaybackRetryDelay,
		},
	)
	scheduler := newOrdinaryObservationScheduler(
		observationHealthRepository, scheduleRepository, metadataService, configGetter,
		observationSchedulerConfig{
			BatchSize:      serviceConfig.ScheduleBatchSize,
			HealthStrategy: serviceConfig.HealthStrategy, LibraryDir: serviceConfig.LibraryDir,
			MaxRetries:      serviceConfig.MaxRetries,
			RecheckInterval: serviceConfig.OrdinaryRecheckInterval,
		}, nil,
	)
	return newObservationServiceForTest(worker, scheduler, serviceConfig), nil
}

func newObservationServiceForTest(
	processor observationProcessor,
	scheduler observationDueScheduler,
	serviceConfig ObservationServiceConfig,
) *ObservationService {
	return &ObservationService{
		processor: processor, scheduler: scheduler, config: serviceConfig.normalized(),
		status: ObservationServiceStopped,
	}
}

func (s *ObservationService) Start(parent context.Context) error {
	if parent == nil {
		parent = context.Background()
	}
	if s == nil || s.processor == nil || s.scheduler == nil {
		return fmt.Errorf("observation service is incomplete")
	}
	s.mu.Lock()
	if s.status != ObservationServiceStopped {
		s.mu.Unlock()
		return ErrObservationServiceRunning
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	s.status = ObservationServiceRunning
	s.cancel = cancel
	s.done = done
	workerCount := s.config.WorkerCount
	s.mu.Unlock()

	var group sync.WaitGroup
	group.Add(workerCount + 1)
	for range workerCount {
		go func() {
			defer group.Done()
			s.runProcessor(ctx)
		}()
	}
	go func() {
		defer group.Done()
		s.runScheduler(ctx)
	}()
	go func() {
		group.Wait()
		s.mu.Lock()
		if s.done == done {
			s.status = ObservationServiceStopped
			s.cancel = nil
			close(done)
		}
		s.mu.Unlock()
	}()
	return nil
}

func (s *ObservationService) runProcessor(ctx context.Context) {
	ticker := time.NewTicker(s.config.PollInterval)
	defer ticker.Stop()
	for {
		_, _ = s.processor.ProcessNext(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *ObservationService) runScheduler(ctx context.Context) {
	ticker := time.NewTicker(s.config.ScheduleInterval)
	defer ticker.Stop()
	for {
		_, _ = s.scheduler.ScheduleDue(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *ObservationService) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.status == ObservationServiceStopped {
		s.mu.Unlock()
		return nil
	}
	s.status = ObservationServiceStopping
	cancel := s.cancel
	done := s.done
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *ObservationService) ProcessNext(ctx context.Context) error {
	if s == nil {
		return ErrObservationServiceStopped
	}
	s.mu.Lock()
	running := s.status == ObservationServiceRunning
	s.mu.Unlock()
	if !running {
		return ErrObservationServiceStopped
	}
	_, err := s.processor.ProcessNext(ctx)
	return err
}

func (s *ObservationService) Status() ObservationServiceStatus {
	if s == nil {
		return ObservationServiceStopped
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

var (
	_ observationTargetSource             = (*metadataObservationTargetSource)(nil)
	_ observationTransport                = (*nntpObservationTransport)(nil)
	_ observationProgressSink             = observationProgressCallback(nil)
	_ ordinaryObservationHealthRepository = (*database.HealthRepository)(nil)
	_ ordinaryObservationStateRepository  = (*database.HealthStateRepository)(nil)
	_ observationProviderRegistry         = (*database.HealthStateRepository)(nil)
)
