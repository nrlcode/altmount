package health

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
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
	ListProviderGenerations(context.Context, string) ([]database.HealthProviderGeneration, error)
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
	// Resolve the immutable endpoint/account dimensions for this exact durable
	// generation before looking at live configuration. Legacy empty-ID configs
	// can then be matched without making endpoint/account text the durable ID.
	generations, err := r.registry.ListProviderGenerations(ctx, target.ID)
	if err != nil {
		return "", fmt.Errorf("load observation provider generation: %w", err)
	}
	var targetGeneration *database.HealthProviderGeneration
	for index := range generations {
		if generations[index].Generation == target.Generation {
			targetGeneration = &generations[index]
			break
		}
	}
	if targetGeneration == nil {
		return "", fmt.Errorf("observation provider transport generation is unavailable")
	}
	cfg := r.config()
	if cfg == nil {
		return "", fmt.Errorf("observation provider configuration is unavailable")
	}
	var matched *config.ProviderConfig
	for i := range cfg.Providers {
		provider := &cfg.Providers[i]
		if provider.Enabled == nil || !*provider.Enabled {
			continue
		}
		configuredID := observationProviderStableID(provider)
		matches := configuredID == target.ID
		if configuredID == "" {
			matches = normalizedObservationProviderEndpoint(provider.Host) ==
				normalizedObservationProviderEndpoint(targetGeneration.Endpoint) &&
				provider.Port == targetGeneration.Port &&
				strings.TrimSpace(provider.Username) == targetGeneration.Account
		}
		if !matches {
			continue
		}
		if matched != nil {
			return "", fmt.Errorf("observation provider configuration is ambiguous")
		}
		matched = provider
	}
	if matched == nil {
		return "", fmt.Errorf("observation provider configuration is missing or disabled")
	}
	if strings.TrimSpace(matched.Host) == "" || matched.Port <= 0 || matched.Port > 65535 {
		return "", fmt.Errorf("observation provider transport is invalid")
	}

	// A config reload reaches the live pool before registry reconciliation can
	// persist the corresponding generation. Fence that window so transport for
	// the new endpoint/account cannot be attributed to the old durable target.
	generationMatches := normalizedObservationProviderEndpoint(targetGeneration.Endpoint) ==
		normalizedObservationProviderEndpoint(matched.Host) &&
		targetGeneration.Port == matched.Port &&
		targetGeneration.Account == strings.TrimSpace(matched.Username)
	if !generationMatches {
		return "", fmt.Errorf("observation provider transport generation is no longer current")
	}
	return matched.NNTPPoolName(), nil
}

func normalizedObservationProviderEndpoint(value string) string {
	return strings.TrimSuffix(strings.ToLower(strings.TrimSpace(value)), ".")
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
		return t.observeSTAT(ctx, client, providerName, request)
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
	request observationTransportRequest,
) ([]observationTransportResult, error) {
	targets := uniqueObservationMessageTargets(request.Targets)
	ids := make([]string, len(targets))
	for i := range targets {
		ids[i] = targets[i].MessageID
	}
	concurrency := min(max(t.concurrency, 1), len(ids))
	if request.WireConcurrency > 0 {
		concurrency = min(concurrency, request.WireConcurrency)
	}
	results := make([]observationTransportResult, 0, len(ids))
	for result := range client.StatMany(ctx, ids, nntppool.StatManyOptions{
		Concurrency: concurrency, Provider: providerName,
	}) {
		observation := observationTransportResultFromNNTPProvider(
			result.MessageID, request.Provider.ID, result.Result, result.Err,
		)
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
	targets := uniqueObservationMessageTargets(request.Targets)
	results := make([]observationTransportResult, 0, len(targets))
	for _, target := range targets {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		body, err := client.BodyTargeted(ctx, target.MessageID, nntppool.TargetedBodyOptions{
			Provider: providerName, FreshTransport: request.FreshTransport,
		})
		results = append(results, observationTransportResultFromBody(
			target.MessageID, request.Provider.ID, body, err,
		))
	}
	return results, nil
}

// A canonical layout may legally reuse one article at multiple positions. One
// wire proof is sufficient for every owner, while emitting duplicate requests
// makes out-of-order streaming results impossible to correlate safely.
func uniqueObservationMessageTargets(targets []observationSegmentTarget) []observationSegmentTarget {
	unique := make([]observationSegmentTarget, 0, len(targets))
	seen := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		if _, exists := seen[target.MessageID]; exists {
			continue
		}
		seen[target.MessageID] = struct{}{}
		unique = append(unique, target)
	}
	return unique
}

func observationTransportResultFromBody(
	messageID, expectedProviderID string,
	body *nntppool.ArticleBody,
	err error,
) observationTransportResult {
	result := observationTransportResult{MessageID: messageID, Err: err}
	var attempts []nntppool.AttemptEvidence
	if body != nil {
		result.ProviderID = body.ProviderID
		attempts = body.Attempts
	}
	var transportError *nntppool.TransportError
	if errors.As(err, &transportError) {
		result.ProviderID = transportError.ProviderID
		attempts = transportError.Attempts
	}
	for _, attempt := range attempts {
		outcome := observationOutcomeFromNNTP(attempt.Outcome)
		result.Attempts = append(result.Attempts, observationTransportAttempt{
			ProviderID: attempt.ProviderID,
			Operation:  string(attempt.Operation), Outcome: outcome,
			ResponseCode: attempt.ResponseCode, BodyValidation: string(attempt.BodyValidation),
			CauseClass: observationCauseClass(outcome), PoolQueue: attempt.PoolQueueDuration,
			PipelineWait:    attempt.PipelineHeadWaitDuration,
			ResponseService: attempt.ResponseServiceDuration,
		})
	}
	if !validObservationProviderEvidence(expectedProviderID, result.ProviderID, result.Attempts) {
		return observationTransportResult{
			MessageID: messageID, Outcome: observationOutcomeInconclusive,
			Err: errObservationProviderIdentity,
		}
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
	ListDueObservationFiles(context.Context, time.Time, time.Time, int) ([]*database.FileHealth, error)
	DeferScheduledHealthCheck(context.Context, string, time.Time) error
	DeferObservationDiscoveryFailure(context.Context, int64, time.Time, time.Time) error
}

type ordinaryObservationStateRepository interface {
	ReconcileProviders(context.Context, []database.ProviderSpec) ([]database.HealthProvider, error)
	ListProviderActivationWork(context.Context, int) ([]database.ProviderActivationWork, error)
	ListDueGapRevalidations(context.Context, time.Time, int) ([]database.GapRevalidationWork, error)
	EnsureObservationFileRevision(context.Context, database.FileRevisionSpec) (*database.HealthFileRevision, error)
	ConsumeReusableCompletedImportSTATCoverageAndDeferHealth(
		context.Context, string, int64, string, time.Time, time.Time,
	) (*database.CompletedImportSTATCoverage, error)
	GetCompletedImportSTATCoverage(context.Context, string, int64) (*database.CompletedImportSTATCoverage, error)
	CaptureActiveProviderSnapshot(context.Context, time.Time) (*database.ProviderSnapshot, error)
	GetActiveScheduledHealthRun(context.Context, string) (*database.HealthRun, error)
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

// ObservationScheduleIntent separates explicit operator work from automatic
// freshness scheduling. A manual request must never be suppressed by reusable
// import coverage.
type ObservationScheduleIntent string

const ObservationScheduleIntentManual ObservationScheduleIntent = "manual"

type observationDueScheduler interface {
	ScheduleDue(context.Context) (ObservationScheduleResult, error)
}

type observationFileScheduler interface {
	ScheduleFile(context.Context, int64, ObservationScheduleIntent) (ObservationScheduleResult, error)
}

type observationFileController interface {
	GetActiveObservationHealthRunForFile(context.Context, int64) (*database.HealthRun, error)
	RequestRunCancel(context.Context, string, time.Time) error
}

type ordinaryObservationScheduler struct {
	health   ordinaryObservationHealthRepository
	state    ordinaryObservationStateRepository
	metadata observationMetadataReader
	config   config.ConfigGetter
	settings observationSchedulerConfig
	clock    observationClock
}

const observationDiscoveryFailureBackoff = 5 * time.Minute

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
	now := s.clock.Now().UTC()
	providerWork, err := s.state.ListProviderActivationWork(ctx, s.settings.BatchSize)
	if err != nil {
		return result, fmt.Errorf("list provider activation observation work: %w", err)
	}
	revalidationWork, err := s.state.ListDueGapRevalidations(ctx, now, s.settings.BatchSize)
	if err != nil {
		return result, fmt.Errorf("list due gap revalidation work: %w", err)
	}
	var snapshot *database.ProviderSnapshot
	ensureSnapshot := func() (*database.ProviderSnapshot, error) {
		if snapshot != nil {
			return snapshot, nil
		}
		captured, err := s.state.CaptureActiveProviderSnapshot(ctx, now)
		if err != nil || captured == nil || captured.ID == "" || len(captured.Entries) == 0 {
			if err == nil {
				err = fmt.Errorf("active observation provider snapshot is empty")
			}
			return nil, err
		}
		snapshot = captured
		return snapshot, nil
	}
	for _, item := range providerWork {
		activeSnapshot, err := ensureSnapshot()
		if err != nil {
			result.Failed++
			continue
		}
		dedupe := fmt.Sprintf(
			"provider-activation:%s:%d:%d:%s:%s",
			item.Provider.ProviderID, item.Provider.ProviderGeneration,
			item.Provider.ProviderActivationEpoch, item.RevisionID, item.GapID,
		)
		_, created, err := s.state.EnsureScheduledHealthRun(ctx, database.ScheduledHealthRunSpec{
			Run: database.HealthRunSpec{
				FileRevisionID: item.RevisionID, ProviderSnapshotID: activeSnapshot.ID,
				Trigger: "provider_activation", Mode: "observation",
				TotalSegments: item.TotalSegments, CreatedAt: now,
			},
			DedupeKey: dedupe, Priority: database.HealthRunPriorityLow, NotBefore: now,
			TargetProviderID:              item.Provider.ProviderID,
			TargetProviderGeneration:      item.Provider.ProviderGeneration,
			TargetProviderActivationEpoch: item.Provider.ProviderActivationEpoch,
			TargetGapID:                   item.GapID,
		})
		if err != nil {
			result.Failed++
			continue
		}
		if created {
			result.Created++
		} else {
			result.Existing++
		}
	}
	for _, item := range revalidationWork {
		activeSnapshot, err := ensureSnapshot()
		if err != nil {
			result.Failed++
			continue
		}
		_, created, err := s.state.EnsureScheduledHealthRun(ctx, database.ScheduledHealthRunSpec{
			Run: database.HealthRunSpec{
				FileRevisionID: item.Gap.FileRevisionID, ProviderSnapshotID: activeSnapshot.ID,
				Trigger: fmt.Sprintf("gap_revalidation_%d", item.Step), Mode: "observation",
				TotalSegments: item.TotalSegments,
				CreatedAt:     now,
			},
			DedupeKey: fmt.Sprintf("gap-revalidation:%s:%d", item.Gap.ID, item.Step),
			Priority:  database.HealthRunPriorityLow, NotBefore: item.NotBefore,
			TargetGapID: item.Gap.ID,
		})
		if err != nil {
			result.Failed++
			continue
		}
		if created {
			result.Created++
		} else {
			result.Existing++
		}
	}
	files, err := s.health.ListDueObservationFiles(
		ctx, now, now.Add(-s.settings.RecheckInterval), s.settings.BatchSize,
	)
	if err != nil {
		return result, err
	}
	var failures []error
	recordDiscoveryFailure := func(file *database.FileHealth, cause error) {
		result.Failed++
		if cause == nil {
			cause = fmt.Errorf("observation discovery failed")
		}
		if file != nil && file.ID > 0 && ctx.Err() == nil {
			if deferErr := s.health.DeferObservationDiscoveryFailure(
				ctx, file.ID, now, now.Add(observationDiscoveryFailureBackoff),
			); deferErr != nil {
				cause = errors.Join(cause, deferErr)
			}
		}
		failures = append(failures, cause)
	}
	for _, file := range files {
		result.Examined++
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if file == nil || file.ID <= 0 || strings.TrimSpace(file.FilePath) == "" {
			recordDiscoveryFailure(file, fmt.Errorf("due observation file record is invalid"))
			continue
		}
		layout, err := readCanonicalObservationLayout(s.metadata, file.FilePath)
		if err != nil {
			recordDiscoveryFailure(file, err)
			continue
		}
		revision, err := s.state.EnsureObservationFileRevision(ctx, database.FileRevisionSpec{
			FilePath: file.FilePath, LayoutFingerprint: layout.Fingerprint,
			VirtualSize: layout.VirtualSize, SegmentCount: int64(len(layout.Segments)),
		})
		if err != nil || revision == nil || revision.ID == "" {
			if err == nil {
				err = fmt.Errorf("ordinary observation revision is missing")
			}
			recordDiscoveryFailure(file, err)
			continue
		}
		reusedCoverage, err := s.state.ConsumeReusableCompletedImportSTATCoverageAndDeferHealth(
			ctx, revision.ID, revision.SegmentCount, file.FilePath,
			now.Add(s.settings.RecheckInterval), now,
		)
		if err != nil {
			recordDiscoveryFailure(file, err)
			continue
		}
		if reusedCoverage != nil {
			if !reusedCoverage.Reusable {
				recordDiscoveryFailure(file, fmt.Errorf("consumed import coverage is not reusable"))
				continue
			}
			result.CoverageReused++
			continue
		}
		coverage, err := s.state.GetCompletedImportSTATCoverage(
			ctx, revision.ID, revision.SegmentCount,
		)
		if err != nil {
			recordDiscoveryFailure(file, err)
			continue
		}
		if coverage != nil && coverage.HealthPending {
			activeSnapshot, snapshotErr := ensureSnapshot()
			if snapshotErr != nil {
				recordDiscoveryFailure(file, snapshotErr)
				continue
			}
			_, created, scheduleErr := s.state.EnsureScheduledHealthRun(ctx, database.ScheduledHealthRunSpec{
				Run: database.HealthRunSpec{
					FileRevisionID: revision.ID, ProviderSnapshotID: activeSnapshot.ID,
					Trigger: "health_pending", Mode: "observation",
					TotalSegments: int64(len(coverage.UnresolvedPositions)), CreatedAt: now,
				},
				DedupeKey: "health-pending:" + revision.ID,
				Priority:  database.HealthRunPriorityHigh, NotBefore: now,
			})
			if scheduleErr != nil {
				recordDiscoveryFailure(file, scheduleErr)
				continue
			}
			if err := s.health.DeferScheduledHealthCheck(
				ctx, file.FilePath, now.Add(s.settings.RecheckInterval),
			); err != nil {
				recordDiscoveryFailure(file, err)
				continue
			}
			if created {
				result.Created++
			} else {
				result.Existing++
			}
			continue
		}
		if snapshot == nil {
			snapshot, err = ensureSnapshot()
			if err != nil {
				recordDiscoveryFailure(file, err)
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
			recordDiscoveryFailure(file, err)
			continue
		}
		if err := s.health.DeferScheduledHealthCheck(
			ctx, file.FilePath, now.Add(s.settings.RecheckInterval),
		); err != nil {
			recordDiscoveryFailure(file, err)
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

// ScheduleFile creates or reuses one immediate durable manual observation
// run. It deliberately does not inspect or consume accepted-import coverage
// and does not rewrite legacy health or repair status.
func (s *ordinaryObservationScheduler) ScheduleFile(
	ctx context.Context,
	fileHealthID int64,
	intent ObservationScheduleIntent,
) (ObservationScheduleResult, error) {
	var result ObservationScheduleResult
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil || s.health == nil || s.state == nil || s.metadata == nil || s.config == nil {
		return result, fmt.Errorf("ordinary observation scheduler is incomplete")
	}
	if fileHealthID <= 0 {
		return result, fmt.Errorf("positive observation file identifier is required")
	}
	if intent != ObservationScheduleIntentManual {
		return result, fmt.Errorf("unsupported observation schedule intent %q", intent)
	}
	cfg := s.config()
	if cfg == nil {
		return result, fmt.Errorf("ordinary observation configuration is unavailable")
	}
	if _, err := s.state.ReconcileProviders(ctx, observationProviderSpecs(cfg.Providers)); err != nil {
		return result, fmt.Errorf("reconcile manual observation providers: %w", err)
	}
	file, err := s.health.GetFileHealthByID(ctx, fileHealthID)
	if err != nil {
		return result, err
	}
	if file == nil || strings.TrimSpace(file.FilePath) == "" {
		return result, fmt.Errorf("manual observation file record is missing")
	}
	result.Examined = 1
	layout, err := readCanonicalObservationLayout(s.metadata, file.FilePath)
	if err != nil {
		result.Failed = 1
		return result, err
	}
	revision, err := s.state.EnsureObservationFileRevision(ctx, database.FileRevisionSpec{
		FilePath: file.FilePath, LayoutFingerprint: layout.Fingerprint,
		VirtualSize: layout.VirtualSize, SegmentCount: int64(len(layout.Segments)),
	})
	if err != nil || revision == nil || revision.ID == "" {
		result.Failed = 1
		if err == nil {
			err = fmt.Errorf("manual observation revision is missing")
		}
		return result, err
	}
	now := s.clock.Now().UTC()
	snapshot, err := s.state.CaptureActiveProviderSnapshot(ctx, now)
	if err != nil || snapshot == nil || snapshot.ID == "" || len(snapshot.Entries) == 0 {
		result.Failed = 1
		if err == nil {
			err = fmt.Errorf("active observation provider snapshot is empty")
		}
		return result, err
	}
	dedupe := "manual:" + revision.ID + ":" + observationProviderMembershipKey(snapshot.Entries)
	if existing, existingErr := s.state.GetActiveScheduledHealthRun(ctx, dedupe); existingErr != nil {
		result.Failed = 1
		return result, existingErr
	} else if validManualObservationRun(existing, revision.ID) {
		result.Existing = 1
		return result, nil
	}
	run, created, err := s.state.EnsureScheduledHealthRun(ctx, database.ScheduledHealthRunSpec{
		Run: database.HealthRunSpec{
			FileRevisionID: revision.ID, ProviderSnapshotID: snapshot.ID,
			Trigger: string(ObservationScheduleIntentManual), Mode: "observation",
			TotalSegments: revision.SegmentCount, CreatedAt: now,
		},
		DedupeKey: dedupe, Priority: database.HealthRunPriorityHigh, NotBefore: now,
	})
	if err != nil {
		// Concurrent callers can capture different snapshot row IDs for the same
		// membership and race on the durable dedupe key. Converge on the winner.
		existing, existingErr := s.state.GetActiveScheduledHealthRun(ctx, dedupe)
		if existingErr == nil && validManualObservationRun(existing, revision.ID) {
			result.Existing = 1
			return result, nil
		}
		result.Failed = 1
		if existingErr != nil {
			return result, errors.Join(err, existingErr)
		}
		return result, err
	}
	if !validManualObservationRun(run, revision.ID) {
		result.Failed = 1
		return result, fmt.Errorf("manual observation schedule returned an incompatible run")
	}
	if created {
		result.Created = 1
	} else {
		result.Existing = 1
	}
	return result, nil
}

func validManualObservationRun(run *database.HealthRun, revisionID string) bool {
	return run != nil && run.FileRevisionID == revisionID &&
		run.Trigger == string(ObservationScheduleIntentManual) && run.Mode == "observation"
}

func observationProviderMembershipKey(entries []database.ProviderSnapshotEntry) string {
	identities := make([]string, 0, len(entries))
	for _, entry := range entries {
		identities = append(identities, fmt.Sprintf(
			"%d:%s:%d:%d", len(entry.ProviderID), entry.ProviderID,
			entry.ProviderGeneration, entry.ProviderActivationEpoch,
		))
	}
	sort.Strings(identities)
	digest := sha256.Sum256([]byte(strings.Join(identities, "\x00")))
	return hex.EncodeToString(digest[:])
}

func observationProviderSpecs(providers []config.ProviderConfig) []database.ProviderSpec {
	specs := make([]database.ProviderSpec, 0, len(providers))
	for index := range providers {
		provider := &providers[index]
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
		specs = append(specs, database.ProviderSpec{
			StableID: observationProviderStableID(provider), DisplayName: displayName,
			Endpoint: provider.Host, Port: provider.Port, Account: provider.Username,
			Role: role, Order: len(specs),
		})
	}
	return specs
}

func observationProviderStableID(provider *config.ProviderConfig) string {
	if provider == nil {
		return ""
	}
	if stableID := strings.TrimSpace(provider.ID); stableID != "" {
		return stableID
	}
	return ""
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
	if c.STATConcurrency <= 0 {
		c.STATConcurrency = 1
	}
	if c.GlobalConcurrency <= 0 {
		c.GlobalConcurrency = c.STATConcurrency
	}
	if c.PerProviderConcurrency <= 0 {
		c.PerProviderConcurrency = c.GlobalConcurrency
	}
	if c.STATConcurrency > c.GlobalConcurrency {
		c.STATConcurrency = c.GlobalConcurrency
	}
	if c.STATConcurrency > c.PerProviderConcurrency {
		c.STATConcurrency = c.PerProviderConcurrency
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
	ErrObservationRunNotActive   = errors.New("observation run is not active")
)

type observationProcessor interface {
	ProcessNext(context.Context) (observationWorkerStep, error)
}

// ObservationService is the application-facing lifecycle around the durable
// worker and scheduler. It performs no padding, deletion, or destructive repair.
type ObservationService struct {
	processor  observationProcessor
	scheduler  observationDueScheduler
	controller observationFileController
	config     ObservationServiceConfig

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
	if capacity := poolManager.ImportConnCapacity(); capacity > 0 && serviceConfig.STATConcurrency > capacity {
		serviceConfig.STATConcurrency = capacity
	}
	targets := newMetadataObservationTargetSource(healthRepository, metadataService)
	resolver := newCurrentObservationProviderResolver(stateRepository, configGetter)
	transport := newNNTPObservationTransport(poolManager, resolver, serviceConfig.STATConcurrency)
	sharedBudget, _ := any(poolManager).(observationSharedWireAdmission)
	gate := newObservationDispatchGateWithSharedBudget(
		newObservationAdmission(serviceConfig.GlobalConcurrency, serviceConfig.PerProviderConcurrency),
		sharedBudget,
		playback,
		serviceConfig.PauseDuringPlayback,
	)
	worker := newObservationWorker(
		stateRepository, targets, transport, gate, observationProgressCallback(progress),
		observationWorkerConfig{
			Owner: serviceConfig.Owner, LeaseTTL: serviceConfig.LeaseTTL,
			ChunkSize: serviceConfig.ChunkSize, ConfirmationDelay: serviceConfig.ConfirmationDelay,
			PlaybackRetryDelay: serviceConfig.PlaybackRetryDelay,
			STATConcurrency:    serviceConfig.STATConcurrency,
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
	service := newObservationServiceForTest(worker, scheduler, serviceConfig)
	service.controller = stateRepository
	return service, nil
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
		_, err := s.processor.ProcessNext(ctx)
		if err != nil && ctx.Err() == nil {
			// The wrapped error can contain provider/account or reconstructed
			// article detail. Keep operational reporting fixed and credential-free.
			slog.WarnContext(ctx, "Health observation worker step failed")
		}
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
		_, err := s.scheduler.ScheduleDue(ctx)
		if err != nil && ctx.Err() == nil {
			slog.WarnContext(ctx, "Health observation scheduling pass failed")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *ObservationService) Stop(ctx context.Context) error {
	return s.StopAndWait(ctx)
}

// StopAndWait cancels and joins the active service generation. A caller that
// times out can call it again to join the same stopping generation before a
// replacement Start; status remains stopping until every goroutine exits.
func (s *ObservationService) StopAndWait(ctx context.Context) error {
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

// ScheduleFile records an explicit per-file observation request through the
// same durable scheduler used by the background service. Compatibility API
// handlers can use this seam without invoking the legacy repair-capable worker.
func (s *ObservationService) ScheduleFile(
	ctx context.Context,
	fileHealthID int64,
	intent ObservationScheduleIntent,
) (ObservationScheduleResult, error) {
	var result ObservationScheduleResult
	if s == nil {
		return result, ErrObservationServiceStopped
	}
	s.mu.Lock()
	running := s.status == ObservationServiceRunning
	scheduler, supported := s.scheduler.(observationFileScheduler)
	s.mu.Unlock()
	if !running {
		return result, ErrObservationServiceStopped
	}
	if !supported {
		return result, fmt.Errorf("observation scheduler does not support per-file requests")
	}
	return scheduler.ScheduleFile(ctx, fileHealthID, intent)
}

// CancelFile cancels the current non-import observation for one file-health
// identity. Selection is bounded to active durable schedules, and the existing
// repository cancel transition invalidates any worker lease before returning.
func (s *ObservationService) CancelFile(ctx context.Context, fileHealthID int64) error {
	if s == nil {
		return ErrObservationServiceStopped
	}
	if fileHealthID <= 0 {
		return fmt.Errorf("positive observation file identifier is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	running := s.status == ObservationServiceRunning
	controller := s.controller
	s.mu.Unlock()
	if !running {
		return ErrObservationServiceStopped
	}
	if controller == nil {
		return fmt.Errorf("observation service does not support per-file cancellation")
	}
	run, err := controller.GetActiveObservationHealthRunForFile(ctx, fileHealthID)
	if err != nil {
		return err
	}
	if run == nil {
		return ErrObservationRunNotActive
	}
	if err := controller.RequestRunCancel(ctx, run.ID, time.Now().UTC()); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrObservationRunNotActive
		}
		return err
	}
	return nil
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
	_ observationFileController           = (*database.HealthStateRepository)(nil)
	_ observationProviderRegistry         = (*database.HealthStateRepository)(nil)
)
