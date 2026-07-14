package health

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/usenet"
	"github.com/javi11/nntppool/v4"
)

// observationRunRepository is deliberately the smallest durable surface the
// observation worker needs. *database.HealthStateRepository implements it;
// keeping the boundary here also makes worker lifecycle tests deterministic.
type observationRunRepository interface {
	ClaimDueHealthRun(context.Context, string, time.Duration) (*database.HealthRun, error)
	GetHealthRunResumeState(context.Context, string) (*database.HealthRunResumeState, error)
	GetFileRevisionForRun(context.Context, string) (*database.HealthFileRevision, error)
	GetProviderSnapshot(context.Context, string) (*database.ProviderSnapshot, error)
	CommitHealthChunk(context.Context, database.HealthChunkCommit) (*database.HealthRun, error)
	ParkHealthRun(context.Context, string, string, int64, time.Time, time.Time) error
	CompleteHealthRun(context.Context, string, string, int64, time.Time) error
	FailHealthRun(context.Context, string, string, int64, string, time.Time) error
	UpsertGapRange(context.Context, database.GapRangeWrite) (*database.HealthGapRange, error)
}

var _ observationRunRepository = (*database.HealthStateRepository)(nil)

type observationTargetSource interface {
	ObservationTargets(context.Context, *database.HealthFileRevision) ([]observationSegmentTarget, error)
}

type observationTransport interface {
	Observe(context.Context, observationTransportRequest) ([]observationTransportResult, error)
}

type observationClock interface {
	Now() time.Time
}

type wallObservationClock struct{}

func (wallObservationClock) Now() time.Time { return time.Now().UTC() }

type observationProgressSink interface {
	PublishObservationProgress(observationProgressEvent)
}

type observationProgressEvent struct {
	RunID                    string
	FileRevisionID           string
	Status                   database.HealthRunStatus
	ResolvedSegments         int64
	TotalSegments            int64
	ProviderChecks           int64
	MissingCandidates        int64
	InconclusiveCount        int64
	Stage                    string
	CurrentProviderID        string
	ChecksPerSecond          float64
	EstimatedCompletionDelay time.Duration
	ObservedAt               time.Time
}

type observationWorkerConfig struct {
	Owner              string
	LeaseTTL           time.Duration
	ChunkSize          int64
	ConfirmationDelay  time.Duration
	PlaybackRetryDelay time.Duration
	Clock              observationClock
}

func (c observationWorkerConfig) normalized() observationWorkerConfig {
	c.Owner = strings.TrimSpace(c.Owner)
	if c.Owner == "" {
		c.Owner = "health-observation-worker"
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = time.Minute
	}
	if c.ChunkSize <= 0 {
		c.ChunkSize = 256
	}
	if c.ConfirmationDelay <= 0 {
		c.ConfirmationDelay = 10 * time.Minute
	}
	if c.PlaybackRetryDelay <= 0 {
		c.PlaybackRetryDelay = time.Second
	}
	if c.Clock == nil {
		c.Clock = wallObservationClock{}
	}
	return c
}

type observationWorkerStep string

const (
	observationWorkerIdle      observationWorkerStep = "idle"
	observationWorkerParked    observationWorkerStep = "parked"
	observationWorkerCompleted observationWorkerStep = "completed"
	observationWorkerFailed    observationWorkerStep = "failed"
)

type observationWorker struct {
	repository observationRunRepository
	targets    observationTargetSource
	transport  observationTransport
	gate       *observationDispatchGate
	progress   observationProgressSink
	config     observationWorkerConfig
}

func newObservationWorker(
	repository observationRunRepository,
	targets observationTargetSource,
	transport observationTransport,
	gate *observationDispatchGate,
	progress observationProgressSink,
	config observationWorkerConfig,
) *observationWorker {
	config = config.normalized()
	if gate == nil {
		gate = newObservationDispatchGate(newObservationAdmission(1, 1), nil, true)
	}
	return &observationWorker{
		repository: repository, targets: targets, transport: transport,
		gate: gate, progress: progress, config: config,
	}
}

type observationTransportRequest struct {
	RunID           string
	Provider        observationDispatchProvider
	Stage           string
	ObservationKind database.HealthObservationKind
	FreshTransport  bool
	Targets         []observationSegmentTarget
}

type observationTransportAttempt struct {
	Operation       string
	Outcome         observationOutcome
	ResponseCode    int
	BodyValidation  string
	CauseClass      string
	PoolQueue       time.Duration
	PipelineWait    time.Duration
	ResponseService time.Duration
}

type observationWork struct {
	provider       observationDispatchProvider
	stage          string
	kind           database.HealthObservationKind
	freshTransport bool
	segmentStart   int64
	segmentCount   int64
	targets        []observationSegmentTarget
	retry          *database.HealthRunRetryState
}

type observationStateAnalysis struct {
	work    *observationWork
	nextDue *time.Time
	gaps    []database.GapRangeWrite
}

// ProcessNext claims one durable run and executes at most one bounded network
// chunk. Releasing the lease between chunks keeps playback pause, process
// shutdown, and restart boundaries explicit and inexpensive.
func (w *observationWorker) ProcessNext(ctx context.Context) (observationWorkerStep, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return observationWorkerIdle, err
	}
	if w.repository == nil || w.targets == nil || w.transport == nil {
		return observationWorkerFailed, fmt.Errorf("observation worker dependencies are incomplete")
	}

	run, err := w.repository.ClaimDueHealthRun(ctx, w.config.Owner, w.config.LeaseTTL)
	if err != nil {
		return observationWorkerIdle, err
	}
	if run == nil {
		return observationWorkerIdle, nil
	}
	if run.LeaseOwner == nil || *run.LeaseOwner != w.config.Owner || run.FencingToken <= 0 {
		return observationWorkerFailed, database.ErrStaleHealthLease
	}
	w.publishProgress(*run, w.config.Clock.Now())

	state, revision, _, targets, providers, err := w.loadRun(ctx, run)
	if err != nil {
		return w.failRun(ctx, run, "invalid observation run", err)
	}

	analysis := analyzeObservationState(state, revision, targets, providers, w.config, w.config.Clock.Now())
	if analysis.work == nil {
		return w.settleRun(ctx, run, state, analysis)
	}

	admissionStarted := w.config.Clock.Now()
	release, err := w.gate.Acquire(ctx, analysis.work.provider.ID)
	if err != nil {
		if errors.Is(err, ErrObservationPausedForPlayback) {
			due := w.config.Clock.Now().Add(w.config.PlaybackRetryDelay)
			if parkErr := w.repository.ParkHealthRun(ctx, run.ID, *run.LeaseOwner,
				run.FencingToken, due, w.config.Clock.Now()); parkErr != nil {
				return observationWorkerParked, parkErr
			}
			return observationWorkerParked, nil
		}
		return observationWorkerParked, err
	}
	admissionWait := w.config.Clock.Now().Sub(admissionStarted)
	defer release()

	request := observationTransportRequest{
		RunID: run.ID, Provider: analysis.work.provider, Stage: analysis.work.stage,
		ObservationKind: analysis.work.kind, FreshTransport: analysis.work.freshTransport,
		Targets: append([]observationSegmentTarget(nil), analysis.work.targets...),
	}
	results, dispatchErr := w.transport.Observe(ctx, request)
	release()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return observationWorkerParked, ctxErr
	}
	if isObservationCancellation(dispatchErr) {
		due := w.config.Clock.Now().Add(w.config.PlaybackRetryDelay)
		if err := w.repository.ParkHealthRun(ctx, run.ID, *run.LeaseOwner,
			run.FencingToken, due, w.config.Clock.Now()); err != nil {
			return observationWorkerParked, err
		}
		return observationWorkerParked, nil
	}
	if dispatchErr != nil {
		results = make([]observationTransportResult, len(request.Targets))
		for i := range request.Targets {
			results[i] = observationTransportResult{MessageID: request.Targets[i].MessageID, Err: dispatchErr}
		}
	}

	normalized, canceled := normalizeObservationBatch(request, results)
	if canceled {
		due := w.config.Clock.Now().Add(w.config.PlaybackRetryDelay)
		if err := w.repository.ParkHealthRun(ctx, run.ID, *run.LeaseOwner,
			run.FencingToken, due, w.config.Clock.Now()); err != nil {
			return observationWorkerParked, err
		}
		return observationWorkerParked, nil
	}
	commit := buildObservationCommit(*run, state, providers, *analysis.work, normalized,
		admissionWait, w.config.Clock.Now())
	committed, err := w.repository.CommitHealthChunk(ctx, commit)
	if err != nil {
		return observationWorkerParked, err
	}
	w.publishProgress(*committed, w.config.Clock.Now())

	state, err = w.repository.GetHealthRunResumeState(ctx, run.ID)
	if err != nil || state == nil {
		if err == nil {
			err = fmt.Errorf("committed observation run disappeared")
		}
		return observationWorkerParked, err
	}
	analysis = analyzeObservationState(state, revision, targets, providers, w.config, w.config.Clock.Now())
	return w.settleRun(ctx, committed, state, analysis)
}

func (w *observationWorker) loadRun(
	ctx context.Context,
	run *database.HealthRun,
) (*database.HealthRunResumeState, *database.HealthFileRevision, *database.ProviderSnapshot,
	[]observationSegmentTarget, []observationDispatchProvider, error) {
	if run.Mode != "observation" {
		return nil, nil, nil, nil, nil, fmt.Errorf("run mode is not observation")
	}
	state, err := w.repository.GetHealthRunResumeState(ctx, run.ID)
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("load committed observation state: %w", err)
	}
	if state == nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("committed observation state is missing")
	}
	revision, err := w.repository.GetFileRevisionForRun(ctx, run.ID)
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("load observation revision: %w", err)
	}
	if revision == nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("observation revision is missing")
	}
	if decision := decideObservationRunResume(run, revision); !decision.Compatible {
		return nil, nil, nil, nil, nil, fmt.Errorf("observation revision is incompatible")
	}
	snapshot, err := w.repository.GetProviderSnapshot(ctx, run.ProviderSnapshotID)
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("load observation provider snapshot: %w", err)
	}
	if snapshot == nil || snapshot.ID != run.ProviderSnapshotID {
		return nil, nil, nil, nil, nil, fmt.Errorf("observation provider snapshot is missing")
	}
	targets, err := w.targets.ObservationTargets(ctx, revision)
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("load canonical observation targets: %w", err)
	}
	if err := validateObservationTargets(targets, revision.SegmentCount); err != nil {
		return nil, nil, nil, nil, nil, err
	}
	providers, err := observationProvidersFromSnapshot(snapshot)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	return state, revision, snapshot, targets, providers, nil
}

func validateObservationTargets(targets []observationSegmentTarget, total int64) error {
	if int64(len(targets)) != total || total <= 0 {
		return fmt.Errorf("canonical observation target count does not match revision")
	}
	seen := make([]bool, total)
	for _, target := range targets {
		if target.Position < 0 || target.Position >= total || seen[target.Position] ||
			strings.TrimSpace(target.MessageID) == "" || target.UsableBytes <= 0 {
			return fmt.Errorf("canonical observation targets are incomplete")
		}
		seen[target.Position] = true
	}
	return nil
}

func observationProvidersFromSnapshot(snapshot *database.ProviderSnapshot) ([]observationDispatchProvider, error) {
	if snapshot == nil || snapshot.ID == "" || len(snapshot.Entries) == 0 {
		return nil, fmt.Errorf("active provider snapshot is empty")
	}
	providers := make([]observationDispatchProvider, 0, len(snapshot.Entries))
	seen := make(map[observationProviderKey]struct{}, len(snapshot.Entries))
	for _, entry := range snapshot.Entries {
		provider := observationDispatchProvider{
			ID: entry.ProviderID, Generation: entry.ProviderGeneration,
			ActivationEpoch: entry.ProviderActivationEpoch, Role: entry.Role, Order: entry.Order,
		}
		if provider.ID == "" || provider.Generation <= 0 || provider.ActivationEpoch <= 0 ||
			(provider.Role != database.ProviderRolePrimary && provider.Role != database.ProviderRoleBackup) {
			return nil, fmt.Errorf("active provider snapshot contains an invalid activation")
		}
		if _, duplicate := seen[provider.key()]; duplicate {
			return nil, fmt.Errorf("active provider snapshot contains a duplicate activation")
		}
		seen[provider.key()] = struct{}{}
		providers = append(providers, provider)
	}
	sortObservationProviders(providers)
	return providers, nil
}

func sortObservationProviders(providers []observationDispatchProvider) {
	sort.SliceStable(providers, func(i, j int) bool {
		if providers[i].Role != providers[j].Role {
			return providers[i].Role == database.ProviderRolePrimary
		}
		if providers[i].Order != providers[j].Order {
			return providers[i].Order < providers[j].Order
		}
		if providers[i].ID != providers[j].ID {
			return providers[i].ID < providers[j].ID
		}
		if providers[i].Generation != providers[j].Generation {
			return providers[i].Generation < providers[j].Generation
		}
		return providers[i].ActivationEpoch < providers[j].ActivationEpoch
	})
}

type normalizedObservationResult struct {
	target   observationSegmentTarget
	outcome  observationOutcome
	attempts []observationTransportAttempt
}

func normalizeObservationBatch(
	request observationTransportRequest,
	results []observationTransportResult,
) ([]normalizedObservationResult, bool) {
	byID := make(map[string][]observationTransportResult, len(results))
	for _, result := range results {
		byID[result.MessageID] = append(byID[result.MessageID], result)
	}
	normalized := make([]normalizedObservationResult, 0, len(request.Targets))
	for _, target := range request.Targets {
		queue := byID[target.MessageID]
		result := observationTransportResult{MessageID: target.MessageID, Outcome: observationOutcomeInconclusive}
		if len(queue) > 0 {
			result = queue[0]
			byID[target.MessageID] = queue[1:]
		}
		outcome := normalizeObservationOutcome(result)
		if outcome == observationOutcomeCanceled {
			return nil, true
		}
		if request.ObservationKind == database.HealthObservationSTAT && outcome == observationOutcomeCorrupt {
			outcome = observationOutcomeInconclusive
		}
		normalized = append(normalized, normalizedObservationResult{
			target: target, outcome: outcome, attempts: sanitizeObservationAttempts(result, outcome, request.ObservationKind),
		})
	}
	return normalized, false
}

func normalizeObservationOutcome(result observationTransportResult) observationOutcome {
	if result.Err != nil {
		return observationOutcomeFromNNTP(usenet.ClassifyNNTPOutcome(result.Err))
	}
	switch result.Outcome {
	case observationOutcomePresent, observationOutcomeHardAbsent, observationOutcomeCorrupt,
		observationOutcomeTemporary, observationOutcomeUnavailable,
		observationOutcomeCanceled, observationOutcomeInconclusive:
		return result.Outcome
	case "":
		return observationOutcomePresent
	default:
		return observationOutcomeInconclusive
	}
}

func observationOutcomeFromNNTP(outcome nntppool.OutcomeKind) observationOutcome {
	switch outcome {
	case nntppool.OutcomeSuccess:
		return observationOutcomePresent
	case nntppool.OutcomeHardArticleAbsence:
		return observationOutcomeHardAbsent
	case nntppool.OutcomeCorruptBody:
		return observationOutcomeCorrupt
	case nntppool.OutcomeTemporaryFailure:
		return observationOutcomeTemporary
	case nntppool.OutcomeProviderUnavailable:
		return observationOutcomeUnavailable
	case nntppool.OutcomeCancellation:
		return observationOutcomeCanceled
	default:
		return observationOutcomeInconclusive
	}
}

func isObservationCancellation(err error) bool {
	return err != nil && observationOutcomeFromNNTP(usenet.ClassifyNNTPOutcome(err)) == observationOutcomeCanceled
}

func observationTransportResultFromNNTP(messageID string, result *nntppool.StatResult, err error) observationTransportResult {
	out := observationTransportResult{MessageID: messageID, Err: err}
	var attempts []nntppool.AttemptEvidence
	if result != nil {
		attempts = result.Attempts
	}
	var transportErr *nntppool.TransportError
	if errors.As(err, &transportErr) {
		attempts = transportErr.Attempts
	}
	for _, attempt := range attempts {
		out.Attempts = append(out.Attempts, observationTransportAttempt{
			Operation: string(attempt.Operation), Outcome: observationOutcomeFromNNTP(attempt.Outcome),
			ResponseCode: attempt.ResponseCode, BodyValidation: string(attempt.BodyValidation),
			CauseClass: observationCauseClass(observationOutcomeFromNNTP(attempt.Outcome)),
			PoolQueue:  attempt.PoolQueueDuration, PipelineWait: attempt.PipelineHeadWaitDuration,
			ResponseService: attempt.ResponseServiceDuration,
		})
	}
	if err == nil && result != nil {
		out.Outcome = observationOutcomePresent
	}
	return out
}

func sanitizeObservationAttempts(
	result observationTransportResult,
	finalOutcome observationOutcome,
	kind database.HealthObservationKind,
) []observationTransportAttempt {
	attempts := append([]observationTransportAttempt(nil), result.Attempts...)
	if len(attempts) == 0 {
		operation := "STAT"
		validation := string(nntppool.BodyValidationNotApplicable)
		if kind == database.HealthObservationValidatedBody {
			operation = "BODY"
			validation = string(nntppool.BodyValidationIncomplete)
			switch finalOutcome {
			case observationOutcomePresent:
				validation = string(nntppool.BodyValidationValid)
			case observationOutcomeCorrupt:
				validation = string(nntppool.BodyValidationInvalid)
			}
		}
		attempts = []observationTransportAttempt{{
			Operation: operation, Outcome: finalOutcome, BodyValidation: validation,
			CauseClass: observationCauseClass(finalOutcome),
		}}
	}
	for i := range attempts {
		if attempts[i].Operation != "STAT" && attempts[i].Operation != "BODY" {
			if kind == database.HealthObservationValidatedBody {
				attempts[i].Operation = "BODY"
			} else {
				attempts[i].Operation = "STAT"
			}
		}
		attempts[i].Outcome = normalizeSafeObservationOutcome(attempts[i].Outcome)
		if attempts[i].ResponseCode < 100 || attempts[i].ResponseCode > 599 {
			attempts[i].ResponseCode = 0
		}
		switch attempts[i].BodyValidation {
		case string(nntppool.BodyValidationNotApplicable), string(nntppool.BodyValidationNotRequested),
			string(nntppool.BodyValidationValid), string(nntppool.BodyValidationInvalid),
			string(nntppool.BodyValidationIncomplete):
		default:
			attempts[i].BodyValidation = string(nntppool.BodyValidationIncomplete)
		}
		attempts[i].CauseClass = observationCauseClass(attempts[i].Outcome)
		if attempts[i].PoolQueue < 0 {
			attempts[i].PoolQueue = 0
		}
		if attempts[i].PipelineWait < 0 {
			attempts[i].PipelineWait = 0
		}
		if attempts[i].ResponseService < 0 {
			attempts[i].ResponseService = 0
		}
	}
	return attempts
}

func normalizeSafeObservationOutcome(outcome observationOutcome) observationOutcome {
	switch outcome {
	case observationOutcomePresent, observationOutcomeHardAbsent, observationOutcomeCorrupt,
		observationOutcomeTemporary, observationOutcomeUnavailable, observationOutcomeCanceled,
		observationOutcomeInconclusive:
		return outcome
	default:
		return observationOutcomeInconclusive
	}
}

func observationCauseClass(outcome observationOutcome) string {
	switch normalizeSafeObservationOutcome(outcome) {
	case observationOutcomePresent:
		return "success"
	case observationOutcomeHardAbsent:
		return "hard_absence"
	case observationOutcomeCorrupt:
		return "corrupt_body"
	case observationOutcomeTemporary:
		return "temporary_failure"
	case observationOutcomeUnavailable:
		return "provider_unavailable"
	case observationOutcomeCanceled:
		return "cancellation"
	default:
		return "inconclusive"
	}
}

func durableObservationOutcome(outcome observationOutcome) string {
	switch normalizeSafeObservationOutcome(outcome) {
	case observationOutcomePresent:
		return string(nntppool.OutcomeSuccess)
	case observationOutcomeHardAbsent:
		return string(nntppool.OutcomeHardArticleAbsence)
	case observationOutcomeCorrupt:
		return string(nntppool.OutcomeCorruptBody)
	case observationOutcomeTemporary:
		return string(nntppool.OutcomeTemporaryFailure)
	case observationOutcomeUnavailable:
		return string(nntppool.OutcomeProviderUnavailable)
	case observationOutcomeCanceled:
		return string(nntppool.OutcomeCancellation)
	default:
		return string(nntppool.OutcomeInconclusive)
	}
}

type observationSample struct {
	outcome    observationOutcome
	observedAt time.Time
	stage      string
}

type observationHistory struct {
	evidence observationEvidence
	samples  map[observationProviderKey]map[int64][]observationSample
}

func observationHistoryFromState(state *database.HealthRunResumeState) observationHistory {
	history := observationHistory{
		evidence: make(observationEvidence),
		samples:  make(map[observationProviderKey]map[int64][]observationSample),
	}
	chunks := append([]database.HealthRunChunkState(nil), state.Chunks...)
	sort.SliceStable(chunks, func(i, j int) bool {
		if !chunks[i].CommittedAt.Equal(chunks[j].CommittedAt) {
			return chunks[i].CommittedAt.Before(chunks[j].CommittedAt)
		}
		return chunks[i].ID < chunks[j].ID
	})
	for _, chunk := range chunks {
		provider := observationProviderKey{
			ID: chunk.ProviderID, Generation: chunk.ProviderGeneration,
			ActivationEpoch: chunk.ProviderActivationEpoch,
		}
		if history.samples[provider] == nil {
			history.samples[provider] = make(map[int64][]observationSample)
		}
		for relative := int64(0); relative < chunk.SegmentCount; relative++ {
			if !observationBitmapSet(chunk.TestedBitmap, relative) {
				continue
			}
			outcome := observationChunkOutcome(chunk, relative)
			position := chunk.SegmentStart + relative
			history.evidence.record(provider, position, outcome)
			history.samples[provider][position] = append(history.samples[provider][position], observationSample{
				outcome: outcome, observedAt: chunk.CommittedAt.UTC(), stage: chunk.Stage,
			})
		}
	}
	return history
}

func observationBitmapSet(bitmap []byte, relative int64) bool {
	return relative >= 0 && relative/8 < int64(len(bitmap)) && bitmap[relative/8]&(1<<uint(relative%8)) != 0
}

func observationChunkOutcome(chunk database.HealthRunChunkState, relative int64) observationOutcome {
	switch {
	case observationBitmapSet(chunk.PresentBitmap, relative):
		return observationOutcomePresent
	case observationBitmapSet(chunk.AbsentBitmap, relative):
		return observationOutcomeHardAbsent
	case observationBitmapSet(chunk.CorruptBitmap, relative):
		return observationOutcomeCorrupt
	case observationBitmapSet(chunk.TemporaryBitmap, relative):
		return observationOutcomeTemporary
	default:
		return observationOutcomeInconclusive
	}
}

func analyzeObservationState(
	state *database.HealthRunResumeState,
	revision *database.HealthFileRevision,
	targets []observationSegmentTarget,
	providers []observationDispatchProvider,
	config observationWorkerConfig,
	now time.Time,
) observationStateAnalysis {
	history := observationHistoryFromState(state)
	if planned, ok := nextObservationChunk(observationPlanInput{
		FileRevisionID: state.Run.FileRevisionID, Targets: targets,
		Providers: providers, Evidence: history.evidence,
	}, config.ChunkSize); ok {
		return observationStateAnalysis{work: &observationWork{
			provider: plannedProvider(planned, providers), stage: "observe_initial",
			kind: database.HealthObservationSTAT, segmentStart: planned.SegmentStart,
			segmentCount: planned.SegmentCount, targets: planned.Targets,
		}}
	}

	retry, retryDue := nextObservationRetry(state, history, targets, providers, config.ChunkSize, now)
	confirmation, confirmationDue, gaps := nextObservationConfirmation(
		state, revision, history, targets, providers, config, now,
	)
	if retry != nil {
		return observationStateAnalysis{work: retry, gaps: gaps}
	}
	if confirmation != nil {
		return observationStateAnalysis{work: confirmation, gaps: gaps}
	}
	return observationStateAnalysis{nextDue: earlierObservationDue(retryDue, confirmationDue), gaps: gaps}
}

func earlierObservationDue(first, second *time.Time) *time.Time {
	if first == nil {
		return second
	}
	if second == nil || first.Before(*second) {
		return first
	}
	return second
}

func plannedProvider(planned observationPlannedChunk, providers []observationDispatchProvider) observationDispatchProvider {
	for _, provider := range providers {
		if provider.ID == planned.ProviderID && provider.Generation == planned.ProviderGeneration &&
			provider.ActivationEpoch == planned.ActivationEpoch {
			return provider
		}
	}
	return observationDispatchProvider{
		ID: planned.ProviderID, Generation: planned.ProviderGeneration,
		ActivationEpoch: planned.ActivationEpoch,
	}
}

func nextObservationRetry(
	state *database.HealthRunResumeState,
	history observationHistory,
	targets []observationSegmentTarget,
	providers []observationDispatchProvider,
	chunkSize int64,
	now time.Time,
) (*observationWork, *time.Time) {
	retries := append([]database.HealthRunRetryState(nil), state.Retries...)
	sort.SliceStable(retries, func(i, j int) bool {
		if !retries[i].NextAttemptAt.Equal(retries[j].NextAttemptAt) {
			return retries[i].NextAttemptAt.Before(retries[j].NextAttemptAt)
		}
		return retries[i].RetryKey < retries[j].RetryKey
	})
	chunks := make(map[string]database.HealthRunChunkState, len(state.Chunks))
	for _, chunk := range state.Chunks {
		chunks[chunk.ID] = chunk
	}
	var earliest *time.Time
	for i := range retries {
		retry := &retries[i]
		if retry.Exhausted {
			continue
		}
		key := observationProviderKey{
			ID: retry.ProviderID, Generation: retry.ProviderGeneration,
			ActivationEpoch: retry.ProviderActivationEpoch,
		}
		selected := make([]observationSegmentTarget, 0, retry.SegmentCount)
		for _, target := range targets {
			if target.Position < retry.SegmentStart || target.Position >= retry.SegmentStart+retry.SegmentCount ||
				history.evidence.presentAnywhere(target.Position) {
				continue
			}
			outcome, ok := history.evidence.outcome(key, target.Position)
			if ok && (outcome == observationOutcomeTemporary || outcome == observationOutcomeUnavailable ||
				outcome == observationOutcomeInconclusive) {
				selected = append(selected, target)
			}
		}
		if len(selected) == 0 {
			continue
		}
		if now.Before(retry.NextAttemptAt) {
			due := retry.NextAttemptAt.UTC()
			if earliest == nil || due.Before(*earliest) {
				earliest = &due
			}
			continue
		}
		source := chunks[retry.SourceChunkID]
		kind := source.ObservationKind
		if kind == "" {
			kind = database.HealthObservationSTAT
		}
		if int64(len(selected)) > chunkSize {
			selected = selected[:chunkSize]
		}
		provider := observationDispatchProvider{
			ID: retry.ProviderID, Generation: retry.ProviderGeneration,
			ActivationEpoch: retry.ProviderActivationEpoch,
		}
		for _, snapshotProvider := range providers {
			if snapshotProvider.key() == provider.key() {
				provider = snapshotProvider
				break
			}
		}
		return &observationWork{
			provider: provider, stage: observationRetryStage(source.Stage, retry.Attempt+1),
			kind: kind, freshTransport: kind == database.HealthObservationValidatedBody,
			segmentStart: retry.SegmentStart, segmentCount: retry.SegmentCount,
			targets: selected, retry: retry,
		}, nil
	}
	return nil, earliest
}

func observationRetryStage(sourceStage string, attempt int) string {
	if strings.HasPrefix(sourceStage, "observe_confirmation_") {
		base := sourceStage
		if index := strings.Index(base, "_retry_"); index >= 0 {
			base = base[:index]
		}
		return fmt.Sprintf("%s_retry_%d", base, attempt)
	}
	return fmt.Sprintf("observe_retry_%d", attempt)
}

func nextObservationConfirmation(
	state *database.HealthRunResumeState,
	revision *database.HealthFileRevision,
	history observationHistory,
	targets []observationSegmentTarget,
	providers []observationDispatchProvider,
	config observationWorkerConfig,
	now time.Time,
) (*observationWork, *time.Time, []database.GapRangeWrite) {
	var earliest *time.Time
	var gaps []database.GapRangeWrite
	for _, target := range targets {
		if history.evidence.presentAnywhere(target.Position) {
			continue
		}
		waves := completedObservationWaves(history, providers, target.Position)
		if len(waves) == 0 {
			continue
		}
		latest := waves[len(waves)-1]
		if len(waves) >= 2 && coherentObservationWavePair(
			waves[len(waves)-2], latest, providers, config.ConfirmationDelay,
		) {
			causes := observationGapCauses(latest, providers)
			kind := database.GapKindConfirmedAbsent
			for _, cause := range causes {
				if cause.Cause == database.GapCauseCorrupt {
					kind = database.GapKindConfirmedUnusable
					break
				}
			}
			createdAt := revision.ActivatedAt.UTC()
			if createdAt.IsZero() {
				createdAt = revision.CreatedAt.UTC()
			}
			if createdAt.IsZero() {
				createdAt = state.Run.CreatedAt.UTC()
			}
			gaps = append(gaps, database.GapRangeWrite{
				ID:             stableObservationID("gap", revision.ID, string(kind), fmt.Sprint(target.Position)),
				FileRevisionID: revision.ID, Kind: kind, StartSegment: target.Position,
				SegmentCount: 1, Status: database.GapStatusActive, CreatedAt: createdAt,
				Causes: causes,
			})
			continue
		}

		nextWave := latest.number + 1
		dueAt := latest.completedAt.Add(config.ConfirmationDelay)
		if now.Before(dueAt) {
			due := dueAt.UTC()
			if earliest == nil || due.Before(*earliest) {
				earliest = &due
			}
			continue
		}
		for _, provider := range providers {
			if _, checked := latestObservationWaveSample(
				history.samples[provider.key()][target.Position], nextWave,
			); checked {
				continue
			}
			kind := database.HealthObservationSTAT
			fresh := false
			if latest.causes[provider.key()] == database.GapCauseCorrupt {
				kind = database.HealthObservationValidatedBody
				fresh = true
			}
			return &observationWork{
				provider: provider, stage: fmt.Sprintf("observe_confirmation_%d", nextWave), kind: kind,
				freshTransport: fresh, segmentStart: target.Position,
				segmentCount: 1, targets: []observationSegmentTarget{target},
			}, earliest, gaps
		}
	}
	return nil, earliest, gaps
}

type completedObservationWave struct {
	number      int
	startedAt   time.Time
	completedAt time.Time
	causes      map[observationProviderKey]database.GapCause
	observedAt  map[observationProviderKey]time.Time
}

func completedObservationWaves(
	history observationHistory,
	providers []observationDispatchProvider,
	position int64,
) []completedObservationWave {
	maxWave := 1
	for _, provider := range providers {
		for _, sample := range history.samples[provider.key()][position] {
			maxWave = max(maxWave, observationWaveNumber(sample.stage))
		}
	}
	completed := make([]completedObservationWave, 0, maxWave)
	for waveNumber := 1; waveNumber <= maxWave; waveNumber++ {
		wave := completedObservationWave{
			number: waveNumber, causes: make(map[observationProviderKey]database.GapCause),
			observedAt: make(map[observationProviderKey]time.Time),
		}
		for _, provider := range providers {
			sample, ok := latestObservationWaveSample(history.samples[provider.key()][position], waveNumber)
			if !ok || (sample.outcome != observationOutcomeHardAbsent && sample.outcome != observationOutcomeCorrupt) {
				return completed
			}
			cause := database.GapCauseAbsent
			if sample.outcome == observationOutcomeCorrupt {
				cause = database.GapCauseCorrupt
			}
			wave.causes[provider.key()] = cause
			wave.observedAt[provider.key()] = sample.observedAt.UTC()
			if wave.startedAt.IsZero() || sample.observedAt.Before(wave.startedAt) {
				wave.startedAt = sample.observedAt.UTC()
			}
			if sample.observedAt.After(wave.completedAt) {
				wave.completedAt = sample.observedAt.UTC()
			}
		}
		completed = append(completed, wave)
	}
	return completed
}

func latestObservationWaveSample(samples []observationSample, wave int) (observationSample, bool) {
	var latest observationSample
	found := false
	for _, sample := range samples {
		if observationWaveNumber(sample.stage) != wave {
			continue
		}
		if !found || !sample.observedAt.Before(latest.observedAt) {
			latest = sample
			found = true
		}
	}
	return latest, found
}

func observationWaveNumber(stage string) int {
	const prefix = "observe_confirmation_"
	if !strings.HasPrefix(stage, prefix) {
		return 1
	}
	remainder := strings.TrimPrefix(stage, prefix)
	if index := strings.IndexByte(remainder, '_'); index >= 0 {
		remainder = remainder[:index]
	}
	wave, err := strconv.Atoi(remainder)
	if err != nil || wave < 2 {
		return 1
	}
	return wave
}

func coherentObservationWavePair(
	first, second completedObservationWave,
	providers []observationDispatchProvider,
	delay time.Duration,
) bool {
	if second.number != first.number+1 || second.startedAt.Before(first.completedAt.Add(delay)) {
		return false
	}
	for _, provider := range providers {
		if first.causes[provider.key()] != second.causes[provider.key()] {
			return false
		}
	}
	return true
}

func observationGapCauses(
	wave completedObservationWave,
	providers []observationDispatchProvider,
) []database.GapProviderCause {
	causes := make([]database.GapProviderCause, 0, len(providers))
	for _, provider := range providers {
		causes = append(causes, database.GapProviderCause{
			ProviderID: provider.ID, ProviderGeneration: provider.Generation,
			ProviderActivationEpoch: provider.ActivationEpoch,
			Cause:                   wave.causes[provider.key()], ConfirmationCount: 2,
			ConfirmedAt: wave.observedAt[provider.key()],
		})
	}
	return causes
}

func buildObservationCommit(
	run database.HealthRun,
	state *database.HealthRunResumeState,
	providers []observationDispatchProvider,
	work observationWork,
	results []normalizedObservationResult,
	admissionWait time.Duration,
	committedAt time.Time,
) database.HealthChunkCommit {
	bitmapBytes := int((work.segmentCount + 7) / 8)
	commit := database.HealthChunkCommit{
		ChunkID: stableObservationID("chunk", run.ID, work.stage, work.provider.ID,
			fmt.Sprint(work.provider.Generation), fmt.Sprint(work.provider.ActivationEpoch),
			fmt.Sprint(work.segmentStart), fmt.Sprint(work.segmentCount)),
		RunID: run.ID, LeaseOwner: *run.LeaseOwner, FencingToken: run.FencingToken,
		ProviderID: work.provider.ID, ProviderGeneration: work.provider.Generation,
		ProviderActivationEpoch: work.provider.ActivationEpoch,
		Stage:                   work.stage, ObservationKind: work.kind,
		SegmentStart: work.segmentStart, SegmentCount: work.segmentCount,
		TestedBitmap: make([]byte, bitmapBytes), PresentBitmap: make([]byte, bitmapBytes),
		AbsentBitmap: make([]byte, bitmapBytes), CorruptBitmap: make([]byte, bitmapBytes),
		TemporaryBitmap: make([]byte, bitmapBytes), InconclusiveBitmap: make([]byte, bitmapBytes),
		ResolvedBitmap: make([]byte, bitmapBytes), CursorSegment: work.segmentStart + work.segmentCount,
		CommittedAt: committedAt.UTC(),
	}
	history := observationHistoryFromState(state)
	for _, result := range results {
		history.evidence.record(work.provider.key(), result.target.Position, result.outcome)
	}
	for _, result := range results {
		relative := result.target.Position - work.segmentStart
		setObservationBitmap(commit.TestedBitmap, relative)
		commit.ProviderChecksDelta++
		switch result.outcome {
		case observationOutcomePresent:
			setObservationBitmap(commit.PresentBitmap, relative)
		case observationOutcomeHardAbsent:
			setObservationBitmap(commit.AbsentBitmap, relative)
			commit.MissingCandidatesDelta++
		case observationOutcomeCorrupt:
			setObservationBitmap(commit.CorruptBitmap, relative)
			commit.MissingCandidatesDelta++
		case observationOutcomeTemporary:
			setObservationBitmap(commit.TemporaryBitmap, relative)
			commit.InconclusiveDelta++
		default:
			setObservationBitmap(commit.InconclusiveBitmap, relative)
			commit.InconclusiveDelta++
		}
		if result.outcome == observationOutcomePresent ||
			allObservationProvidersConclusive(history.evidence, providers, result.target.Position) {
			setObservationBitmap(commit.ResolvedBitmap, relative)
			commit.ResolvedDelta++
		}
		for attemptIndex, attempt := range result.attempts {
			var responseCode *int
			if attempt.ResponseCode != 0 {
				code := attempt.ResponseCode
				responseCode = &code
			}
			wait := time.Duration(0)
			if attemptIndex == 0 {
				wait = max(admissionWait, 0)
			}
			commit.Attempts = append(commit.Attempts, database.HealthAttemptEvidence{
				IdempotencyKey: stableObservationID("attempt", commit.ChunkID,
					fmt.Sprint(result.target.Position), fmt.Sprint(attemptIndex)),
				SegmentIndex: result.target.Position, Operation: attempt.Operation,
				Outcome: durableObservationOutcome(attempt.Outcome), ResponseCode: responseCode,
				BodyValidation: attempt.BodyValidation, CauseClass: attempt.CauseClass,
				AdmissionWait: wait, PoolQueue: attempt.PoolQueue,
				PipelineWait: attempt.PipelineWait, ResponseService: attempt.ResponseService,
				ObservedAt: committedAt.UTC(),
			})
		}
		if result.outcome == observationOutcomeHardAbsent || result.outcome == observationOutcomeCorrupt {
			cause := database.GapCauseAbsent
			if result.outcome == observationOutcomeCorrupt {
				cause = database.GapCauseCorrupt
			}
			commit.Confirmations = append(commit.Confirmations, database.HealthConfirmationEvent{
				IdempotencyKey: stableObservationID("confirmation", commit.ChunkID,
					fmt.Sprint(result.target.Position), string(cause)),
				SegmentIndex: result.target.Position, Cause: cause, ObservedAt: committedAt.UTC(),
			})
		}
	}
	commit.Retry = observationRetryAfterCommit(run.ID, work, results, committedAt)
	return commit
}

func allObservationProvidersConclusive(evidence observationEvidence, providers []observationDispatchProvider, position int64) bool {
	if len(providers) == 0 {
		return false
	}
	for _, provider := range providers {
		outcome, ok := evidence.outcome(provider.key(), position)
		if !ok || (outcome != observationOutcomeHardAbsent && outcome != observationOutcomeCorrupt) {
			return false
		}
	}
	return true
}

func setObservationBitmap(bitmap []byte, relative int64) {
	if relative >= 0 && relative/8 < int64(len(bitmap)) {
		bitmap[relative/8] |= 1 << uint(relative%8)
	}
}

func observationRetryAfterCommit(
	runID string,
	work observationWork,
	results []normalizedObservationResult,
	committedAt time.Time,
) *database.HealthRetryState {
	hasInconclusive := false
	for _, result := range results {
		if result.outcome == observationOutcomeTemporary || result.outcome == observationOutcomeUnavailable ||
			result.outcome == observationOutcomeInconclusive {
			hasInconclusive = true
			break
		}
	}
	if work.retry == nil && !hasInconclusive {
		return nil
	}
	retryKey := stableObservationID("retry", runID, work.provider.ID,
		fmt.Sprint(work.provider.Generation), fmt.Sprint(work.provider.ActivationEpoch),
		fmt.Sprint(work.segmentStart), fmt.Sprint(work.segmentCount))
	attempt := 0
	if work.retry != nil {
		retryKey = work.retry.RetryKey
		attempt = work.retry.Attempt + 1
	}
	due, scheduled := nextHealthRetryAt(committedAt.UTC(), attempt)
	return &database.HealthRetryState{
		RetryKey: retryKey, SegmentStart: work.segmentStart, SegmentCount: work.segmentCount,
		Outcome: "temporary", Attempt: attempt, NextAttemptAt: due, Exhausted: !hasInconclusive || !scheduled,
	}
}

func stableObservationID(parts ...string) string {
	digest := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return "obs-" + hex.EncodeToString(digest[:])
}

func (w *observationWorker) settleRun(
	ctx context.Context,
	run *database.HealthRun,
	state *database.HealthRunResumeState,
	analysis observationStateAnalysis,
) (observationWorkerStep, error) {
	for _, gap := range analysis.gaps {
		if _, err := w.repository.UpsertGapRange(ctx, gap); err != nil {
			return observationWorkerParked, err
		}
	}
	now := w.config.Clock.Now()
	if analysis.work != nil {
		if err := w.repository.ParkHealthRun(ctx, run.ID, *run.LeaseOwner,
			run.FencingToken, now, now); err != nil {
			return observationWorkerParked, err
		}
		return observationWorkerParked, nil
	}
	if analysis.nextDue != nil {
		if err := w.repository.ParkHealthRun(ctx, run.ID, *run.LeaseOwner,
			run.FencingToken, *analysis.nextDue, now); err != nil {
			return observationWorkerParked, err
		}
		return observationWorkerParked, nil
	}
	if err := w.repository.CompleteHealthRun(ctx, run.ID, *run.LeaseOwner, run.FencingToken, now); err != nil {
		return observationWorkerCompleted, err
	}
	completed := state.Run
	completed.Status = database.HealthRunCompleted
	completed.LeaseOwner = nil
	completed.LeaseExpiresAt = nil
	completed.UpdatedAt = now
	w.publishProgress(completed, now)
	return observationWorkerCompleted, nil
}

func (w *observationWorker) failRun(
	ctx context.Context,
	run *database.HealthRun,
	reason string,
	cause error,
) (observationWorkerStep, error) {
	now := w.config.Clock.Now()
	if err := w.repository.FailHealthRun(ctx, run.ID, *run.LeaseOwner,
		run.FencingToken, reason, now); err != nil {
		return observationWorkerFailed, err
	}
	failed := *run
	failed.Status = database.HealthRunFailed
	failed.LastError = reason
	w.publishProgress(failed, now)
	return observationWorkerFailed, cause
}

func (w *observationWorker) publishProgress(run database.HealthRun, at time.Time) {
	if w.progress == nil {
		return
	}
	event := observationProgressEvent{
		RunID: run.ID, FileRevisionID: run.FileRevisionID, Status: run.Status,
		ResolvedSegments: run.ResolvedSegments, TotalSegments: run.TotalSegments,
		ProviderChecks: run.ProviderChecks, MissingCandidates: run.MissingCandidates,
		InconclusiveCount: run.InconclusiveCount, Stage: run.Stage, ObservedAt: at.UTC(),
	}
	if run.CurrentProviderID != nil {
		event.CurrentProviderID = *run.CurrentProviderID
	}
	start := run.CreatedAt
	if run.StartedAt != nil {
		start = *run.StartedAt
	}
	if elapsed := at.Sub(start); elapsed > 0 && run.ProviderChecks > 0 {
		event.ChecksPerSecond = float64(run.ProviderChecks) / elapsed.Seconds()
		remaining := max(run.TotalSegments-run.ResolvedSegments, 0)
		event.EstimatedCompletionDelay = time.Duration(float64(remaining) / event.ChecksPerSecond * float64(time.Second))
	}
	w.progress.PublishObservationProgress(event)
}
