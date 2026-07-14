package health

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/javi11/altmount/internal/database"
)

type observationOutcome string

const (
	observationOutcomePresent      observationOutcome = "present"
	observationOutcomeHardAbsent   observationOutcome = "hard_absent"
	observationOutcomeCorrupt      observationOutcome = "corrupt"
	observationOutcomeTemporary    observationOutcome = "temporary"
	observationOutcomeUnavailable  observationOutcome = "unavailable"
	observationOutcomeCanceled     observationOutcome = "canceled"
	observationOutcomeInconclusive observationOutcome = "inconclusive"
)

type observationChunkRange struct {
	FileRevisionID string
	SegmentStart   int64
	SegmentCount   int64
}

func deterministicObservationChunks(fileRevisionID string, totalSegments, chunkSize int64) []observationChunkRange {
	if fileRevisionID == "" || totalSegments <= 0 || chunkSize <= 0 {
		return nil
	}
	chunks := make([]observationChunkRange, 0, (totalSegments+chunkSize-1)/chunkSize)
	for start := int64(0); start < totalSegments; start += chunkSize {
		chunks = append(chunks, observationChunkRange{
			FileRevisionID: fileRevisionID,
			SegmentStart:   start,
			SegmentCount:   min(chunkSize, totalSegments-start),
		})
	}
	return chunks
}

type observationSegmentTarget struct {
	Position    int64
	MessageID   string
	UsableBytes int64
}

type observationProviderKey struct {
	ID              string
	Generation      int64
	ActivationEpoch int64
}

type observationDispatchProvider struct {
	ID              string
	Generation      int64
	ActivationEpoch int64
	Role            database.ProviderRole
	Order           int
}

func (p observationDispatchProvider) key() observationProviderKey {
	return observationProviderKey{ID: p.ID, Generation: p.Generation, ActivationEpoch: p.ActivationEpoch}
}

type observationEvidence map[observationProviderKey]map[int64]observationOutcome

func (e observationEvidence) record(provider observationProviderKey, position int64, outcome observationOutcome) {
	if e == nil {
		return
	}
	positions := e[provider]
	if positions == nil {
		positions = make(map[int64]observationOutcome)
		e[provider] = positions
	}
	positions[position] = outcome
}

func (e observationEvidence) outcome(provider observationProviderKey, position int64) (observationOutcome, bool) {
	positions := e[provider]
	if positions == nil {
		return "", false
	}
	outcome, ok := positions[position]
	return outcome, ok
}

func (e observationEvidence) presentAnywhere(position int64) bool {
	for _, positions := range e {
		if positions[position] == observationOutcomePresent {
			return true
		}
	}
	return false
}

type observationPlanInput struct {
	FileRevisionID      string
	Targets             []observationSegmentTarget
	Providers           []observationDispatchProvider
	Evidence            observationEvidence
	RefreshProviderKeys map[observationProviderKey]bool
}

type observationPlannedChunk struct {
	FileRevisionID     string
	ProviderID         string
	ProviderGeneration int64
	ActivationEpoch    int64
	SegmentStart       int64
	SegmentCount       int64
	Targets            []observationSegmentTarget
}

func (c observationPlannedChunk) providerKey() observationProviderKey {
	return observationProviderKey{
		ID: c.ProviderID, Generation: c.ProviderGeneration, ActivationEpoch: c.ActivationEpoch,
	}
}

func nextObservationChunk(input observationPlanInput, chunkSize int64) (observationPlannedChunk, bool) {
	if input.FileRevisionID == "" || chunkSize <= 0 || len(input.Targets) == 0 || len(input.Providers) == 0 {
		return observationPlannedChunk{}, false
	}
	providers := append([]observationDispatchProvider(nil), input.Providers...)
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
	targets := append([]observationSegmentTarget(nil), input.Targets...)
	sort.SliceStable(targets, func(i, j int) bool { return targets[i].Position < targets[j].Position })

	for _, provider := range providers {
		key := provider.key()
		refresh := input.RefreshProviderKeys[key]
		for _, target := range targets {
			if input.Evidence.presentAnywhere(target.Position) {
				continue
			}
			if _, checked := input.Evidence.outcome(key, target.Position); checked && !refresh {
				continue
			}
			windowStart := (target.Position / chunkSize) * chunkSize
			windowEnd := windowStart + chunkSize
			selected := make([]observationSegmentTarget, 0, chunkSize)
			windowCount := int64(0)
			for _, candidate := range targets {
				if candidate.Position < windowStart || candidate.Position >= windowEnd {
					continue
				}
				windowCount = max(windowCount, candidate.Position-windowStart+1)
				if input.Evidence.presentAnywhere(candidate.Position) {
					continue
				}
				if _, checked := input.Evidence.outcome(key, candidate.Position); checked && !refresh {
					continue
				}
				selected = append(selected, candidate)
			}
			if len(selected) == 0 {
				continue
			}
			return observationPlannedChunk{
				FileRevisionID: input.FileRevisionID,
				ProviderID:     provider.ID, ProviderGeneration: provider.Generation,
				ActivationEpoch: provider.ActivationEpoch,
				SegmentStart:    windowStart, SegmentCount: windowCount,
				Targets: selected,
			}, true
		}
	}
	return observationPlannedChunk{}, false
}

func observationTargetPositions(targets []observationSegmentTarget) []int64 {
	positions := make([]int64, len(targets))
	for i := range targets {
		positions[i] = targets[i].Position
	}
	return positions
}

type observationTransportResult struct {
	MessageID string
	Outcome   observationOutcome
	Attempts  []observationTransportAttempt
	Err       error
}

type correlatedObservationResults struct {
	Outcomes         map[int64]observationOutcome
	OmittedPositions []int64
}

func correlateObservationResults(targets []observationSegmentTarget, results []observationTransportResult) correlatedObservationResults {
	positionsByID := make(map[string][]int64, len(targets))
	for _, target := range targets {
		positionsByID[target.MessageID] = append(positionsByID[target.MessageID], target.Position)
	}
	used := make(map[string]int, len(positionsByID))
	out := correlatedObservationResults{Outcomes: make(map[int64]observationOutcome, len(results))}
	for _, result := range results {
		positions := positionsByID[result.MessageID]
		index := used[result.MessageID]
		if index >= len(positions) {
			continue
		}
		out.Outcomes[positions[index]] = result.Outcome
		used[result.MessageID] = index + 1
	}
	for _, target := range targets {
		if _, ok := out.Outcomes[target.Position]; !ok {
			out.OmittedPositions = append(out.OmittedPositions, target.Position)
		}
	}
	return out
}

type observationAdmission struct {
	global      chan struct{}
	perProvider int
	mu          sync.Mutex
	providers   map[string]chan struct{}
}

func newObservationAdmission(global, perProvider int) *observationAdmission {
	if global <= 0 {
		global = 1
	}
	if perProvider <= 0 {
		perProvider = 1
	}
	return &observationAdmission{
		global: make(chan struct{}, global), perProvider: perProvider,
		providers: make(map[string]chan struct{}),
	}
}

func (a *observationAdmission) provider(providerID string) chan struct{} {
	a.mu.Lock()
	defer a.mu.Unlock()
	semaphore := a.providers[providerID]
	if semaphore == nil {
		semaphore = make(chan struct{}, a.perProvider)
		a.providers[providerID] = semaphore
	}
	return semaphore
}

func acquireSemaphore(ctx context.Context, semaphore chan struct{}) error {
	select {
	case semaphore <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *observationAdmission) Acquire(ctx context.Context, providerID string) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	provider := a.provider(providerID)
	// Provider capacity is acquired first so a same-provider waiter cannot
	// occupy global capacity needed by another provider.
	if err := acquireSemaphore(ctx, provider); err != nil {
		return nil, err
	}
	if err := acquireSemaphore(ctx, a.global); err != nil {
		<-provider
		return nil, err
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			<-a.global
			<-provider
		})
	}, nil
}

var ErrObservationPausedForPlayback = errors.New("observation admission paused for playback")

type observationDispatchGate struct {
	admission        *observationAdmission
	playback         PlaybackActivitySource
	pauseForPlayback bool
}

func newObservationDispatchGate(admission *observationAdmission, playback PlaybackActivitySource, pause bool) *observationDispatchGate {
	return &observationDispatchGate{admission: admission, playback: playback, pauseForPlayback: pause}
}

func (g *observationDispatchGate) playbackActive() bool {
	return g.pauseForPlayback && g.playback != nil && g.playback.ActiveStreams() > 0
}

func (g *observationDispatchGate) Acquire(ctx context.Context, providerID string) (func(), error) {
	if g.playbackActive() {
		return nil, ErrObservationPausedForPlayback
	}
	release, err := g.admission.Acquire(ctx, providerID)
	if err != nil {
		return nil, err
	}
	if g.playbackActive() {
		release()
		return nil, ErrObservationPausedForPlayback
	}
	return release, nil
}

func nextHealthRetryAt(at time.Time, attempt int) (time.Time, bool) {
	delays := [...]time.Duration{30 * time.Second, 2 * time.Minute, 10 * time.Minute, time.Hour}
	if attempt < 0 || attempt >= len(delays) {
		return time.Time{}, false
	}
	return at.Add(delays[attempt]), true
}

func importSecondPassDueAt(firstPassCompletedAt time.Time, delay time.Duration) time.Time {
	return firstPassCompletedAt.Add(delay)
}

func importSecondPassDue(now, due time.Time) bool { return !now.Before(due) }

func confirmationEligible(previous *time.Time, observedAt time.Time, minimumDelay time.Duration) bool {
	return previous == nil || !observedAt.Before(previous.Add(minimumDelay))
}

func nextGapRevalidationAt(confirmedAt time.Time, milestone int) (time.Time, bool) {
	ages := [...]time.Duration{24 * time.Hour, 3 * 24 * time.Hour, 7 * 24 * time.Hour, 14 * 24 * time.Hour}
	if milestone < 0 || milestone >= len(ages) {
		return time.Time{}, false
	}
	return confirmedAt.Add(ages[milestone]), true
}

func advanceGapRevalidationMilestone(current int, outcome observationOutcome) int {
	switch outcome {
	case observationOutcomePresent, observationOutcomeHardAbsent, observationOutcomeCorrupt:
		return current + 1
	default:
		return current
	}
}

type observationRunResumeDecision struct {
	Compatible    bool
	Abandon       bool
	CursorSegment int64
}

func decideObservationRunResume(run *database.HealthRun, revision *database.HealthFileRevision) observationRunResumeDecision {
	if run == nil || revision == nil || run.FileRevisionID != revision.ID || run.TotalSegments != revision.SegmentCount {
		return observationRunResumeDecision{Abandon: true}
	}
	return observationRunResumeDecision{Compatible: true, CursorSegment: run.CursorSegment}
}

type committedObservationCoverage struct {
	LayoutFingerprint  string
	ProviderSnapshotID string
	ObservationKind    database.HealthObservationKind
	TotalSegments      int64
	CoveredSegments    int64
	CanonicalLayout    bool
	Completed          bool
}

type observationCoverageRequirement struct {
	LayoutFingerprint  string
	ProviderSnapshotID string
	ObservationKind    database.HealthObservationKind
	TotalSegments      int64
}

func canReuseCommittedCoverage(coverage committedObservationCoverage, requirement observationCoverageRequirement) bool {
	return coverage.Completed && coverage.CanonicalLayout &&
		coverage.LayoutFingerprint == requirement.LayoutFingerprint &&
		coverage.ProviderSnapshotID == requirement.ProviderSnapshotID &&
		coverage.ObservationKind == requirement.ObservationKind &&
		coverage.TotalSegments == requirement.TotalSegments &&
		coverage.CoveredSegments == requirement.TotalSegments
}

type revalidationDispatch struct {
	ObservationKind database.HealthObservationKind
	FreshTransport  bool
}

func revalidationDispatchForCause(cause database.GapCause) revalidationDispatch {
	if cause == database.GapCauseCorrupt {
		return revalidationDispatch{ObservationKind: database.HealthObservationValidatedBody, FreshTransport: true}
	}
	return revalidationDispatch{ObservationKind: database.HealthObservationSTAT}
}

type observationEffects struct {
	PersistEvidence   bool
	PersistentPadding bool
	DestructiveRepair bool
	DeleteFile        bool
}

func observationSideEffects(_ database.GapKind, observationMode bool) observationEffects {
	if observationMode {
		return observationEffects{PersistEvidence: true}
	}
	return observationEffects{PersistEvidence: true}
}
