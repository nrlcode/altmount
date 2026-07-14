package health

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/nntppool/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pr5ObservationWorkerRepository struct {
	mu       sync.Mutex
	now      func() time.Time
	run      database.HealthRun
	revision database.HealthFileRevision
	snapshot database.ProviderSnapshot
	state    database.HealthRunResumeState
	due      time.Time
	commits  []database.HealthChunkCommit
	gaps     []database.GapRangeWrite
}

func newPR5ObservationWorkerRepository(clock *pr5FakeClock, total int64, providers ...database.ProviderSnapshotEntry) *pr5ObservationWorkerRepository {
	now := clock.Now()
	owner := ""
	run := database.HealthRun{
		ID: "synthetic-run", FileRevisionID: "synthetic-revision",
		ProviderSnapshotID: "synthetic-snapshot", Trigger: "scheduled",
		Mode: "observation", Status: database.HealthRunPending,
		LeaseOwner: &owner, TotalSegments: total, CreatedAt: now, UpdatedAt: now,
	}
	revision := database.HealthFileRevision{
		ID: run.FileRevisionID, LayoutFingerprint: "sha256:synthetic-layout",
		VirtualSize: total * 100, SegmentCount: total, Active: true,
	}
	repo := &pr5ObservationWorkerRepository{
		now: clock.Now, run: run, revision: revision,
		snapshot: database.ProviderSnapshot{ID: run.ProviderSnapshotID, CreatedAt: now, Entries: providers},
		due:      now,
	}
	repo.state.Run = run
	return repo
}

func (r *pr5ObservationWorkerRepository) ClaimDueHealthRun(_ context.Context, owner string, ttl time.Duration) (*database.HealthRun, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.run.Status != database.HealthRunPending || r.now().Before(r.due) {
		return nil, nil
	}
	r.run.Status = database.HealthRunRunning
	r.run.FencingToken++
	r.run.LeaseOwner = &owner
	expires := r.now().Add(ttl)
	r.run.LeaseExpiresAt = &expires
	r.run.UpdatedAt = r.now()
	r.state.Run = r.run
	copy := r.run
	return &copy, nil
}

func (r *pr5ObservationWorkerRepository) GetHealthRunResumeState(_ context.Context, runID string) (*database.HealthRunResumeState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if runID != r.run.ID {
		return nil, nil
	}
	copy := r.state
	copy.Run = r.run
	copy.Chunks = append([]database.HealthRunChunkState(nil), r.state.Chunks...)
	copy.Coverage = append([]database.HealthProviderCoverageState(nil), r.state.Coverage...)
	copy.Retries = append([]database.HealthRunRetryState(nil), r.state.Retries...)
	return &copy, nil
}

func (r *pr5ObservationWorkerRepository) GetFileRevisionForRun(_ context.Context, runID string) (*database.HealthFileRevision, error) {
	if runID != r.run.ID {
		return nil, nil
	}
	copy := r.revision
	return &copy, nil
}

func (r *pr5ObservationWorkerRepository) GetProviderSnapshot(_ context.Context, snapshotID string) (*database.ProviderSnapshot, error) {
	if snapshotID != r.snapshot.ID {
		return nil, nil
	}
	copy := r.snapshot
	copy.Entries = append([]database.ProviderSnapshotEntry(nil), r.snapshot.Entries...)
	return &copy, nil
}

func (r *pr5ObservationWorkerRepository) CommitHealthChunk(_ context.Context, commit database.HealthChunkCommit) (*database.HealthRun, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.run.Status != database.HealthRunRunning || r.run.LeaseOwner == nil ||
		*r.run.LeaseOwner != commit.LeaseOwner || r.run.FencingToken != commit.FencingToken {
		return nil, database.ErrStaleHealthLease
	}
	for _, existing := range r.commits {
		if existing.ChunkID == commit.ChunkID {
			copy := r.run
			return &copy, nil
		}
	}
	r.commits = append(r.commits, commit)
	chunk := database.HealthRunChunkState{
		ID: commit.ChunkID, RunID: commit.RunID, ProviderID: commit.ProviderID,
		ProviderGeneration:      commit.ProviderGeneration,
		ProviderActivationEpoch: commit.ProviderActivationEpoch,
		Stage:                   commit.Stage, ObservationKind: commit.ObservationKind,
		SegmentStart: commit.SegmentStart, SegmentCount: commit.SegmentCount,
		TestedBitmap:       append([]byte(nil), commit.TestedBitmap...),
		PresentBitmap:      append([]byte(nil), commit.PresentBitmap...),
		AbsentBitmap:       append([]byte(nil), commit.AbsentBitmap...),
		CorruptBitmap:      append([]byte(nil), commit.CorruptBitmap...),
		TemporaryBitmap:    append([]byte(nil), commit.TemporaryBitmap...),
		InconclusiveBitmap: append([]byte(nil), commit.InconclusiveBitmap...),
		ResolvedBitmap:     append([]byte(nil), commit.ResolvedBitmap...),
		FencingToken:       commit.FencingToken, ResolvedDelta: commit.ResolvedDelta,
		ProviderChecksDelta:    commit.ProviderChecksDelta,
		MissingCandidatesDelta: commit.MissingCandidatesDelta,
		InconclusiveDelta:      commit.InconclusiveDelta, CommittedAt: commit.CommittedAt,
	}
	r.state.Chunks = append(r.state.Chunks, chunk)
	if commit.Retry != nil {
		retry := database.HealthRunRetryState{
			RetryKey: commit.Retry.RetryKey, SourceChunkID: commit.ChunkID,
			FileRevisionID: r.run.FileRevisionID, ProviderID: commit.ProviderID,
			ProviderGeneration:      commit.ProviderGeneration,
			ProviderActivationEpoch: commit.ProviderActivationEpoch,
			SegmentStart:            commit.Retry.SegmentStart, SegmentCount: commit.Retry.SegmentCount,
			Outcome: commit.Retry.Outcome, Attempt: commit.Retry.Attempt,
			NextAttemptAt: commit.Retry.NextAttemptAt, Exhausted: commit.Retry.Exhausted,
			UpdatedAt: commit.CommittedAt,
		}
		updated := false
		for i := range r.state.Retries {
			if r.state.Retries[i].RetryKey == retry.RetryKey {
				r.state.Retries[i] = retry
				updated = true
			}
		}
		if !updated {
			r.state.Retries = append(r.state.Retries, retry)
		}
	}
	r.run.ResolvedSegments += commit.ResolvedDelta
	r.run.ProviderChecks += commit.ProviderChecksDelta
	r.run.MissingCandidates += commit.MissingCandidatesDelta
	r.run.InconclusiveCount += commit.InconclusiveDelta
	r.run.CursorSegment = max(r.run.CursorSegment, commit.CursorSegment)
	r.run.Stage = commit.Stage
	providerID := commit.ProviderID
	providerGeneration := commit.ProviderGeneration
	r.run.CurrentProviderID = &providerID
	r.run.CurrentProviderGeneration = &providerGeneration
	r.run.UpdatedAt = commit.CommittedAt
	r.state.Run = r.run
	copy := r.run
	return &copy, nil
}

func (r *pr5ObservationWorkerRepository) ParkHealthRun(_ context.Context, runID, owner string, token int64, due, at time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if runID != r.run.ID || r.run.Status != database.HealthRunRunning ||
		r.run.LeaseOwner == nil || *r.run.LeaseOwner != owner || r.run.FencingToken != token {
		return database.ErrStaleHealthLease
	}
	r.run.Status = database.HealthRunPending
	r.run.LeaseOwner = nil
	r.run.LeaseExpiresAt = nil
	r.run.UpdatedAt = at
	r.due = due
	r.state.Run = r.run
	return nil
}

func (r *pr5ObservationWorkerRepository) CompleteHealthRun(_ context.Context, runID, owner string, token int64, at time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if runID != r.run.ID || r.run.Status != database.HealthRunRunning ||
		r.run.LeaseOwner == nil || *r.run.LeaseOwner != owner || r.run.FencingToken != token {
		return database.ErrStaleHealthLease
	}
	r.run.Status = database.HealthRunCompleted
	r.run.LeaseOwner = nil
	r.run.LeaseExpiresAt = nil
	r.run.UpdatedAt = at
	r.state.Run = r.run
	return nil
}

func (r *pr5ObservationWorkerRepository) FailHealthRun(_ context.Context, runID, owner string, token int64, reason string, at time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if runID != r.run.ID || r.run.Status != database.HealthRunRunning ||
		r.run.LeaseOwner == nil || *r.run.LeaseOwner != owner || r.run.FencingToken != token {
		return database.ErrStaleHealthLease
	}
	r.run.Status = database.HealthRunFailed
	r.run.LastError = reason
	r.run.LeaseOwner = nil
	r.run.LeaseExpiresAt = nil
	r.run.UpdatedAt = at
	r.state.Run = r.run
	return nil
}

func (r *pr5ObservationWorkerRepository) UpsertGapRange(_ context.Context, write database.GapRangeWrite) (*database.HealthGapRange, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gaps = append(r.gaps, write)
	return &database.HealthGapRange{
		ID: write.ID, FileRevisionID: write.FileRevisionID, Kind: write.Kind,
		StartSegment: write.StartSegment, SegmentCount: write.SegmentCount,
		Status: write.Status, CreatedAt: write.CreatedAt, Causes: write.Causes,
	}, nil
}

func (r *pr5ObservationWorkerRepository) forceFenceTakeover() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.run.FencingToken++
	owner := "replacement-worker"
	r.run.LeaseOwner = &owner
	r.state.Run = r.run
}

type pr5StaticObservationTargets struct {
	targets []observationSegmentTarget
}

func (s pr5StaticObservationTargets) ObservationTargets(context.Context, *database.HealthFileRevision) ([]observationSegmentTarget, error) {
	return append([]observationSegmentTarget(nil), s.targets...), nil
}

type pr5ScriptedObservationTransport struct {
	mu    sync.Mutex
	calls []observationTransportRequest
	fn    func(context.Context, observationTransportRequest) ([]observationTransportResult, error)
}

func (t *pr5ScriptedObservationTransport) Observe(ctx context.Context, request observationTransportRequest) ([]observationTransportResult, error) {
	t.mu.Lock()
	t.calls = append(t.calls, request)
	t.mu.Unlock()
	return t.fn(ctx, request)
}

func (t *pr5ScriptedObservationTransport) snapshotCalls() []observationTransportRequest {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]observationTransportRequest(nil), t.calls...)
}

type pr5ObservationProgressRecorder struct {
	mu     sync.Mutex
	events []observationProgressEvent
}

func (r *pr5ObservationProgressRecorder) PublishObservationProgress(event observationProgressEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func pr5ObservationProviders() []database.ProviderSnapshotEntry {
	return []database.ProviderSnapshotEntry{
		{ProviderID: "provider-primary-a", ProviderGeneration: 3, ProviderActivationEpoch: 7, Role: database.ProviderRolePrimary, Order: 0},
		{ProviderID: "provider-primary-b", ProviderGeneration: 4, ProviderActivationEpoch: 9, Role: database.ProviderRolePrimary, Order: 1},
		{ProviderID: "provider-backup", ProviderGeneration: 2, ProviderActivationEpoch: 5, Role: database.ProviderRoleBackup, Order: 0},
	}
}

func pr5ObservationTargets(total int) []observationSegmentTarget {
	targets := make([]observationSegmentTarget, total)
	for i := range targets {
		targets[i] = observationSegmentTarget{Position: int64(i), MessageID: "synthetic-message-" + string(rune('a'+i)), UsableBytes: 100}
	}
	return targets
}

func newPR5ObservationWorkerForTest(
	repo *pr5ObservationWorkerRepository,
	clock *pr5FakeClock,
	transport observationTransport,
	chunkSize int64,
	progress observationProgressSink,
) *observationWorker {
	return newObservationWorker(
		repo,
		pr5StaticObservationTargets{targets: pr5ObservationTargets(int(repo.revision.SegmentCount))},
		transport,
		newObservationDispatchGate(newObservationAdmission(2, 1), nil, true),
		progress,
		observationWorkerConfig{
			Owner: "synthetic-worker", LeaseTTL: time.Minute, ChunkSize: chunkSize,
			ConfirmationDelay: 10 * time.Minute, PlaybackRetryDelay: time.Second,
			Clock: clock,
		},
	)
}

func TestPR5ObservationWorkerResumesCommittedStateAndAttachesDispatchSnapshot(t *testing.T) {
	clock := &pr5FakeClock{now: time.Unix(1_801_000_000, 0).UTC()}
	provider := pr5ObservationProviders()[0]
	repo := newPR5ObservationWorkerRepository(clock, 4, provider)
	repo.state.Chunks = append(repo.state.Chunks, database.HealthRunChunkState{
		ID: "already-committed", RunID: repo.run.ID, ProviderID: provider.ProviderID,
		ProviderGeneration: provider.ProviderGeneration, ProviderActivationEpoch: provider.ProviderActivationEpoch,
		Stage: "observe_initial", ObservationKind: database.HealthObservationSTAT,
		SegmentStart: 0, SegmentCount: 2, TestedBitmap: []byte{0b11}, PresentBitmap: []byte{0b11},
		AbsentBitmap: []byte{0}, CorruptBitmap: []byte{0}, TemporaryBitmap: []byte{0},
		InconclusiveBitmap: []byte{0}, ResolvedBitmap: []byte{0b11}, CommittedAt: clock.Now(),
	})
	repo.run.ResolvedSegments = 2
	repo.run.CursorSegment = 2
	repo.state.Run = repo.run

	transport := &pr5ScriptedObservationTransport{fn: func(_ context.Context, request observationTransportRequest) ([]observationTransportResult, error) {
		assert.Equal(t, int64(7), request.Provider.ActivationEpoch)
		results := make([]observationTransportResult, len(request.Targets))
		for i, target := range request.Targets {
			results[i] = observationTransportResult{MessageID: target.MessageID, Outcome: observationOutcomePresent}
		}
		return results, nil
	}}
	progress := &pr5ObservationProgressRecorder{}
	worker := newPR5ObservationWorkerForTest(repo, clock, transport, 2, progress)

	step, err := worker.ProcessNext(context.Background())
	require.NoError(t, err)
	assert.Equal(t, observationWorkerCompleted, step)
	require.Len(t, transport.snapshotCalls(), 1)
	assert.Equal(t, []int64{2, 3}, observationTargetPositions(transport.snapshotCalls()[0].Targets),
		"restart must derive work from committed coverage instead of restarting at zero")
	require.Len(t, repo.commits, 1)
	assert.Equal(t, provider.ProviderID, repo.commits[0].ProviderID)
	assert.Equal(t, provider.ProviderGeneration, repo.commits[0].ProviderGeneration)
	assert.Equal(t, provider.ProviderActivationEpoch, repo.commits[0].ProviderActivationEpoch)
	assert.Equal(t, int64(1), repo.commits[0].FencingToken)
	require.NotEmpty(t, progress.events)
	assert.Equal(t, int64(4), progress.events[len(progress.events)-1].ResolvedSegments)
}

func TestPR5ObservationWorkerRejectsLateCommitAfterFenceTakeover(t *testing.T) {
	clock := &pr5FakeClock{now: time.Unix(1_801_000_100, 0).UTC()}
	repo := newPR5ObservationWorkerRepository(clock, 1, pr5ObservationProviders()[0])
	transport := &pr5ScriptedObservationTransport{fn: func(_ context.Context, request observationTransportRequest) ([]observationTransportResult, error) {
		repo.forceFenceTakeover()
		return []observationTransportResult{{MessageID: request.Targets[0].MessageID, Outcome: observationOutcomePresent}}, nil
	}}
	worker := newPR5ObservationWorkerForTest(repo, clock, transport, 1, nil)

	_, err := worker.ProcessNext(context.Background())
	require.ErrorIs(t, err, database.ErrStaleHealthLease)
	assert.Empty(t, repo.commits)
}

func TestPR5ObservationWorkerUsesOrderedSparseFallbackAndAnySuccessWins(t *testing.T) {
	clock := &pr5FakeClock{now: time.Unix(1_801_001_000, 0).UTC()}
	providers := pr5ObservationProviders()
	repo := newPR5ObservationWorkerRepository(clock, 3, providers...)
	transport := &pr5ScriptedObservationTransport{fn: func(_ context.Context, request observationTransportRequest) ([]observationTransportResult, error) {
		outcomes := map[string][]observationOutcome{
			"provider-primary-a": {observationOutcomePresent, observationOutcomeHardAbsent, observationOutcomeTemporary},
			"provider-primary-b": {observationOutcomePresent, observationOutcomeHardAbsent},
			"provider-backup":    {observationOutcomePresent},
		}[request.Provider.ID]
		require.Len(t, outcomes, len(request.Targets))
		results := make([]observationTransportResult, len(request.Targets))
		for i := range request.Targets {
			results[i] = observationTransportResult{MessageID: request.Targets[i].MessageID, Outcome: outcomes[i]}
		}
		return results, nil
	}}
	worker := newPR5ObservationWorkerForTest(repo, clock, transport, 8, nil)

	for repo.run.Status != database.HealthRunCompleted {
		_, err := worker.ProcessNext(context.Background())
		require.NoError(t, err)
	}
	calls := transport.snapshotCalls()
	require.Len(t, calls, 3)
	assert.Equal(t, "provider-primary-a", calls[0].Provider.ID)
	assert.Equal(t, []int64{0, 1, 2}, observationTargetPositions(calls[0].Targets))
	assert.Equal(t, "provider-primary-b", calls[1].Provider.ID)
	assert.Equal(t, []int64{1, 2}, observationTargetPositions(calls[1].Targets))
	assert.Equal(t, "provider-backup", calls[2].Provider.ID)
	assert.Equal(t, []int64{2}, observationTargetPositions(calls[2].Targets))
	assert.Empty(t, repo.gaps)
}

func TestPR5ObservationWorkerPersistsOnlyTypedSanitizedAttemptEvidence(t *testing.T) {
	clock := &pr5FakeClock{now: time.Unix(1_801_002_000, 0).UTC()}
	repo := newPR5ObservationWorkerRepository(clock, 1, pr5ObservationProviders()[0])
	malicious := errors.New("raw <synthetic-message-a> provider-password=must-not-persist")
	typed := &nntppool.TransportError{
		Kind: nntppool.OutcomeTemporaryFailure, Cause: malicious,
		Attempts: []nntppool.AttemptEvidence{{
			ProviderID: "transport-provider-name", Operation: nntppool.OperationStat,
			Outcome: nntppool.OutcomeTemporaryFailure, ResponseCode: 451,
			BodyValidation: nntppool.BodyValidationNotApplicable, Cause: malicious,
			PoolQueueDuration: time.Millisecond, PipelineHeadWaitDuration: 2 * time.Millisecond,
			ResponseServiceDuration: 3 * time.Millisecond,
		}},
	}
	transport := &pr5ScriptedObservationTransport{fn: func(_ context.Context, request observationTransportRequest) ([]observationTransportResult, error) {
		return []observationTransportResult{
			observationTransportResultFromNNTP(request.Targets[0].MessageID, nil, typed),
		}, nil
	}}
	worker := newPR5ObservationWorkerForTest(repo, clock, transport, 1, nil)

	step, err := worker.ProcessNext(context.Background())
	require.NoError(t, err)
	assert.Equal(t, observationWorkerParked, step)
	require.Len(t, repo.commits, 1)
	commit := repo.commits[0]
	assert.Equal(t, []byte{1}, commit.TemporaryBitmap)
	require.Len(t, commit.Attempts, 1)
	assert.Equal(t, "temporary_failure", commit.Attempts[0].Outcome)
	assert.Equal(t, "temporary_failure", commit.Attempts[0].CauseClass)
	assert.Equal(t, 451, *commit.Attempts[0].ResponseCode)
	assert.NotContains(t, commit.Attempts[0].CauseClass, "password")
	assert.NotContains(t, commit.Attempts[0].IdempotencyKey, "synthetic-message")
	assert.Equal(t, repo.snapshot.Entries[0].ProviderID, commit.ProviderID,
		"AltMount attribution comes from the captured dispatch snapshot, not transport diagnostics")
}

func TestPR5ObservationWorkerPersistsStagedRetryScheduleAndExhaustion(t *testing.T) {
	clock := &pr5FakeClock{now: time.Unix(1_801_003_000, 0).UTC()}
	repo := newPR5ObservationWorkerRepository(clock, 1, pr5ObservationProviders()[0])
	transport := &pr5ScriptedObservationTransport{fn: func(_ context.Context, request observationTransportRequest) ([]observationTransportResult, error) {
		return []observationTransportResult{{MessageID: request.Targets[0].MessageID, Outcome: observationOutcomeTemporary}}, nil
	}}
	worker := newPR5ObservationWorkerForTest(repo, clock, transport, 1, nil)
	wantDelays := []time.Duration{30 * time.Second, 2 * time.Minute, 10 * time.Minute, time.Hour}

	for attempt, delay := range wantDelays {
		committedAt := clock.Now()
		step, err := worker.ProcessNext(context.Background())
		require.NoError(t, err)
		assert.Equal(t, observationWorkerParked, step)
		require.Len(t, repo.state.Retries, 1)
		retry := repo.state.Retries[0]
		assert.Equal(t, attempt, retry.Attempt)
		assert.False(t, retry.Exhausted)
		assert.Equal(t, committedAt.Add(delay), retry.NextAttemptAt)

		clock.Advance(delay - time.Nanosecond)
		step, err = worker.ProcessNext(context.Background())
		require.NoError(t, err)
		assert.Equal(t, observationWorkerIdle, step)
		clock.Advance(time.Nanosecond)
	}

	step, err := worker.ProcessNext(context.Background())
	require.NoError(t, err)
	assert.Equal(t, observationWorkerCompleted, step)
	require.Len(t, repo.state.Retries, 1)
	assert.True(t, repo.state.Retries[0].Exhausted)
	assert.Equal(t, 4, repo.state.Retries[0].Attempt)
	assert.Len(t, transport.snapshotCalls(), 5, "initial attempt plus four durable retries")
}

func TestPR5ObservationWorkerRetryIdentityIsScopedToItsDurableRun(t *testing.T) {
	clock := &pr5FakeClock{now: time.Unix(1_801_003_500, 0).UTC()}
	newTemporaryWorker := func(runID string) (*pr5ObservationWorkerRepository, *observationWorker) {
		repo := newPR5ObservationWorkerRepository(clock, 1, pr5ObservationProviders()[0])
		repo.run.ID = runID
		repo.state.Run = repo.run
		transport := &pr5ScriptedObservationTransport{fn: func(_ context.Context, request observationTransportRequest) ([]observationTransportResult, error) {
			return []observationTransportResult{{MessageID: request.Targets[0].MessageID, Outcome: observationOutcomeTemporary}}, nil
		}}
		return repo, newPR5ObservationWorkerForTest(repo, clock, transport, 1, nil)
	}

	firstRepo, firstWorker := newTemporaryWorker("synthetic-run-one")
	_, err := firstWorker.ProcessNext(context.Background())
	require.NoError(t, err)
	secondRepo, secondWorker := newTemporaryWorker("synthetic-run-two")
	_, err = secondWorker.ProcessNext(context.Background())
	require.NoError(t, err)

	require.Len(t, firstRepo.state.Retries, 1)
	require.Len(t, secondRepo.state.Retries, 1)
	assert.NotEqual(t, firstRepo.state.Retries[0].RetryKey, secondRepo.state.Retries[0].RetryKey,
		"retry keys are globally unique in SQL and cannot alias the same provider/range in another run")
}

func TestPR5ObservationWorkerDoesNotLetFutureRetryDelayDueConfirmation(t *testing.T) {
	clock := &pr5FakeClock{now: time.Unix(1_801_003_800, 0).UTC()}
	providers := pr5ObservationProviders()[:2]
	repo := newPR5ObservationWorkerRepository(clock, 2, providers...)
	observedAt := clock.Now().Add(-10 * time.Minute)
	for index, provider := range providers {
		chunkID := "initial-provider-" + string(rune('a'+index))
		repo.state.Chunks = append(repo.state.Chunks, database.HealthRunChunkState{
			ID: chunkID, RunID: repo.run.ID, ProviderID: provider.ProviderID,
			ProviderGeneration: provider.ProviderGeneration, ProviderActivationEpoch: provider.ProviderActivationEpoch,
			Stage: "observe_initial", ObservationKind: database.HealthObservationSTAT,
			SegmentStart: 0, SegmentCount: 2, TestedBitmap: []byte{0b11},
			AbsentBitmap: []byte{0b01}, TemporaryBitmap: []byte{0b10},
			PresentBitmap: []byte{0}, CorruptBitmap: []byte{0}, InconclusiveBitmap: []byte{0},
			ResolvedBitmap: []byte{0b01}, CommittedAt: observedAt,
		})
		if index == 0 {
			repo.state.Retries = append(repo.state.Retries, database.HealthRunRetryState{
				RetryKey: "future-position-one", SourceChunkID: chunkID,
				FileRevisionID: repo.revision.ID, ProviderID: provider.ProviderID,
				ProviderGeneration:      provider.ProviderGeneration,
				ProviderActivationEpoch: provider.ProviderActivationEpoch,
				SegmentStart:            1, SegmentCount: 1, Outcome: "temporary", Attempt: 3,
				NextAttemptAt: clock.Now().Add(time.Hour), UpdatedAt: observedAt,
			})
		}
	}

	analysis := analyzeObservationState(&repo.state, &repo.revision, pr5ObservationTargets(2),
		[]observationDispatchProvider{
			{ID: providers[0].ProviderID, Generation: providers[0].ProviderGeneration, ActivationEpoch: providers[0].ProviderActivationEpoch, Role: providers[0].Role, Order: providers[0].Order},
			{ID: providers[1].ProviderID, Generation: providers[1].ProviderGeneration, ActivationEpoch: providers[1].ProviderActivationEpoch, Role: providers[1].Role, Order: providers[1].Order},
		}, observationWorkerConfig{ChunkSize: 8, ConfirmationDelay: 10 * time.Minute}, clock.Now())
	require.NotNil(t, analysis.work)
	assert.Equal(t, "observe_confirmation_2", analysis.work.stage)
	assert.Equal(t, []int64{0}, observationTargetPositions(analysis.work.targets))
}

func TestPR5ObservationWorkerConfirmationChunkCannotOverlapLaterDuePosition(t *testing.T) {
	clock := &pr5FakeClock{now: time.Unix(1_801_003_900, 0).UTC()}
	providers := pr5ObservationProviders()[:2]
	repo := newPR5ObservationWorkerRepository(clock, 3, providers...)
	for index, provider := range providers {
		repo.state.Chunks = append(repo.state.Chunks,
			database.HealthRunChunkState{
				ID: "due-outer-" + string(rune('a'+index)), RunID: repo.run.ID,
				ProviderID: provider.ProviderID, ProviderGeneration: provider.ProviderGeneration,
				ProviderActivationEpoch: provider.ProviderActivationEpoch,
				Stage:                   "observe_initial", ObservationKind: database.HealthObservationSTAT,
				SegmentStart: 0, SegmentCount: 3, TestedBitmap: []byte{0b101}, AbsentBitmap: []byte{0b101},
				PresentBitmap: []byte{0}, CorruptBitmap: []byte{0}, TemporaryBitmap: []byte{0},
				InconclusiveBitmap: []byte{0}, ResolvedBitmap: []byte{0b101},
				CommittedAt: clock.Now().Add(-10 * time.Minute),
			},
			database.HealthRunChunkState{
				ID: "later-middle-" + string(rune('a'+index)), RunID: repo.run.ID,
				ProviderID: provider.ProviderID, ProviderGeneration: provider.ProviderGeneration,
				ProviderActivationEpoch: provider.ProviderActivationEpoch,
				Stage:                   "observe_initial_middle", ObservationKind: database.HealthObservationSTAT,
				SegmentStart: 1, SegmentCount: 1, TestedBitmap: []byte{1}, AbsentBitmap: []byte{1},
				PresentBitmap: []byte{0}, CorruptBitmap: []byte{0}, TemporaryBitmap: []byte{0},
				InconclusiveBitmap: []byte{0}, ResolvedBitmap: []byte{1},
				CommittedAt: clock.Now().Add(-5 * time.Minute),
			},
		)
	}
	configuredProviders := []observationDispatchProvider{
		{ID: providers[0].ProviderID, Generation: providers[0].ProviderGeneration, ActivationEpoch: providers[0].ProviderActivationEpoch, Role: providers[0].Role, Order: providers[0].Order},
		{ID: providers[1].ProviderID, Generation: providers[1].ProviderGeneration, ActivationEpoch: providers[1].ProviderActivationEpoch, Role: providers[1].Role, Order: providers[1].Order},
	}
	analysis := analyzeObservationState(&repo.state, &repo.revision, pr5ObservationTargets(3), configuredProviders,
		observationWorkerConfig{ChunkSize: 8, ConfirmationDelay: 10 * time.Minute}, clock.Now())
	require.NotNil(t, analysis.work)
	assert.Equal(t, []int64{0}, observationTargetPositions(analysis.work.targets),
		"a sparse chunk spanning position 1 would make its later confirmation overlap in SQL")
	assert.Equal(t, int64(0), analysis.work.segmentStart)
	assert.Equal(t, int64(1), analysis.work.segmentCount)
}

func TestPR5ObservationWorkerRequiresTwoAllProviderAbsenceWaves(t *testing.T) {
	clock := &pr5FakeClock{now: time.Unix(1_801_004_000, 0).UTC()}
	providers := pr5ObservationProviders()[:2]
	repo := newPR5ObservationWorkerRepository(clock, 1, providers...)
	transport := &pr5ScriptedObservationTransport{fn: func(_ context.Context, request observationTransportRequest) ([]observationTransportResult, error) {
		return []observationTransportResult{{MessageID: request.Targets[0].MessageID, Outcome: observationOutcomeHardAbsent}}, nil
	}}
	worker := newPR5ObservationWorkerForTest(repo, clock, transport, 1, nil)

	_, err := worker.ProcessNext(context.Background())
	require.NoError(t, err)
	step, err := worker.ProcessNext(context.Background())
	require.NoError(t, err)
	assert.Equal(t, observationWorkerParked, step)
	assert.Empty(t, repo.gaps, "one all-provider wave cannot establish a persistent gap")

	clock.Advance(10*time.Minute - time.Nanosecond)
	step, err = worker.ProcessNext(context.Background())
	require.NoError(t, err)
	assert.Equal(t, observationWorkerIdle, step)
	clock.Advance(time.Nanosecond)

	_, err = worker.ProcessNext(context.Background())
	require.NoError(t, err)
	step, err = worker.ProcessNext(context.Background())
	require.NoError(t, err)
	assert.Equal(t, observationWorkerCompleted, step)
	require.Len(t, repo.gaps, 1)
	assert.Equal(t, database.GapKindConfirmedAbsent, repo.gaps[0].Kind)
	assert.Equal(t, int64(0), repo.gaps[0].StartSegment)
	assert.Equal(t, int64(1), repo.gaps[0].SegmentCount)
	require.Len(t, repo.gaps[0].Causes, 2)
	for _, cause := range repo.gaps[0].Causes {
		assert.Equal(t, database.GapCauseAbsent, cause.Cause)
		assert.Equal(t, 2, cause.ConfirmationCount)
	}
	assert.Len(t, transport.snapshotCalls(), 4)
}

func TestPR5ObservationWorkerDoesNotMixAsynchronousProviderConfirmations(t *testing.T) {
	clock := &pr5FakeClock{now: time.Unix(1_801_004_500, 0).UTC()}
	providers := pr5ObservationProviders()[:2]
	repo := newPR5ObservationWorkerRepository(clock, 1, providers...)
	firstWave := clock.Now().Add(-20 * time.Minute)
	secondTimestamp := clock.Now().Add(-10 * time.Minute)
	for index, provider := range providers {
		repo.state.Chunks = append(repo.state.Chunks, database.HealthRunChunkState{
			ID: "coherent-wave-one-" + string(rune('a'+index)), RunID: repo.run.ID,
			ProviderID: provider.ProviderID, ProviderGeneration: provider.ProviderGeneration,
			ProviderActivationEpoch: provider.ProviderActivationEpoch,
			Stage:                   "observe_initial", ObservationKind: database.HealthObservationSTAT,
			SegmentStart: 0, SegmentCount: 1, TestedBitmap: []byte{1}, AbsentBitmap: []byte{1},
			PresentBitmap: []byte{0}, CorruptBitmap: []byte{0}, TemporaryBitmap: []byte{0},
			InconclusiveBitmap: []byte{0}, ResolvedBitmap: []byte{1}, CommittedAt: firstWave,
		})
	}
	repo.state.Chunks = append(repo.state.Chunks,
		database.HealthRunChunkState{
			ID: "provider-a-wave-two", RunID: repo.run.ID,
			ProviderID: providers[0].ProviderID, ProviderGeneration: providers[0].ProviderGeneration,
			ProviderActivationEpoch: providers[0].ProviderActivationEpoch,
			Stage:                   "observe_confirmation_2", ObservationKind: database.HealthObservationSTAT,
			SegmentStart: 0, SegmentCount: 1, TestedBitmap: []byte{1}, AbsentBitmap: []byte{1},
			PresentBitmap: []byte{0}, CorruptBitmap: []byte{0}, TemporaryBitmap: []byte{0},
			InconclusiveBitmap: []byte{0}, ResolvedBitmap: []byte{1}, CommittedAt: secondTimestamp,
		},
		database.HealthRunChunkState{
			ID: "provider-b-late-wave-one-retry", RunID: repo.run.ID,
			ProviderID: providers[1].ProviderID, ProviderGeneration: providers[1].ProviderGeneration,
			ProviderActivationEpoch: providers[1].ProviderActivationEpoch,
			Stage:                   "observe_retry_3", ObservationKind: database.HealthObservationSTAT,
			SegmentStart: 0, SegmentCount: 1, TestedBitmap: []byte{1}, AbsentBitmap: []byte{1},
			PresentBitmap: []byte{0}, CorruptBitmap: []byte{0}, TemporaryBitmap: []byte{0},
			InconclusiveBitmap: []byte{0}, ResolvedBitmap: []byte{1}, CommittedAt: secondTimestamp,
		},
	)
	configuredProviders := []observationDispatchProvider{
		{ID: providers[0].ProviderID, Generation: providers[0].ProviderGeneration, ActivationEpoch: providers[0].ProviderActivationEpoch, Role: providers[0].Role, Order: providers[0].Order},
		{ID: providers[1].ProviderID, Generation: providers[1].ProviderGeneration, ActivationEpoch: providers[1].ProviderActivationEpoch, Role: providers[1].Role, Order: providers[1].Order},
	}
	analysis := analyzeObservationState(&repo.state, &repo.revision, pr5ObservationTargets(1), configuredProviders,
		observationWorkerConfig{ChunkSize: 1, ConfirmationDelay: 10 * time.Minute}, clock.Now())

	assert.Empty(t, analysis.gaps,
		"events from provider A's second wave and provider B's late first-wave retry are not one all-provider wave")
	require.NotNil(t, analysis.work)
	assert.Equal(t, "observe_confirmation_2", analysis.work.stage)
	assert.Equal(t, providers[1].ProviderID, analysis.work.provider.ID)
}

func TestPR5ObservationWorkerConfirmationRetryRetainsWaveIdentity(t *testing.T) {
	clock := &pr5FakeClock{now: time.Unix(1_801_004_800, 0).UTC()}
	providers := pr5ObservationProviders()[:2]
	repo := newPR5ObservationWorkerRepository(clock, 1, providers...)
	transport := &pr5ScriptedObservationTransport{fn: func(_ context.Context, request observationTransportRequest) ([]observationTransportResult, error) {
		outcome := observationOutcomeHardAbsent
		if request.Stage == "observe_confirmation_2" && request.Provider.ID == providers[0].ProviderID {
			outcome = observationOutcomeTemporary
		}
		return []observationTransportResult{{MessageID: request.Targets[0].MessageID, Outcome: outcome}}, nil
	}}
	worker := newPR5ObservationWorkerForTest(repo, clock, transport, 1, nil)

	_, err := worker.ProcessNext(context.Background())
	require.NoError(t, err)
	_, err = worker.ProcessNext(context.Background())
	require.NoError(t, err)
	clock.Advance(10 * time.Minute)
	_, err = worker.ProcessNext(context.Background())
	require.NoError(t, err)
	_, err = worker.ProcessNext(context.Background())
	require.NoError(t, err)
	clock.Advance(30 * time.Second)
	step, err := worker.ProcessNext(context.Background())
	require.NoError(t, err)
	assert.Equal(t, observationWorkerCompleted, step)

	calls := transport.snapshotCalls()
	require.Len(t, calls, 5)
	assert.Equal(t, "observe_confirmation_2", calls[2].Stage)
	assert.Equal(t, "observe_confirmation_2", calls[3].Stage)
	assert.Equal(t, "observe_confirmation_2_retry_1", calls[4].Stage,
		"a retry remains part of its durable logical wave instead of becoming unrelated evidence")
	require.Len(t, repo.gaps, 1)
	assert.Equal(t, database.GapKindConfirmedAbsent, repo.gaps[0].Kind)
}

func TestPR5ObservationWorkerPlaybackPauseAndCancellationNeverBecomeAbsence(t *testing.T) {
	clock := &pr5FakeClock{now: time.Unix(1_801_005_000, 0).UTC()}
	repo := newPR5ObservationWorkerRepository(clock, 1, pr5ObservationProviders()[0])
	playback := &mutablePlaybackActivity{}
	playback.SetActive(1)
	transport := &pr5ScriptedObservationTransport{fn: func(context.Context, observationTransportRequest) ([]observationTransportResult, error) {
		t.Fatal("playback-paused work reached transport")
		return nil, nil
	}}
	worker := newObservationWorker(
		repo, pr5StaticObservationTargets{targets: pr5ObservationTargets(1)}, transport,
		newObservationDispatchGate(newObservationAdmission(1, 1), playback, true), nil,
		observationWorkerConfig{
			Owner: "synthetic-worker", LeaseTTL: time.Minute, ChunkSize: 1,
			ConfirmationDelay: 10 * time.Minute, PlaybackRetryDelay: time.Second, Clock: clock,
		},
	)

	step, err := worker.ProcessNext(context.Background())
	require.NoError(t, err)
	assert.Equal(t, observationWorkerParked, step)
	assert.Empty(t, repo.commits)
	assert.Empty(t, repo.gaps)

	clock.Advance(time.Second)
	playback.SetActive(0)
	dispatched := make(chan struct{})
	transport.fn = func(ctx context.Context, _ observationTransportRequest) ([]observationTransportResult, error) {
		close(dispatched)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, processErr := worker.ProcessNext(ctx)
		result <- processErr
	}()
	<-dispatched
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
	assert.Empty(t, repo.commits)
	assert.Empty(t, repo.gaps)
}

func TestPR5ObservationWorkerDiscardsPartialBatchOnTypedCancellation(t *testing.T) {
	clock := &pr5FakeClock{now: time.Unix(1_801_005_500, 0).UTC()}
	repo := newPR5ObservationWorkerRepository(clock, 2, pr5ObservationProviders()[0])
	transport := &pr5ScriptedObservationTransport{fn: func(_ context.Context, request observationTransportRequest) ([]observationTransportResult, error) {
		return []observationTransportResult{
			{MessageID: request.Targets[0].MessageID, Outcome: observationOutcomeHardAbsent},
			observationTransportResultFromNNTP(request.Targets[1].MessageID, nil, &nntppool.TransportError{
				Kind: nntppool.OutcomeCancellation, Cause: context.Canceled,
			}),
		}, nil
	}}
	worker := newPR5ObservationWorkerForTest(repo, clock, transport, 2, nil)

	step, err := worker.ProcessNext(context.Background())
	require.NoError(t, err)
	assert.Equal(t, observationWorkerParked, step)
	assert.Empty(t, repo.commits,
		"a partial batch containing cancellation must not persist its earlier absence as a complete provider pass")
	assert.Empty(t, repo.gaps)
}

func TestPR5ObservationWorkerProgressIsCommittedAndObservationOnly(t *testing.T) {
	clock := &pr5FakeClock{now: time.Unix(1_801_006_000, 0).UTC()}
	repo := newPR5ObservationWorkerRepository(clock, 1, pr5ObservationProviders()[0])
	progress := &pr5ObservationProgressRecorder{}
	transport := &pr5ScriptedObservationTransport{fn: func(_ context.Context, request observationTransportRequest) ([]observationTransportResult, error) {
		return []observationTransportResult{{MessageID: request.Targets[0].MessageID, Outcome: observationOutcomePresent}}, nil
	}}
	worker := newPR5ObservationWorkerForTest(repo, clock, transport, 1, progress)

	step, err := worker.ProcessNext(context.Background())
	require.NoError(t, err)
	assert.Equal(t, observationWorkerCompleted, step)
	require.NotEmpty(t, progress.events)
	last := progress.events[len(progress.events)-1]
	assert.Equal(t, database.HealthRunCompleted, last.Status)
	assert.Equal(t, int64(1), last.ResolvedSegments)
	assert.Equal(t, int64(1), last.TotalSegments)
	assert.Equal(t, int64(1), last.ProviderChecks)
	assert.NotContains(t, last.Stage, "synthetic-message")
	assert.Empty(t, repo.gaps)
	assert.Equal(t, observationEffects{PersistEvidence: true}, observationSideEffects(database.GapKindProvisional, true),
		"the worker's observation mode cannot pad, repair, or delete content")
}

func TestPR5ObservationWorkerCommitsThroughSQLiteRepository(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "observation-worker.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	repository := database.NewHealthStateRepository(db.Connection(), database.DialectSQLite)
	ctx := context.Background()
	now := time.Now().UTC()
	revision, err := repository.EnsureFileRevision(ctx, database.FileRevisionSpec{
		FilePath: "synthetic/observation-worker.mkv", LayoutFingerprint: "sha256:worker-sqlite",
		VirtualSize: 300, SegmentCount: 3,
	})
	require.NoError(t, err)
	providers, err := repository.ReconcileProviders(ctx, []database.ProviderSpec{{
		StableID: "sqlite-observation-provider", DisplayName: "Synthetic provider",
		Endpoint: "observation.invalid", Port: 119, Account: "synthetic-account",
		Role: database.ProviderRolePrimary, Order: 0,
	}})
	require.NoError(t, err)
	require.Len(t, providers, 1)
	snapshot, err := repository.CaptureActiveProviderSnapshot(ctx, now)
	require.NoError(t, err)
	run, created, err := repository.EnsureScheduledHealthRun(ctx, database.ScheduledHealthRunSpec{
		Run: database.HealthRunSpec{
			ID: "sqlite-observation-run", FileRevisionID: revision.ID,
			ProviderSnapshotID: snapshot.ID, Trigger: "scheduled", Mode: "observation",
			TotalSegments: revision.SegmentCount, CreatedAt: now,
		},
		DedupeKey: "sqlite-observation-worker", Priority: database.HealthRunPriorityNormal,
		NotBefore: now,
	})
	require.NoError(t, err)
	require.True(t, created)

	transport := &pr5ScriptedObservationTransport{fn: func(_ context.Context, request observationTransportRequest) ([]observationTransportResult, error) {
		results := make([]observationTransportResult, len(request.Targets))
		for i, target := range request.Targets {
			results[i] = observationTransportResult{MessageID: target.MessageID, Outcome: observationOutcomePresent}
		}
		return results, nil
	}}
	worker := newObservationWorker(
		repository, pr5StaticObservationTargets{targets: pr5ObservationTargets(3)}, transport,
		newObservationDispatchGate(newObservationAdmission(1, 1), nil, true), nil,
		observationWorkerConfig{
			Owner: "sqlite-observation-worker", LeaseTTL: time.Minute, ChunkSize: 2,
			ConfirmationDelay: 10 * time.Minute, PlaybackRetryDelay: time.Second,
		},
	)
	step, err := worker.ProcessNext(ctx)
	require.NoError(t, err)
	assert.Equal(t, observationWorkerParked, step)
	step, err = worker.ProcessNext(ctx)
	require.NoError(t, err)
	assert.Equal(t, observationWorkerCompleted, step)

	completed, err := repository.GetHealthRun(ctx, run.ID)
	require.NoError(t, err)
	require.NotNil(t, completed)
	assert.Equal(t, database.HealthRunCompleted, completed.Status)
	assert.Equal(t, int64(3), completed.ResolvedSegments)
	resume, err := repository.GetHealthRunResumeState(ctx, run.ID)
	require.NoError(t, err)
	require.Len(t, resume.Chunks, 2)
	for _, chunk := range resume.Chunks {
		assert.Equal(t, providers[0].ID, chunk.ProviderID)
		assert.Equal(t, providers[0].CurrentGeneration, chunk.ProviderGeneration)
		assert.Equal(t, providers[0].ActivationEpoch, chunk.ProviderActivationEpoch)
		assert.LessOrEqual(t, chunk.SegmentCount, int64(2))
	}
}

func TestPR5ObservationWorkerResumesDurableSQLiteRetry(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "observation-retry.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	repository := database.NewHealthStateRepository(db.Connection(), database.DialectSQLite)
	ctx := context.Background()
	now := time.Now().UTC()
	revision, err := repository.EnsureFileRevision(ctx, database.FileRevisionSpec{
		FilePath: "synthetic/observation-retry.mkv", LayoutFingerprint: "sha256:worker-retry",
		VirtualSize: 100, SegmentCount: 1,
	})
	require.NoError(t, err)
	_, err = repository.ReconcileProviders(ctx, []database.ProviderSpec{{
		StableID: "sqlite-retry-provider", DisplayName: "Synthetic provider",
		Endpoint: "retry.invalid", Port: 119, Account: "synthetic-account",
		Role: database.ProviderRolePrimary, Order: 0,
	}})
	require.NoError(t, err)
	snapshot, err := repository.CaptureActiveProviderSnapshot(ctx, now)
	require.NoError(t, err)
	run, created, err := repository.EnsureScheduledHealthRun(ctx, database.ScheduledHealthRunSpec{
		Run: database.HealthRunSpec{
			ID: "sqlite-observation-retry-run", FileRevisionID: revision.ID,
			ProviderSnapshotID: snapshot.ID, Trigger: "scheduled", Mode: "observation",
			TotalSegments: 1, CreatedAt: now,
		},
		DedupeKey: "sqlite-observation-retry", Priority: database.HealthRunPriorityNormal,
		NotBefore: now,
	})
	require.NoError(t, err)
	require.True(t, created)

	var calls int
	transport := &pr5ScriptedObservationTransport{fn: func(_ context.Context, request observationTransportRequest) ([]observationTransportResult, error) {
		calls++
		outcome := observationOutcomeTemporary
		if calls == 2 {
			outcome = observationOutcomePresent
		}
		return []observationTransportResult{{MessageID: request.Targets[0].MessageID, Outcome: outcome}}, nil
	}}
	worker := newObservationWorker(
		repository, pr5StaticObservationTargets{targets: pr5ObservationTargets(1)}, transport,
		newObservationDispatchGate(newObservationAdmission(1, 1), nil, true), nil,
		observationWorkerConfig{
			Owner: "sqlite-retry-worker", LeaseTTL: time.Minute, ChunkSize: 1,
			ConfirmationDelay: 10 * time.Minute, PlaybackRetryDelay: time.Second,
		},
	)
	step, err := worker.ProcessNext(ctx)
	require.NoError(t, err)
	assert.Equal(t, observationWorkerParked, step)

	forceDue := time.Now().UTC().Add(-time.Second)
	_, err = db.Connection().ExecContext(ctx,
		`UPDATE health_retry_states SET next_attempt_at = ? WHERE file_revision_id = ?`,
		forceDue, revision.ID)
	require.NoError(t, err)
	_, err = db.Connection().ExecContext(ctx,
		`UPDATE health_run_schedule SET not_before = ? WHERE run_id = ?`, forceDue, run.ID)
	require.NoError(t, err)

	step, err = worker.ProcessNext(ctx)
	require.NoError(t, err)
	assert.Equal(t, observationWorkerCompleted, step)
	resume, err := repository.GetHealthRunResumeState(ctx, run.ID)
	require.NoError(t, err)
	require.Len(t, resume.Chunks, 2)
	require.Len(t, resume.Retries, 1)
	assert.Equal(t, 1, resume.Retries[0].Attempt)
	assert.True(t, resume.Retries[0].Exhausted)
	assert.Equal(t, int64(1), resume.Run.ResolvedSegments)
}
