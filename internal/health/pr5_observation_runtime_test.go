package health

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/metadata"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	altpool "github.com/javi11/altmount/internal/pool"
	"github.com/javi11/nntppool/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func pr5RuntimeMetadata() *metapb.FileMetadata {
	return &metapb.FileMetadata{
		FileSize: 6,
		SegmentData: []*metapb.SegmentData{
			{Id: "fixture-article-one", SegmentSize: 3, StartOffset: 0, EndOffset: 2},
			{Id: "fixture-article-two", SegmentSize: 3, StartOffset: 0, EndOffset: 2},
		},
	}
}

type pr5RuntimeMetadataReader struct {
	metadata *metapb.FileMetadata
	err      error
	paths    []string
}

func (r *pr5RuntimeMetadataReader) ReadFileMetadata(path string) (*metapb.FileMetadata, error) {
	r.paths = append(r.paths, path)
	return r.metadata, r.err
}

type pr5RuntimeFileLookup struct {
	files map[int64]*database.FileHealth
}

func (r *pr5RuntimeFileLookup) GetFileHealthByID(_ context.Context, id int64) (*database.FileHealth, error) {
	return r.files[id], nil
}

func TestPR5MetadataObservationTargetsAreCanonicalAndRevisionFenced(t *testing.T) {
	meta := pr5RuntimeMetadata()
	layout, err := metadata.ResolveCanonicalSegmentLayout(meta)
	require.NoError(t, err)
	reader := &pr5RuntimeMetadataReader{metadata: meta}
	source := newMetadataObservationTargetSource(
		&pr5RuntimeFileLookup{files: map[int64]*database.FileHealth{
			41: {ID: 41, FilePath: "library/movie.mkv"},
		}},
		reader,
	)
	revision := &database.HealthFileRevision{
		ID: "revision-a", FileHealthID: 41, LayoutFingerprint: layout.Fingerprint,
		VirtualSize: layout.VirtualSize, SegmentCount: int64(len(layout.Segments)), Active: true,
	}

	targets, err := source.ObservationTargets(context.Background(), revision)
	require.NoError(t, err)
	require.Len(t, targets, 2)
	assert.Equal(t, []int64{0, 1}, []int64{targets[0].Position, targets[1].Position})
	assert.Equal(t, []int64{3, 3}, []int64{targets[0].UsableBytes, targets[1].UsableBytes})
	assert.Equal(t, []string{"library/movie.mkv"}, reader.paths)

	for name, mutate := range map[string]func(*database.HealthFileRevision){
		"fingerprint": func(r *database.HealthFileRevision) { r.LayoutFingerprint = "sha256:changed" },
		"size":        func(r *database.HealthFileRevision) { r.VirtualSize++ },
		"count":       func(r *database.HealthFileRevision) { r.SegmentCount++ },
		"inactive":    func(r *database.HealthFileRevision) { r.Active = false },
	} {
		t.Run(name, func(t *testing.T) {
			changed := *revision
			mutate(&changed)
			_, err := source.ObservationTargets(context.Background(), &changed)
			require.Error(t, err)
			assert.NotContains(t, err.Error(), "fixture-article",
				"revision fencing errors must never expose article identities")
		})
	}
}

type pr5RuntimePoolGetter struct {
	client altpool.NntpClient
	err    error
}

func (g pr5RuntimePoolGetter) GetPool() (altpool.NntpClient, error) { return g.client, g.err }

type pr5RuntimeNNTPClient struct {
	altpool.NntpClient
	mu          sync.Mutex
	statOptions []nntppool.StatManyOptions
	statIDs     [][]string
	statResults []nntppool.StatManyResult
	bodyOptions []nntppool.TargetedBodyOptions
}

func (c *pr5RuntimeNNTPClient) StatMany(
	_ context.Context,
	ids []string,
	opts nntppool.StatManyOptions,
) <-chan nntppool.StatManyResult {
	c.mu.Lock()
	c.statOptions = append(c.statOptions, opts)
	c.statIDs = append(c.statIDs, append([]string(nil), ids...))
	results := append([]nntppool.StatManyResult(nil), c.statResults...)
	c.mu.Unlock()
	out := make(chan nntppool.StatManyResult, len(results))
	for _, result := range results {
		out <- result
	}
	close(out)
	return out
}

func (c *pr5RuntimeNNTPClient) BodyTargeted(
	_ context.Context,
	messageID string,
	opts nntppool.TargetedBodyOptions,
	_ ...func(nntppool.YEncMeta),
) (*nntppool.ArticleBody, error) {
	c.mu.Lock()
	c.bodyOptions = append(c.bodyOptions, opts)
	c.mu.Unlock()
	return &nntppool.ArticleBody{
		MessageID: messageID, ProviderID: opts.Provider,
		Attempts: []nntppool.AttemptEvidence{{
			Operation: nntppool.OperationBody, Outcome: nntppool.OutcomeSuccess,
			BodyValidation: nntppool.BodyValidationValid,
		}},
	}, nil
}

type pr5RuntimeProviderResolver struct {
	name string
	err  error
}

func (r pr5RuntimeProviderResolver) ResolveObservationProvider(
	context.Context,
	observationDispatchProvider,
) (string, error) {
	return r.name, r.err
}

func TestPR5NNTPObservationTransportTargetsOneProviderAndRequiresFreshBody(t *testing.T) {
	client := &pr5RuntimeNNTPClient{statResults: []nntppool.StatManyResult{
		{MessageID: "fixture-article-two", Err: &nntppool.TransportError{Kind: nntppool.OutcomeHardArticleAbsence}},
		{MessageID: "fixture-article-one", Result: &nntppool.StatResult{MessageID: "fixture-article-one"}},
	}}
	transport := newNNTPObservationTransport(
		pr5RuntimePoolGetter{client: client},
		pr5RuntimeProviderResolver{name: "synthetic.invalid:119+account"},
		4,
	)
	provider := observationDispatchProvider{ID: "provider-stable", Generation: 2, ActivationEpoch: 3}
	targets := []observationSegmentTarget{
		{Position: 0, MessageID: "fixture-article-one", UsableBytes: 3},
		{Position: 1, MessageID: "fixture-article-two", UsableBytes: 3},
	}

	stat, err := transport.Observe(context.Background(), observationTransportRequest{
		RunID: "run-a", Provider: provider, Stage: "observe_initial",
		ObservationKind: database.HealthObservationSTAT, Targets: targets,
	})
	require.NoError(t, err)
	require.Len(t, stat, 2)
	assert.Equal(t, "synthetic.invalid:119+account", client.statOptions[0].Provider)
	assert.Equal(t, 2, client.statOptions[0].Concurrency)
	assert.False(t, client.statOptions[0].Priority)
	assert.Equal(t, observationOutcomeHardAbsent, stat[0].Outcome)
	assert.Equal(t, observationOutcomePresent, stat[1].Outcome)

	body, err := transport.Observe(context.Background(), observationTransportRequest{
		RunID: "run-a", Provider: provider, Stage: "observe_confirmation_2",
		ObservationKind: database.HealthObservationValidatedBody, FreshTransport: true,
		Targets: targets[:1],
	})
	require.NoError(t, err)
	require.Len(t, body, 1)
	assert.Equal(t, observationOutcomePresent, body[0].Outcome)
	require.Len(t, client.bodyOptions, 1)
	assert.Equal(t, "synthetic.invalid:119+account", client.bodyOptions[0].Provider)
	assert.True(t, client.bodyOptions[0].FreshTransport)
	assert.False(t, client.bodyOptions[0].Priority)
}

type pr5RuntimeRegistry struct {
	providers []database.HealthProvider
}

func (r pr5RuntimeRegistry) ListProviders(context.Context, bool) ([]database.HealthProvider, error) {
	return append([]database.HealthProvider(nil), r.providers...), nil
}

func TestPR5CurrentProviderResolverFencesGenerationAndActivation(t *testing.T) {
	enabled := true
	getter := func() *config.Config {
		return &config.Config{Providers: []config.ProviderConfig{{
			ID: "provider-a", Host: "synthetic.invalid", Port: 563,
			Username: "account", Enabled: &enabled,
		}}}
	}
	resolver := newCurrentObservationProviderResolver(pr5RuntimeRegistry{providers: []database.HealthProvider{{
		ID: "provider-a", Active: true, CurrentGeneration: 4, ActivationEpoch: 7,
	}}}, getter)

	name, err := resolver.ResolveObservationProvider(context.Background(), observationDispatchProvider{
		ID: "provider-a", Generation: 4, ActivationEpoch: 7,
	})
	require.NoError(t, err)
	assert.Equal(t, "synthetic.invalid:563+account", name)

	for _, provider := range []observationDispatchProvider{
		{ID: "provider-a", Generation: 3, ActivationEpoch: 7},
		{ID: "provider-a", Generation: 4, ActivationEpoch: 6},
		{ID: "removed", Generation: 4, ActivationEpoch: 7},
	} {
		_, err := resolver.ResolveObservationProvider(context.Background(), provider)
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "account")
	}
}

type pr5RuntimeDueRepository struct {
	files       []*database.FileHealth
	rescheduled map[string]time.Time
}

func (r *pr5RuntimeDueRepository) GetUnhealthyFiles(
	context.Context, int, string, string, int,
) ([]*database.FileHealth, error) {
	return append([]*database.FileHealth(nil), r.files...), nil
}

func (r *pr5RuntimeDueRepository) GetFileHealthByID(_ context.Context, id int64) (*database.FileHealth, error) {
	for _, file := range r.files {
		if file.ID == id {
			copy := *file
			return &copy, nil
		}
	}
	return nil, nil
}

func (r *pr5RuntimeDueRepository) DeferScheduledHealthCheck(_ context.Context, path string, at time.Time) error {
	if r.rescheduled == nil {
		r.rescheduled = make(map[string]time.Time)
	}
	r.rescheduled[path] = at
	return nil
}

type pr5RuntimeScheduleRepository struct {
	revision        database.HealthFileRevision
	snapshot        database.ProviderSnapshot
	reusable        bool
	reuseChecks     int
	snapshotCalls   int
	scheduled       []database.ScheduledHealthRunSpec
	reconciledSpecs []database.ProviderSpec
}

func (r *pr5RuntimeScheduleRepository) ReconcileProviders(
	_ context.Context,
	specs []database.ProviderSpec,
) ([]database.HealthProvider, error) {
	r.reconciledSpecs = append([]database.ProviderSpec(nil), specs...)
	return nil, nil
}

func (r *pr5RuntimeScheduleRepository) EnsureFileRevision(
	_ context.Context,
	spec database.FileRevisionSpec,
) (*database.HealthFileRevision, error) {
	r.revision.LayoutFingerprint = spec.LayoutFingerprint
	r.revision.VirtualSize = spec.VirtualSize
	r.revision.SegmentCount = spec.SegmentCount
	return &r.revision, nil
}

func (r *pr5RuntimeScheduleRepository) HasReusableCompletedImportSTATCoverage(
	context.Context,
	string,
	int64,
) (bool, error) {
	r.reuseChecks++
	return r.reusable, nil
}

func (r *pr5RuntimeScheduleRepository) CaptureActiveProviderSnapshot(context.Context, time.Time) (*database.ProviderSnapshot, error) {
	r.snapshotCalls++
	copy := r.snapshot
	return &copy, nil
}

func (r *pr5RuntimeScheduleRepository) EnsureScheduledHealthRun(
	_ context.Context,
	spec database.ScheduledHealthRunSpec,
) (*database.HealthRun, bool, error) {
	r.scheduled = append(r.scheduled, spec)
	return &database.HealthRun{ID: "ordinary-run", FileRevisionID: spec.Run.FileRevisionID}, true, nil
}

func TestPR5OrdinarySchedulerReusesCompletedImportCoverage(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	meta := pr5RuntimeMetadata()
	layout, err := metadata.ResolveCanonicalSegmentLayout(meta)
	require.NoError(t, err)
	due := &pr5RuntimeDueRepository{files: []*database.FileHealth{{
		ID: 81, FilePath: "library/imported.mkv", Status: database.HealthStatusHealthy,
	}}}
	state := &pr5RuntimeScheduleRepository{
		revision: database.HealthFileRevision{ID: "revision-import", FileHealthID: 81, Active: true},
		reusable: true,
	}
	scheduler := newOrdinaryObservationScheduler(
		due, state, &pr5RuntimeMetadataReader{metadata: meta},
		func() *config.Config { return &config.Config{} },
		observationSchedulerConfig{BatchSize: 8, MaxRetries: 3, RecheckInterval: 24 * time.Hour},
		pr5RuntimeClock{now: now},
	)

	result, err := scheduler.ScheduleDue(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, result.CoverageReused)
	assert.Zero(t, result.Created)
	assert.Equal(t, 1, state.reuseChecks)
	assert.Zero(t, state.snapshotCalls, "reused import coverage must not create a redundant snapshot")
	assert.Empty(t, state.scheduled)
	assert.Equal(t, now.Add(24*time.Hour), due.rescheduled["library/imported.mkv"])
	assert.Equal(t, layout.Fingerprint, state.revision.LayoutFingerprint)
}

func TestPR5OrdinarySchedulerCreatesIndependentDurableRunWhenCoverageIsNotReusable(t *testing.T) {
	now := time.Date(2026, 7, 14, 13, 0, 0, 0, time.UTC)
	enabled := true
	due := &pr5RuntimeDueRepository{files: []*database.FileHealth{{
		ID: 82, FilePath: "library/due.mkv", Status: database.HealthStatusPending,
	}}}
	state := &pr5RuntimeScheduleRepository{
		revision: database.HealthFileRevision{ID: "revision-due", FileHealthID: 82, Active: true},
		snapshot: database.ProviderSnapshot{ID: "snapshot-current", Entries: []database.ProviderSnapshotEntry{{
			ProviderID: "provider-a", ProviderGeneration: 1, ProviderActivationEpoch: 1,
			Role: database.ProviderRolePrimary,
		}},
		},
	}
	getter := func() *config.Config {
		return &config.Config{Providers: []config.ProviderConfig{{
			ID: "provider-a", Name: "Provider A", Host: "synthetic.invalid", Port: 119,
			Username: "account", Enabled: &enabled,
		}}}
	}
	scheduler := newOrdinaryObservationScheduler(
		due, state, &pr5RuntimeMetadataReader{metadata: pr5RuntimeMetadata()}, getter,
		observationSchedulerConfig{BatchSize: 8, MaxRetries: 3, RecheckInterval: 12 * time.Hour},
		pr5RuntimeClock{now: now},
	)

	result, err := scheduler.ScheduleDue(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, result.Created)
	assert.Zero(t, result.CoverageReused)
	require.Len(t, state.reconciledSpecs, 1)
	assert.Equal(t, "provider-a", state.reconciledSpecs[0].StableID)
	require.Len(t, state.scheduled, 1)
	spec := state.scheduled[0]
	assert.Equal(t, "ordinary", spec.Run.Trigger)
	assert.Equal(t, "observation", spec.Run.Mode)
	assert.Equal(t, "snapshot-current", spec.Run.ProviderSnapshotID)
	assert.Equal(t, "ordinary:revision-due", spec.DedupeKey)
	assert.Equal(t, database.HealthRunPriorityHigh, spec.Priority)
	assert.Equal(t, now, spec.NotBefore)
	assert.Equal(t, now.Add(12*time.Hour), due.rescheduled["library/due.mkv"])
}

type pr5RuntimeClock struct{ now time.Time }

func (c pr5RuntimeClock) Now() time.Time { return c.now }

type pr5RuntimeProcessor struct {
	started chan struct{}
}

func (p *pr5RuntimeProcessor) ProcessNext(ctx context.Context) (observationWorkerStep, error) {
	select {
	case p.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return observationWorkerIdle, ctx.Err()
}

type pr5RuntimeScheduler struct{}

func (pr5RuntimeScheduler) ScheduleDue(context.Context) (ObservationScheduleResult, error) {
	return ObservationScheduleResult{}, nil
}

func TestPR5ObservationServiceStartStopIsInterruptible(t *testing.T) {
	processor := &pr5RuntimeProcessor{started: make(chan struct{}, 1)}
	service := newObservationServiceForTest(processor, pr5RuntimeScheduler{}, ObservationServiceConfig{
		WorkerCount: 1, PollInterval: time.Hour, ScheduleInterval: time.Hour,
	})
	require.NoError(t, service.Start(context.Background()))
	select {
	case <-processor.started:
	case <-time.After(time.Second):
		t.Fatal("observation processor did not start")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, service.Stop(stopCtx))
	assert.Equal(t, ObservationServiceStopped, service.Status())
	assert.ErrorIs(t, service.ProcessNext(context.Background()), ErrObservationServiceStopped)
}

func TestPR5ObservationProgressCallbackPublishesOnlySanitizedDurableFields(t *testing.T) {
	var got ObservationProgress
	sink := observationProgressCallback(func(progress ObservationProgress) { got = progress })
	sink.PublishObservationProgress(observationProgressEvent{
		RunID: "run-progress", FileRevisionID: "revision-progress",
		Status: database.HealthRunRunning, ResolvedSegments: 4, TotalSegments: 10,
		ProviderChecks: 7, CurrentProviderID: "provider-a", Stage: "observe_initial",
		ChecksPerSecond: 3.5, EstimatedCompletionDelay: 2 * time.Second,
	})
	assert.Equal(t, "run-progress", got.RunID)
	assert.Equal(t, "revision-progress", got.FileRevisionID)
	assert.Equal(t, "provider-a", got.CurrentProviderID)
	assert.Equal(t, int64(7), got.ProviderChecks)
	assert.NotContains(t, got.Stage, "fixture-article")
}

// Compile-time checks keep the test doubles honest without reimplementing the
// many unrelated methods on pool.NntpClient.
var _ altpool.NntpClient = (*pr5RuntimeNNTPClient)(nil)

func TestPR5NNTPObservationTransportPropagatesCancellationWithoutFabricatingEvidence(t *testing.T) {
	transport := newNNTPObservationTransport(
		pr5RuntimePoolGetter{err: context.Canceled},
		pr5RuntimeProviderResolver{name: "synthetic.invalid:119"},
		1,
	)
	_, err := transport.Observe(context.Background(), observationTransportRequest{
		Provider:        observationDispatchProvider{ID: "provider-a", Generation: 1, ActivationEpoch: 1},
		ObservationKind: database.HealthObservationSTAT,
		Targets:         []observationSegmentTarget{{Position: 0, MessageID: "fixture-article-one", UsableBytes: 3}},
	})
	assert.True(t, errors.Is(err, context.Canceled))
}
