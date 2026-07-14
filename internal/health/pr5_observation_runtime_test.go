package health

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
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

type pr5SelectiveMetadataReader struct {
	metadata *metapb.FileMetadata
	fail     map[string]bool
}

func (r *pr5SelectiveMetadataReader) ReadFileMetadata(path string) (*metapb.FileMetadata, error) {
	if r.fail[path] {
		return nil, errors.New("synthetic metadata preparation failure")
	}
	return r.metadata, nil
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
	providerID  string
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
		MessageID: messageID, ProviderID: c.providerID,
		Attempts: []nntppool.AttemptEvidence{{
			ProviderID: c.providerID,
			Operation:  nntppool.OperationBody, Outcome: nntppool.OutcomeSuccess,
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
	client := &pr5RuntimeNNTPClient{providerID: "provider-stable", statResults: []nntppool.StatManyResult{
		{MessageID: "fixture-article-two", Err: &nntppool.TransportError{
			Kind: nntppool.OutcomeHardArticleAbsence, ProviderID: "provider-stable",
			Attempts: []nntppool.AttemptEvidence{{
				ProviderID: "provider-stable", Operation: nntppool.OperationStat,
				Outcome: nntppool.OutcomeHardArticleAbsence,
			}},
		}},
		{MessageID: "fixture-article-one", Result: &nntppool.StatResult{
			MessageID: "fixture-article-one", ProviderID: "provider-stable",
		}},
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
		{Position: 2, MessageID: "fixture-article-one", UsableBytes: 3},
	}

	stat, err := transport.Observe(context.Background(), observationTransportRequest{
		RunID: "run-a", Provider: provider, Stage: "observe_initial",
		ObservationKind: database.HealthObservationSTAT, WireConcurrency: 1, Targets: targets,
	})
	require.NoError(t, err)
	require.Len(t, stat, 2)
	require.Len(t, client.statIDs, 1)
	assert.Equal(t, []string{"fixture-article-one", "fixture-article-two"}, client.statIDs[0],
		"STAT submits one wire request per unique canonical message identity")
	assert.Equal(t, "synthetic.invalid:119+account", client.statOptions[0].Provider)
	assert.Equal(t, 1, client.statOptions[0].Concurrency)
	assert.False(t, client.statOptions[0].Priority)
	assert.Equal(t, observationOutcomeHardAbsent, stat[0].Outcome)
	assert.Equal(t, observationOutcomePresent, stat[1].Outcome)

	body, err := transport.Observe(context.Background(), observationTransportRequest{
		RunID: "run-a", Provider: provider, Stage: "observe_confirmation_2",
		ObservationKind: database.HealthObservationValidatedBody, FreshTransport: true,
		Targets: []observationSegmentTarget{targets[0], targets[2]},
	})
	require.NoError(t, err)
	require.Len(t, body, 1)
	assert.Equal(t, observationOutcomePresent, body[0].Outcome)
	require.Len(t, client.bodyOptions, 1)
	assert.Equal(t, "fixture-article-one", body[0].MessageID,
		"one validated BODY proof is shared by duplicate canonical owners")
	assert.Equal(t, "synthetic.invalid:119+account", client.bodyOptions[0].Provider)
	assert.True(t, client.bodyOptions[0].FreshTransport)
	assert.False(t, client.bodyOptions[0].Priority)
}

type pr5RuntimeRegistry struct {
	providers   []database.HealthProvider
	generations map[string][]database.HealthProviderGeneration
}

func (r pr5RuntimeRegistry) ListProviders(context.Context, bool) ([]database.HealthProvider, error) {
	return append([]database.HealthProvider(nil), r.providers...), nil
}

func (r pr5RuntimeRegistry) ListProviderGenerations(
	_ context.Context,
	providerID string,
) ([]database.HealthProviderGeneration, error) {
	return append([]database.HealthProviderGeneration(nil), r.generations[providerID]...), nil
}

func TestPR5CurrentProviderResolverFencesGenerationAndActivation(t *testing.T) {
	enabled := true
	getter := func() *config.Config {
		return &config.Config{Providers: []config.ProviderConfig{{
			ID: "provider-a", Host: "synthetic.invalid", Port: 563,
			Username: "account", Enabled: &enabled,
		}}}
	}
	resolver := newCurrentObservationProviderResolver(pr5RuntimeRegistry{
		providers: []database.HealthProvider{{
			ID: "provider-a", Active: true, CurrentGeneration: 4, ActivationEpoch: 7,
		}},
		generations: map[string][]database.HealthProviderGeneration{
			"provider-a": {{
				ProviderID: "provider-a", Generation: 4, Endpoint: "synthetic.invalid",
				Port: 563, Account: "account",
			}},
		},
	}, getter)

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

func TestPR5CurrentProviderResolverUsesEffectiveIDAndRejectsNilEnabled(t *testing.T) {
	enabled := true
	provider := config.ProviderConfig{
		Host: "fallback.invalid", Port: 119, Username: "synthetic-account", Enabled: &enabled,
	}
	effectiveID := uuid.NewString()
	cfg := &config.Config{Providers: []config.ProviderConfig{provider}}
	resolver := newCurrentObservationProviderResolver(pr5RuntimeRegistry{
		providers: []database.HealthProvider{{
			ID: effectiveID, Active: true, CurrentGeneration: 2, ActivationEpoch: 3,
		}},
		generations: map[string][]database.HealthProviderGeneration{
			effectiveID: {{
				ProviderID: effectiveID, Generation: 2, Endpoint: "fallback.invalid",
				Port: 119, Account: "synthetic-account",
			}},
		},
	}, func() *config.Config { return cfg })
	target := observationDispatchProvider{ID: effectiveID, Generation: 2, ActivationEpoch: 3}

	name, err := resolver.ResolveObservationProvider(context.Background(), target)
	require.NoError(t, err)
	assert.Equal(t, provider.NNTPPoolName(), name)

	cfg.Providers[0].Enabled = nil
	_, err = resolver.ResolveObservationProvider(context.Background(), target)
	require.Error(t, err)
}

func TestPR5CurrentProviderResolverFencesConfigAheadOfPersistedGeneration(t *testing.T) {
	enabled := true
	cfg := &config.Config{Providers: []config.ProviderConfig{{
		ID: "provider-a", Host: "new.invalid", Port: 563,
		Username: "new-account", Enabled: &enabled,
	}}}
	resolver := newCurrentObservationProviderResolver(pr5RuntimeRegistry{
		providers: []database.HealthProvider{{
			ID: "provider-a", Active: true, CurrentGeneration: 4, ActivationEpoch: 7,
		}},
		generations: map[string][]database.HealthProviderGeneration{
			"provider-a": {{
				ProviderID: "provider-a", Generation: 4, Endpoint: "old.invalid",
				Port: 119, Account: "old-account",
			}},
		},
	}, func() *config.Config { return cfg })

	_, err := resolver.ResolveObservationProvider(context.Background(), observationDispatchProvider{
		ID: "provider-a", Generation: 4, ActivationEpoch: 7,
	})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "old-account")
	assert.NotContains(t, err.Error(), "new-account")
}

func TestPR5ObservationProviderSpecsUseContiguousActiveOrderAndStableFallback(t *testing.T) {
	enabled := true
	disabled := false
	providers := []config.ProviderConfig{
		{ID: "provider-a", Name: "A", Host: "a.invalid", Port: 119, Enabled: &enabled},
		{ID: "disabled", Name: "X", Host: "x.invalid", Port: 119, Enabled: &disabled},
		{Host: "fallback.invalid", Port: 563, Username: "synthetic-account", Enabled: &enabled},
		{ID: "nil-enabled", Name: "N", Host: "n.invalid", Port: 119},
	}

	specs := observationProviderSpecs(providers)
	require.Len(t, specs, 2)
	assert.Equal(t, []int{0, 1}, []int{specs[0].Order, specs[1].Order})
	assert.Equal(t, "provider-a", specs[0].StableID)
	assert.Empty(t, specs[1].StableID)
}

func TestPR5ObservationProviderEmptyIDReconcileIsStable(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "provider-reconcile.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	repository := database.NewHealthStateRepository(db.Connection(), database.DialectSQLite)
	enabled := true
	provider := config.ProviderConfig{
		Host: "stable.invalid", Port: 119, Username: "synthetic-account", Enabled: &enabled,
	}
	specs := observationProviderSpecs([]config.ProviderConfig{provider})

	first, err := repository.ReconcileProviders(context.Background(), specs)
	require.NoError(t, err)
	second, err := repository.ReconcileProviders(context.Background(), specs)
	require.NoError(t, err)
	require.Len(t, first, 1)
	require.Len(t, second, 1)
	assert.NoError(t, uuid.Validate(first[0].ID))
	assert.NotContains(t, first[0].ID, "synthetic-account")
	assert.Equal(t, first[0].ID, second[0].ID)
	assert.Equal(t, first[0].CurrentGeneration, second[0].CurrentGeneration)
}

type pr5RuntimeDueRepository struct {
	files           []*database.FileHealth
	rescheduled     map[string]time.Time
	discoveryDefers map[int64]time.Time
}

func (r *pr5RuntimeDueRepository) ListDueObservationFiles(
	context.Context, time.Time, time.Time, int,
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

func (r *pr5RuntimeDueRepository) DeferObservationDiscoveryFailure(
	_ context.Context,
	fileHealthID int64,
	_ time.Time,
	retryAt time.Time,
) error {
	if r.discoveryDefers == nil {
		r.discoveryDefers = make(map[int64]time.Time)
	}
	r.discoveryDefers[fileHealthID] = retryAt
	return nil
}

type pr5RuntimeScheduleRepository struct {
	revision         database.HealthFileRevision
	revisionErr      error
	revisionCalls    int
	snapshot         database.ProviderSnapshot
	reusable         bool
	coverage         *database.CompletedImportSTATCoverage
	reuseChecks      int
	coverageReads    int
	coverageConsumed bool
	coverageDefers   map[string]time.Time
	snapshotCalls    int
	scheduled        []database.ScheduledHealthRunSpec
	reconciledSpecs  []database.ProviderSpec
	providerWork     []database.ProviderActivationWork
	revalidationWork []database.GapRevalidationWork
}

func (r *pr5RuntimeScheduleRepository) ListProviderActivationWork(
	context.Context,
	int,
) ([]database.ProviderActivationWork, error) {
	return append([]database.ProviderActivationWork(nil), r.providerWork...), nil
}

func (r *pr5RuntimeScheduleRepository) ListDueGapRevalidations(
	context.Context,
	time.Time,
	int,
) ([]database.GapRevalidationWork, error) {
	return append([]database.GapRevalidationWork(nil), r.revalidationWork...), nil
}

func (r *pr5RuntimeScheduleRepository) ReconcileProviders(
	_ context.Context,
	specs []database.ProviderSpec,
) ([]database.HealthProvider, error) {
	r.reconciledSpecs = append([]database.ProviderSpec(nil), specs...)
	return nil, nil
}

func (r *pr5RuntimeScheduleRepository) EnsureObservationFileRevision(
	_ context.Context,
	spec database.FileRevisionSpec,
) (*database.HealthFileRevision, error) {
	r.revisionCalls++
	if r.revisionErr != nil {
		return nil, r.revisionErr
	}
	r.revision.LayoutFingerprint = spec.LayoutFingerprint
	r.revision.VirtualSize = spec.VirtualSize
	r.revision.SegmentCount = spec.SegmentCount
	return &r.revision, nil
}

func TestPR5OrdinarySchedulerDoesNotReactivateInactiveImportRevision(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	due := &pr5RuntimeDueRepository{files: []*database.FileHealth{{
		ID: 80, FilePath: "library/inactive-import.mkv", Status: database.HealthStatusPending,
	}}}
	state := &pr5RuntimeScheduleRepository{revisionErr: database.ErrInactiveFileRevision}
	scheduler := newOrdinaryObservationScheduler(
		due, state, &pr5RuntimeMetadataReader{metadata: pr5RuntimeMetadata()},
		func() *config.Config { return &config.Config{} },
		observationSchedulerConfig{BatchSize: 8, MaxRetries: 3, RecheckInterval: 24 * time.Hour},
		pr5RuntimeClock{now: now},
	)

	result, err := scheduler.ScheduleDue(context.Background())
	require.ErrorIs(t, err, database.ErrInactiveFileRevision)
	assert.Equal(t, 1, result.Examined)
	assert.Equal(t, 1, result.Failed)
	assert.Equal(t, 1, state.revisionCalls)
	assert.Zero(t, state.snapshotCalls)
	assert.Empty(t, state.scheduled)
}

func (r *pr5RuntimeScheduleRepository) GetCompletedImportSTATCoverage(
	context.Context,
	string,
	int64,
) (*database.CompletedImportSTATCoverage, error) {
	r.coverageReads++
	if r.coverage != nil {
		if r.coverage.Reusable && r.coverageConsumed {
			return nil, nil
		}
		copy := *r.coverage
		copy.UnresolvedPositions = append([]int64(nil), r.coverage.UnresolvedPositions...)
		return &copy, nil
	}
	if r.reusable && !r.coverageConsumed {
		return &database.CompletedImportSTATCoverage{Reusable: true}, nil
	}
	return nil, nil
}

func (r *pr5RuntimeScheduleRepository) ConsumeReusableCompletedImportSTATCoverageAndDeferHealth(
	_ context.Context,
	_ string,
	_ int64,
	filePath string,
	nextCheckAt time.Time,
	_ time.Time,
) (*database.CompletedImportSTATCoverage, error) {
	r.reuseChecks++
	if r.coverageConsumed {
		return nil, nil
	}
	if r.coverage != nil && r.coverage.Reusable {
		r.coverageConsumed = true
		if r.coverageDefers == nil {
			r.coverageDefers = make(map[string]time.Time)
		}
		r.coverageDefers[filePath] = nextCheckAt
		copy := *r.coverage
		copy.UnresolvedPositions = append([]int64(nil), r.coverage.UnresolvedPositions...)
		return &copy, nil
	}
	if r.reusable {
		r.coverageConsumed = true
		if r.coverageDefers == nil {
			r.coverageDefers = make(map[string]time.Time)
		}
		r.coverageDefers[filePath] = nextCheckAt
		return &database.CompletedImportSTATCoverage{Reusable: true}, nil
	}
	return nil, nil
}

func (r *pr5RuntimeScheduleRepository) CaptureActiveProviderSnapshot(context.Context, time.Time) (*database.ProviderSnapshot, error) {
	r.snapshotCalls++
	copy := r.snapshot
	return &copy, nil
}

func (r *pr5RuntimeScheduleRepository) GetActiveScheduledHealthRun(
	_ context.Context,
	dedupeKey string,
) (*database.HealthRun, error) {
	for _, scheduled := range r.scheduled {
		if scheduled.DedupeKey != dedupeKey {
			continue
		}
		return &database.HealthRun{
			ID: "existing-run", FileRevisionID: scheduled.Run.FileRevisionID,
			ProviderSnapshotID: scheduled.Run.ProviderSnapshotID,
			Trigger:            scheduled.Run.Trigger, Mode: scheduled.Run.Mode,
			TotalSegments: scheduled.Run.TotalSegments,
		}, nil
	}
	return nil, nil
}

func (r *pr5RuntimeScheduleRepository) EnsureScheduledHealthRun(
	_ context.Context,
	spec database.ScheduledHealthRunSpec,
) (*database.HealthRun, bool, error) {
	if existing, _ := r.GetActiveScheduledHealthRun(context.Background(), spec.DedupeKey); existing != nil {
		return existing, false, nil
	}
	r.scheduled = append(r.scheduled, spec)
	return &database.HealthRun{
		ID: "ordinary-run", FileRevisionID: spec.Run.FileRevisionID,
		ProviderSnapshotID: spec.Run.ProviderSnapshotID,
		Trigger:            spec.Run.Trigger, Mode: spec.Run.Mode,
		TotalSegments: spec.Run.TotalSegments,
	}, true, nil
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
		snapshot: database.ProviderSnapshot{ID: "snapshot-after-reuse", Entries: []database.ProviderSnapshotEntry{{
			ProviderID: "provider-a", ProviderGeneration: 1, ProviderActivationEpoch: 1,
			Role: database.ProviderRolePrimary,
		}},
		},
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
	assert.Equal(t, now.Add(24*time.Hour), state.coverageDefers["library/imported.mkv"])
	assert.Equal(t, layout.Fingerprint, state.revision.LayoutFingerprint)

	second, err := scheduler.ScheduleDue(context.Background())
	require.NoError(t, err)
	assert.Zero(t, second.CoverageReused,
		"accepted import coverage suppresses only the immediate duplicate sweep")
	assert.Equal(t, 1, second.Created)
	assert.Equal(t, 2, state.reuseChecks)
	require.Len(t, state.scheduled, 1)
	assert.Equal(t, "ordinary", state.scheduled[0].Run.Trigger)
}

func TestPR5ManualObservationBypassesImportCoverageAndSchedulesIdempotently(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 30, 0, 0, time.UTC)
	meta := pr5RuntimeMetadata()
	due := &pr5RuntimeDueRepository{files: []*database.FileHealth{{
		ID: 811, FilePath: "library/manual-imported.mkv",
		Status: database.HealthStatusCorrupted, RetryCount: 9, MaxRetries: 3,
	}}}
	state := &pr5RuntimeScheduleRepository{
		revision: database.HealthFileRevision{
			ID: "revision-manual-import", FileHealthID: 811, Active: true,
		},
		reusable: true,
		snapshot: database.ProviderSnapshot{
			ID: "snapshot-manual-import",
			Entries: []database.ProviderSnapshotEntry{{
				ProviderID: "provider-a", ProviderGeneration: 1,
				ProviderActivationEpoch: 1, Role: database.ProviderRolePrimary,
			}},
		},
	}
	scheduler := newOrdinaryObservationScheduler(
		due, state, &pr5RuntimeMetadataReader{metadata: meta},
		func() *config.Config { return &config.Config{} },
		observationSchedulerConfig{BatchSize: 8, RecheckInterval: 24 * time.Hour},
		pr5RuntimeClock{now: now},
	)

	result, err := scheduler.ScheduleFile(
		context.Background(), 811, ObservationScheduleIntentManual,
	)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Examined)
	assert.Equal(t, 1, result.Created)
	assert.Zero(t, result.CoverageReused)
	assert.Zero(t, state.reuseChecks,
		"an explicit manual request must not consult or consume accepted import coverage")
	assert.False(t, state.coverageConsumed)
	require.Len(t, state.scheduled, 1)
	assert.Equal(t, "manual", state.scheduled[0].Run.Trigger)
	assert.Equal(t, database.HealthRunPriorityHigh, state.scheduled[0].Priority)
	assert.Equal(t, now, state.scheduled[0].NotBefore)
	assert.Empty(t, due.rescheduled, "manual scheduling must preserve the automatic cadence")

	second, err := scheduler.ScheduleFile(
		context.Background(), 811, ObservationScheduleIntentManual,
	)
	require.NoError(t, err)
	assert.Equal(t, 1, second.Examined)
	assert.Equal(t, 1, second.Existing)
	assert.Zero(t, second.Created)
	assert.Zero(t, state.reuseChecks)
	assert.False(t, state.coverageConsumed)
	assert.Len(t, state.scheduled, 1, "repeated manual requests must reuse the durable schedule")
}

func TestPR5ManualObservationCoexistsIdempotentlyWithRunningOrdinaryRun(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "manual-existing-owner.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	healthRepository := database.NewHealthRepository(db.Connection(), database.DialectSQLite)
	stateRepository := database.NewHealthStateRepository(db.Connection(), database.DialectSQLite)
	ctx := context.Background()
	now := time.Date(2026, 7, 14, 12, 45, 0, 0, time.UTC)
	meta := pr5RuntimeMetadata()
	layout, err := metadata.ResolveCanonicalSegmentLayout(meta)
	require.NoError(t, err)
	revision, err := stateRepository.EnsureFileRevision(ctx, database.FileRevisionSpec{
		FilePath: "library/manual-with-owner.mkv", LayoutFingerprint: layout.Fingerprint,
		VirtualSize: layout.VirtualSize, SegmentCount: int64(len(layout.Segments)),
	})
	require.NoError(t, err)
	enabled := true
	cfg := &config.Config{Providers: []config.ProviderConfig{{
		ID: "manual-owner-provider", Name: "Synthetic provider",
		Host: "manual-owner.invalid", Port: 119,
		Username: "synthetic-account", Enabled: &enabled,
	}}}
	_, err = stateRepository.ReconcileProviders(ctx, observationProviderSpecs(cfg.Providers))
	require.NoError(t, err)
	snapshot, err := stateRepository.CaptureActiveProviderSnapshot(ctx, now)
	require.NoError(t, err)
	ordinary, _, err := stateRepository.EnsureScheduledHealthRun(ctx, database.ScheduledHealthRunSpec{
		Run: database.HealthRunSpec{
			ID: "manual-existing-ordinary", FileRevisionID: revision.ID,
			ProviderSnapshotID: snapshot.ID, Trigger: "ordinary", Mode: "observation",
			TotalSegments: revision.SegmentCount, CreatedAt: now,
		},
		DedupeKey: "ordinary:" + revision.ID, Priority: database.HealthRunPriorityNormal,
		NotBefore: now,
	})
	require.NoError(t, err)
	lease, err := stateRepository.ClaimDueObservationHealthRun(ctx, "manual-existing-owner", time.Minute)
	require.NoError(t, err)
	require.Equal(t, ordinary.ID, lease.ID)

	scheduler := newOrdinaryObservationScheduler(
		healthRepository, stateRepository, &pr5RuntimeMetadataReader{metadata: meta},
		func() *config.Config { return cfg }, observationSchedulerConfig{BatchSize: 8},
		pr5RuntimeClock{now: now},
	)
	first, err := scheduler.ScheduleFile(ctx, revision.FileHealthID, ObservationScheduleIntentManual)
	require.NoError(t, err)
	assert.Equal(t, 1, first.Created)
	second, err := scheduler.ScheduleFile(ctx, revision.FileHealthID, ObservationScheduleIntentManual)
	require.NoError(t, err)
	assert.Equal(t, 1, second.Existing)

	var activeSchedules, runningRuns int
	require.NoError(t, db.Connection().QueryRowContext(ctx, `
		SELECT COUNT(*) FROM health_run_schedule schedule
		JOIN health_runs run ON run.id = schedule.run_id
		WHERE run.file_revision_id = ? AND schedule.active = TRUE
	`, revision.ID).Scan(&activeSchedules))
	require.NoError(t, db.Connection().QueryRowContext(ctx, `
		SELECT COUNT(*) FROM health_runs
		WHERE file_revision_id = ? AND status = 'running'
	`, revision.ID).Scan(&runningRuns))
	assert.Equal(t, 2, activeSchedules,
		"manual work may queue behind the current revision owner without a uniqueness failure")
	assert.Equal(t, 1, runningRuns)
	selected, err := stateRepository.GetActiveObservationHealthRunForFile(ctx, revision.FileHealthID)
	require.NoError(t, err)
	require.NotNil(t, selected)
	assert.Equal(t, ordinary.ID, selected.ID)
}

func TestPR5ObservationSchedulerIncludesRediscoveredTerminalAndRetryExhaustedFiles(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "observation-due.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	healthRepository := database.NewHealthRepository(db.Connection(), database.DialectSQLite)
	stateRepository := database.NewHealthStateRepository(db.Connection(), database.DialectSQLite)
	ctx := context.Background()
	now := time.Date(2026, 7, 14, 13, 0, 0, 0, time.UTC)
	past := now.Add(-time.Hour)
	lastChecked := now.Add(-48 * time.Hour)
	type retainedState struct {
		path                         string
		status                       database.HealthStatus
		retryCount, repairRetryCount int
		lastError, details           string
		schedule                     any
	}
	states := []retainedState{
		{
			path: "library/rediscovered-corrupted.mkv", status: database.HealthStatusCorrupted,
			retryCount: 7, repairRetryCount: 2, lastError: "synthetic terminal result",
			details: "synthetic retained evidence", schedule: past,
		},
		{
			path: "library/rediscovered-retry-exhausted.mkv", status: database.HealthStatusPending,
			retryCount: 3, repairRetryCount: 1, lastError: "synthetic retry result",
			details: "synthetic retry evidence", schedule: nil,
		},
	}
	for _, state := range states {
		_, err := db.Connection().ExecContext(ctx, `
			INSERT INTO file_health
				(file_path, status, last_checked, last_error, error_details,
				 retry_count, max_retries, repair_retry_count, max_repair_retries,
				 scheduled_check_at, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, 3, ?, 4, ?, ?, ?)
		`, state.path, state.status, lastChecked, state.lastError, state.details,
			state.retryCount, state.repairRetryCount, state.schedule,
			now.Add(-72*time.Hour), now.Add(-48*time.Hour))
		require.NoError(t, err)
	}
	sourceA, sourceB := "synthetic-a.nzb", "synthetic-b.nzb"
	release := now.Add(-30 * 24 * time.Hour)
	require.NoError(t, healthRepository.BatchUpsertObservationDiscoveries(ctx, []database.AutomaticHealthCheckRecord{
		{
			FilePath: states[0].path, ReleaseDate: &release, ScheduledCheckAt: &now,
			SourceNzbPath: &sourceA, MaxRetries: 3, MaxRepairRetries: 4,
		},
		{
			FilePath: states[1].path, ReleaseDate: &release, ScheduledCheckAt: &now,
			SourceNzbPath: &sourceB, MaxRetries: 3, MaxRepairRetries: 4,
		},
	}))

	enabled := true
	cfg := &config.Config{Providers: []config.ProviderConfig{{
		ID: "observation-due-provider", Name: "Synthetic provider",
		Host: "observation-due.invalid", Port: 119,
		Username: "synthetic-account", Enabled: &enabled,
	}}}
	scheduler := newOrdinaryObservationScheduler(
		healthRepository, stateRepository,
		&pr5RuntimeMetadataReader{metadata: pr5RuntimeMetadata()},
		func() *config.Config { return cfg },
		observationSchedulerConfig{BatchSize: 8, RecheckInterval: 24 * time.Hour},
		pr5RuntimeClock{now: now},
	)

	result, err := scheduler.ScheduleDue(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Examined)
	assert.Equal(t, 2, result.Created)
	assert.Zero(t, result.Failed)
	runs, err := stateRepository.ListHealthRuns(ctx, 10)
	require.NoError(t, err)
	require.Len(t, runs, 2)
	for _, run := range runs {
		assert.Equal(t, "ordinary", run.Trigger)
		assert.Equal(t, database.HealthRunPending, run.Status)
	}

	var rowCount int
	require.NoError(t, db.Connection().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM file_health`).Scan(&rowCount))
	assert.Equal(t, 2, rowCount, "observation scheduling must not delete retained records")
	for _, expected := range states {
		var (
			status                       database.HealthStatus
			retryCount, repairRetryCount int
			lastError, details           string
			scheduled                    time.Time
		)
		require.NoError(t, db.Connection().QueryRowContext(ctx, `
			SELECT status, retry_count, repair_retry_count, last_error,
			       error_details, scheduled_check_at
			FROM file_health WHERE file_path = ?
		`, expected.path).Scan(
			&status, &retryCount, &repairRetryCount, &lastError, &details, &scheduled,
		))
		assert.Equal(t, expected.status, status)
		assert.Equal(t, expected.retryCount, retryCount)
		assert.Equal(t, expected.repairRetryCount, repairRetryCount)
		assert.Equal(t, expected.lastError, lastError)
		assert.Equal(t, expected.details, details)
		assert.True(t, scheduled.Equal(now.Add(24*time.Hour)), "%v", scheduled)
	}
}

func TestPR5ObservationDiscoveryFailureBackoffPreventsStableLimitStarvation(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "observation-fairness.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	healthRepository := database.NewHealthRepository(db.Connection(), database.DialectSQLite)
	stateRepository := database.NewHealthStateRepository(db.Connection(), database.DialectSQLite)
	ctx := context.Background()
	now := time.Date(2026, 7, 14, 13, 30, 0, 0, time.UTC)
	due := now.Add(-time.Minute)
	paths := []string{
		"library/malformed-one.mkv",
		"library/malformed-two.mkv",
		"library/malformed-three.mkv",
		"library/valid-after-malformed.mkv",
	}
	for _, path := range paths {
		_, err := db.Connection().ExecContext(ctx, `
			INSERT INTO file_health
				(file_path, status, last_error, error_details, retry_count, max_retries,
				 repair_retry_count, max_repair_retries, source_nzb_path,
				 scheduled_check_at, created_at, updated_at)
			VALUES (?, 'corrupted', 'retained result', 'retained detail', 7, 3,
			        2, 4, 'synthetic.nzb', ?, ?, ?)
		`, path, due, now.Add(-time.Hour), now.Add(-time.Hour))
		require.NoError(t, err)
	}
	enabled := true
	cfg := &config.Config{Providers: []config.ProviderConfig{{
		ID: "observation-fairness-provider", Name: "Synthetic provider",
		Host: "observation-fairness.invalid", Port: 119,
		Username: "synthetic-account", Enabled: &enabled,
	}}}
	reader := &pr5SelectiveMetadataReader{
		metadata: pr5RuntimeMetadata(),
		fail: map[string]bool{
			paths[0]: true,
			paths[1]: true,
			paths[2]: true,
		},
	}
	scheduler := newOrdinaryObservationScheduler(
		healthRepository, stateRepository, reader,
		func() *config.Config { return cfg },
		observationSchedulerConfig{BatchSize: 2, RecheckInterval: 24 * time.Hour},
		pr5RuntimeClock{now: now},
	)

	first, err := scheduler.ScheduleDue(ctx)
	require.Error(t, err)
	assert.Equal(t, 2, first.Examined)
	assert.Equal(t, 2, first.Failed)
	assert.Zero(t, first.Created)

	second, err := scheduler.ScheduleDue(ctx)
	require.Error(t, err)
	assert.Equal(t, 2, second.Examined,
		"the durable backoff must move the first failed batch behind later due rows")
	assert.Equal(t, 1, second.Failed)
	assert.Equal(t, 1, second.Created)

	runs, err := stateRepository.ListHealthRuns(ctx, 10)
	require.NoError(t, err)
	require.Len(t, runs, 1)
	assert.Equal(t, "ordinary", runs[0].Trigger)
	var rowCount int
	require.NoError(t, db.Connection().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM file_health`).Scan(&rowCount))
	assert.Equal(t, len(paths), rowCount)
	for index, path := range paths {
		var (
			status                       database.HealthStatus
			retryCount, repairRetryCount int
			lastError, details           string
			scheduled                    time.Time
		)
		require.NoError(t, db.Connection().QueryRowContext(ctx, `
			SELECT status, retry_count, repair_retry_count, last_error,
			       error_details, scheduled_check_at
			FROM file_health WHERE file_path = ?
		`, path).Scan(
			&status, &retryCount, &repairRetryCount, &lastError, &details, &scheduled,
		))
		assert.Equal(t, database.HealthStatusCorrupted, status)
		assert.Equal(t, 7, retryCount)
		assert.Equal(t, 2, repairRetryCount)
		assert.Equal(t, "retained result", lastError)
		assert.Equal(t, "retained detail", details)
		if index < 3 {
			assert.True(t, scheduled.Equal(now.Add(observationDiscoveryFailureBackoff)), "%v", scheduled)
		} else {
			assert.True(t, scheduled.Equal(now.Add(24*time.Hour)), "%v", scheduled)
		}
	}
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

func TestPR5OrdinarySchedulerQueuesHealthPendingInsteadOfDeferringItAsReusable(t *testing.T) {
	now := time.Date(2026, 7, 14, 14, 0, 0, 0, time.UTC)
	due := &pr5RuntimeDueRepository{files: []*database.FileHealth{{
		ID: 83, FilePath: "library/health-pending.mkv", Status: database.HealthStatusPending,
	}}}
	state := &pr5RuntimeScheduleRepository{
		revision: database.HealthFileRevision{ID: "revision-health-pending", FileHealthID: 83, Active: true},
		coverage: &database.CompletedImportSTATCoverage{
			HealthPending: true, UnresolvedPositions: []int64{1},
		},
		snapshot: database.ProviderSnapshot{ID: "snapshot-health-pending", Entries: []database.ProviderSnapshotEntry{{
			ProviderID: "provider-a", ProviderGeneration: 1, ProviderActivationEpoch: 1,
			Role: database.ProviderRolePrimary,
		}}},
	}
	scheduler := newOrdinaryObservationScheduler(
		due, state, &pr5RuntimeMetadataReader{metadata: pr5RuntimeMetadata()},
		func() *config.Config { return &config.Config{} },
		observationSchedulerConfig{BatchSize: 8, MaxRetries: 3, RecheckInterval: 24 * time.Hour},
		pr5RuntimeClock{now: now},
	)

	result, err := scheduler.ScheduleDue(context.Background())
	require.NoError(t, err)
	assert.Zero(t, result.CoverageReused)
	assert.Equal(t, 1, result.Created)
	require.Len(t, state.scheduled, 1)
	assert.Equal(t, "health_pending", state.scheduled[0].Run.Trigger)
	assert.Equal(t, "health-pending:revision-health-pending", state.scheduled[0].DedupeKey)
	assert.Equal(t, database.HealthRunPriorityHigh, state.scheduled[0].Priority)
	assert.Equal(t, 1, state.reuseChecks)
	assert.Equal(t, 1, state.coverageReads,
		"health-pending coverage remains read-only and preserves exact unresolved targeting")
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

type pr5RuntimeFileController struct {
	run       *database.HealthRun
	requested []string
	cancelErr error
	lookupErr error
}

func (c *pr5RuntimeFileController) GetActiveObservationHealthRunForFile(
	context.Context,
	int64,
) (*database.HealthRun, error) {
	if c.lookupErr != nil || c.run == nil {
		return nil, c.lookupErr
	}
	copy := *c.run
	return &copy, nil
}

func (c *pr5RuntimeFileController) RequestRunCancel(
	_ context.Context,
	runID string,
	_ time.Time,
) error {
	c.requested = append(c.requested, runID)
	if c.cancelErr != nil {
		return c.cancelErr
	}
	c.run = nil
	return nil
}

type pr5BlockingRuntimeProcessor struct {
	started chan struct{}
	release chan struct{}
}

type pr5ErrorRuntimeProcessor struct{ called chan struct{} }

type pr5LockedLogBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *pr5LockedLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}

func (b *pr5LockedLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.String()
}

func (p *pr5ErrorRuntimeProcessor) ProcessNext(context.Context) (observationWorkerStep, error) {
	select {
	case p.called <- struct{}{}:
	default:
	}
	return observationWorkerIdle, errors.New("fixture-article-id synthetic-credential")
}

type pr5ErrorRuntimeScheduler struct{ called chan struct{} }

func (s *pr5ErrorRuntimeScheduler) ScheduleDue(context.Context) (ObservationScheduleResult, error) {
	select {
	case s.called <- struct{}{}:
	default:
	}
	return ObservationScheduleResult{}, errors.New("fixture-article-id synthetic-credential")
}

func (p *pr5BlockingRuntimeProcessor) ProcessNext(context.Context) (observationWorkerStep, error) {
	p.started <- struct{}{}
	<-p.release
	return observationWorkerIdle, nil
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

func TestPR5ObservationServiceFileCancelUsesDurableControlAndReportsNotActive(t *testing.T) {
	controller := &pr5RuntimeFileController{run: &database.HealthRun{
		ID: "file-cancel-run", Trigger: "manual", Mode: "observation",
		Status: database.HealthRunPending,
	}}
	service := newObservationServiceForTest(
		&pr5RuntimeProcessor{started: make(chan struct{}, 1)},
		pr5RuntimeScheduler{}, ObservationServiceConfig{},
	)
	service.status = ObservationServiceRunning
	service.controller = controller

	require.NoError(t, service.CancelFile(context.Background(), 41))
	assert.Equal(t, []string{"file-cancel-run"}, controller.requested)
	require.ErrorIs(t, service.CancelFile(context.Background(), 41), ErrObservationRunNotActive)
	assert.Error(t, service.CancelFile(context.Background(), 0))
}

func TestPR5ObservationServiceTimedOutStopCanBeJoinedBeforeRestart(t *testing.T) {
	processor := &pr5BlockingRuntimeProcessor{
		started: make(chan struct{}, 2), release: make(chan struct{}),
	}
	service := newObservationServiceForTest(processor, pr5RuntimeScheduler{}, ObservationServiceConfig{
		WorkerCount: 1, PollInterval: time.Hour, ScheduleInterval: time.Hour,
	})
	require.NoError(t, service.Start(context.Background()))
	select {
	case <-processor.started:
	case <-time.After(time.Second):
		t.Fatal("observation processor did not start")
	}

	stopCtx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, service.StopAndWait(stopCtx), context.Canceled)
	assert.Equal(t, ObservationServiceStopping, service.Status())
	assert.ErrorIs(t, service.Start(context.Background()), ErrObservationServiceRunning)

	close(processor.release)
	joinCtx, joinCancel := context.WithTimeout(context.Background(), time.Second)
	defer joinCancel()
	require.NoError(t, service.StopAndWait(joinCtx))
	assert.Equal(t, ObservationServiceStopped, service.Status())

	require.NoError(t, service.Start(context.Background()))
	select {
	case <-processor.started:
	case <-time.After(time.Second):
		t.Fatal("replacement observation processor did not start")
	}
	finalStopCtx, finalStopCancel := context.WithTimeout(context.Background(), time.Second)
	defer finalStopCancel()
	require.NoError(t, service.StopAndWait(finalStopCtx))
}

func TestPR5ObservationServiceReportsFailuresWithoutSensitiveEvidence(t *testing.T) {
	processor := &pr5ErrorRuntimeProcessor{called: make(chan struct{}, 1)}
	scheduler := &pr5ErrorRuntimeScheduler{called: make(chan struct{}, 1)}
	service := newObservationServiceForTest(processor, scheduler, ObservationServiceConfig{
		WorkerCount: 1, PollInterval: time.Hour, ScheduleInterval: time.Hour,
	})
	var logs pr5LockedLogBuffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	require.NoError(t, service.Start(context.Background()))
	select {
	case <-processor.called:
	case <-time.After(time.Second):
		t.Fatal("observation processor did not report its failure")
	}
	select {
	case <-scheduler.called:
	case <-time.After(time.Second):
		t.Fatal("observation scheduler did not report its failure")
	}
	require.Eventually(t, func() bool {
		text := logs.String()
		return strings.Contains(text, "Health observation worker step failed") &&
			strings.Contains(text, "Health observation scheduling pass failed")
	}, time.Second, time.Millisecond)
	require.NoError(t, service.StopAndWait(context.Background()))
	finalLogs := logs.String()
	require.NotContains(t, finalLogs, "fixture-article-id")
	require.NotContains(t, finalLogs, "synthetic-credential")
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
