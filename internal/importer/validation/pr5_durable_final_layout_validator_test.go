package validation

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/javi11/nntppool/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pr5DurableImportClock struct {
	now time.Time
}

func (c *pr5DurableImportClock) advance(delta time.Duration) { c.now = c.now.Add(delta) }

type pr5DurableImportRepository struct {
	mu sync.Mutex

	revision        database.HealthFileRevision
	providers       []database.HealthProvider
	snapshot        database.ProviderSnapshot
	run             database.HealthRun
	validation      *database.ImportValidation
	chunks          []database.HealthRunChunkState
	reconcileCalls  int
	ensureRunCalls  int
	confirmationDue time.Time
}

func (r *pr5DurableImportRepository) EnsureFileRevision(
	_ context.Context,
	spec database.FileRevisionSpec,
) (*database.HealthFileRevision, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.revision.ID == "" {
		r.revision = database.HealthFileRevision{
			ID: "fixture-revision", LayoutFingerprint: spec.LayoutFingerprint,
			VirtualSize: spec.VirtualSize, SegmentCount: spec.SegmentCount, Active: true,
		}
	}
	copy := r.revision
	return &copy, nil
}

func (r *pr5DurableImportRepository) ReconcileProviders(
	_ context.Context,
	specs []database.ProviderSpec,
) ([]database.HealthProvider, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reconcileCalls++
	r.providers = make([]database.HealthProvider, len(specs))
	for i, spec := range specs {
		r.providers[i] = database.HealthProvider{
			ID: spec.StableID, DisplayName: spec.DisplayName, Role: spec.Role,
			Order: spec.Order, Active: true, CurrentGeneration: 1, ActivationEpoch: 1,
		}
	}
	return append([]database.HealthProvider(nil), r.providers...), nil
}

func (r *pr5DurableImportRepository) CaptureActiveProviderSnapshot(
	_ context.Context,
	at time.Time,
) (*database.ProviderSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snapshot = database.ProviderSnapshot{ID: "fixture-snapshot", CreatedAt: at}
	for _, provider := range r.providers {
		r.snapshot.Entries = append(r.snapshot.Entries, database.ProviderSnapshotEntry{
			ProviderID: provider.ID, ProviderGeneration: provider.CurrentGeneration,
			ProviderActivationEpoch: provider.ActivationEpoch,
			Role:                    provider.Role, Order: provider.Order,
		})
	}
	copy := r.snapshot
	copy.Entries = append([]database.ProviderSnapshotEntry(nil), r.snapshot.Entries...)
	return &copy, nil
}

func (r *pr5DurableImportRepository) GetProviderSnapshot(
	_ context.Context,
	id string,
) (*database.ProviderSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.snapshot.ID != id {
		return nil, nil
	}
	copy := r.snapshot
	copy.Entries = append([]database.ProviderSnapshotEntry(nil), r.snapshot.Entries...)
	return &copy, nil
}

func (r *pr5DurableImportRepository) EnsureScheduledHealthRun(
	_ context.Context,
	spec database.ScheduledHealthRunSpec,
) (*database.HealthRun, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureRunCalls++
	created := r.run.ID == ""
	if created {
		r.run = database.HealthRun{
			ID: spec.Run.ID, FileRevisionID: spec.Run.FileRevisionID,
			ProviderSnapshotID: spec.Run.ProviderSnapshotID, Trigger: spec.Run.Trigger,
			Mode: spec.Run.Mode, Status: database.HealthRunPending,
			TotalSegments: spec.Run.TotalSegments, CreatedAt: spec.Run.CreatedAt,
			UpdatedAt: spec.Run.CreatedAt,
		}
	}
	copy := r.run
	return &copy, created, nil
}

func (r *pr5DurableImportRepository) GetImportValidation(
	_ context.Context,
	queueItemID int64,
	revisionID string,
) (*database.ImportValidation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.validation == nil || r.validation.QueueItemID != queueItemID ||
		r.validation.FileRevisionID != revisionID {
		return nil, nil
	}
	copy := *r.validation
	copy.UnresolvedBitmap = append([]byte(nil), r.validation.UnresolvedBitmap...)
	return &copy, nil
}

func (r *pr5DurableImportRepository) UpsertImportValidation(
	_ context.Context,
	write database.ImportValidationWrite,
) (*database.ImportValidation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.validation == nil {
		r.validation = &database.ImportValidation{
			ID: write.ID, QueueItemID: write.QueueItemID, FileRevisionID: write.FileRevisionID,
			RunID: write.RunID, Phase: database.ImportValidationPhaseInitialPass,
			DamagePolicy: write.DamagePolicy, CreatedAt: write.CreatedAt, UpdatedAt: write.UpdatedAt,
			UnresolvedBitmap: make([]byte, durableBitmapLength(r.run.TotalSegments)),
		}
	} else {
		r.validation.Phase = write.Phase
		r.validation.ConfirmationDueAt = write.ConfirmationDueAt
		r.validation.UnresolvedSegments = write.UnresolvedSegments
		r.validation.UnresolvedBitmap = append([]byte(nil), write.UnresolvedBitmap...)
		r.validation.InitialPassComplete = write.InitialPassComplete
		r.validation.SecondPassComplete = write.SecondPassComplete
		r.validation.UpdatedAt = write.UpdatedAt
		if write.ConfirmationDueAt != nil {
			r.confirmationDue = *write.ConfirmationDueAt
		}
		if write.Phase == database.ImportValidationPhaseAccepted ||
			write.Phase == database.ImportValidationPhaseHealthPending ||
			write.Phase == database.ImportValidationPhaseRejected {
			r.run.Status = database.HealthRunCompleted
			r.run.LeaseOwner = nil
			r.run.LeaseExpiresAt = nil
		}
	}
	copy := *r.validation
	copy.UnresolvedBitmap = append([]byte(nil), r.validation.UnresolvedBitmap...)
	return &copy, nil
}

func (r *pr5DurableImportRepository) AcquireRunLease(
	_ context.Context,
	runID, owner string,
	ttl time.Duration,
) (*database.HealthRun, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.run.ID != runID {
		return nil, database.ErrStaleHealthLease
	}
	r.run.Status = database.HealthRunRunning
	r.run.FencingToken++
	r.run.LeaseOwner = &owner
	expires := r.run.UpdatedAt.Add(ttl)
	r.run.LeaseExpiresAt = &expires
	copy := r.run
	return &copy, nil
}

func (r *pr5DurableImportRepository) RenewHealthRunLease(
	_ context.Context,
	runID, owner string,
	token int64,
	ttl time.Duration,
) (*database.HealthRun, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.run.ID != runID || r.run.FencingToken != token || r.run.LeaseOwner == nil || *r.run.LeaseOwner != owner {
		return nil, database.ErrStaleHealthLease
	}
	expires := r.run.UpdatedAt.Add(ttl)
	r.run.LeaseExpiresAt = &expires
	copy := r.run
	return &copy, nil
}

func (r *pr5DurableImportRepository) ParkHealthRun(
	_ context.Context,
	runID, owner string,
	token int64,
	notBefore, at time.Time,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.run.ID != runID || r.run.FencingToken != token || r.run.LeaseOwner == nil || *r.run.LeaseOwner != owner {
		return database.ErrStaleHealthLease
	}
	r.run.Status = database.HealthRunPending
	r.run.LeaseOwner = nil
	r.run.LeaseExpiresAt = nil
	r.run.UpdatedAt = at
	r.confirmationDue = notBefore
	return nil
}

func (r *pr5DurableImportRepository) GetHealthRunResumeState(
	_ context.Context,
	runID string,
) (*database.HealthRunResumeState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.run.ID != runID {
		return nil, nil
	}
	return &database.HealthRunResumeState{
		Run: r.run, Chunks: append([]database.HealthRunChunkState(nil), r.chunks...),
	}, nil
}

func (r *pr5DurableImportRepository) CommitHealthChunk(
	_ context.Context,
	commit database.HealthChunkCommit,
) (*database.HealthRun, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.run.ID != commit.RunID || r.run.FencingToken != commit.FencingToken ||
		r.run.LeaseOwner == nil || *r.run.LeaseOwner != commit.LeaseOwner {
		return nil, database.ErrStaleHealthLease
	}
	r.chunks = append(r.chunks, database.HealthRunChunkState{
		ID: commit.ChunkID, RunID: commit.RunID, ProviderID: commit.ProviderID,
		ProviderGeneration:      commit.ProviderGeneration,
		ProviderActivationEpoch: commit.ProviderActivationEpoch,
		Stage:                   commit.Stage, ObservationKind: commit.ObservationKind,
		SegmentStart: commit.SegmentStart, SegmentCount: commit.SegmentCount,
		TestedBitmap:       append([]byte(nil), commit.TestedBitmap...),
		PresentBitmap:      append([]byte(nil), commit.PresentBitmap...),
		AbsentBitmap:       append([]byte(nil), commit.AbsentBitmap...),
		TemporaryBitmap:    append([]byte(nil), commit.TemporaryBitmap...),
		InconclusiveBitmap: append([]byte(nil), commit.InconclusiveBitmap...),
		ResolvedBitmap:     append([]byte(nil), commit.ResolvedBitmap...),
		FencingToken:       commit.FencingToken, CommittedAt: commit.CommittedAt,
	})
	r.run.Stage = commit.Stage
	r.run.UpdatedAt = commit.CommittedAt
	copy := r.run
	return &copy, nil
}

type pr5TargetedSTATCall struct {
	providerID string
	messageID  string
}

type pr5TargetedSTATTransport struct {
	mu            sync.Mutex
	results       map[string]TargetedSTATResult
	defaultResult TargetedSTATResult
	err           error
	calls         []pr5TargetedSTATCall
	invalidTarget bool
	omitAll       bool
}

func (t *pr5TargetedSTATTransport) TargetedSTAT(
	_ context.Context,
	provider TargetedSTATProvider,
	requests []TargetedSTATRequest,
) ([]TargetedSTATObservation, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	providerID := provider.ID
	if providerID == "" || provider.Generation != 1 || provider.ActivationEpoch != 1 {
		t.invalidTarget = true
	}
	if t.err != nil {
		return nil, t.err
	}
	if t.omitAll {
		return nil, nil
	}
	observations := make([]TargetedSTATObservation, 0, len(requests))
	for _, request := range requests {
		t.calls = append(t.calls, pr5TargetedSTATCall{providerID: providerID, messageID: request.MessageID})
		result := t.defaultResult
		if scripted, ok := t.results[providerID+"/"+request.MessageID]; ok {
			result = scripted
		}
		observations = append(observations, TargetedSTATObservation{
			Position: request.Position, Result: result,
		})
	}
	return observations, nil
}

func pr5STATResult(outcome nntppool.OutcomeKind) TargetedSTATResult {
	disposition := ImportCheckDispositionAttempted
	responseCode := 0
	if outcome == nntppool.OutcomeProviderUnavailable {
		disposition = ImportCheckDispositionExplicitUnavailable
	}
	if outcome == nntppool.OutcomeHardArticleAbsence {
		responseCode = 430
	}
	return TargetedSTATResult{
		Outcome: outcome, ResponseCode: responseCode, CompletionDisposition: disposition,
	}
}

func pr5DurableImportProviders() []database.ProviderSpec {
	return []database.ProviderSpec{
		{StableID: "primary-a", DisplayName: "Primary A", Endpoint: "primary-a.invalid", Port: 119, Role: database.ProviderRolePrimary, Order: 0},
		{StableID: "primary-b", DisplayName: "Primary B", Endpoint: "primary-b.invalid", Port: 119, Role: database.ProviderRolePrimary, Order: 1},
		{StableID: "backup-a", DisplayName: "Backup A", Endpoint: "backup-a.invalid", Port: 119, Role: database.ProviderRoleBackup, Order: 2},
	}
}

func pr5DurableImportMetadata(count int, segmentBytes int64) *metapb.FileMetadata {
	meta := &metapb.FileMetadata{FileSize: int64(count) * segmentBytes}
	for i := 0; i < count; i++ {
		meta.SegmentData = append(meta.SegmentData, &metapb.SegmentData{
			Id: fmt.Sprintf("fixture-article-%03d", i), SegmentSize: segmentBytes,
			StartOffset: 0, EndOffset: segmentBytes - 1,
		})
	}
	return meta
}

func pr5NewDurableValidator(
	repo *pr5DurableImportRepository,
	transport *pr5TargetedSTATTransport,
	clock *pr5DurableImportClock,
	options DurableFinalLayoutValidatorOptions,
) *DurableFinalLayoutValidator {
	validator, err := NewDurableFinalLayoutValidator(repo, transport, options)
	if err != nil {
		panic(err)
	}
	validator.now = func() time.Time { return clock.now }
	return validator
}

func TestPR5DurableFinalLayoutValidatorResumesExactAllProviderConfirmation(t *testing.T) {
	clock := &pr5DurableImportClock{now: time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)}
	repo := &pr5DurableImportRepository{}
	meta := pr5DurableImportMetadata(3, 100)
	initial := &pr5TargetedSTATTransport{
		defaultResult: pr5STATResult(nntppool.OutcomeSuccess),
		results: map[string]TargetedSTATResult{
			"primary-a/fixture-article-001": pr5STATResult(nntppool.OutcomeHardArticleAbsence),
			"primary-a/fixture-article-002": pr5STATResult(nntppool.OutcomeTemporaryFailure),
			"primary-b/fixture-article-001": pr5STATResult(nntppool.OutcomeSuccess),
			"primary-b/fixture-article-002": pr5STATResult(nntppool.OutcomeHardArticleAbsence),
			"backup-a/fixture-article-002":  pr5STATResult(nntppool.OutcomeHardArticleAbsence),
		},
	}
	options := DurableFinalLayoutValidatorOptions{
		ProviderSpecs: pr5DurableImportProviders(), DamagePolicy: config.ImportDamagePolicyStrict,
	}
	validator := pr5NewDurableValidator(repo, initial, clock, options)

	result, err := validator.ValidateFinalLayout(
		context.Background(), 41, "library/movie.mkv", meta,
		FinalLayoutProvenance{Kind: FinalLayoutProvenanceStandalone},
	)
	require.NoError(t, err)
	assert.Equal(t, ImportAdmissionAwaitConfirmation, result.Status)
	require.NotNil(t, result.RetryAt)
	assert.Equal(t, clock.now.Add(30*time.Second), *result.RetryAt)
	assert.Equal(t, []int{2}, result.UnresolvedPositions)
	assert.Equal(t, 1, repo.reconcileCalls)
	assert.Equal(t, 1, repo.ensureRunCalls)
	assert.Equal(t, []pr5TargetedSTATCall{
		{"primary-a", "fixture-article-000"},
		{"primary-a", "fixture-article-001"},
		{"primary-a", "fixture-article-002"},
		{"primary-b", "fixture-article-001"},
		{"primary-b", "fixture-article-002"},
		{"backup-a", "fixture-article-002"},
	}, initial.calls, "initial STAT must remain primary-first with failure-only fallback")
	assert.False(t, initial.invalidTarget, "dispatch must retain exact snapshot generation and activation identity")

	clock.advance(30 * time.Second)
	confirmation := &pr5TargetedSTATTransport{
		results: map[string]TargetedSTATResult{
			"primary-a/fixture-article-002": pr5STATResult(nntppool.OutcomeSuccess),
			"primary-b/fixture-article-002": pr5STATResult(nntppool.OutcomeHardArticleAbsence),
			"backup-a/fixture-article-002":  pr5STATResult(nntppool.OutcomeTemporaryFailure),
		},
	}
	restarted := pr5NewDurableValidator(repo, confirmation, clock, options)
	result, err = restarted.ValidateFinalLayout(
		context.Background(), 41, "library/movie.mkv", meta,
		FinalLayoutProvenance{Kind: FinalLayoutProvenanceStandalone},
	)
	require.NoError(t, err)
	assert.Equal(t, ImportAdmissionAccept, result.Status,
		"one provider success wins without erasing the other providers' exact pass evidence")
	assert.Equal(t, []pr5TargetedSTATCall{
		{"primary-a", "fixture-article-002"},
		{"primary-b", "fixture-article-002"},
		{"backup-a", "fixture-article-002"},
	}, confirmation.calls, "confirmation must check the exact unresolved set on every snapshot provider")
	assert.False(t, confirmation.invalidTarget, "restart must retain the frozen snapshot target identity")
	assert.Equal(t, 1, repo.reconcileCalls, "restart must reuse the frozen run snapshot")
	assert.Equal(t, 1, repo.ensureRunCalls, "restart must reuse the durable observation run")

	for _, chunk := range repo.chunks {
		assert.Equal(t, int64(1), chunk.ProviderActivationEpoch)
		assert.Equal(t, database.HealthObservationSTAT, chunk.ObservationKind)
	}
	durable := fmt.Sprintf("%#v", repo.chunks)
	for _, segment := range meta.SegmentData {
		assert.NotContains(t, durable, segment.Id, "durable evidence must not retain article identities")
	}
}

func TestPR5DurableFinalLayoutValidatorIncompleteWorkNeverRejects(t *testing.T) {
	for _, tt := range []struct {
		name      string
		result    TargetedSTATResult
		transport error
		omit      bool
	}{
		{
			name: "canceled logical check",
			result: TargetedSTATResult{
				Outcome:               nntppool.OutcomeCancellation,
				CompletionDisposition: ImportCheckDispositionIncomplete,
			},
		},
		{
			name: "breaker skipped target",
			result: TargetedSTATResult{
				Outcome:               nntppool.OutcomeTemporaryFailure,
				CompletionDisposition: ImportCheckDispositionAttempted,
				CauseClass:            TargetedSTATCauseBreakerOpen,
			},
		},
		{name: "omitted result", omit: true},
		{name: "unclassified transport return", transport: errors.New("synthetic transport interruption")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			clock := &pr5DurableImportClock{now: time.Date(2026, 7, 14, 13, 0, 0, 0, time.UTC)}
			repo := &pr5DurableImportRepository{}
			transport := &pr5TargetedSTATTransport{
				defaultResult: tt.result, err: tt.transport, omitAll: tt.omit,
			}
			validator := pr5NewDurableValidator(repo, transport, clock, DurableFinalLayoutValidatorOptions{
				ProviderSpecs: pr5DurableImportProviders()[:1], DamagePolicy: config.ImportDamagePolicyStrict,
			})

			result, err := validator.ValidateFinalLayout(
				context.Background(), 42, "library/movie.mkv", pr5DurableImportMetadata(1, 100),
				FinalLayoutProvenance{Kind: FinalLayoutProvenanceStandalone},
			)
			require.NoError(t, err)
			assert.Equal(t, ImportAdmissionAwaitConfirmation, result.Status)
			assert.True(t, result.ResumeRequired)
			require.NotNil(t, repo.validation)
			assert.Equal(t, database.ImportValidationPhaseInitialPass, repo.validation.Phase)
			assert.Empty(t, repo.chunks, "incomplete work cannot become a durable terminal provider check")
		})
	}
}

func TestPR5DurableFinalLayoutValidatorStrictAndProvenanceBoundTolerantOutcomes(t *testing.T) {
	for _, tt := range []struct {
		name       string
		policy     config.ImportDamagePolicy
		provenance FinalLayoutProvenance
		mutate     func(*metapb.FileMetadata)
		want       ImportAdmissionStatus
	}{
		{
			name: "strict rejects bounded unresolved damage", policy: config.ImportDamagePolicyStrict,
			provenance: FinalLayoutProvenance{Kind: FinalLayoutProvenanceStandalone},
			want:       ImportAdmissionReject,
		},
		{
			name:       "tolerant admits uncomplicated standalone video as health pending",
			policy:     config.ImportDamagePolicyTolerant,
			provenance: FinalLayoutProvenance{Kind: FinalLayoutProvenanceStandalone},
			want:       ImportAdmissionHealthPending,
		},
		{
			name:       "tolerant rejects archive-derived provenance",
			policy:     config.ImportDamagePolicyTolerant,
			provenance: FinalLayoutProvenance{Kind: FinalLayoutProvenanceArchiveMember},
			want:       ImportAdmissionReject,
		},
		{
			name:       "tolerant rejects encrypted metadata despite standalone claim",
			policy:     config.ImportDamagePolicyTolerant,
			provenance: FinalLayoutProvenance{Kind: FinalLayoutProvenanceStandalone},
			mutate:     func(meta *metapb.FileMetadata) { meta.Encryption = metapb.Encryption_AES },
			want:       ImportAdmissionReject,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			clock := &pr5DurableImportClock{now: time.Date(2026, 7, 14, 14, 0, 0, 0, time.UTC)}
			repo := &pr5DurableImportRepository{}
			meta := pr5DurableImportMetadata(50, 100)
			if tt.mutate != nil {
				tt.mutate(meta)
			}
			provider := pr5DurableImportProviders()[:1]
			initial := &pr5TargetedSTATTransport{
				defaultResult: pr5STATResult(nntppool.OutcomeSuccess),
				results: map[string]TargetedSTATResult{
					"primary-a/fixture-article-000": pr5STATResult(nntppool.OutcomeHardArticleAbsence),
				},
			}
			options := DurableFinalLayoutValidatorOptions{
				ProviderSpecs: provider, DamagePolicy: tt.policy, ConfirmationDelay: 7 * time.Second,
			}
			validator := pr5NewDurableValidator(repo, initial, clock, options)
			first, err := validator.ValidateFinalLayout(context.Background(), 43, "library/movie.mkv", meta, tt.provenance)
			require.NoError(t, err)
			assert.Equal(t, ImportAdmissionAwaitConfirmation, first.Status)
			require.NotNil(t, first.RetryAt)
			assert.Equal(t, clock.now.Add(7*time.Second), *first.RetryAt,
				"configured confirmation delay must replace the 30-second default durably")

			clock.advance(7 * time.Second)
			confirmation := &pr5TargetedSTATTransport{defaultResult: pr5STATResult(nntppool.OutcomeHardArticleAbsence)}
			restarted := pr5NewDurableValidator(repo, confirmation, clock, options)
			final, err := restarted.ValidateFinalLayout(context.Background(), 43, "library/movie.mkv", meta, tt.provenance)
			require.NoError(t, err)
			assert.Equal(t, tt.want, final.Status)
			assert.Equal(t, []int{0}, final.UnresolvedPositions)
			if tt.want == ImportAdmissionHealthPending {
				assert.Equal(t, database.ImportValidationPhaseHealthPending, repo.validation.Phase)
			} else {
				assert.Equal(t, database.ImportValidationPhaseRejected, repo.validation.Phase)
			}
		})
	}
}

func TestPR5DurableFinalLayoutValidatorRejectsUnsafeDurableTransportClasses(t *testing.T) {
	clock := &pr5DurableImportClock{now: time.Date(2026, 7, 14, 15, 0, 0, 0, time.UTC)}
	repo := &pr5DurableImportRepository{}
	transport := &pr5TargetedSTATTransport{defaultResult: TargetedSTATResult{
		Outcome:               nntppool.OutcomeTemporaryFailure,
		CompletionDisposition: ImportCheckDispositionAttempted,
		CauseClass:            TargetedSTATCauseClass("unsafe class <fixture-article-000>"),
	}}
	validator := pr5NewDurableValidator(repo, transport, clock, DurableFinalLayoutValidatorOptions{
		ProviderSpecs: pr5DurableImportProviders()[:1], DamagePolicy: config.ImportDamagePolicyStrict,
	})

	result, err := validator.ValidateFinalLayout(
		context.Background(), 44, "library/movie.mkv", pr5DurableImportMetadata(1, 100),
		FinalLayoutProvenance{Kind: FinalLayoutProvenanceStandalone},
	)
	require.NoError(t, err)
	assert.True(t, result.ResumeRequired)
	assert.Empty(t, repo.chunks)
	assert.False(t, strings.Contains(fmt.Sprintf("%#v", repo), "fixture-article-000"),
		"unsafe transport text must not cross the durable boundary")
}

func TestPR5DurableFinalLayoutValidatorDoesNotPersistMismatchedNNTPClassification(t *testing.T) {
	clock := &pr5DurableImportClock{now: time.Date(2026, 7, 14, 15, 30, 0, 0, time.UTC)}
	repo := &pr5DurableImportRepository{}
	transport := &pr5TargetedSTATTransport{defaultResult: TargetedSTATResult{
		Outcome: nntppool.OutcomeHardArticleAbsence, ResponseCode: 451,
		CompletionDisposition: ImportCheckDispositionAttempted,
	}}
	validator := pr5NewDurableValidator(repo, transport, clock, DurableFinalLayoutValidatorOptions{
		ProviderSpecs: pr5DurableImportProviders()[:1], DamagePolicy: config.ImportDamagePolicyStrict,
	})

	result, err := validator.ValidateFinalLayout(
		context.Background(), 46, "library/movie.mkv", pr5DurableImportMetadata(1, 100),
		FinalLayoutProvenance{Kind: FinalLayoutProvenanceStandalone},
	)
	require.NoError(t, err)
	assert.True(t, result.ResumeRequired)
	assert.Empty(t, repo.chunks, "451 must remain temporary and cannot be stored as hard absence")
}

func TestPR5DurableFinalLayoutValidatorUsesHealthStateRepositoryContract(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "durable-import.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Connection().ExecContext(context.Background(), `
		INSERT INTO import_queue (id, nzb_path, status, priority)
		VALUES (?, ?, 'processing', 1)
	`, 45, "synthetic-final-layout-fixture.nzb")
	require.NoError(t, err)

	repository := database.NewHealthStateRepository(db.Connection(), database.DialectSQLite)
	transport := &pr5TargetedSTATTransport{defaultResult: pr5STATResult(nntppool.OutcomeSuccess)}
	validator, err := NewDurableFinalLayoutValidator(repository, transport, DurableFinalLayoutValidatorOptions{
		ProviderSpecs: pr5DurableImportProviders()[:1], DamagePolicy: config.ImportDamagePolicyStrict,
	})
	require.NoError(t, err)

	result, err := validator.ValidateFinalLayout(
		context.Background(), 45, "library/repository-contract.mkv",
		pr5DurableImportMetadata(2, 100),
		FinalLayoutProvenance{Kind: FinalLayoutProvenanceStandalone},
	)
	require.NoError(t, err)
	assert.Equal(t, ImportAdmissionAccept, result.Status)
	require.NotEmpty(t, result.FileRevisionID)
	require.NotEmpty(t, result.RunID)

	validation, err := repository.GetImportValidation(context.Background(), 45, result.FileRevisionID)
	require.NoError(t, err)
	require.NotNil(t, validation)
	assert.Equal(t, database.ImportValidationPhaseAccepted, validation.Phase)
	assert.True(t, validation.InitialPassComplete)
	assert.False(t, validation.SecondPassComplete)

	resume, err := repository.GetHealthRunResumeState(context.Background(), result.RunID)
	require.NoError(t, err)
	require.NotNil(t, resume)
	require.NotEmpty(t, resume.Chunks)
	for _, chunk := range resume.Chunks {
		assert.Equal(t, database.HealthRunStageImportInitialSTAT, chunk.Stage)
		assert.Equal(t, int64(1), chunk.ProviderActivationEpoch)
	}
}

func TestPR5DurableFinalLayoutValidatorCompletesRepositoryBackedSecondPass(t *testing.T) {
	db, err := database.NewDB(database.Config{
		Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "durable-confirmation.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Connection().ExecContext(context.Background(), `
		INSERT INTO import_queue (id, nzb_path, status, priority)
		VALUES (?, ?, 'processing', 1)
	`, 47, "synthetic-confirmation-fixture.nzb")
	require.NoError(t, err)

	repository := database.NewHealthStateRepository(db.Connection(), database.DialectSQLite)
	options := DurableFinalLayoutValidatorOptions{
		ProviderSpecs: pr5DurableImportProviders()[:1],
		DamagePolicy:  config.ImportDamagePolicyStrict, ConfirmationDelay: time.Hour,
	}
	initialTransport := &pr5TargetedSTATTransport{
		defaultResult: pr5STATResult(nntppool.OutcomeHardArticleAbsence),
	}
	validator, err := NewDurableFinalLayoutValidator(repository, initialTransport, options)
	require.NoError(t, err)
	meta := pr5DurableImportMetadata(1, 100)
	first, err := validator.ValidateFinalLayout(
		context.Background(), 47, "library/repository-confirmation.mkv", meta,
		FinalLayoutProvenance{Kind: FinalLayoutProvenanceStandalone},
	)
	require.NoError(t, err)
	assert.Equal(t, ImportAdmissionAwaitConfirmation, first.Status)

	// Move only the durable test clock boundary; no wall-clock sleep or live
	// transport is needed to prove the restart transition.
	_, err = db.Connection().ExecContext(context.Background(), `
		UPDATE health_import_validations SET confirmation_due_at = ?
		WHERE queue_item_id = ? AND file_revision_id = ?
	`, time.Now().UTC().Add(-time.Minute), 47, first.FileRevisionID)
	require.NoError(t, err)

	confirmationTransport := &pr5TargetedSTATTransport{
		defaultResult: pr5STATResult(nntppool.OutcomeHardArticleAbsence),
	}
	restarted, err := NewDurableFinalLayoutValidator(repository, confirmationTransport, options)
	require.NoError(t, err)
	final, err := restarted.ValidateFinalLayout(
		context.Background(), 47, "library/repository-confirmation.mkv", meta,
		FinalLayoutProvenance{Kind: FinalLayoutProvenanceStandalone},
	)
	require.NoError(t, err)
	assert.Equal(t, ImportAdmissionReject, final.Status)
	assert.Equal(t, []int{0}, final.UnresolvedPositions)

	validation, err := repository.GetImportValidation(context.Background(), 47, first.FileRevisionID)
	require.NoError(t, err)
	require.NotNil(t, validation)
	assert.True(t, validation.InitialPassComplete)
	assert.True(t, validation.SecondPassComplete)
	assert.Equal(t, database.ImportValidationPhaseRejected, validation.Phase)
}
