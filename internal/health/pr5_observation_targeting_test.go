package health

import (
	"context"
	"testing"
	"time"

	"github.com/javi11/altmount/internal/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPR5ObservationWorkerHonorsExactProviderAndGapTarget(t *testing.T) {
	clock := &pr5FakeClock{now: time.Unix(1_801_100_000, 0).UTC()}
	providers := pr5ObservationProviders()
	repo := newPR5ObservationWorkerRepository(clock, 6, providers...)
	target := providers[1]
	repo.schedule.TargetProviderID = target.ProviderID
	repo.schedule.TargetProviderGeneration = target.ProviderGeneration
	repo.schedule.TargetProviderActivationEpoch = target.ProviderActivationEpoch
	repo.schedule.TargetGapID = "target-gap"
	repo.gap = &database.HealthGapRange{
		ID: "target-gap", FileRevisionID: repo.revision.ID,
		Kind: database.GapKindConfirmedAbsent, Status: database.GapStatusActive,
		StartSegment: 3, SegmentCount: 2, CreatedAt: clock.Now(),
	}
	transport := &pr5ScriptedObservationTransport{fn: func(
		_ context.Context,
		request observationTransportRequest,
	) ([]observationTransportResult, error) {
		results := make([]observationTransportResult, len(request.Targets))
		for i, item := range request.Targets {
			results[i] = observationTransportResult{
				MessageID: item.MessageID, Outcome: observationOutcomeHardAbsent,
			}
		}
		return results, nil
	}}
	worker := newPR5ObservationWorkerForTest(repo, clock, transport, 8, nil)

	_, err := worker.ProcessNext(context.Background())
	require.NoError(t, err)
	calls := transport.snapshotCalls()
	require.Len(t, calls, 1)
	assert.Equal(t, target.ProviderID, calls[0].Provider.ID)
	assert.Equal(t, []int64{3, 4}, observationTargetPositions(calls[0].Targets))
	assert.Equal(t, database.HealthObservationSTAT, calls[0].ObservationKind)
}

func TestPR5ObservationWorkerRejectsMissingScheduleTarget(t *testing.T) {
	clock := &pr5FakeClock{now: time.Unix(1_801_100_100, 0).UTC()}
	repo := newPR5ObservationWorkerRepository(clock, 2, pr5ObservationProviders()[0])
	repo.schedule.TargetGapID = "missing-gap"
	transport := &pr5ScriptedObservationTransport{fn: func(
		context.Context,
		observationTransportRequest,
	) ([]observationTransportResult, error) {
		t.Fatal("stale target must fail before transport dispatch")
		return nil, nil
	}}
	worker := newPR5ObservationWorkerForTest(repo, clock, transport, 2, nil)

	step, err := worker.ProcessNext(context.Background())
	require.Error(t, err)
	assert.Equal(t, observationWorkerFailed, step)
	assert.Empty(t, transport.snapshotCalls())
}

func TestPR5HealthPendingWorkerChecksOnlyDurableUnresolvedPositions(t *testing.T) {
	clock := &pr5FakeClock{now: time.Unix(1_801_100_200, 0).UTC()}
	repo := newPR5ObservationWorkerRepository(clock, 5, pr5ObservationProviders()...)
	repo.run.Trigger = "health_pending"
	repo.state.Run.Trigger = "health_pending"
	repo.coverage = &database.CompletedImportSTATCoverage{
		HealthPending: true, UnresolvedPositions: []int64{1, 4},
	}
	transport := &pr5ScriptedObservationTransport{fn: func(
		_ context.Context,
		request observationTransportRequest,
	) ([]observationTransportResult, error) {
		results := make([]observationTransportResult, len(request.Targets))
		for i, item := range request.Targets {
			results[i] = observationTransportResult{MessageID: item.MessageID, Outcome: observationOutcomePresent}
		}
		return results, nil
	}}
	worker := newPR5ObservationWorkerForTest(repo, clock, transport, 8, nil)

	_, err := worker.ProcessNext(context.Background())
	require.NoError(t, err)
	calls := transport.snapshotCalls()
	require.Len(t, calls, 1)
	assert.Equal(t, []int64{1, 4}, observationTargetPositions(calls[0].Targets))
}

func TestPR5CorruptGapRevalidationUsesFreshTargetedBODY(t *testing.T) {
	clock := &pr5FakeClock{now: time.Unix(1_801_100_300, 0).UTC()}
	provider := pr5ObservationProviders()[0]
	repo := newPR5ObservationWorkerRepository(clock, 3, provider)
	repo.run.Trigger = "gap_revalidation_0"
	repo.state.Run.Trigger = repo.run.Trigger
	repo.schedule.TargetGapID = "corrupt-gap"
	repo.gap = &database.HealthGapRange{
		ID: "corrupt-gap", FileRevisionID: repo.revision.ID,
		Kind: database.GapKindConfirmedUnusable, Status: database.GapStatusActive,
		StartSegment: 2, SegmentCount: 1, CreatedAt: clock.Now(),
		Causes: []database.GapProviderCause{{
			ProviderID: provider.ProviderID, ProviderGeneration: provider.ProviderGeneration,
			ProviderActivationEpoch: provider.ProviderActivationEpoch,
			Cause:                   database.GapCauseCorrupt, ConfirmationCount: 2,
		}},
	}
	transport := &pr5ScriptedObservationTransport{fn: func(
		_ context.Context,
		request observationTransportRequest,
	) ([]observationTransportResult, error) {
		return []observationTransportResult{{
			MessageID: request.Targets[0].MessageID, Outcome: observationOutcomeCorrupt,
		}}, nil
	}}
	worker := newPR5ObservationWorkerForTest(repo, clock, transport, 2, nil)

	_, err := worker.ProcessNext(context.Background())
	require.NoError(t, err)
	calls := transport.snapshotCalls()
	require.Len(t, calls, 1)
	assert.Equal(t, database.HealthObservationValidatedBody, calls[0].ObservationKind)
	assert.True(t, calls[0].FreshTransport)
	assert.Equal(t, []int64{2}, observationTargetPositions(calls[0].Targets))
}

func TestPR5GapSTATPresenceTransitionsToFreshBODYAndClearsImmediately(t *testing.T) {
	clock := &pr5FakeClock{now: time.Unix(1_801_100_400, 0).UTC()}
	provider := pr5ObservationProviders()[0]
	repo := newPR5ObservationWorkerRepository(clock, 2, provider)
	repo.run.Trigger = "gap_revalidation_0"
	repo.state.Run.Trigger = repo.run.Trigger
	repo.schedule.TargetGapID = "absent-gap"
	repo.gap = &database.HealthGapRange{
		ID: "absent-gap", FileRevisionID: repo.revision.ID,
		Kind: database.GapKindConfirmedAbsent, Status: database.GapStatusActive,
		StartSegment: 1, SegmentCount: 1, CreatedAt: clock.Now(),
		Causes: []database.GapProviderCause{{
			ProviderID: provider.ProviderID, ProviderGeneration: provider.ProviderGeneration,
			ProviderActivationEpoch: provider.ProviderActivationEpoch,
			Cause:                   database.GapCauseAbsent, ConfirmationCount: 2,
		}},
	}
	transport := &pr5ScriptedObservationTransport{fn: func(
		_ context.Context,
		request observationTransportRequest,
	) ([]observationTransportResult, error) {
		return []observationTransportResult{{
			MessageID: request.Targets[0].MessageID, Outcome: observationOutcomePresent,
		}}, nil
	}}
	worker := newPR5ObservationWorkerForTest(repo, clock, transport, 1, nil)

	_, err := worker.ProcessNext(context.Background())
	require.NoError(t, err)
	_, err = worker.ProcessNext(context.Background())
	require.NoError(t, err)
	calls := transport.snapshotCalls()
	require.Len(t, calls, 2)
	assert.Equal(t, database.HealthObservationSTAT, calls[0].ObservationKind)
	assert.Equal(t, database.HealthObservationValidatedBody, calls[1].ObservationKind)
	assert.True(t, calls[1].FreshTransport)
	assert.Equal(t, "absent-gap", repo.clearedGapID)
	assert.Equal(t, database.HealthRunCompleted, repo.run.Status)
}
