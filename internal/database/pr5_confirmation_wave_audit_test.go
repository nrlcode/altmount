package database

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pr5ConfirmationWaveFixture struct {
	pr4Fixture
	snapshot  *ProviderSnapshot
	providers []string
}

func newPR5ConfirmationWaveFixture(t *testing.T) pr5ConfirmationWaveFixture {
	t.Helper()
	f := newPR4RunFixture(t)
	ctx := context.Background()
	providers, err := f.repo.ReconcileProviders(ctx, []ProviderSpec{
		{
			StableID: f.providerID, DisplayName: "A", Endpoint: "provider-a.invalid",
			Port: 119, Account: "a", Role: ProviderRolePrimary, Order: 0,
		},
		{
			StableID: "provider-b", DisplayName: "B", Endpoint: "provider-b.invalid",
			Port: 119, Account: "b", Role: ProviderRolePrimary, Order: 1,
		},
	})
	require.NoError(t, err)
	require.Len(t, providers, 2)
	snapshot, err := f.repo.CaptureActiveProviderSnapshot(ctx, f.now)
	require.NoError(t, err)
	require.Len(t, snapshot.Entries, 2)
	return pr5ConfirmationWaveFixture{
		pr4Fixture: f,
		snapshot:   snapshot,
		providers:  []string{f.providerID, providers[1].ID},
	}
}

func (f pr5ConfirmationWaveFixture) commitWaveRun(
	t *testing.T,
	runID string,
	providerIDs []string,
	positions []int64,
	at time.Time,
) {
	t.Helper()
	run, lease := f.createWaveRun(t, runID, at)
	f.commitWaveStage(t, run, lease, "confirmation_stat", providerIDs, positions, at)
}

func (f pr5ConfirmationWaveFixture) createWaveRun(
	t *testing.T,
	runID string,
	at time.Time,
) (*HealthRun, *HealthRun) {
	t.Helper()
	ctx := context.Background()
	f.clock.now = at
	run, err := f.repo.CreateHealthRun(ctx, HealthRunSpec{
		ID: runID, FileRevisionID: f.run.FileRevisionID,
		ProviderSnapshotID: f.snapshot.ID, Trigger: "confirmation",
		Mode: "observation", TotalSegments: f.run.TotalSegments, CreatedAt: at,
	})
	require.NoError(t, err)
	lease, err := f.repo.AcquireRunLease(ctx, run.ID, runID+"-worker", time.Hour)
	require.NoError(t, err)
	return run, lease
}

func (f pr5ConfirmationWaveFixture) commitWaveStage(
	t *testing.T,
	run, lease *HealthRun,
	stage string,
	providerIDs []string,
	positions []int64,
	at time.Time,
) {
	t.Helper()
	ctx := context.Background()
	f.clock.now = at
	for _, providerID := range providerIDs {
		for _, position := range positions {
			chunkID := fmt.Sprintf("%s-%s-%s-position-%d", run.ID, stage, providerID, position)
			_, err := f.repo.CommitHealthChunk(ctx, HealthChunkCommit{
				ChunkID: chunkID, RunID: run.ID, LeaseOwner: *lease.LeaseOwner,
				FencingToken: lease.FencingToken, ProviderID: providerID,
				ProviderGeneration: 1, ProviderActivationEpoch: 1,
				Stage: stage, ObservationKind: HealthObservationSTAT,
				SegmentStart: position, SegmentCount: 1,
				TestedBitmap: []byte{1}, PresentBitmap: []byte{0}, AbsentBitmap: []byte{1},
				CorruptBitmap: []byte{0}, TemporaryBitmap: []byte{0}, InconclusiveBitmap: []byte{0},
				ResolvedBitmap: []byte{1}, CursorSegment: position + 1,
				ResolvedDelta: 1, ProviderChecksDelta: 1, MissingCandidatesDelta: 1,
				CommittedAt: at,
				Confirmations: []HealthConfirmationEvent{{
					IdempotencyKey: chunkID + ":confirmation", SegmentIndex: position,
					Cause: GapCauseAbsent, ObservedAt: at,
				}},
			})
			require.NoError(t, err)
		}
	}
}

func (f pr5ConfirmationWaveFixture) confirmedGapWrite(id string, position, count int64) GapRangeWrite {
	causes := make([]GapProviderCause, 0, len(f.providers))
	for _, providerID := range f.providers {
		causes = append(causes, GapProviderCause{
			ProviderID: providerID, ProviderGeneration: 1,
			ProviderActivationEpoch: 1, Cause: GapCauseAbsent,
		})
	}
	return GapRangeWrite{
		ID: id, FileRevisionID: f.run.FileRevisionID, Kind: GapKindConfirmedAbsent,
		StartSegment: position, SegmentCount: count, Status: GapStatusActive,
		CreatedAt: f.now, Causes: causes,
	}
}

func TestPR5ConfirmedGapRequiresTwoCoherentAllProviderRunWaves(t *testing.T) {
	f := newPR5ConfirmationWaveFixture(t)
	ctx := context.Background()
	position := int64(1)
	firstAt := f.now.Add(time.Minute)

	f.commitWaveRun(t, "complete-wave-one", f.providers, []int64{position}, firstAt)
	f.commitWaveRun(t, "partial-wave-a", f.providers[:1], []int64{position}, firstAt.Add(10*time.Minute))
	f.commitWaveRun(t, "partial-wave-b", f.providers[1:], []int64{position}, firstAt.Add(20*time.Minute))

	write := f.confirmedGapWrite("coherent-wave-gap", position, 1)
	_, err := f.repo.UpsertGapRange(ctx, write)
	require.Error(t, err,
		"provider-local evidence pairs from different runs must not manufacture a second all-provider wave")

	secondAt := firstAt.Add(20 * time.Minute)
	f.commitWaveRun(t, "complete-wave-two", f.providers, []int64{position}, secondAt)
	gap, err := f.repo.UpsertGapRange(ctx, write)
	require.NoError(t, err)
	require.Len(t, gap.Causes, 2)
	for _, cause := range gap.Causes {
		assert.Equal(t, 2, cause.ConfirmationCount)
		assert.Equal(t, secondAt, cause.ConfirmedAt)
	}
}

func TestPR5OneRunCanSupplyTimeSeparatedStageWaves(t *testing.T) {
	f := newPR5ConfirmationWaveFixture(t)
	ctx := context.Background()
	position := int64(2)
	firstAt := f.now.Add(time.Minute)

	run, lease := f.createWaveRun(t, "resumable-confirmation-run", firstAt)
	f.commitWaveStage(t, run, lease, "observe_initial", f.providers, []int64{position}, firstAt)
	secondAt := firstAt.Add(10 * time.Minute)
	f.commitWaveStage(t, run, lease, "observe_confirmation_2", f.providers, []int64{position}, secondAt)

	gap, err := f.repo.UpsertGapRange(ctx, f.confirmedGapWrite("resumable-run-gap", position, 1))
	require.NoError(t, err, "a resumable run may contain distinct durable confirmation stages")
	require.Len(t, gap.Causes, 2)
	for _, cause := range gap.Causes {
		assert.Equal(t, 2, cause.ConfirmationCount)
		assert.Equal(t, secondAt, cause.ConfirmedAt)
	}
}

func TestPR5GapConfirmationDelayDefaultsToTenMinutes(t *testing.T) {
	f := newPR5ConfirmationWaveFixture(t)
	ctx := context.Background()
	position := int64(5)
	firstAt := f.now.Add(time.Minute)

	run, lease := f.createWaveRun(t, "default-delay-run", firstAt)
	f.commitWaveStage(t, run, lease, "default-delay-first", f.providers, []int64{position}, firstAt)
	f.commitWaveStage(
		t, run, lease, "default-delay-too-soon", f.providers, []int64{position},
		firstAt.Add(10*time.Minute-time.Nanosecond),
	)

	write := f.confirmedGapWrite("default-delay-gap", position, 1)
	_, err := f.repo.UpsertGapRange(ctx, write)
	require.Error(t, err, "the default must not accept a wave even one nanosecond before ten minutes")

	secondAt := firstAt.Add(10 * time.Minute)
	f.commitWaveStage(t, run, lease, "default-delay-boundary", f.providers, []int64{position}, secondAt)
	gap, err := f.repo.UpsertGapRange(ctx, write)
	require.NoError(t, err, "the default must accept a distinct coherent wave at exactly ten minutes")
	for _, cause := range gap.Causes {
		assert.Equal(t, 2, cause.ConfirmationCount)
		assert.Equal(t, secondAt, cause.ConfirmedAt)
	}
}

func TestPR5GapConfirmationDelayOverrideIsValidatedAndRepositoryLocal(t *testing.T) {
	f := newPR5ConfirmationWaveFixture(t)
	ctx := context.Background()
	position := int64(6)
	firstAt := f.now.Add(time.Minute)
	secondAt := firstAt.Add(2 * time.Minute)

	run, lease := f.createWaveRun(t, "override-delay-run", firstAt)
	f.commitWaveStage(t, run, lease, "override-delay-first", f.providers, []int64{position}, firstAt)
	f.commitWaveStage(t, run, lease, "override-delay-second", f.providers, []int64{position}, secondAt)

	defaultRepo := NewHealthStateRepository(f.db.Connection(), DialectSQLite)
	_, err := defaultRepo.UpsertGapRange(
		ctx, f.confirmedGapWrite("default-repository-gap", position, 1),
	)
	require.Error(t, err, "an override on another repository must not alter the default")

	require.Error(t, f.repo.SetGapConfirmationMinimumDelay(0))
	require.Error(t, f.repo.SetGapConfirmationMinimumDelay(-time.Second))
	_, err = f.repo.UpsertGapRange(
		ctx, f.confirmedGapWrite("invalid-override-gap", position, 1),
	)
	require.Error(t, err, "an invalid override must leave the ten-minute default intact")

	require.NoError(t, f.repo.SetGapConfirmationMinimumDelay(2*time.Minute))
	gap, err := f.repo.UpsertGapRange(
		ctx, f.confirmedGapWrite("override-repository-gap", position, 1),
	)
	require.NoError(t, err)
	for _, cause := range gap.Causes {
		assert.Equal(t, 2, cause.ConfirmationCount)
		assert.Equal(t, secondAt, cause.ConfirmedAt)
	}

	_, err = defaultRepo.UpsertGapRange(
		ctx, f.confirmedGapWrite("still-default-repository-gap", position, 1),
	)
	require.Error(t, err, "the configured minimum must remain local to its repository")
}

func TestPR5OneRunStageCannotCombineAsynchronousProviderPairs(t *testing.T) {
	f := newPR5ConfirmationWaveFixture(t)
	ctx := context.Background()
	position := int64(2)
	firstAt := f.now.Add(time.Minute)
	run, lease := f.createWaveRun(t, "same-stage-asynchronous-run", firstAt)
	f.commitWaveStage(t, run, lease, "observe_initial", f.providers[:1], []int64{position}, firstAt)
	f.commitWaveStage(t, run, lease, "observe_initial", f.providers[1:], []int64{position}, firstAt.Add(10*time.Minute))

	_, err := f.repo.UpsertGapRange(ctx, f.confirmedGapWrite("same-stage-asynchronous-gap", position, 1))
	require.Error(t, err, "one run/stage identity is exactly one coherent confirmation wave")
}

func TestPR5ConfirmationWaveRequiresTheWholeRangeInOneRun(t *testing.T) {
	f := newPR5ConfirmationWaveFixture(t)
	ctx := context.Background()
	positions := []int64{3, 4}
	firstAt := f.now.Add(time.Minute)

	f.commitWaveRun(t, "whole-range-wave-one", f.providers, positions, firstAt)
	f.commitWaveRun(t, "partial-range-first", f.providers, positions[:1], firstAt.Add(10*time.Minute))
	f.commitWaveRun(t, "partial-range-second", f.providers, positions[1:], firstAt.Add(20*time.Minute))

	write := f.confirmedGapWrite("whole-range-gap", positions[0], int64(len(positions)))
	_, err := f.repo.UpsertGapRange(ctx, write)
	require.Error(t, err,
		"position-local evidence pairs from different runs must not manufacture a second whole-range wave")

	secondAt := firstAt.Add(20 * time.Minute)
	f.commitWaveRun(t, "whole-range-wave-two", f.providers, positions, secondAt)
	gap, err := f.repo.UpsertGapRange(ctx, write)
	require.NoError(t, err)
	require.Len(t, gap.Causes, 2)
	for _, cause := range gap.Causes {
		assert.Equal(t, 2, cause.ConfirmationCount)
		assert.Equal(t, secondAt, cause.ConfirmedAt)
	}
}
