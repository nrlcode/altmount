package health

import (
	"testing"

	"github.com/javi11/altmount/internal/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPR5DeterministicChunksAreBoundedAndPerFile(t *testing.T) {
	first := deterministicObservationChunks("revision-a", 10, 4)
	second := deterministicObservationChunks("revision-b", 3, 4)

	require.Equal(t, []observationChunkRange{
		{FileRevisionID: "revision-a", SegmentStart: 0, SegmentCount: 4},
		{FileRevisionID: "revision-a", SegmentStart: 4, SegmentCount: 4},
		{FileRevisionID: "revision-a", SegmentStart: 8, SegmentCount: 2},
	}, first)
	require.Equal(t, []observationChunkRange{
		{FileRevisionID: "revision-b", SegmentStart: 0, SegmentCount: 3},
	}, second)

	for _, chunk := range append(first, second...) {
		assert.LessOrEqual(t, chunk.SegmentCount, int64(4))
		assert.NotEmpty(t, chunk.FileRevisionID)
	}
}

func TestPR5NextObservationChunkReturnsExactlyOneBoundedWindow(t *testing.T) {
	targets := make([]observationSegmentTarget, 9)
	for i := range targets {
		targets[i] = observationSegmentTarget{
			Position:  int64(i),
			MessageID: "synthetic-segment-" + string(rune('a'+i)),
		}
	}
	provider := observationDispatchProvider{
		ID: "provider-a", Generation: 1, Role: database.ProviderRolePrimary, Order: 0,
	}
	evidence := observationEvidence{}
	input := observationPlanInput{
		FileRevisionID: "revision-one-chunk",
		Targets:        targets,
		Providers:      []observationDispatchProvider{provider},
		Evidence:       evidence,
	}

	first, ok := nextObservationChunk(input, 4)
	require.True(t, ok)
	assert.Equal(t, int64(0), first.SegmentStart)
	assert.Equal(t, int64(4), first.SegmentCount)
	assert.Equal(t, []int64{0, 1, 2, 3}, observationTargetPositions(first.Targets))
	for _, target := range first.Targets {
		evidence.record(first.providerKey(), target.Position, observationOutcomePresent)
	}

	second, ok := nextObservationChunk(input, 4)
	require.True(t, ok)
	assert.Equal(t, int64(4), second.SegmentStart)
	assert.Equal(t, int64(4), second.SegmentCount)
	assert.Equal(t, []int64{4, 5, 6, 7}, observationTargetPositions(second.Targets))
}

func TestPR5PlannerKeepsDuplicateMessageIDsAsDistinctPositions(t *testing.T) {
	targets := []observationSegmentTarget{
		{Position: 0, MessageID: "synthetic-duplicate"},
		{Position: 1, MessageID: "synthetic-duplicate"},
		{Position: 2, MessageID: "synthetic-distinct"},
	}
	providers := []observationDispatchProvider{
		{ID: "provider-a", Generation: 1, Role: database.ProviderRolePrimary, Order: 0},
	}

	chunk, ok := nextObservationChunk(observationPlanInput{
		FileRevisionID: "revision-duplicates",
		Targets:        targets,
		Providers:      providers,
	}, 8)
	require.True(t, ok)
	require.Len(t, chunk.Targets, 3)
	assert.Equal(t, int64(0), chunk.Targets[0].Position)
	assert.Equal(t, int64(1), chunk.Targets[1].Position)
	assert.Equal(t, "synthetic-duplicate", chunk.Targets[0].MessageID)
	assert.Equal(t, "synthetic-duplicate", chunk.Targets[1].MessageID)
}

func TestPR5TransportResultsFanOneUniqueProofToEveryCanonicalOwner(t *testing.T) {
	targets := []observationSegmentTarget{
		{Position: 4, MessageID: "synthetic-duplicate"},
		{Position: 5, MessageID: "synthetic-distinct"},
		{Position: 6, MessageID: "synthetic-duplicate"},
	}
	request := observationTransportRequest{
		ObservationKind: database.HealthObservationSTAT,
		Targets:         targets,
	}
	resultMap := func(t *testing.T, results []observationTransportResult) map[int64]normalizedObservationResult {
		t.Helper()
		normalized, canceled := normalizeObservationBatch(request, results)
		require.False(t, canceled)
		require.Len(t, normalized, len(targets))
		byPosition := make(map[int64]normalizedObservationResult, len(normalized))
		for _, result := range normalized {
			byPosition[result.target.Position] = result
		}
		return byPosition
	}

	t.Run("out of order unique results fan out", func(t *testing.T) {
		byPosition := resultMap(t, []observationTransportResult{
			{MessageID: "synthetic-distinct", Outcome: observationOutcomeHardAbsent},
			{MessageID: "synthetic-duplicate", Outcome: observationOutcomePresent,
				Attempts: []observationTransportAttempt{{Operation: "STAT", Outcome: observationOutcomePresent}}},
		})
		assert.Equal(t, observationOutcomePresent, byPosition[4].outcome)
		assert.Equal(t, observationOutcomeHardAbsent, byPosition[5].outcome)
		assert.Equal(t, observationOutcomePresent, byPosition[6].outcome)
		require.Len(t, byPosition[4].attempts, 1)
		require.Len(t, byPosition[6].attempts, 1)
		assert.Equal(t, byPosition[4].attempts, byPosition[6].attempts,
			"the same validated wire proof and attempts belong to every canonical owner")
	})

	t.Run("missing result leaves every owner incomplete", func(t *testing.T) {
		byPosition := resultMap(t, []observationTransportResult{{
			MessageID: "synthetic-distinct", Outcome: observationOutcomeHardAbsent,
		}})
		assert.Equal(t, observationOutcomeInconclusive, byPosition[4].outcome)
		assert.Equal(t, observationOutcomeHardAbsent, byPosition[5].outcome)
		assert.Equal(t, observationOutcomeInconclusive, byPosition[6].outcome)
	})

	t.Run("duplicate result leaves only its owners incomplete", func(t *testing.T) {
		byPosition := resultMap(t, []observationTransportResult{
			{MessageID: "synthetic-duplicate", Outcome: observationOutcomePresent},
			{MessageID: "synthetic-distinct", Outcome: observationOutcomeHardAbsent},
			{MessageID: "synthetic-duplicate", Outcome: observationOutcomePresent},
		})
		assert.Equal(t, observationOutcomeInconclusive, byPosition[4].outcome)
		assert.Equal(t, observationOutcomeHardAbsent, byPosition[5].outcome)
		assert.Equal(t, observationOutcomeInconclusive, byPosition[6].outcome)
	})

	t.Run("unexpected result invalidates the whole mixed batch", func(t *testing.T) {
		byPosition := resultMap(t, []observationTransportResult{
			{MessageID: "synthetic-duplicate", Outcome: observationOutcomePresent},
			{MessageID: "synthetic-distinct", Outcome: observationOutcomeHardAbsent},
			{MessageID: "synthetic-unexpected", Outcome: observationOutcomePresent},
		})
		for _, position := range []int64{4, 5, 6} {
			assert.Equal(t, observationOutcomeInconclusive, byPosition[position].outcome)
		}
	})
}

func TestPR5PlannerUsesOrderedPrimaryThenSparseFallback(t *testing.T) {
	targets := []observationSegmentTarget{
		{Position: 0, MessageID: "synthetic-0"},
		{Position: 1, MessageID: "synthetic-1"},
		{Position: 2, MessageID: "synthetic-2"},
	}
	providers := []observationDispatchProvider{
		// Backup has the numerically lowest order on purpose: role still puts it
		// after every configured primary.
		{ID: "provider-backup", Generation: 1, Role: database.ProviderRoleBackup, Order: 0},
		{ID: "provider-primary-b", Generation: 2, Role: database.ProviderRolePrimary, Order: 2},
		{ID: "provider-primary-a", Generation: 4, Role: database.ProviderRolePrimary, Order: 1},
	}
	evidence := observationEvidence{}
	input := observationPlanInput{
		FileRevisionID: "revision-fallback",
		Targets:        targets,
		Providers:      providers,
		Evidence:       evidence,
	}

	first, ok := nextObservationChunk(input, 8)
	require.True(t, ok)
	assert.Equal(t, "provider-primary-a", first.ProviderID)
	assert.Equal(t, int64(4), first.ProviderGeneration)
	assert.Equal(t, []int64{0, 1, 2}, observationTargetPositions(first.Targets))

	evidence.record(first.providerKey(), 0, observationOutcomePresent)
	evidence.record(first.providerKey(), 1, observationOutcomeHardAbsent)
	evidence.record(first.providerKey(), 2, observationOutcomeTemporary)

	second, ok := nextObservationChunk(input, 8)
	require.True(t, ok)
	assert.Equal(t, "provider-primary-b", second.ProviderID)
	assert.Equal(t, []int64{1, 2}, observationTargetPositions(second.Targets),
		"a success from any earlier provider must remove that position from fallback")

	evidence.record(second.providerKey(), 1, observationOutcomePresent)
	evidence.record(second.providerKey(), 2, observationOutcomeHardAbsent)

	third, ok := nextObservationChunk(input, 8)
	require.True(t, ok)
	assert.Equal(t, "provider-backup", third.ProviderID)
	assert.Equal(t, []int64{2}, observationTargetPositions(third.Targets),
		"failure-only backups must receive only positions still unresolved after all primaries")

	evidence.record(third.providerKey(), 2, observationOutcomePresent)
	_, ok = nextObservationChunk(input, 8)
	assert.False(t, ok, "any-provider success must finish every position without redundant checks")
}

func TestPR5PlannerQueuesOnlyUnresolvedPositionsForProviderChanges(t *testing.T) {
	targets := []observationSegmentTarget{
		{Position: 0, MessageID: "synthetic-0"},
		{Position: 1, MessageID: "synthetic-1"},
		{Position: 2, MessageID: "synthetic-2"},
	}
	primary := observationDispatchProvider{
		ID: "provider-primary", Generation: 1, Role: database.ProviderRolePrimary, Order: 0,
	}
	removed := observationDispatchProvider{
		ID: "provider-removed", Generation: 1, Role: database.ProviderRolePrimary, Order: 1,
	}
	evidence := observationEvidence{}
	for _, position := range []int64{0, 1} {
		evidence.record(primary.key(), position, observationOutcomePresent)
	}
	evidence.record(primary.key(), 2, observationOutcomeHardAbsent)
	for _, position := range []int64{0, 1} {
		evidence.record(removed.key(), position, observationOutcomePresent)
	}
	evidence.record(removed.key(), 2, observationOutcomeHardAbsent)

	added := observationDispatchProvider{
		ID: "provider-added", Generation: 1, Role: database.ProviderRolePrimary, Order: 1,
	}
	chunk, ok := nextObservationChunk(observationPlanInput{
		FileRevisionID: "revision-provider-change",
		Targets:        targets,
		Providers:      []observationDispatchProvider{primary, added},
		Evidence:       evidence,
	}, 8)
	require.True(t, ok)
	assert.Equal(t, "provider-added", chunk.ProviderID)
	assert.Equal(t, []int64{2}, observationTargetPositions(chunk.Targets),
		"adding a provider must not rescan positions already known available")

	// Endpoint/account changes create a new generation. Historical generation
	// evidence remains retained, while only unresolved/known-gap positions are
	// checked against the new generation.
	changedGeneration := added
	changedGeneration.Generation = 2
	chunk, ok = nextObservationChunk(observationPlanInput{
		FileRevisionID: "revision-provider-change",
		Targets:        targets,
		Providers:      []observationDispatchProvider{primary, changedGeneration},
		Evidence:       evidence,
	}, 8)
	require.True(t, ok)
	assert.Equal(t, int64(2), chunk.ProviderGeneration)
	assert.Equal(t, []int64{2}, observationTargetPositions(chunk.Targets))

	// A same-generation reactivation is an explicit freshness boundary, but it
	// likewise targets unresolved/known-gap positions rather than the whole file.
	reactivated := removed
	chunk, ok = nextObservationChunk(observationPlanInput{
		FileRevisionID:      "revision-provider-change",
		Targets:             targets,
		Providers:           []observationDispatchProvider{primary, reactivated},
		Evidence:            evidence,
		RefreshProviderKeys: map[observationProviderKey]bool{reactivated.key(): true},
	}, 8)
	require.True(t, ok)
	assert.Equal(t, "provider-removed", chunk.ProviderID)
	assert.Equal(t, []int64{2}, observationTargetPositions(chunk.Targets))
}
