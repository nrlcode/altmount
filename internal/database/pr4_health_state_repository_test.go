package database

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newPR4Repository(t *testing.T) (*DB, *HealthStateRepository) {
	t.Helper()
	db, err := NewDB(Config{Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "health-state.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db, NewHealthStateRepository(db.Connection(), DialectSQLite)
}

func TestPR4FileRevisionIdentityIsStructuralAndReusable(t *testing.T) {
	_, repo := newPR4Repository(t)
	ctx := context.Background()
	first, err := repo.EnsureFileRevision(ctx, FileRevisionSpec{
		FilePath: "library/movie.mkv", LayoutFingerprint: "sha256:layout-a", VirtualSize: 1000, SegmentCount: 10,
	})
	require.NoError(t, err)
	require.True(t, first.Active)
	require.NotEmpty(t, first.ID)

	same, err := repo.EnsureFileRevision(ctx, FileRevisionSpec{
		FilePath: "/library/movie.mkv", LayoutFingerprint: "sha256:layout-a", VirtualSize: 1000, SegmentCount: 10,
	})
	require.NoError(t, err)
	assert.Equal(t, first.ID, same.ID, "same canonical layout must retain revision identity")

	replaced, err := repo.EnsureFileRevision(ctx, FileRevisionSpec{
		FilePath: "library/movie.mkv", LayoutFingerprint: "sha256:layout-b", VirtualSize: 1000, SegmentCount: 10,
	})
	require.NoError(t, err)
	assert.NotEqual(t, first.ID, replaced.ID)

	reactivated, err := repo.EnsureFileRevision(ctx, FileRevisionSpec{
		FilePath: "library/movie.mkv", LayoutFingerprint: "sha256:layout-a", VirtualSize: 1000, SegmentCount: 10,
	})
	require.NoError(t, err)
	assert.Equal(t, first.ID, reactivated.ID, "returning to an exact retained layout must reuse its revision")

	revisions, err := repo.ListFileRevisions(ctx, "library/movie.mkv")
	require.NoError(t, err)
	require.Len(t, revisions, 2)
	for _, revision := range revisions {
		assert.Equal(t, revision.ID == first.ID, revision.Active)
	}
}

func TestPR4ProviderRegistryRetainsIdentityAndGenerations(t *testing.T) {
	_, repo := newPR4Repository(t)
	ctx := context.Background()
	initial, err := repo.ReconcileProviders(ctx, []ProviderSpec{
		{StableID: "legacy-provider-1", DisplayName: "Preferred", Endpoint: "News.Example.Invalid", Port: 563, Account: "account-a", Role: ProviderRolePrimary, Order: 0},
		{DisplayName: "Backup", Endpoint: "backup.example.invalid", Port: 563, Account: "account-b", Role: ProviderRoleBackup, Order: 1},
	})
	require.NoError(t, err)
	require.Len(t, initial, 2)
	assert.Equal(t, "legacy-provider-1", initial[0].ID, "existing stable IDs must be reused directly")
	assert.NoError(t, uuid.Validate(initial[1].ID), "new provider IDs must be UUIDs")
	assert.Equal(t, int64(1), initial[0].CurrentGeneration)

	reordered, err := repo.ReconcileProviders(ctx, []ProviderSpec{
		{StableID: initial[1].ID, DisplayName: "Backup renamed", Endpoint: "backup.example.invalid", Port: 563, Account: "account-b", Role: ProviderRolePrimary, Order: 0},
		{StableID: initial[0].ID, DisplayName: "Preferred renamed", Endpoint: "news.example.invalid", Port: 563, Account: "account-a", Role: ProviderRoleBackup, Order: 1},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), reordered[0].CurrentGeneration)
	assert.Equal(t, int64(1), reordered[1].CurrentGeneration)

	changed, err := repo.ReconcileProviders(ctx, []ProviderSpec{
		{StableID: initial[0].ID, DisplayName: "Preferred renamed", Endpoint: "new-endpoint.example.invalid", Port: 563, Account: "account-a", Role: ProviderRolePrimary, Order: 0},
	})
	require.NoError(t, err)
	require.Len(t, changed, 1)
	assert.Equal(t, int64(2), changed[0].CurrentGeneration)

	generations, err := repo.ListProviderGenerations(ctx, initial[0].ID)
	require.NoError(t, err)
	require.Len(t, generations, 2)
	assert.Equal(t, "news.example.invalid", generations[0].Endpoint)
	assert.Equal(t, "new-endpoint.example.invalid", generations[1].Endpoint)

	_, err = repo.ReconcileProviders(ctx, nil)
	require.NoError(t, err)
	relinked, err := repo.ReconcileProviders(ctx, []ProviderSpec{
		{DisplayName: "Re-added", Endpoint: "new-endpoint.example.invalid", Port: 563, Account: "account-a", Role: ProviderRolePrimary, Order: 0},
	})
	require.NoError(t, err)
	require.Len(t, relinked, 1)
	assert.Equal(t, initial[0].ID, relinked[0].ID, "one unambiguous tombstoned identity must relink")
	assert.Equal(t, int64(2), relinked[0].CurrentGeneration)

	all, err := repo.ListProviders(ctx, true)
	require.NoError(t, err)
	require.Len(t, all, 2)
	assert.True(t, all[0].Active || all[1].Active)
	assert.False(t, all[0].Active && all[1].Active)
}

func TestPR4ProviderSnapshotIsOrderedAndImmutable(t *testing.T) {
	_, repo := newPR4Repository(t)
	ctx := context.Background()
	providers, err := repo.ReconcileProviders(ctx, []ProviderSpec{
		{StableID: "primary", DisplayName: "Primary", Endpoint: "primary.invalid", Port: 119, Account: "a", Role: ProviderRolePrimary, Order: 0},
		{StableID: "backup", DisplayName: "Backup", Endpoint: "backup.invalid", Port: 119, Account: "b", Role: ProviderRoleBackup, Order: 1},
	})
	require.NoError(t, err)
	snapshot, err := repo.CaptureActiveProviderSnapshot(ctx, time.Unix(100, 0).UTC())
	require.NoError(t, err)
	require.Len(t, snapshot.Entries, 2)
	assert.Equal(t, providers[0].ID, snapshot.Entries[0].ProviderID)
	assert.Equal(t, ProviderRolePrimary, snapshot.Entries[0].Role)
	assert.Equal(t, providers[1].ID, snapshot.Entries[1].ProviderID)

	_, err = repo.ReconcileProviders(ctx, []ProviderSpec{
		{StableID: "backup", DisplayName: "Backup", Endpoint: "backup.invalid", Port: 119, Account: "b", Role: ProviderRolePrimary, Order: 0},
	})
	require.NoError(t, err)
	retained, err := repo.GetProviderSnapshot(ctx, snapshot.ID)
	require.NoError(t, err)
	require.Equal(t, snapshot.Entries, retained.Entries, "in-flight dispatch snapshot must not be reattributed")
}

type pr4Fixture struct {
	repo       *HealthStateRepository
	db         *DB
	run        *HealthRun
	providerID string
	now        time.Time
}

func newPR4RunFixture(t *testing.T) pr4Fixture {
	t.Helper()
	db, repo := newPR4Repository(t)
	ctx := context.Background()
	revision, err := repo.EnsureFileRevision(ctx, FileRevisionSpec{
		FilePath: "library/run.mkv", LayoutFingerprint: "sha256:run-layout", VirtualSize: 800, SegmentCount: 8,
	})
	require.NoError(t, err)
	providers, err := repo.ReconcileProviders(ctx, []ProviderSpec{
		{StableID: "provider-a", DisplayName: "A", Endpoint: "provider-a.invalid", Port: 119, Account: "a", Role: ProviderRolePrimary, Order: 0},
	})
	require.NoError(t, err)
	now := time.Unix(1_700_000_000, 0).UTC()
	snapshot, err := repo.CaptureActiveProviderSnapshot(ctx, now)
	require.NoError(t, err)
	run, err := repo.CreateHealthRun(ctx, HealthRunSpec{
		ID: "run-1", FileRevisionID: revision.ID, ProviderSnapshotID: snapshot.ID,
		Trigger: "manual", Mode: "observation", TotalSegments: 8, CreatedAt: now,
	})
	require.NoError(t, err)
	return pr4Fixture{repo: repo, db: db, run: run, providerID: providers[0].ID, now: now}
}

func pr4Commit(f pr4Fixture, chunkID string, token int64, owner string, start int64) HealthChunkCommit {
	return HealthChunkCommit{
		ChunkID: chunkID, RunID: f.run.ID, LeaseOwner: owner, FencingToken: token,
		ProviderID: f.providerID, ProviderGeneration: 1, Stage: "primary_stat",
		SegmentStart: start, SegmentCount: 4,
		TestedBitmap: []byte{0b00001111}, PresentBitmap: []byte{0b00000011},
		AbsentBitmap: []byte{0b00000100}, TemporaryBitmap: []byte{0b00001000},
		CorruptBitmap: []byte{0}, InconclusiveBitmap: []byte{0},
		CursorSegment: start + 4, ResolvedDelta: 3, ProviderChecksDelta: 4,
		MissingCandidatesDelta: 1, InconclusiveDelta: 1,
		CommittedAt: f.now.Add(time.Minute),
		Attempts: []HealthAttemptEvidence{{
			IdempotencyKey: chunkID + ":attempt:2", SegmentIndex: start + 2,
			Operation: "STAT", Outcome: "hard_absence", ResponseCode: new(430),
			BodyValidation: "not_requested", PoolQueue: time.Millisecond,
			PipelineWait: 2 * time.Millisecond, ResponseService: 3 * time.Millisecond,
			ObservedAt: f.now.Add(time.Minute),
		}},
		Confirmations: []HealthConfirmationEvent{{
			IdempotencyKey: chunkID + ":confirm:2", SegmentIndex: start + 2,
			Cause: GapCauseAbsent, ObservedAt: f.now.Add(time.Minute),
		}},
		Retry: &HealthRetryState{
			RetryKey: chunkID + ":temporary", SegmentStart: start + 3, SegmentCount: 1,
			Outcome: "temporary_failure", Attempt: 1, NextAttemptAt: f.now.Add(2 * time.Minute),
		},
	}
}

func TestPR4ChunkCommitIsFencedAtomicAndIdempotent(t *testing.T) {
	f := newPR4RunFixture(t)
	ctx := context.Background()
	lease1, err := f.repo.AcquireRunLease(ctx, f.run.ID, "worker-one", f.now, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, int64(1), lease1.FencingToken)

	commit := pr4Commit(f, "chunk-0", lease1.FencingToken, "worker-one", 0)
	after, err := f.repo.CommitHealthChunk(ctx, commit)
	require.NoError(t, err)
	assert.Equal(t, int64(3), after.ResolvedSegments)
	assert.Equal(t, int64(4), after.ProviderChecks)
	assert.Equal(t, int64(4), after.CursorSegment)

	afterRetry, err := f.repo.CommitHealthChunk(ctx, commit)
	require.NoError(t, err)
	assert.Equal(t, after.ResolvedSegments, afterRetry.ResolvedSegments)
	assert.Equal(t, after.ProviderChecks, afterRetry.ProviderChecks)

	checks := map[string]int{
		"health_run_chunks":          1,
		"health_provider_coverage":   1,
		"health_segment_exceptions":  2,
		"health_attempt_evidence":    1,
		"health_confirmation_events": 1,
		"health_retry_states":        1,
	}
	for table, want := range checks {
		var got int
		require.NoError(t, f.db.Connection().QueryRow(`SELECT COUNT(*) FROM `+table).Scan(&got))
		assert.Equalf(t, want, got, "unexpected idempotency count for %s", table)
	}

	lease2, err := f.repo.AcquireRunLease(ctx, f.run.ID, "worker-two", f.now.Add(2*time.Minute), time.Minute)
	require.NoError(t, err)
	assert.Equal(t, int64(2), lease2.FencingToken)
	stale := pr4Commit(f, "chunk-stale", lease1.FencingToken, "worker-one", 4)
	stale.CommittedAt = f.now.Add(2 * time.Minute)
	_, err = f.repo.CommitHealthChunk(ctx, stale)
	require.ErrorIs(t, err, ErrStaleHealthLease)

	fresh := pr4Commit(f, "chunk-1", lease2.FencingToken, "worker-two", 4)
	fresh.CommittedAt = f.now.Add(2*time.Minute + time.Second)
	completed, err := f.repo.CommitHealthChunk(ctx, fresh)
	require.NoError(t, err)
	assert.Equal(t, int64(6), completed.ResolvedSegments)
	assert.Equal(t, int64(8), completed.CursorSegment)

	conflict := fresh
	conflict.ResolvedDelta = 4
	_, err = f.repo.CommitHealthChunk(ctx, conflict)
	require.ErrorIs(t, err, ErrHealthChunkConflict)
}

func TestPR4ConcurrentChunkReplayAdvancesProgressOnce(t *testing.T) {
	f := newPR4RunFixture(t)
	ctx := context.Background()
	lease, err := f.repo.AcquireRunLease(ctx, f.run.ID, "worker", f.now, time.Minute)
	require.NoError(t, err)
	commit := pr4Commit(f, "chunk-concurrent", lease.FencingToken, "worker", 0)

	const callers = 8
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.repo.CommitHealthChunk(ctx, commit)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	run, err := f.repo.GetHealthRun(ctx, f.run.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(3), run.ResolvedSegments)
	var chunks int
	require.NoError(t, f.db.Connection().QueryRow(`SELECT COUNT(*) FROM health_run_chunks`).Scan(&chunks))
	assert.Equal(t, 1, chunks)
}

func TestPR4ChunkCommitRollsBackEveryWriteOnFailure(t *testing.T) {
	f := newPR4RunFixture(t)
	ctx := context.Background()
	lease, err := f.repo.AcquireRunLease(ctx, f.run.ID, "worker", f.now, time.Minute)
	require.NoError(t, err)
	_, err = f.db.Connection().Exec(`CREATE TRIGGER pr4_force_attempt_failure
		BEFORE INSERT ON health_attempt_evidence BEGIN SELECT RAISE(ABORT, 'injected'); END`)
	require.NoError(t, err)

	_, err = f.repo.CommitHealthChunk(ctx, pr4Commit(f, "chunk-rollback", lease.FencingToken, "worker", 0))
	require.Error(t, err)
	for _, table := range []string{"health_run_chunks", "health_provider_coverage", "health_segment_exceptions", "health_confirmation_events", "health_retry_states"} {
		var count int
		require.NoError(t, f.db.Connection().QueryRow(`SELECT COUNT(*) FROM `+table).Scan(&count))
		assert.Zerof(t, count, "%s write escaped rollback", table)
	}
	run, err := f.repo.GetHealthRun(ctx, f.run.ID)
	require.NoError(t, err)
	assert.Zero(t, run.ResolvedSegments)
	assert.Zero(t, run.CursorSegment)
}

func TestPR4GapCausesAndSyntheticCacheStateAreDurable(t *testing.T) {
	f := newPR4RunFixture(t)
	ctx := context.Background()
	revision, err := f.repo.GetFileRevisionForRun(ctx, f.run.ID)
	require.NoError(t, err)
	gap, err := f.repo.UpsertGapRange(ctx, GapRangeWrite{
		ID: "gap-1", FileRevisionID: revision.ID, Kind: GapKindConfirmedUnusable,
		StartSegment: 2, SegmentCount: 2, Status: GapStatusActive, CreatedAt: f.now,
		Causes: []GapProviderCause{
			{ProviderID: f.providerID, ProviderGeneration: 1, Cause: GapCauseCorrupt, ConfirmationCount: 2, ConfirmedAt: f.now},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, GapKindConfirmedUnusable, gap.Kind)

	state, err := f.repo.RecordSyntheticOutput(ctx, SyntheticOutputWrite{
		ID: "synthetic-1", GapID: gap.ID, FileRevisionID: revision.ID,
		ByteStart: 200, ByteEnd: 299, EmittedAt: f.now.Add(time.Minute),
	})
	require.NoError(t, err)
	assert.Equal(t, CacheRecoveryPending, state.Status)
	assert.Equal(t, int64(1), state.ContentRevision)

	retained, err := f.repo.GetCacheRecoveryState(ctx, revision.ID)
	require.NoError(t, err)
	require.NotNil(t, retained)
	assert.Equal(t, CacheRecoveryPending, retained.Status)
	var causes, ranges int
	require.NoError(t, f.db.Connection().QueryRow(`SELECT COUNT(*) FROM health_gap_provider_causes WHERE gap_id = ?`, gap.ID).Scan(&causes))
	require.NoError(t, f.db.Connection().QueryRow(`SELECT COUNT(*) FROM health_synthetic_ranges WHERE gap_id = ?`, gap.ID).Scan(&ranges))
	assert.Equal(t, 1, causes)
	assert.Equal(t, 1, ranges)
}

func TestPR4InvalidCommitDoesNotBecomeProgress(t *testing.T) {
	f := newPR4RunFixture(t)
	ctx := context.Background()
	lease, err := f.repo.AcquireRunLease(ctx, f.run.ID, "worker", f.now, time.Minute)
	require.NoError(t, err)
	commit := pr4Commit(f, "invalid", lease.FencingToken, "worker", 0)
	commit.PresentBitmap = []byte{0b10000000} // outside the four-segment chunk and not tested
	_, err = f.repo.CommitHealthChunk(ctx, commit)
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrStaleHealthLease))
	run, getErr := f.repo.GetHealthRun(ctx, f.run.ID)
	require.NoError(t, getErr)
	assert.Zero(t, run.ResolvedSegments)
}
