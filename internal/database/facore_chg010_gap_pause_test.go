package database

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type facoreCHG010PauseAcknowledger interface {
	AcknowledgeRunPause(context.Context, string, string, int64, time.Time) (*HealthRun, error)
}

func requireFACORECHG010PauseAcknowledger(
	t *testing.T,
	repo *HealthStateRepository,
) facoreCHG010PauseAcknowledger {
	t.Helper()
	api, ok := any(repo).(facoreCHG010PauseAcknowledger)
	require.True(t, ok,
		"HealthStateRepository must export AcknowledgeRunPause(context.Context,string,string,int64,time.Time) (*HealthRun,error)")
	return api
}

func facoreCHG010ReadGap(
	t *testing.T,
	ctx context.Context,
	repo *HealthStateRepository,
	revisionID string,
	kind GapKind,
	start, count int64,
) HealthGapRange {
	t.Helper()
	var gap HealthGapRange
	require.NoError(t, repo.db.QueryRowContext(ctx, `
		SELECT id, file_revision_id, kind, start_segment, segment_count, status,
		       created_at, confirmed_at, cleared_at
		FROM health_gap_ranges
		WHERE file_revision_id = ? AND kind = ? AND start_segment = ? AND segment_count = ?
	`, revisionID, kind, start, count).Scan(
		&gap.ID, &gap.FileRevisionID, &gap.Kind, &gap.StartSegment, &gap.SegmentCount,
		&gap.Status, &gap.CreatedAt, &gap.ConfirmedAt, &gap.ClearedAt,
	))
	rows, err := repo.db.QueryContext(ctx, `
		SELECT provider_id, provider_generation, cause, confirmation_count, confirmed_at
		FROM health_gap_provider_causes WHERE gap_id = ?
		ORDER BY provider_id, provider_generation
	`, gap.ID)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var cause GapProviderCause
		var confirmedAt *time.Time
		require.NoError(t, rows.Scan(
			&cause.ProviderID, &cause.ProviderGeneration, &cause.Cause,
			&cause.ConfirmationCount, &confirmedAt,
		))
		if confirmedAt != nil {
			cause.ConfirmedAt = confirmedAt.UTC()
		}
		gap.Causes = append(gap.Causes, cause)
	}
	require.NoError(t, rows.Err())
	return gap
}

// facoreCHG010CloseAndReopen replaces only the repository's SQL client. It
// intentionally does not model a running worker or a process/server restart.
func facoreCHG010CloseAndReopen(
	t *testing.T,
	ctx context.Context,
	f chg009Fixture,
) chg009Fixture {
	t.Helper()
	var reopened *sql.DB
	if f.dialect == DialectSQLite {
		var sequence int
		var name, path string
		require.NoError(t, f.db.QueryRowContext(ctx, `PRAGMA database_list`).Scan(&sequence, &name, &path))
		require.Equal(t, "main", name)
		require.NoError(t, f.db.Close())
		var err error
		reopened, err = sql.Open("sqlite3", path+"?_journal_mode=WAL&_busy_timeout=30000&_foreign_keys=on&_txlock=immediate")
		require.NoError(t, err)
		reopened.SetMaxOpenConns(8)
		reopened.SetMaxIdleConns(3)
	} else {
		var schema string
		require.NoError(t, f.db.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&schema))
		config, err := pgx.ParseConfig(os.Getenv("ALTMOUNT_TEST_POSTGRES_DSN"))
		require.NoError(t, err)
		if config.RuntimeParams == nil {
			config.RuntimeParams = make(map[string]string)
		}
		config.RuntimeParams["search_path"] = schema
		require.NoError(t, f.db.Close())
		reopened = stdlib.OpenDB(*config)
		reopened.SetMaxOpenConns(2)
		reopened.SetMaxIdleConns(1)
	}
	require.NoError(t, reopened.PingContext(ctx))
	t.Cleanup(func() { assert.NoError(t, reopened.Close()) })
	f.db = reopened
	f.repo = NewHealthStateRepository(reopened, f.dialect)
	f.repo.now = f.clock.Now
	return f
}

type facoreCHG010Progress struct {
	resolved, checks, missing, inconclusive int64
	stage                                   string
	provider                                *string
	generation                              *int64
	sequence, cursor                        int64
}

func facoreCHG010RunProgress(run *HealthRun) facoreCHG010Progress {
	return facoreCHG010Progress{
		resolved: run.ResolvedSegments, checks: run.ProviderChecks,
		missing: run.MissingCandidates, inconclusive: run.InconclusiveCount,
		stage: run.Stage, provider: run.CurrentProviderID,
		generation: run.CurrentProviderGeneration,
		sequence:   run.CursorSequence, cursor: run.CursorSegment,
	}
}

func TestFACORECHG010GapNaturalKeyReactivatesCanonicalIdentity(t *testing.T) {
	forEachFACORECHG009RepositoryBackend(t, func(t *testing.T, dialect Dialect) {
		f := newCHG009Fixture(t, 8, dialect)
		ctx := context.Background()
		for _, transition := range []struct {
			name       string
			canonical  string
			status     GapStatus
			incomingID string
			start      int64
		}{
			{name: "cleared then empty ID", canonical: "canonical-cleared", status: GapStatusCleared, start: 0},
			{name: "dormant then new ID", canonical: "canonical-dormant", status: GapStatusDormant, incomingID: "replacement", start: 2},
		} {
			t.Run(transition.name, func(t *testing.T) {
				createdAt := f.now.Add(time.Minute)
				initialConfirmedAt := f.now.Add(30 * time.Second)
				identity := GapRangeWrite{
					ID: transition.canonical, FileRevisionID: f.run.FileRevisionID,
					Kind: GapKindConfirmedAbsent, StartSegment: transition.start, SegmentCount: 2,
					Status: GapStatusActive, CreatedAt: createdAt, ConfirmedAt: &initialConfirmedAt,
				}
				original, err := f.repo.UpsertGapRange(ctx, identity)
				require.NoError(t, err)
				prior := identity
				prior.Status = transition.status
				if transition.status == GapStatusCleared {
					clearedAt := f.now.Add(2 * time.Minute)
					prior.ClearedAt = &clearedAt
				}
				_, err = f.repo.UpsertGapRange(ctx, prior)
				require.NoError(t, err)

				confirmedAt := f.now.Add(4 * time.Minute)
				recurrence := identity
				recurrence.ID = transition.incomingID
				recurrence.Status = GapStatusActive
				recurrence.CreatedAt = f.now.Add(3 * time.Minute)
				recurrence.ConfirmedAt = &confirmedAt
				reactivated, err := f.repo.UpsertGapRange(ctx, recurrence)
				require.NoError(t, err)
				assert.Equal(t, original.ID, reactivated.ID)
				assert.True(t, reactivated.CreatedAt.Equal(createdAt), "the first occurrence owns creation identity")
				assert.Equal(t, GapStatusActive, reactivated.Status)
				assert.Nil(t, reactivated.ClearedAt)
				require.NotNil(t, reactivated.ConfirmedAt)
				assert.True(t, reactivated.ConfirmedAt.Equal(confirmedAt))
				assert.Equal(t, 1, chg009Count(t, f, `
					SELECT COUNT(*) FROM health_gap_ranges
					WHERE file_revision_id = ? AND kind = ? AND start_segment = ? AND segment_count = ?
				`, f.run.FileRevisionID, identity.Kind, identity.StartSegment, identity.SegmentCount))
			})
		}
	})
}

func TestFACORECHG010GapExplicitIdentityConflictRollsBack(t *testing.T) {
	forEachFACORECHG009RepositoryBackend(t, func(t *testing.T, dialect Dialect) {
		f := newCHG009Fixture(t, 8, dialect)
		ctx := context.Background()
		base := GapRangeWrite{
			ID: "fixed-gap", FileRevisionID: f.run.FileRevisionID, Kind: GapKindConfirmedAbsent,
			StartSegment: 0, SegmentCount: 2, Status: GapStatusActive, CreatedAt: f.now,
			Causes: []GapProviderCause{{
				ProviderID: f.providers[0].ID, ProviderGeneration: 1, Cause: GapCauseAbsent,
				ConfirmationCount: 3, ConfirmedAt: f.now.Add(time.Minute),
			}},
		}
		_, err := f.repo.UpsertGapRange(ctx, base)
		require.NoError(t, err)
		for _, conflict := range []struct {
			name   string
			mutate func(*GapRangeWrite)
		}{
			{name: "different range", mutate: func(w *GapRangeWrite) { w.StartSegment++ }},
			{name: "different creation identity", mutate: func(w *GapRangeWrite) { w.CreatedAt = w.CreatedAt.Add(time.Second) }},
		} {
			t.Run(conflict.name, func(t *testing.T) {
				write := base
				conflict.mutate(&write)
				clearedAt := f.now.Add(5 * time.Minute)
				write.Status, write.ClearedAt = GapStatusCleared, &clearedAt
				write.Causes = []GapProviderCause{{
					ProviderID: f.providers[1].ID, ProviderGeneration: 1,
					Cause: GapCauseCorrupt, ConfirmationCount: 9, ConfirmedAt: clearedAt,
				}}
				_, err := f.repo.UpsertGapRange(ctx, write)
				assert.ErrorIs(t, err, ErrHealthChunkConflict)
				retained := facoreCHG010ReadGap(t, ctx, f.repo, base.FileRevisionID,
					base.Kind, base.StartSegment, base.SegmentCount)
				assert.Equal(t, GapStatusActive, retained.Status)
				assert.Nil(t, retained.ClearedAt)
				require.Len(t, retained.Causes, 1)
				assert.Equal(t, base.Causes[0], retained.Causes[0])
				assert.Equal(t, 1, chg009Count(t, f,
					`SELECT COUNT(*) FROM health_gap_ranges WHERE file_revision_id = ?`, base.FileRevisionID))
			})
		}
		invalidCause := GapRangeWrite{
			ID: "invalid-cause-gap", FileRevisionID: base.FileRevisionID, Kind: GapKindProvisional,
			StartSegment: 4, SegmentCount: 1, Status: GapStatusActive, CreatedAt: f.now,
			Causes: []GapProviderCause{{
				ProviderID: "missing-provider", ProviderGeneration: 1,
				Cause: GapCauseAbsent, ConfirmationCount: 1, ConfirmedAt: f.now,
			}},
		}
		_, err = f.repo.UpsertGapRange(ctx, invalidCause)
		require.Error(t, err)
		assert.Zero(t, chg009Count(t, f,
			`SELECT COUNT(*) FROM health_gap_ranges WHERE id = ?`, invalidCause.ID))
		assert.Equal(t, 1, chg009Count(t, f,
			`SELECT COUNT(*) FROM health_gap_ranges WHERE file_revision_id = ?`, base.FileRevisionID))
	})
}

func TestFACORECHG010GapProviderCauseTupleIsCoherent(t *testing.T) {
	forEachFACORECHG009RepositoryBackend(t, func(t *testing.T, dialect Dialect) {
		f := newCHG009Fixture(t, 8, dialect)
		ctx := context.Background()
		write := GapRangeWrite{
			ID: "cause-gap", FileRevisionID: f.run.FileRevisionID, Kind: GapKindConfirmedUnusable,
			StartSegment: 1, SegmentCount: 1, Status: GapStatusActive, CreatedAt: f.now,
		}
		t1, t2, t3, t4 := f.now.Add(time.Minute), f.now.Add(2*time.Minute),
			f.now.Add(3*time.Minute), f.now.Add(4*time.Minute)
		upsert := func(cause GapCause, count int, at time.Time) GapProviderCause {
			write.Causes = []GapProviderCause{{
				ProviderID: f.providers[0].ID, ProviderGeneration: 1,
				Cause: cause, ConfirmationCount: count, ConfirmedAt: at,
			}}
			gap, err := f.repo.UpsertGapRange(ctx, write)
			require.NoError(t, err)
			require.Len(t, gap.Causes, 1)
			return gap.Causes[0]
		}
		assert.Equal(t, GapProviderCause{ProviderID: f.providers[0].ID, ProviderGeneration: 1,
			Cause: GapCauseAbsent, ConfirmationCount: 5, ConfirmedAt: t2},
			upsert(GapCauseAbsent, 5, t2))
		assert.Equal(t, GapProviderCause{ProviderID: f.providers[0].ID, ProviderGeneration: 1,
			Cause: GapCauseAbsent, ConfirmationCount: 5, ConfirmedAt: t2},
			upsert(GapCauseCorrupt, 1, t1), "an older write must retain the entire newer tuple")
		assert.Equal(t, GapProviderCause{ProviderID: f.providers[0].ID, ProviderGeneration: 1,
			Cause: GapCauseCorrupt, ConfirmationCount: 1, ConfirmedAt: t3},
			upsert(GapCauseCorrupt, 1, t3))
		assert.Equal(t, GapProviderCause{ProviderID: f.providers[0].ID, ProviderGeneration: 1,
			Cause: GapCauseAbsent, ConfirmationCount: 7, ConfirmedAt: t3},
			upsert(GapCauseAbsent, 7, t3), "equal time is serialized incoming-wins")
		assert.Equal(t, GapProviderCause{ProviderID: f.providers[0].ID, ProviderGeneration: 1,
			Cause: GapCauseAbsent, ConfirmationCount: 7, ConfirmedAt: t4},
			upsert(GapCauseAbsent, 2, t4), "same-cause confirmation count is monotonic")
	})
}

func TestFACORECHG010GapConcurrentCanonicalizationAndClientReplay(t *testing.T) {
	forEachFACORECHG009RepositoryBackend(t, func(t *testing.T, dialect Dialect) {
		f := newCHG009Fixture(t, 8, dialect)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		base := GapRangeWrite{
			FileRevisionID: f.run.FileRevisionID, Kind: GapKindConfirmedAbsent,
			StartSegment: 0, SegmentCount: 2, Status: GapStatusActive,
		}
		writes := [2]GapRangeWrite{base, base}
		for i := range writes {
			writes[i].ID = []string{"concurrent-a", "concurrent-b"}[i]
			writes[i].CreatedAt = f.now.Add(time.Duration(i+1) * time.Minute)
			writes[i].Causes = []GapProviderCause{{
				ProviderID: f.providers[i].ID, ProviderGeneration: 1,
				Cause: GapCauseAbsent, ConfirmationCount: i + 1,
				ConfirmedAt: f.now.Add(time.Duration(i+3) * time.Minute),
			}}
		}
		values, errs := facoreCHG007RunConcurrentPair(t, ctx, [2]func() (*HealthGapRange, error){
			func() (*HealthGapRange, error) { return f.repo.UpsertGapRange(ctx, writes[0]) },
			func() (*HealthGapRange, error) { return f.repo.UpsertGapRange(ctx, writes[1]) },
		})
		require.NoError(t, errs[0])
		require.NoError(t, errs[1])
		assert.Equal(t, values[0].ID, values[1].ID)
		canonical := facoreCHG010ReadGap(t, ctx, f.repo, base.FileRevisionID,
			base.Kind, base.StartSegment, base.SegmentCount)
		assert.Contains(t, []string{"concurrent-a", "concurrent-b"}, canonical.ID)
		require.Len(t, canonical.Causes, 2)
		assert.Equal(t, 1, chg009Count(t, f, `
			SELECT COUNT(*) FROM health_gap_ranges
			WHERE file_revision_id = ? AND kind = ? AND start_segment = ? AND segment_count = ?
		`, base.FileRevisionID, base.Kind, base.StartSegment, base.SegmentCount))

		lost := GapRangeWrite{
			FileRevisionID: f.run.FileRevisionID, Kind: GapKindProvisional,
			StartSegment: 4, SegmentCount: 1, Status: GapStatusActive,
			CreatedAt: f.now.Add(10 * time.Minute),
		}
		_, err := f.repo.UpsertGapRange(ctx, lost) // response is intentionally discarded
		require.NoError(t, err)
		f = facoreCHG010CloseAndReopen(t, ctx, f)
		replay, err := f.repo.UpsertGapRange(ctx, lost)
		require.NoError(t, err)
		persisted := facoreCHG010ReadGap(t, ctx, f.repo, lost.FileRevisionID,
			lost.Kind, lost.StartSegment, lost.SegmentCount)
		assert.Equal(t, persisted.ID, replay.ID)
		assert.Equal(t, 1, chg009Count(t, f, `
			SELECT COUNT(*) FROM health_gap_ranges
			WHERE file_revision_id = ? AND kind = ? AND start_segment = ? AND segment_count = ?
		`, lost.FileRevisionID, lost.Kind, lost.StartSegment, lost.SegmentCount))
	})
}

func TestFACORECHG010PauseMethodAndAcquireOrdering(t *testing.T) {
	forEachFACORECHG009RepositoryBackend(t, func(t *testing.T, dialect Dialect) {
		f := newCHG009Fixture(t, 8, dialect)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		require.NoError(t, f.repo.RequestRunPause(ctx, f.run.ID, true, f.now.Add(time.Minute)))
		_, err := f.repo.AcquireRunLease(ctx, f.run.ID, "blocked-worker", time.Minute)
		assert.ErrorIs(t, err, ErrStaleHealthLease, "a requested pending run must not start")
		blocked, err := f.repo.GetHealthRun(ctx, f.run.ID)
		require.NoError(t, err)
		assert.Equal(t, HealthRunPending, blocked.Status)
		assert.True(t, blocked.PauseRequested)
		assert.Nil(t, blocked.LeaseOwner)

		running := f.siblingRun(t)
		_, err = f.repo.AcquireRunLease(ctx, running.ID, "running-worker", time.Minute)
		require.NoError(t, err)
		require.NoError(t, f.repo.RequestRunPause(ctx, running.ID, true, f.now.Add(90*time.Second)))
		requestedRunning, err := f.repo.GetHealthRun(ctx, running.ID)
		require.NoError(t, err)
		_, err = f.repo.AcquireRunLease(ctx, running.ID, "running-worker", time.Minute)
		assert.ErrorIs(t, err, ErrStaleHealthLease,
			"a current owner must not reacquire while a pause is requested")
		afterReacquire, err := f.repo.GetHealthRun(ctx, running.ID)
		require.NoError(t, err)
		assert.Equal(t, requestedRunning, afterReacquire,
			"rejected pause-requested reacquisition must not advance the fence")

		contended := f.siblingRun(t)
		values, errs := facoreCHG007RunConcurrentPair(t, ctx, [2]func() (*HealthRun, error){
			func() (*HealthRun, error) {
				return nil, f.repo.RequestRunPause(ctx, contended.ID, true, f.now.Add(2*time.Minute))
			},
			func() (*HealthRun, error) {
				return f.repo.AcquireRunLease(ctx, contended.ID, "ordered-worker", time.Minute)
			},
		})
		require.NoError(t, errs[0])
		after, getErr := f.repo.GetHealthRun(ctx, contended.ID)
		require.NoError(t, getErr)
		assert.True(t, after.PauseRequested)
		if errs[1] == nil {
			require.NotNil(t, values[1])
			assert.Equal(t, HealthRunRunning, after.Status)
			require.NotNil(t, after.LeaseOwner)
			assert.Equal(t, "ordered-worker", *after.LeaseOwner)
		} else {
			assert.ErrorIs(t, errs[1], ErrStaleHealthLease)
			assert.Equal(t, HealthRunPending, after.Status)
			assert.Nil(t, after.LeaseOwner)
		}
		_ = requireFACORECHG010PauseAcknowledger(t, f.repo) // runtime, not compile-time, API assertion
	})
}

func TestFACORECHG010PauseFenceExpiryReplayAndResume(t *testing.T) {
	forEachFACORECHG009RepositoryBackend(t, func(t *testing.T, dialect Dialect) {
		f := newCHG009Fixture(t, 8, dialect)
		api := requireFACORECHG010PauseAcknowledger(t, f.repo)
		ctx := context.Background()
		expiring, err := f.repo.AcquireRunLease(ctx, f.run.ID, "expiring", time.Minute)
		require.NoError(t, err)
		beforeRequest, err := f.repo.GetHealthRun(ctx, f.run.ID)
		require.NoError(t, err)
		_, err = api.AcknowledgeRunPause(ctx, f.run.ID, "expiring", expiring.FencingToken, f.now)
		assert.ErrorIs(t, err, ErrStaleHealthLease, "an unrequested pause cannot be acknowledged")
		afterUnrequested, err := f.repo.GetHealthRun(ctx, f.run.ID)
		require.NoError(t, err)
		assert.Equal(t, beforeRequest, afterUnrequested)
		require.NoError(t, f.repo.RequestRunPause(ctx, f.run.ID, true, f.now.Add(10*time.Second)))
		requested, err := f.repo.GetHealthRun(ctx, f.run.ID)
		require.NoError(t, err)
		_, err = api.AcknowledgeRunPause(ctx, f.run.ID, "wrong-owner", expiring.FencingToken, f.now)
		assert.ErrorIs(t, err, ErrStaleHealthLease)
		afterWrongOwner, err := f.repo.GetHealthRun(ctx, f.run.ID)
		require.NoError(t, err)
		assert.Equal(t, requested, afterWrongOwner)
		_, err = api.AcknowledgeRunPause(ctx, f.run.ID, "expiring", expiring.FencingToken+1, f.now)
		assert.ErrorIs(t, err, ErrStaleHealthLease)
		afterWrongToken, err := f.repo.GetHealthRun(ctx, f.run.ID)
		require.NoError(t, err)
		assert.Equal(t, requested, afterWrongToken)
		f.clock.now = *expiring.LeaseExpiresAt
		_, err = api.AcknowledgeRunPause(ctx, f.run.ID, "expiring", expiring.FencingToken, f.now)
		assert.ErrorIs(t, err, ErrStaleHealthLease, "the exact repository-time deadline is expired")
		retained, getErr := f.repo.GetHealthRun(ctx, f.run.ID)
		require.NoError(t, getErr)
		assert.Equal(t, requested, retained, "stale acknowledgements must not mutate run state")

		f.clock.now = f.now
		live := f.siblingRun(t)
		lease, err := f.repo.AcquireRunLease(ctx, live.ID, "live-owner", time.Minute)
		require.NoError(t, err)
		require.NoError(t, f.repo.RequestRunPause(ctx, live.ID, true, f.now.Add(20*time.Second)))
		callerAt := f.now.Add(365 * 24 * time.Hour)
		paused, err := api.AcknowledgeRunPause(ctx, live.ID, "live-owner", lease.FencingToken, callerAt)
		require.NoError(t, err, "caller time must not decide lease validity")
		assert.Equal(t, HealthRunPaused, paused.Status)
		assert.True(t, paused.PauseRequested)
		assert.Nil(t, paused.LeaseOwner)
		assert.Nil(t, paused.LeaseExpiresAt)
		assert.Equal(t, lease.FencingToken, paused.FencingToken)
		assert.True(t, paused.UpdatedAt.Equal(callerAt), "caller time owns only updated_at")
		replayed, err := api.AcknowledgeRunPause(ctx, live.ID, "live-owner", lease.FencingToken, callerAt.Add(time.Hour))
		require.NoError(t, err)
		assert.Equal(t, paused, replayed, "exact-token replay must not rewrite paused state")
		_, err = f.repo.AcquireRunLease(ctx, live.ID, "premature-owner", time.Minute)
		assert.ErrorIs(t, err, ErrStaleHealthLease, "paused work resumes only through an explicit unpause")

		oldCommit := chg009PresentCommit(f, live, lease, "old-paused-work", "live-owner",
			f.providers[0].ID, "stat", 0, 1, 1, 1, 1)
		_, err = f.repo.CommitHealthChunk(ctx, oldCommit)
		assert.ErrorIs(t, err, ErrStaleHealthLease)
		assert.Zero(t, chg009Count(t, f, `SELECT COUNT(*) FROM health_run_chunks WHERE id = ?`, oldCommit.ChunkID))

		resumeAt := callerAt.Add(2 * time.Hour)
		require.NoError(t, f.repo.RequestRunPause(ctx, live.ID, false, resumeAt))
		pending, err := f.repo.GetHealthRun(ctx, live.ID)
		require.NoError(t, err)
		assert.Equal(t, HealthRunPending, pending.Status)
		assert.False(t, pending.PauseRequested)
		assert.Equal(t, lease.FencingToken, pending.FencingToken)
		next, err := f.repo.AcquireRunLease(ctx, live.ID, "next-owner", time.Minute)
		require.NoError(t, err)
		assert.Equal(t, lease.FencingToken+1, next.FencingToken)
	})
}

func TestFACORECHG010PausePreservesEvidenceAndSerializesCommit(t *testing.T) {
	forEachFACORECHG009RepositoryBackend(t, func(t *testing.T, dialect Dialect) {
		f := newCHG009Fixture(t, 8, dialect)
		api := requireFACORECHG010PauseAcknowledger(t, f.repo)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		lease, err := f.repo.AcquireRunLease(ctx, f.run.ID, "evidence-owner", time.Minute)
		require.NoError(t, err)
		commit := chg009PresentCommit(f, f.run, lease, "pause-evidence", "evidence-owner",
			f.providers[0].ID, "stat", 0, 3, 3, 1, 0b011)
		commit.PresentBitmap = []byte{0b001}
		commit.AbsentBitmap = []byte{0b010}
		commit.TemporaryBitmap = []byte{0b100}
		commit.MissingCandidatesDelta, commit.InconclusiveDelta = 1, 1
		commit.Attempts = []HealthAttemptEvidence{{
			IdempotencyKey: "pause-attempt", SegmentIndex: 1, Operation: "STAT",
			Outcome: "hard_absence", BodyValidation: "not_requested", ObservedAt: f.now,
		}}
		commit.Confirmations = []HealthConfirmationEvent{{
			IdempotencyKey: "pause-confirmation", SegmentIndex: 1,
			Cause: GapCauseAbsent, ObservedAt: f.now,
		}}
		commit.Retry = &HealthRetryState{
			RetryKey: "pause-retry", SegmentStart: 2, SegmentCount: 1,
			Outcome: "temporary_failure", Attempt: 1, NextAttemptAt: f.now.Add(time.Minute),
		}
		require.NoError(t, f.repo.RequestRunPause(ctx, f.run.ID, true, f.now.Add(2*time.Minute)))
		before, err := f.repo.CommitHealthChunk(ctx, commit)
		require.NoError(t, err)
		after, err := api.AcknowledgeRunPause(ctx, f.run.ID, "evidence-owner",
			lease.FencingToken, f.now.Add(3*time.Minute))
		require.NoError(t, err)
		assert.Equal(t, facoreCHG010RunProgress(before), facoreCHG010RunProgress(after))
		assert.Equal(t, HealthRunPaused, after.Status)
		assert.Equal(t, 1, chg009Count(t, f,
			`SELECT COUNT(*) FROM health_run_chunks WHERE id = ?`, commit.ChunkID))
		for table, want := range map[string]int{
			"health_provider_coverage": 1, "health_segment_exceptions": 2,
			"health_attempt_evidence":    1,
			"health_confirmation_events": 1, "health_retry_states": 1,
		} {
			assert.Equal(t, want, chg009Count(t, f,
				`SELECT COUNT(*) FROM `+table+` WHERE source_chunk_id = ?`, commit.ChunkID), table)
		}

		ordered := f.siblingRun(t)
		orderedLease, err := f.repo.AcquireRunLease(ctx, ordered.ID, "ordered-owner", time.Minute)
		require.NoError(t, err)
		require.NoError(t, f.repo.RequestRunPause(ctx, ordered.ID, true, f.now.Add(4*time.Minute)))
		orderedCommit := chg009PresentCommit(f, ordered, orderedLease, "ordered-commit",
			"ordered-owner", f.providers[1].ID, "stat", 4, 1, 5, 1, 1)
		_, errs := facoreCHG007RunConcurrentPair(t, ctx, [2]func() (*HealthRun, error){
			func() (*HealthRun, error) { return f.repo.CommitHealthChunk(ctx, orderedCommit) },
			func() (*HealthRun, error) {
				return api.AcknowledgeRunPause(ctx, ordered.ID, "ordered-owner",
					orderedLease.FencingToken, f.now.Add(5*time.Minute))
			},
		})
		require.NoError(t, errs[1])
		final, getErr := f.repo.GetHealthRun(ctx, ordered.ID)
		require.NoError(t, getErr)
		assert.Equal(t, HealthRunPaused, final.Status)
		assert.Nil(t, final.LeaseOwner)
		if errs[0] == nil {
			assert.Equal(t, 1, chg009Count(t, f,
				`SELECT COUNT(*) FROM health_run_chunks WHERE id = ?`, orderedCommit.ChunkID))
			assert.Equal(t, int64(1), final.ResolvedSegments)
		} else {
			assert.ErrorIs(t, errs[0], ErrStaleHealthLease)
			assert.Zero(t, chg009Count(t, f,
				`SELECT COUNT(*) FROM health_run_chunks WHERE id = ?`, orderedCommit.ChunkID))
			assert.Zero(t, final.ResolvedSegments)
		}
	})
}

func TestFACORECHG010PauseClientReplayAndTerminalGuards(t *testing.T) {
	forEachFACORECHG009RepositoryBackend(t, func(t *testing.T, dialect Dialect) {
		f := newCHG009Fixture(t, 8, dialect)
		api := requireFACORECHG010PauseAcknowledger(t, f.repo)
		ctx := context.Background()
		lease, err := f.repo.AcquireRunLease(ctx, f.run.ID, "lost-response-owner", time.Minute)
		require.NoError(t, err)
		require.NoError(t, f.repo.RequestRunPause(ctx, f.run.ID, true, f.now.Add(time.Minute)))
		_, err = api.AcknowledgeRunPause(ctx, f.run.ID, "lost-response-owner",
			lease.FencingToken, f.now.Add(2*time.Minute)) // response is intentionally discarded
		require.NoError(t, err)
		f = facoreCHG010CloseAndReopen(t, ctx, f)
		api = requireFACORECHG010PauseAcknowledger(t, f.repo)
		beforeReplay, err := f.repo.GetHealthRun(ctx, f.run.ID)
		require.NoError(t, err)
		replayed, err := api.AcknowledgeRunPause(ctx, f.run.ID, "lost-response-owner",
			lease.FencingToken, f.now.Add(20*time.Minute))
		require.NoError(t, err)
		assert.Equal(t, beforeReplay, replayed)

		canceled := f.siblingRun(t)
		canceledLease, err := f.repo.AcquireRunLease(ctx, canceled.ID, "canceled-owner", time.Minute)
		require.NoError(t, err)
		require.NoError(t, f.repo.RequestRunPause(ctx, canceled.ID, true, f.now.Add(3*time.Minute)))
		require.NoError(t, f.repo.RequestRunCancel(ctx, canceled.ID, f.now.Add(4*time.Minute)))
		_, err = api.AcknowledgeRunPause(ctx, canceled.ID, "canceled-owner",
			canceledLease.FencingToken, f.now.Add(5*time.Minute))
		assert.ErrorIs(t, err, ErrStaleHealthLease)

		terminal := []struct {
			status HealthRunStatus
			run    *HealthRun
		}{{HealthRunCanceled, canceled}, {HealthRunCompleted, f.siblingRun(t)}, {HealthRunFailed, f.siblingRun(t)}}
		for _, test := range terminal {
			t.Run(string(test.status), func(t *testing.T) {
				if test.status != HealthRunCanceled {
					_, err := f.repo.db.ExecContext(ctx, `
						UPDATE health_runs SET status = ?, completed_at = ?, updated_at = ? WHERE id = ?
					`, test.status, f.now.Add(4*time.Minute), f.now.Add(4*time.Minute), test.run.ID)
					require.NoError(t, err)
				}
				before, err := f.repo.GetHealthRun(ctx, test.run.ID)
				require.NoError(t, err)
				for _, requested := range []bool{true, false} {
					err = f.repo.RequestRunPause(ctx, test.run.ID, requested, f.now.Add(30*time.Minute))
					assert.ErrorIs(t, err, sql.ErrNoRows)
					after, getErr := f.repo.GetHealthRun(ctx, test.run.ID)
					require.NoError(t, getErr)
					assert.Equal(t, before, after, "pause control must not resurrect or rewrite terminal runs")
				}
			})
		}
	})
}
