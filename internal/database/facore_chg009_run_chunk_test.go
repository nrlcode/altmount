package database

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CHG-009 adds fields to these production structs. Reflection keeps this
// tests-only parent commit buildable while the contract test below makes their
// absence an intentional red assertion.
func chg009SetIntegrity(commit *HealthChunkCommit, sequence int64, resolved []byte) {
	v := reflect.ValueOf(commit).Elem()
	if field := v.FieldByName("CursorSequence"); field.IsValid() && field.CanSet() {
		field.SetInt(sequence)
	}
	if field := v.FieldByName("ResolvedBitmap"); field.IsValid() && field.CanSet() {
		var clone []byte
		if resolved != nil {
			clone = make([]byte, len(resolved))
			copy(clone, resolved)
		}
		field.SetBytes(clone)
	}
}

func chg009CursorSequence(run *HealthRun) (int64, bool) {
	field := reflect.ValueOf(run).Elem().FieldByName("CursorSequence")
	if !field.IsValid() {
		return 0, false
	}
	return field.Int(), true
}

type chg009Fixture struct {
	db        *sql.DB
	dialect   Dialect
	repo      *HealthStateRepository
	run       *HealthRun
	providers []HealthProvider
	now       time.Time
	clock     *pr4TestClock
}

func forEachFACORECHG009RepositoryBackend(t *testing.T, test func(*testing.T, Dialect)) {
	t.Helper()
	for _, dialect := range []Dialect{DialectSQLite, DialectPostgres} {
		dialect := dialect
		t.Run(string(dialect), func(t *testing.T) { test(t, dialect) })
	}
}

func newCHG009Fixture(t *testing.T, segments int64, dialect Dialect) chg009Fixture {
	t.Helper()
	ctx := context.Background()
	var db *sql.DB
	var repo *HealthStateRepository
	if dialect == DialectPostgres {
		backend := newFACORECHG009PostgresMigrationBackend(t, ctx)
		goose.SetBaseFS(embedMigrations)
		require.NoError(t, goose.SetDialect(backend.gooseDialect))
		require.NoError(t, goose.UpContext(ctx, backend.db, backend.migrationsDir))
		db = backend.db
		repo = NewHealthStateRepository(db, dialect)
	} else {
		wrapper, sqliteRepo := newPR4Repository(t)
		db = wrapper.Connection()
		repo = sqliteRepo
	}
	suffix := uuid.NewString()
	revision, err := repo.EnsureFileRevision(ctx, FileRevisionSpec{
		FilePath: "chg009/" + suffix + ".mkv", LayoutFingerprint: "sha256:" + suffix,
		VirtualSize: segments * 100, SegmentCount: segments,
	})
	require.NoError(t, err)
	providers, err := repo.ReconcileProviders(ctx, []ProviderSpec{
		{StableID: "chg009-a-" + suffix, DisplayName: "A", Endpoint: "a-" + suffix + ".invalid", Port: 119, Account: "a", Role: ProviderRolePrimary, Order: 0},
		{StableID: "chg009-b-" + suffix, DisplayName: "B", Endpoint: "b-" + suffix + ".invalid", Port: 119, Account: "b", Role: ProviderRoleBackup, Order: 1},
	})
	require.NoError(t, err)
	now := time.Unix(1_710_000_000, 0).UTC()
	clock := &pr4TestClock{now: now}
	repo.now = clock.Now
	snapshot, err := repo.CaptureActiveProviderSnapshot(ctx, now)
	require.NoError(t, err)
	run, err := repo.CreateHealthRun(ctx, HealthRunSpec{
		ID: "chg009-run-" + suffix, FileRevisionID: revision.ID, ProviderSnapshotID: snapshot.ID,
		Trigger: "manual", Mode: "observation", TotalSegments: segments, CreatedAt: now,
	})
	require.NoError(t, err)
	return chg009Fixture{db: db, dialect: dialect, repo: repo, run: run, providers: providers, now: now, clock: clock}
}

func (f chg009Fixture) siblingRun(t *testing.T) *HealthRun {
	t.Helper()
	run, err := f.repo.CreateHealthRun(context.Background(), HealthRunSpec{
		ID: "chg009-run-" + uuid.NewString(), FileRevisionID: f.run.FileRevisionID,
		ProviderSnapshotID: f.run.ProviderSnapshotID, Trigger: "scheduled", Mode: "observation",
		TotalSegments: f.run.TotalSegments, CreatedAt: f.now,
	})
	require.NoError(t, err)
	return run
}

func chg009PresentCommit(
	f chg009Fixture, run *HealthRun, lease *HealthRun, id, owner, provider, stage string,
	start, count, cursor, sequence int64, resolved byte,
) HealthChunkCommit {
	mask := byte((1 << count) - 1)
	commit := HealthChunkCommit{
		ChunkID: id, RunID: run.ID, LeaseOwner: owner, FencingToken: lease.FencingToken,
		ProviderID: provider, ProviderGeneration: 1, Stage: stage, ObservationKind: HealthObservationSTAT,
		SegmentStart: start, SegmentCount: count, TestedBitmap: []byte{mask}, PresentBitmap: []byte{mask},
		AbsentBitmap: []byte{0}, CorruptBitmap: []byte{0}, TemporaryBitmap: []byte{0}, InconclusiveBitmap: []byte{0},
		CursorSegment: cursor, ResolvedDelta: bitmapPopulation([]byte{resolved}), ProviderChecksDelta: count,
		CommittedAt: f.now.Add(time.Minute),
	}
	chg009SetIntegrity(&commit, sequence, []byte{resolved})
	return commit
}

func chg009CloneCommit(commit HealthChunkCommit) HealthChunkCommit {
	commit.TestedBitmap = append([]byte(nil), commit.TestedBitmap...)
	commit.PresentBitmap = append([]byte(nil), commit.PresentBitmap...)
	commit.AbsentBitmap = append([]byte(nil), commit.AbsentBitmap...)
	commit.CorruptBitmap = append([]byte(nil), commit.CorruptBitmap...)
	commit.TemporaryBitmap = append([]byte(nil), commit.TemporaryBitmap...)
	commit.InconclusiveBitmap = append([]byte(nil), commit.InconclusiveBitmap...)
	value := reflect.ValueOf(&commit).Elem()
	if field := value.FieldByName("ResolvedBitmap"); field.IsValid() && field.CanSet() {
		field.SetBytes(append([]byte(nil), field.Bytes()...))
	}
	commit.Attempts = append([]HealthAttemptEvidence(nil), commit.Attempts...)
	commit.Confirmations = append([]HealthConfirmationEvent(nil), commit.Confirmations...)
	if commit.Retry != nil {
		retry := *commit.Retry
		commit.Retry = &retry
	}
	return commit
}

func chg009Count(t *testing.T, f chg009Fixture, query string, args ...any) int {
	t.Helper()
	var count int
	require.NoError(t, f.repo.db.QueryRowContext(context.Background(), query, args...).Scan(&count))
	return count
}

func chg009EnsureLegacyResolvedColumn(t *testing.T, f chg009Fixture) {
	t.Helper()
	if hasColumn(f.db, f.dialect, "health_run_chunks", "resolved_bitmap") {
		return
	}
	dataType := "BLOB"
	if f.dialect == DialectPostgres {
		dataType = "BYTEA"
	}
	_, err := f.repo.db.ExecContext(context.Background(),
		`ALTER TABLE health_run_chunks ADD COLUMN resolved_bitmap `+dataType)
	require.NoError(t, err)
}

func TestFACORECHG009IntegrityContractIsRepresentable(t *testing.T) {
	_, runField := reflect.TypeOf(HealthRun{}).FieldByName("CursorSequence")
	_, sequenceField := reflect.TypeOf(HealthChunkCommit{}).FieldByName("CursorSequence")
	_, bitmapField := reflect.TypeOf(HealthChunkCommit{}).FieldByName("ResolvedBitmap")
	assert.True(t, runField, "HealthRun.CursorSequence is required")
	assert.True(t, sequenceField, "HealthChunkCommit.CursorSequence is required")
	assert.True(t, bitmapField, "HealthChunkCommit.ResolvedBitmap is required")

	db, _ := newPR4Repository(t)
	assert.True(t, hasColumn(db.Connection(), DialectSQLite, "health_runs", "cursor_sequence"))
	assert.True(t, hasColumn(db.Connection(), DialectSQLite, "health_run_chunks", "resolved_bitmap"))
}

func TestFACORECHG009ResolvedBitmapValidationAndUniqueUnion(t *testing.T) {
	forEachFACORECHG009RepositoryBackend(t, func(t *testing.T, dialect Dialect) {
		t.Run("valid partial subset control", func(t *testing.T) {
			f := newCHG009Fixture(t, 8, dialect)
			lease, err := f.repo.AcquireRunLease(context.Background(), f.run.ID, "worker", 10*time.Minute)
			require.NoError(t, err)
			commit := chg009PresentCommit(f, f.run, lease, "partial", "worker", f.providers[0].ID, "stat", 0, 4, 4, 1, 0b0101)
			after, err := f.repo.CommitHealthChunk(context.Background(), commit)
			require.NoError(t, err)
			assert.Equal(t, int64(2), after.ResolvedSegments)
		})

		invalid := []struct {
			name      string
			bitmap    []byte
			delta     int64
			temporary byte
		}{
			{name: "exact byte length", bitmap: nil, delta: 0},
			{name: "tail bits", bitmap: []byte{0b10000001}, delta: 2},
			{name: "conclusive subset", bitmap: []byte{0b1000}, delta: 1, temporary: 0b1000},
			{name: "population matches delta", bitmap: []byte{0b0101}, delta: 1},
		}
		for _, tc := range invalid {
			t.Run(tc.name, func(t *testing.T) {
				f := newCHG009Fixture(t, 8, dialect)
				lease, err := f.repo.AcquireRunLease(context.Background(), f.run.ID, "worker", 10*time.Minute)
				require.NoError(t, err)
				commit := chg009PresentCommit(f, f.run, lease, "invalid", "worker", f.providers[0].ID, "stat", 0, 4, 4, 1, 0)
				if tc.temporary != 0 {
					commit.PresentBitmap[0] &^= tc.temporary
					commit.TemporaryBitmap[0] = tc.temporary
					commit.InconclusiveDelta = 1
				}
				commit.ResolvedDelta = tc.delta
				chg009SetIntegrity(&commit, 1, tc.bitmap)
				_, err = f.repo.CommitHealthChunk(context.Background(), commit)
				assert.Error(t, err)
				assert.Zero(t, chg009Count(t, f, `SELECT COUNT(*) FROM health_run_chunks WHERE run_id = ?`, f.run.ID))
			})
		}

		t.Run("zero sequence explicit empty is not compatibility nil", func(t *testing.T) {
			f := newCHG009Fixture(t, 8, dialect)
			lease, err := f.repo.AcquireRunLease(context.Background(), f.run.ID, "worker", 10*time.Minute)
			require.NoError(t, err)
			commit := chg009PresentCommit(f, f.run, lease, "explicit-empty", "worker", f.providers[0].ID, "stat", 0, 4, 4, 0, 0)
			chg009SetIntegrity(&commit, 0, []byte{})
			_, err = f.repo.CommitHealthChunk(context.Background(), commit)
			assert.Error(t, err, "a non-nil empty bitmap must fail exact-length validation")
			assert.Zero(t, chg009Count(t, f, `SELECT COUNT(*) FROM health_run_chunks WHERE run_id = ?`, f.run.ID))
		})

		t.Run("repeated positions across stage and provider count once", func(t *testing.T) {
			f := newCHG009Fixture(t, 8, dialect)
			lease, err := f.repo.AcquireRunLease(context.Background(), f.run.ID, "worker", 10*time.Minute)
			require.NoError(t, err)
			commits := []HealthChunkCommit{
				chg009PresentCommit(f, f.run, lease, "stage-a", "worker", f.providers[0].ID, "stat", 0, 4, 4, 1, 0b0101),
				chg009PresentCommit(f, f.run, lease, "stage-b", "worker", f.providers[0].ID, "body", 0, 4, 4, 2, 0b0011),
				chg009PresentCommit(f, f.run, lease, "provider-b", "worker", f.providers[1].ID, "body", 0, 4, 4, 3, 0b0110),
			}
			var after *HealthRun
			for _, commit := range commits {
				after, err = f.repo.CommitHealthChunk(context.Background(), commit)
				require.NoError(t, err)
			}
			assert.Equal(t, int64(3), after.ResolvedSegments)
			assert.Equal(t, int64(12), after.ProviderChecks, "provider work remains additive")
		})
	})
}

func TestFACORECHG009CursorEpochTransitionsAndRejectsDelayedOldWorkAtomically(t *testing.T) {
	forEachFACORECHG009RepositoryBackend(t, func(t *testing.T, dialect Dialect) {
		f := newCHG009Fixture(t, 16, dialect)
		ctx := context.Background()
		lease, err := f.repo.AcquireRunLease(ctx, f.run.ID, "worker", 10*time.Minute)
		require.NoError(t, err)
		first := chg009PresentCommit(f, f.run, lease, "a-late", "worker", f.providers[0].ID, "stat", 8, 4, 12, 1, 0b1111)
		_, err = f.repo.CommitHealthChunk(ctx, first)
		require.NoError(t, err)
		second := chg009PresentCommit(f, f.run, lease, "b-early", "worker", f.providers[1].ID, "stat", 0, 4, 4, 2, 0b1111)
		afterTransition, err := f.repo.CommitHealthChunk(ctx, second)
		require.NoError(t, err)
		assert.Equal(t, int64(4), afterTransition.CursorSegment, "same-stage provider transition resets its cursor")
		if sequence, ok := chg009CursorSequence(afterTransition); ok {
			assert.Equal(t, int64(2), sequence)
		}

		delayed := chg009PresentCommit(f, f.run, lease, "delayed-a", "worker", f.providers[0].ID, "stat", 4, 4, 8, 1, 0b0111)
		delayed.PresentBitmap = []byte{0b0011}
		delayed.AbsentBitmap = []byte{0b0100}
		delayed.TemporaryBitmap = []byte{0b1000}
		delayed.MissingCandidatesDelta = 1
		delayed.InconclusiveDelta = 1
		delayed.Attempts = []HealthAttemptEvidence{{IdempotencyKey: "delayed-attempt", SegmentIndex: 6, Operation: "STAT", Outcome: "hard_absence", BodyValidation: "not_requested", ObservedAt: f.now}}
		delayed.Confirmations = []HealthConfirmationEvent{{IdempotencyKey: "delayed-confirm", SegmentIndex: 6, Cause: GapCauseAbsent, ObservedAt: f.now}}
		delayed.Retry = &HealthRetryState{RetryKey: "delayed-retry", SegmentStart: 7, SegmentCount: 1, Outcome: "temporary_failure", Attempt: 1, NextAttemptAt: f.now.Add(time.Minute)}
		_, err = f.repo.CommitHealthChunk(ctx, delayed)
		assert.Error(t, err, "a delayed old cursor epoch must fail")
		assert.Zero(t, chg009Count(t, f, `SELECT COUNT(*) FROM health_run_chunks WHERE id = ?`, delayed.ChunkID), "chunk escaped rollback")
		for _, table := range []string{"health_provider_coverage", "health_segment_exceptions", "health_attempt_evidence", "health_confirmation_events", "health_retry_states"} {
			assert.Zero(t, chg009Count(t, f, `SELECT COUNT(*) FROM `+table+` WHERE source_chunk_id = ?`, delayed.ChunkID), "%s escaped rollback", table)
		}
		retained, getErr := f.repo.GetHealthRun(ctx, f.run.ID)
		require.NoError(t, getErr)
		assert.Equal(t, int64(8), retained.ResolvedSegments)
		assert.Equal(t, int64(8), retained.ProviderChecks)
		assert.Zero(t, retained.MissingCandidates)
		assert.Zero(t, retained.InconclusiveCount)
		assert.Equal(t, int64(4), retained.CursorSegment)
		assert.Equal(t, f.providers[1].ID, *retained.CurrentProviderID)
		if sequence, ok := chg009CursorSequence(retained); ok {
			assert.Equal(t, int64(2), sequence)
		}
	})
}

func TestFACORECHG009DatabaseApplyOrderIgnoresCallerFutureTime(t *testing.T) {
	forEachFACORECHG009RepositoryBackend(t, func(t *testing.T, dialect Dialect) {
		f := newCHG009Fixture(t, 4, dialect)
		ctx := context.Background()
		negativeRun, presentRun, finalRun := f.run, f.siblingRun(t), f.siblingRun(t)
		commitOutcome := func(
			run *HealthRun,
			owner, id string,
			absent bool,
			payloadAt, applicationAt time.Time,
		) time.Time {
			f.clock.now = applicationAt
			lease, err := f.repo.AcquireRunLease(ctx, run.ID, owner, 20*365*24*time.Hour)
			require.NoError(t, err)
			commit := chg009PresentCommit(f, run, lease, id, owner, f.providers[0].ID, "stat", 0, 1, 1, 1, 1)
			commit.CommittedAt = payloadAt
			if absent {
				commit.PresentBitmap[0] = 0
				commit.AbsentBitmap[0] = 1
				commit.MissingCandidatesDelta = 1
			}
			_, err = f.repo.CommitHealthChunk(ctx, commit)
			require.NoError(t, err)
			var chunkAt, coverageAt time.Time
			require.NoError(t, f.repo.db.QueryRowContext(ctx, `
				SELECT c.committed_at, p.observed_at
				FROM health_run_chunks c
				JOIN health_provider_coverage p ON p.source_chunk_id = c.id
				WHERE c.id = ?
			`, id).Scan(&chunkAt, &coverageAt))
			assert.True(t, chunkAt.Equal(coverageAt), "one database time must own chunk and coverage ordering")
			assert.False(t, chunkAt.Equal(payloadAt), "caller payload time cannot own durable ordering")
			assert.False(t, chunkAt.Equal(applicationAt), "application clock cannot own durable ordering")
			assert.WithinDuration(t, time.Now().UTC(), chunkAt, 10*time.Second)
			return chunkAt
		}
		firstAt := commitOutcome(negativeRun, "future", "future-negative", true,
			f.now.Add(365*24*time.Hour), f.now.Add(10*365*24*time.Hour))
		secondAt := commitOutcome(presentRun, "present", "later-present", false,
			f.now.Add(-365*24*time.Hour), f.now.Add(5*365*24*time.Hour))
		assert.False(t, secondAt.Before(firstAt), "database apply time must follow transaction order")
		assert.Zero(t, chg009Count(t, f, `SELECT COUNT(*) FROM health_segment_exceptions WHERE file_revision_id = ? AND segment_index = 0`, f.run.FileRevisionID), "later DB transaction must clear the exception")
		thirdAt := commitOutcome(finalRun, "final", "latest-negative", true,
			f.now.Add(-730*24*time.Hour), f.now)
		assert.False(t, thirdAt.Before(secondAt), "database apply time must follow transaction order")
		var source string
		require.NoError(t, f.repo.db.QueryRowContext(ctx, `SELECT source_chunk_id FROM health_segment_exceptions WHERE file_revision_id = ? AND segment_index = 0`, f.run.FileRevisionID).Scan(&source))
		assert.Equal(t, "latest-negative", source, "latest DB transaction must win despite its older caller time")
	})
}

func TestFACORECHG009LostAckReplayUsesNewLeaseButRejectsStaleCredentials(t *testing.T) {
	forEachFACORECHG009RepositoryBackend(t, func(t *testing.T, dialect Dialect) {
		f := newCHG009Fixture(t, 4, dialect)
		ctx := context.Background()
		lease1, err := f.repo.AcquireRunLease(ctx, f.run.ID, "old-worker", time.Minute)
		require.NoError(t, err)
		commit := chg009PresentCommit(f, f.run, lease1, "lost-ack", "old-worker", f.providers[0].ID, "stat", 0, 4, 4, 1, 0b0101)
		_, err = f.repo.CommitHealthChunk(ctx, commit)
		require.NoError(t, err)
		f.clock.now = f.now.Add(2 * time.Minute)
		lease2, err := f.repo.AcquireRunLease(ctx, f.run.ID, "new-worker", time.Minute)
		require.NoError(t, err)
		replay := chg009CloneCommit(commit)
		replay.LeaseOwner, replay.FencingToken = "new-worker", lease2.FencingToken
		after, replayErr := f.repo.CommitHealthChunk(ctx, replay)
		require.NoError(t, replayErr, "transport credentials are not committed payload identity")
		require.NotNil(t, after)
		assert.Equal(t, int64(2), after.ResolvedSegments)
		assert.Equal(t, int64(4), after.ProviderChecks)
		_, staleErr := f.repo.CommitHealthChunk(ctx, commit)
		assert.ErrorIs(t, staleErr, ErrStaleHealthLease, "fencing is checked before replay identity")
		assert.Equal(t, 1, chg009Count(t, f, `SELECT COUNT(*) FROM health_run_chunks WHERE id = ?`, commit.ChunkID))
		assert.Equal(t, 1, chg009Count(t, f, `SELECT COUNT(*) FROM health_provider_coverage WHERE source_chunk_id = ?`, commit.ChunkID))
	})
}

func TestFACORECHG009DigestCanonicalizesNestedTimeZones(t *testing.T) {
	forEachFACORECHG009RepositoryBackend(t, func(t *testing.T, dialect Dialect) {
		f := newCHG009Fixture(t, 4, dialect)
		ctx := context.Background()
		lease, err := f.repo.AcquireRunLease(ctx, f.run.ID, "worker", 10*time.Minute)
		require.NoError(t, err)
		zone := time.FixedZone("NPT", 5*60*60+45*60)
		commit := chg009PresentCommit(f, f.run, lease, "nested-time", "worker", f.providers[0].ID, "stat", 0, 2, 2, 1, 0b01)
		commit.PresentBitmap = []byte{0}
		commit.AbsentBitmap = []byte{0b01}
		commit.TemporaryBitmap = []byte{0b10}
		commit.MissingCandidatesDelta, commit.InconclusiveDelta = 1, 1
		commit.CommittedAt = f.now.Add(time.Minute).In(zone)
		commit.Attempts = []HealthAttemptEvidence{{IdempotencyKey: "nested-attempt", SegmentIndex: 0, Operation: "STAT", Outcome: "hard_absence", BodyValidation: "not_requested", ObservedAt: f.now.Add(2 * time.Minute).In(zone)}}
		commit.Confirmations = []HealthConfirmationEvent{{IdempotencyKey: "nested-confirm", SegmentIndex: 0, Cause: GapCauseAbsent, ObservedAt: f.now.Add(3 * time.Minute).In(zone)}}
		commit.Retry = &HealthRetryState{RetryKey: "nested-retry", SegmentStart: 1, SegmentCount: 1, Outcome: "temporary_failure", Attempt: 1, NextAttemptAt: f.now.Add(4 * time.Minute).In(zone)}
		original := chg009CloneCommit(commit)
		_, err = f.repo.CommitHealthChunk(ctx, commit)
		require.NoError(t, err)
		assert.Equal(t, original, commit, "digest canonicalization must not mutate caller state")
		replay := chg009CloneCommit(commit)
		replay.CommittedAt = replay.CommittedAt.UTC()
		replay.Attempts[0].ObservedAt = replay.Attempts[0].ObservedAt.UTC()
		replay.Confirmations[0].ObservedAt = replay.Confirmations[0].ObservedAt.UTC()
		replay.Retry.NextAttemptAt = replay.Retry.NextAttemptAt.UTC()
		_, replayErr := f.repo.CommitHealthChunk(ctx, replay)
		assert.NoError(t, replayErr, "equal instants in another location must have one digest")

		conflicts := []struct {
			name   string
			mutate func(*HealthChunkCommit)
		}{
			{name: "committed", mutate: func(c *HealthChunkCommit) { c.CommittedAt = c.CommittedAt.Add(time.Nanosecond) }},
			{name: "attempt", mutate: func(c *HealthChunkCommit) { c.Attempts[0].ObservedAt = c.Attempts[0].ObservedAt.Add(time.Nanosecond) }},
			{name: "confirmation", mutate: func(c *HealthChunkCommit) {
				c.Confirmations[0].ObservedAt = c.Confirmations[0].ObservedAt.Add(time.Nanosecond)
			}},
			{name: "retry", mutate: func(c *HealthChunkCommit) { c.Retry.NextAttemptAt = c.Retry.NextAttemptAt.Add(time.Nanosecond) }},
		}
		for _, test := range conflicts {
			t.Run(test.name, func(t *testing.T) {
				conflict := chg009CloneCommit(replay)
				test.mutate(&conflict)
				_, conflictErr := f.repo.CommitHealthChunk(ctx, conflict)
				assert.ErrorIs(t, conflictErr, ErrHealthChunkConflict,
					"a genuinely different payload instant still conflicts")
			})
		}
	})
}

func TestFACORECHG009LegacyNullResolvedBitmapFallback(t *testing.T) {
	forEachFACORECHG009RepositoryBackend(t, func(t *testing.T, dialect Dialect) {
		t.Run("zero delta is unambiguous", func(t *testing.T) {
			f := newCHG009Fixture(t, 8, dialect)
			chg009EnsureLegacyResolvedColumn(t, f)
			lease, err := f.repo.AcquireRunLease(context.Background(), f.run.ID, "worker", 10*time.Minute)
			require.NoError(t, err)
			legacy := chg009PresentCommit(f, f.run, lease, "legacy-zero", "worker", f.providers[0].ID, "stat", 0, 3, 3, 1, 0)
			_, err = f.repo.CommitHealthChunk(context.Background(), legacy)
			require.NoError(t, err)
			_, err = f.repo.db.ExecContext(context.Background(), `UPDATE health_run_chunks SET resolved_bitmap = NULL WHERE id = ?`, legacy.ChunkID)
			require.NoError(t, err)
			next := chg009PresentCommit(f, f.run, lease, "after-zero", "worker", f.providers[0].ID, "stat", 3, 1, 4, 1, 1)
			after, err := f.repo.CommitHealthChunk(context.Background(), next)
			require.NoError(t, err)
			assert.Equal(t, int64(1), after.ResolvedSegments)
		})

		t.Run("full conclusive delta is reconstructable", func(t *testing.T) {
			f := newCHG009Fixture(t, 8, dialect)
			chg009EnsureLegacyResolvedColumn(t, f)
			lease, err := f.repo.AcquireRunLease(context.Background(), f.run.ID, "worker", 10*time.Minute)
			require.NoError(t, err)
			legacy := chg009PresentCommit(f, f.run, lease, "legacy-full", "worker", f.providers[0].ID, "stat", 0, 3, 3, 1, 0b111)
			_, err = f.repo.CommitHealthChunk(context.Background(), legacy)
			require.NoError(t, err)
			_, err = f.repo.db.ExecContext(context.Background(), `UPDATE health_run_chunks SET resolved_bitmap = NULL WHERE id = ?`, legacy.ChunkID)
			require.NoError(t, err)
			next := chg009PresentCommit(f, f.run, lease, "after-full", "worker", f.providers[1].ID, "stat", 0, 4, 4, 2, 0b1001)
			after, err := f.repo.CommitHealthChunk(context.Background(), next)
			require.NoError(t, err)
			assert.Equal(t, int64(4), after.ResolvedSegments)
		})

		t.Run("intermediate delta is ambiguous and atomic", func(t *testing.T) {
			f := newCHG009Fixture(t, 8, dialect)
			chg009EnsureLegacyResolvedColumn(t, f)
			lease, err := f.repo.AcquireRunLease(context.Background(), f.run.ID, "worker", 10*time.Minute)
			require.NoError(t, err)
			legacy := chg009PresentCommit(f, f.run, lease, "legacy-ambiguous", "worker", f.providers[0].ID, "stat", 0, 3, 3, 1, 0b101)
			_, err = f.repo.CommitHealthChunk(context.Background(), legacy)
			require.NoError(t, err)
			_, err = f.repo.db.ExecContext(context.Background(), `UPDATE health_run_chunks SET resolved_bitmap = NULL WHERE id = ?`, legacy.ChunkID)
			require.NoError(t, err)
			next := chg009PresentCommit(f, f.run, lease, "after-ambiguous", "worker", f.providers[1].ID, "stat", 4, 4, 8, 2, 0b1111)
			_, err = f.repo.CommitHealthChunk(context.Background(), next)
			assert.Error(t, err)
			assert.Zero(t, chg009Count(t, f, `SELECT COUNT(*) FROM health_run_chunks WHERE id = ?`, next.ChunkID))
			assert.Zero(t, chg009Count(t, f, `SELECT COUNT(*) FROM health_provider_coverage WHERE source_chunk_id = ?`, next.ChunkID))
			retained, getErr := f.repo.GetHealthRun(context.Background(), f.run.ID)
			require.NoError(t, getErr)
			assert.Equal(t, int64(2), retained.ResolvedSegments)
			assert.Equal(t, int64(3), retained.ProviderChecks)
			assert.Equal(t, int64(3), retained.CursorSegment)
			assert.Equal(t, f.providers[0].ID, *retained.CurrentProviderID)
		})
	})
}
