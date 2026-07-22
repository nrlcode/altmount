package database

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newFACORECHG007SQLiteDB(t *testing.T) *DB {
	t.Helper()
	db, err := NewDB(Config{
		Type:         "sqlite",
		DatabasePath: filepath.Join(t.TempDir(), "facore-chg-007.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	var journalMode string
	require.NoError(t, db.Connection().QueryRow(`PRAGMA journal_mode`).Scan(&journalMode))
	require.Equal(t, "wal", journalMode)
	return db
}

func facoreCHG007SQLiteConn(t *testing.T, db *DB) *sql.Conn {
	t.Helper()
	conn, err := db.Connection().Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return conn
}

func requireFACORECHG007SQLiteBusy(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var sqliteErr sqlite3.Error
	require.True(t, errors.As(err, &sqliteErr), "expected SQLite lock error, got %v", err)
	assert.Equal(t, sqlite3.ErrBusy, sqliteErr.Code)
}

func requireFACORECHG007SQLiteBeginBusy(t *testing.T, err error) {
	t.Helper()
	require.ErrorContains(t, err, "begin health state transaction")
	requireFACORECHG007SQLiteBusy(t, err)
}

func TestFACORECHG007SQLiteExplicitTransactionsOwnTheWriterAtBegin(t *testing.T) {
	db := newFACORECHG007SQLiteDB(t)
	ctx := context.Background()
	holderConn := facoreCHG007SQLiteConn(t, db)
	contenderConn := facoreCHG007SQLiteConn(t, db)
	_, err := contenderConn.ExecContext(ctx, `PRAGMA busy_timeout = 0`)
	require.NoError(t, err)

	// No statement precedes the second BeginTx. The first explicit transaction
	// must therefore own SQLite's single writer solely because it began.
	holder, err := holderConn.BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = holder.Rollback() })
	contender, beginErr := contenderConn.BeginTx(ctx, nil)
	if beginErr == nil {
		t.Cleanup(func() { _ = contender.Rollback() })
		require.NoError(t, contender.Rollback())
		t.Errorf("a second explicit transaction began before the first released write ownership")
	} else {
		requireFACORECHG007SQLiteBusy(t, beginErr)
	}

	var partial int
	require.NoError(t, db.Connection().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM file_health WHERE file_path = 'chg007/retry.mkv'`).Scan(&partial))
	assert.Zero(t, partial, "bounded writer contention must not publish partial state")
	require.NoError(t, holder.Rollback())

	retry, err := contenderConn.BeginTx(ctx, nil)
	require.NoError(t, err, "the bounded contender must be retryable after writer release")
	t.Cleanup(func() { _ = retry.Rollback() })
	_, err = retry.ExecContext(ctx,
		`INSERT INTO file_health (file_path, status) VALUES ('chg007/retry.mkv', 'pending')`)
	require.NoError(t, err)
	require.NoError(t, retry.Commit())
	require.NoError(t, db.Connection().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM file_health WHERE file_path = 'chg007/retry.mkv'`).Scan(&partial))
	assert.Equal(t, 1, partial)
}

type facoreCHG007SQLiteFixture struct {
	db       *DB
	repo     *HealthStateRepository
	revision *HealthFileRevision
	snapshot *ProviderSnapshot
	now      time.Time
}

func newFACORECHG007ImmediateSQLiteFixture(t *testing.T) facoreCHG007SQLiteFixture {
	t.Helper()
	path := filepath.Join(t.TempDir(), "facore-chg-007-immediate.db")
	migrated, err := NewDB(Config{Type: "sqlite", DatabasePath: path})
	require.NoError(t, err)
	require.NoError(t, migrated.Close())

	// The zero busy timeout makes the bounded contention result immediate and
	// deterministic while retaining the production candidate's transaction mode.
	dsn := path + "?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=0&_foreign_keys=on&_txlock=immediate"
	raw, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)
	raw.SetMaxOpenConns(8)
	require.NoError(t, raw.Ping())
	db := &DB{conn: raw, dialect: dialectHelper{d: DialectSQLite}}
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	repo := NewHealthStateRepository(raw, DialectSQLite)
	ctx := context.Background()
	revision, err := repo.EnsureFileRevision(ctx, FileRevisionSpec{
		FilePath: "chg007/ownership.mkv", LayoutFingerprint: "sha256:chg007-ownership",
		VirtualSize: 800, SegmentCount: 8,
	})
	require.NoError(t, err)
	now := time.Unix(1_700_000_000, 0).UTC()
	snapshot, err := repo.CaptureActiveProviderSnapshot(ctx, now)
	require.NoError(t, err)
	return facoreCHG007SQLiteFixture{
		db: db, repo: repo, revision: revision, snapshot: snapshot, now: now,
	}
}

func holdFACORECHG007SQLiteWriter(t *testing.T, db *DB) *sql.Tx {
	t.Helper()
	holderConn := facoreCHG007SQLiteConn(t, db)
	holder, err := holderConn.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = holder.Rollback() })
	return holder
}

func TestFACORECHG007SQLiteReadBeforeWriteBoundariesHonorImmediateOwnership(t *testing.T) {
	t.Run("CreateHealthRun", func(t *testing.T) {
		f := newFACORECHG007ImmediateSQLiteFixture(t)
		ctx := context.Background()
		spec := HealthRunSpec{
			ID: "chg007-contended-run", FileRevisionID: f.revision.ID,
			ProviderSnapshotID: f.snapshot.ID, Trigger: "manual", Mode: "observation",
			TotalSegments: f.revision.SegmentCount, CreatedAt: f.now.Add(time.Minute),
		}
		holder := holdFACORECHG007SQLiteWriter(t, f.db)
		_, err := f.repo.CreateHealthRun(ctx, spec)
		requireFACORECHG007SQLiteBeginBusy(t, err)
		var count int
		require.NoError(t, f.db.Connection().QueryRow(
			`SELECT COUNT(*) FROM health_runs WHERE id = ?`, spec.ID).Scan(&count))
		assert.Zero(t, count, "failed creation must not publish a partial run")
		require.NoError(t, holder.Rollback())

		_, err = f.repo.CreateHealthRun(ctx, spec)
		require.NoError(t, err, "creation must succeed when retried after writer release")
		require.NoError(t, f.db.Connection().QueryRow(
			`SELECT COUNT(*) FROM health_runs WHERE id = ?`, spec.ID).Scan(&count))
		assert.Equal(t, 1, count)
	})

	t.Run("RecordSyntheticOutput", func(t *testing.T) {
		f := newFACORECHG007ImmediateSQLiteFixture(t)
		gap, err := f.repo.UpsertGapRange(context.Background(), GapRangeWrite{
			ID: "chg007-gap", FileRevisionID: f.revision.ID, Kind: GapKindProvisional,
			StartSegment: 0, SegmentCount: 1, Status: GapStatusActive, CreatedAt: f.now,
		})
		require.NoError(t, err)
		write := SyntheticOutputWrite{
			ID: "chg007-synthetic", GapID: gap.ID, FileRevisionID: f.revision.ID,
			ByteStart: 0, ByteEnd: 99, EmittedAt: f.now.Add(time.Minute),
		}
		holder := holdFACORECHG007SQLiteWriter(t, f.db)
		_, err = f.repo.RecordSyntheticOutput(context.Background(), write)
		requireFACORECHG007SQLiteBeginBusy(t, err)
		var ranges, recovery int
		require.NoError(t, f.db.Connection().QueryRow(
			`SELECT COUNT(*) FROM health_synthetic_ranges WHERE id = ?`, write.ID).Scan(&ranges))
		require.NoError(t, f.db.Connection().QueryRow(
			`SELECT COUNT(*) FROM health_cache_recovery WHERE file_revision_id = ?`, write.FileRevisionID).Scan(&recovery))
		assert.Zero(t, ranges, "failed synthetic output must not publish its range")
		assert.Zero(t, recovery, "failed synthetic output must not publish cache state")
		require.NoError(t, holder.Rollback())

		state, err := f.repo.RecordSyntheticOutput(context.Background(), write)
		require.NoError(t, err, "synthetic output must succeed when retried after writer release")
		assert.Equal(t, CacheRecoverySynthetic, state.Status)
		state, err = f.repo.RecordSyntheticOutput(context.Background(), write)
		require.NoError(t, err, "successful synthetic output must remain replayable")
		assert.Equal(t, CacheRecoverySynthetic, state.Status)
		require.NoError(t, f.db.Connection().QueryRow(
			`SELECT COUNT(*) FROM health_synthetic_ranges WHERE id = ?`, write.ID).Scan(&ranges))
		require.NoError(t, f.db.Connection().QueryRow(
			`SELECT COUNT(*) FROM health_cache_recovery WHERE file_revision_id = ?`, write.FileRevisionID).Scan(&recovery))
		assert.Equal(t, 1, ranges)
		assert.Equal(t, 1, recovery)
	})

	t.Run("MarkSyntheticRangeRecovered", func(t *testing.T) {
		f := newFACORECHG007ImmediateSQLiteFixture(t)
		ctx := context.Background()
		gap, err := f.repo.UpsertGapRange(ctx, GapRangeWrite{
			ID: "chg007-recovery-gap", FileRevisionID: f.revision.ID, Kind: GapKindConfirmedAbsent,
			StartSegment: 1, SegmentCount: 1, Status: GapStatusActive, CreatedAt: f.now,
		})
		require.NoError(t, err)
		write := SyntheticOutputWrite{
			ID: "chg007-recovery-range", GapID: gap.ID, FileRevisionID: f.revision.ID,
			ByteStart: 100, ByteEnd: 199, EmittedAt: f.now.Add(time.Minute),
		}
		_, err = f.repo.RecordSyntheticOutput(ctx, write)
		require.NoError(t, err)
		recoveredAt := f.now.Add(2 * time.Minute)
		holder := holdFACORECHG007SQLiteWriter(t, f.db)
		_, err = f.repo.MarkSyntheticRangeRecovered(ctx, write.ID, recoveredAt)
		requireFACORECHG007SQLiteBeginBusy(t, err)
		var retained *time.Time
		require.NoError(t, f.db.Connection().QueryRow(
			`SELECT recovered_at FROM health_synthetic_ranges WHERE id = ?`, write.ID).Scan(&retained))
		assert.Nil(t, retained, "failed recovery must not mark the synthetic range")
		state, err := f.repo.GetCacheRecoveryState(ctx, write.FileRevisionID)
		require.NoError(t, err)
		require.NotNil(t, state)
		assert.Equal(t, CacheRecoverySynthetic, state.Status,
			"failed recovery must not publish pending cache state")
		require.NoError(t, holder.Rollback())

		state, err = f.repo.MarkSyntheticRangeRecovered(ctx, write.ID, recoveredAt)
		require.NoError(t, err, "recovery must succeed when retried after writer release")
		assert.Equal(t, CacheRecoveryPending, state.Status)
		state, err = f.repo.MarkSyntheticRangeRecovered(ctx, write.ID, recoveredAt.Add(time.Minute))
		require.NoError(t, err, "successful recovery must remain replayable")
		assert.Equal(t, CacheRecoveryPending, state.Status)
		require.NoError(t, f.db.Connection().QueryRow(
			`SELECT recovered_at FROM health_synthetic_ranges WHERE id = ?`, write.ID).Scan(&retained))
		require.NotNil(t, retained)
		assert.True(t, retained.Equal(recoveredAt), "replay changed the first recovery time")
	})
}
