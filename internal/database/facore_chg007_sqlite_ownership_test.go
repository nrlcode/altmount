package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newFACORECHG007SQLiteDB(t *testing.T) (*DB, *HealthStateRepository) {
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
	return db, NewHealthStateRepository(db.Connection(), DialectSQLite)
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

func TestFACORECHG007SQLiteExplicitTransactionsOwnTheWriterAtBegin(t *testing.T) {
	db, _ := newFACORECHG007SQLiteDB(t)
	ctx := context.Background()
	holderConn := facoreCHG007SQLiteConn(t, db)
	contenderConn := facoreCHG007SQLiteConn(t, db)
	_, err := contenderConn.ExecContext(ctx, `PRAGMA busy_timeout = 0`)
	require.NoError(t, err)

	// No statement precedes the second BeginTx. The first explicit transaction
	// must therefore own SQLite's single writer solely because it began.
	holder, err := holderConn.BeginTx(ctx, nil)
	require.NoError(t, err)
	contender, beginErr := contenderConn.BeginTx(ctx, nil)
	if beginErr == nil {
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
	_, err = retry.ExecContext(ctx,
		`INSERT INTO file_health (file_path, status) VALUES ('chg007/retry.mkv', 'pending')`)
	require.NoError(t, err)
	require.NoError(t, retry.Commit())
	require.NoError(t, db.Connection().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM file_health WHERE file_path = 'chg007/retry.mkv'`).Scan(&partial))
	assert.Equal(t, 1, partial)
}

type facoreCHG007BeginBarrier struct {
	mu   sync.Mutex
	next chan struct{}
}

func (b *facoreCHG007BeginBarrier) arm() <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.next != nil {
		panic("FACORE CHG-007 BeginTx barrier already armed")
	}
	b.next = make(chan struct{})
	return b.next
}

func (b *facoreCHG007BeginBarrier) signal() {
	b.mu.Lock()
	next := b.next
	b.next = nil
	b.mu.Unlock()
	if next != nil {
		close(next)
	}
}

type facoreCHG007SQLiteConnector struct {
	driver  *sqlite3.SQLiteDriver
	dsn     string
	barrier *facoreCHG007BeginBarrier
}

func (c *facoreCHG007SQLiteConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &facoreCHG007SQLiteBarrierConn{Conn: conn, barrier: c.barrier}, nil
}

func (c *facoreCHG007SQLiteConnector) Driver() driver.Driver { return c.driver }

type facoreCHG007SQLiteBarrierConn struct {
	driver.Conn
	barrier *facoreCHG007BeginBarrier
}

func (c *facoreCHG007SQLiteBarrierConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.barrier.signal()
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}

type facoreCHG007SQLiteFixture struct {
	db       *DB
	repo     *HealthStateRepository
	barrier  *facoreCHG007BeginBarrier
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

	barrier := &facoreCHG007BeginBarrier{}
	dsn := path + "?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=30000&_foreign_keys=on&_txlock=immediate"
	raw := sql.OpenDB(&facoreCHG007SQLiteConnector{
		driver: &sqlite3.SQLiteDriver{}, dsn: dsn, barrier: barrier,
	})
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
		db: db, repo: repo, barrier: barrier, revision: revision, snapshot: snapshot, now: now,
	}
}

func runFACORECHG007AfterBeginBlocked(
	t *testing.T,
	f facoreCHG007SQLiteFixture,
	operation func(context.Context) error,
) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	holderConn := facoreCHG007SQLiteConn(t, f.db)
	holder, err := holderConn.BeginTx(ctx, nil)
	require.NoError(t, err)
	attempted := f.barrier.arm()
	result := make(chan error, 1)
	go func() { result <- operation(ctx) }()
	select {
	case <-attempted:
	case <-ctx.Done():
		_ = holder.Rollback()
		t.Fatalf("operation did not reach BeginTx barrier: %v", ctx.Err())
	}
	select {
	case operationErr := <-result:
		_ = holder.Rollback()
		t.Fatalf("operation completed before writer release: %v", operationErr)
	default:
	}
	require.NoError(t, holder.Rollback())
	select {
	case operationErr := <-result:
		return operationErr
	case <-ctx.Done():
		t.Fatalf("operation did not finish after writer release: %v", ctx.Err())
		return ctx.Err()
	}
}

func TestFACORECHG007SQLiteReadBeforeWriteBoundariesHonorImmediateOwnership(t *testing.T) {
	t.Run("CreateHealthRun", func(t *testing.T) {
		f := newFACORECHG007ImmediateSQLiteFixture(t)
		spec := HealthRunSpec{
			ID: "chg007-contended-run", FileRevisionID: f.revision.ID,
			ProviderSnapshotID: f.snapshot.ID, Trigger: "manual", Mode: "observation",
			TotalSegments: f.revision.SegmentCount, CreatedAt: f.now.Add(time.Minute),
		}
		err := runFACORECHG007AfterBeginBlocked(t, f, func(ctx context.Context) error {
			_, err := f.repo.CreateHealthRun(ctx, spec)
			return err
		})
		require.NoError(t, err)
		var count int
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
		err = runFACORECHG007AfterBeginBlocked(t, f, func(ctx context.Context) error {
			_, err := f.repo.RecordSyntheticOutput(ctx, write)
			return err
		})
		require.NoError(t, err)
		state, err := f.repo.RecordSyntheticOutput(context.Background(), write)
		require.NoError(t, err, "successful synthetic output must remain replayable")
		assert.Equal(t, CacheRecoverySynthetic, state.Status)
		var ranges int
		require.NoError(t, f.db.Connection().QueryRow(
			`SELECT COUNT(*) FROM health_synthetic_ranges WHERE id = ?`, write.ID).Scan(&ranges))
		assert.Equal(t, 1, ranges)
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
		err = runFACORECHG007AfterBeginBlocked(t, f, func(ctx context.Context) error {
			_, err := f.repo.MarkSyntheticRangeRecovered(ctx, write.ID, recoveredAt)
			return err
		})
		require.NoError(t, err)
		state, err := f.repo.MarkSyntheticRangeRecovered(ctx, write.ID, recoveredAt.Add(time.Minute))
		require.NoError(t, err, "successful recovery must remain replayable")
		assert.Equal(t, CacheRecoveryPending, state.Status)
		var retained time.Time
		require.NoError(t, f.db.Connection().QueryRow(
			`SELECT recovered_at FROM health_synthetic_ranges WHERE id = ?`, write.ID).Scan(&retained))
		assert.True(t, retained.Equal(recoveredAt), "replay changed the first recovery time")
	})
}
