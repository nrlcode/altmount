package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// facoreCHG007ReconcileBarrier adapts to both sides of the CHG-007 change.
//
// Before the correction there is no table lock, so matching identity lookups
// are released in pairs and both successful commits are made visible before
// either caller can perform its post-commit return read. This deterministically
// exposes both the duplicate-active-provider race and the stale return value.
//
// After the correction, the first identity lookup is held until the other
// reconcile has attempted the self-conflicting PostgreSQL table lock. The
// first reconcile can then commit, allowing the second to continue from the
// newly committed registry state.
type facoreCHG007ReconcileBarrier struct {
	mu sync.Mutex

	armed bool
	done  <-chan struct{}

	lockAttempts    int
	identityResults int
	commits         int

	secondLockAttempt chan struct{}
	identityPairReady chan struct{}
	commitPairReady   chan struct{}
	secondLockClosed  bool
	commitPairClosed  bool
}

func newFACORECHG007ReconcileBarrier() *facoreCHG007ReconcileBarrier {
	return &facoreCHG007ReconcileBarrier{
		secondLockAttempt: make(chan struct{}),
		identityPairReady: make(chan struct{}),
		commitPairReady:   make(chan struct{}),
	}
}

func (b *facoreCHG007ReconcileBarrier) arm(done <-chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.done = done
	b.armed = true
}

func (b *facoreCHG007ReconcileBarrier) beforeLock() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.armed {
		return
	}
	b.lockAttempts++
	if b.lockAttempts == 2 && !b.secondLockClosed {
		close(b.secondLockAttempt)
		b.secondLockClosed = true
	}
}

func (b *facoreCHG007ReconcileBarrier) afterIdentityResult() {
	b.mu.Lock()
	if !b.armed {
		b.mu.Unlock()
		return
	}
	b.identityResults++
	ordinal := b.identityResults
	lockAware := b.lockAttempts > 0
	pairReady := b.identityPairReady
	if !lockAware && ordinal%2 == 0 {
		close(pairReady)
		b.identityPairReady = make(chan struct{})
	}
	done := b.done
	b.mu.Unlock()

	if lockAware {
		if ordinal != 1 {
			return
		}
		select {
		case <-b.secondLockAttempt:
		case <-done:
		}
		return
	}

	select {
	case <-pairReady:
	case <-done:
	}
}

func (b *facoreCHG007ReconcileBarrier) afterCommit() {
	b.mu.Lock()
	if !b.armed {
		b.mu.Unlock()
		return
	}
	b.commits++
	if b.commits == 2 && !b.commitPairClosed {
		close(b.commitPairReady)
		b.commitPairClosed = true
	}
	done := b.done
	b.mu.Unlock()

	select {
	case <-b.commitPairReady:
	case <-done:
	}
}

func (b *facoreCHG007ReconcileBarrier) stats() (locks, identityResults int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lockAttempts, b.identityResults
}

func (b *facoreCHG007ReconcileBarrier) beforeExec(query string) {
	if facoreCHG007IsReconcileLock(query) {
		b.beforeLock()
	}
}

func (b *facoreCHG007ReconcileBarrier) afterQueryResult(query string) {
	if facoreCHG007IsIdentityLookup(query) {
		b.afterIdentityResult()
	}
}

type facoreCHG007DriverHooks interface {
	beforeExec(query string)
	afterQueryResult(query string)
	afterCommit()
}

type facoreCHG007BarrierDriver struct {
	driver.Driver
	hooks facoreCHG007DriverHooks
}

func (d *facoreCHG007BarrierDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.Driver.Open(name)
	if err != nil {
		return nil, err
	}
	return &facoreCHG007BarrierConn{Conn: conn, hooks: d.hooks}, nil
}

type facoreCHG007BarrierConn struct {
	driver.Conn
	hooks facoreCHG007DriverHooks
}

func (c *facoreCHG007BarrierConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	beginner, ok := c.Conn.(driver.ConnBeginTx)
	if !ok {
		return nil, driver.ErrSkip
	}
	tx, err := beginner.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &facoreCHG007BarrierTx{Tx: tx, hooks: c.hooks}, nil
}

func (c *facoreCHG007BarrierConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	preparer, ok := c.Conn.(driver.ConnPrepareContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return preparer.PrepareContext(ctx, query)
}

func (c *facoreCHG007BarrierConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.hooks.beforeExec(query)
	execer, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return execer.ExecContext(ctx, query, args)
}

func (c *facoreCHG007BarrierConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryer, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	rows, err := queryer.QueryContext(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return &facoreCHG007BarrierRows{Rows: rows, hooks: c.hooks, query: query}, nil
}

type facoreCHG007BarrierTx struct {
	driver.Tx
	hooks facoreCHG007DriverHooks
}

func (tx *facoreCHG007BarrierTx) Commit() error {
	err := tx.Tx.Commit()
	if err == nil {
		tx.hooks.afterCommit()
	}
	return err
}

type facoreCHG007BarrierRows struct {
	driver.Rows
	hooks facoreCHG007DriverHooks
	query string
	once  sync.Once
}

func (r *facoreCHG007BarrierRows) Next(dest []driver.Value) error {
	err := r.Rows.Next(dest)
	if err == nil || errors.Is(err, io.EOF) {
		r.once.Do(func() { r.hooks.afterQueryResult(r.query) })
	}
	return err
}

type facoreCHG007QueryPairBarrier struct {
	mu sync.Mutex

	fragments []string
	armed     bool
	done      <-chan struct{}
	hits      int
	ready     chan struct{}
	closed    bool
}

func newFACORECHG007QueryPairBarrier(fragments ...string) *facoreCHG007QueryPairBarrier {
	for i := range fragments {
		fragments[i] = strings.ToUpper(fragments[i])
	}
	return &facoreCHG007QueryPairBarrier{fragments: fragments, ready: make(chan struct{})}
}

func (b *facoreCHG007QueryPairBarrier) arm(done <-chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.armed = true
	b.done = done
}

func (b *facoreCHG007QueryPairBarrier) beforeExec(string) {}

func (b *facoreCHG007QueryPairBarrier) afterCommit() {}

func (b *facoreCHG007QueryPairBarrier) afterQueryResult(query string) {
	normalized := facoreCHG007NormalizedSQL(query)
	for _, fragment := range b.fragments {
		if !strings.Contains(normalized, fragment) {
			return
		}
	}

	b.mu.Lock()
	if !b.armed {
		b.mu.Unlock()
		return
	}
	b.hits++
	if b.hits == 2 && !b.closed {
		close(b.ready)
		b.closed = true
	}
	ready := b.ready
	done := b.done
	b.mu.Unlock()

	select {
	case <-ready:
	case <-done:
	}
}

func (b *facoreCHG007QueryPairBarrier) hitCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.hits
}

func facoreCHG007NormalizedSQL(query string) string {
	return strings.ToUpper(strings.Join(strings.Fields(query), " "))
}

func facoreCHG007IsReconcileLock(query string) bool {
	return strings.Contains(
		facoreCHG007NormalizedSQL(query),
		"LOCK TABLE HEALTH_PROVIDERS IN SHARE ROW EXCLUSIVE MODE",
	)
}

func facoreCHG007IsIdentityLookup(query string) bool {
	normalized := facoreCHG007NormalizedSQL(query)
	return strings.Contains(normalized, "SELECT DISTINCT PROVIDER_ID") &&
		strings.Contains(normalized, "FROM HEALTH_PROVIDER_GENERATIONS") &&
		strings.Contains(normalized, "IDENTITY_FINGERPRINT")
}

func facoreCHG007OpenHookedPostgresRepositories(
	t *testing.T,
	ctx context.Context,
	dsn string,
	hooks facoreCHG007DriverHooks,
) (*HealthStateRepository, *HealthStateRepository) {
	t.Helper()
	driverName := "facore-chg007-postgres-" + uuid.NewString()
	sql.Register(driverName, &facoreCHG007BarrierDriver{
		Driver: stdlib.GetDefaultDriver(),
		hooks:  hooks,
	})
	open := func() *sql.DB {
		db, err := sql.Open(driverName, dsn)
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		require.NoError(t, db.PingContext(ctx))
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		return db
	}
	return NewHealthStateRepository(open(), DialectPostgres),
		NewHealthStateRepository(open(), DialectPostgres)
}

type facoreCHG007ConcurrentResult[T any] struct {
	caller int
	value  T
	err    error
}

func facoreCHG007RunConcurrentPair[T any](
	t *testing.T,
	ctx context.Context,
	calls [2]func() (T, error),
) ([2]T, [2]error) {
	t.Helper()
	ready := make(chan struct{}, len(calls))
	start := make(chan struct{})
	results := make(chan facoreCHG007ConcurrentResult[T], len(calls))
	for caller, call := range calls {
		go func() {
			ready <- struct{}{}
			<-start
			value, err := call()
			results <- facoreCHG007ConcurrentResult[T]{caller: caller, value: value, err: err}
		}()
	}
	for range calls {
		<-ready
	}
	close(start)

	var values [2]T
	var errs [2]error
	for range calls {
		select {
		case result := <-results:
			values[result.caller] = result.value
			errs[result.caller] = result.err
		case <-ctx.Done():
			t.Fatalf("concurrent PostgreSQL control did not finish: %v", ctx.Err())
		}
	}
	return values, errs
}

type facoreCHG007PostgresControlFixture struct {
	admin    *DB
	repo     *HealthStateRepository
	repoA    *HealthStateRepository
	repoB    *HealthStateRepository
	revision *HealthFileRevision
	snapshot *ProviderSnapshot
	barrier  *facoreCHG007QueryPairBarrier
	now      time.Time
	suffix   string
}

func newFACORECHG007PostgresControlFixture(
	t *testing.T,
	ctx context.Context,
	dsn string,
	barrierFragments ...string,
) facoreCHG007PostgresControlFixture {
	t.Helper()
	admin, err := NewDB(Config{Type: "postgres", DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close()) })
	truncate := func() error {
		_, truncateErr := admin.Connection().Exec(`
			TRUNCATE TABLE file_health, health_provider_snapshots, health_providers CASCADE
		`)
		return truncateErr
	}
	require.NoError(t, truncate())
	t.Cleanup(func() { assert.NoError(t, truncate()) })

	suffix := uuid.NewString()
	now := time.Unix(1_700_200_000, 0).UTC()
	repo := NewHealthStateRepository(admin.Connection(), DialectPostgres)
	revision, err := repo.EnsureFileRevision(ctx, FileRevisionSpec{
		FilePath:          "chg007/postgres-control-" + suffix + ".mkv",
		LayoutFingerprint: "sha256:chg007-postgres-control-" + suffix,
		VirtualSize:       800,
		SegmentCount:      8,
	})
	require.NoError(t, err)
	snapshot, err := repo.CaptureActiveProviderSnapshot(ctx, now)
	require.NoError(t, err)
	barrier := newFACORECHG007QueryPairBarrier(barrierFragments...)
	repoA, repoB := facoreCHG007OpenHookedPostgresRepositories(t, ctx, dsn, barrier)
	barrier.arm(ctx.Done())
	return facoreCHG007PostgresControlFixture{
		admin: admin, repo: repo, repoA: repoA, repoB: repoB,
		revision: revision, snapshot: snapshot, barrier: barrier, now: now, suffix: suffix,
	}
}

type facoreCHG007ReconcileResult struct {
	caller    string
	providers []HealthProvider
	err       error
}

type facoreCHG007ProviderConfiguration struct {
	displayName string
	role        ProviderRole
	order       int
	identity    string
}

func TestFACORECHG007PostgresConcurrentReconcileIsSerialized(t *testing.T) {
	dsn := os.Getenv("ALTMOUNT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("ALTMOUNT_TEST_POSTGRES_DSN is not configured")
	}

	for _, test := range []struct {
		name           string
		seedTombstoned bool
	}{
		{name: "empty_registry"},
		{name: "all_tombstoned_registry", seedTombstoned: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			testFACORECHG007PostgresConcurrentReconcile(t, dsn, test.seedTombstoned)
		})
	}
}

func testFACORECHG007PostgresConcurrentReconcile(t *testing.T, dsn string, seedTombstoned bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	admin, err := NewDB(Config{Type: "postgres", DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close()) })
	_, err = admin.Connection().ExecContext(ctx, `TRUNCATE TABLE health_providers CASCADE`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, cleanupErr := admin.Connection().Exec(`TRUNCATE TABLE health_providers CASCADE`)
		assert.NoError(t, cleanupErr)
	})

	suffix := uuid.NewString()
	if seedTombstoned {
		facoreCHG007SeedTombstonedProvider(t, ctx, admin.Connection(), suffix)
	}

	barrier := newFACORECHG007ReconcileBarrier()
	driverName := fmt.Sprintf("facore-chg007-postgres-%p", barrier)
	sql.Register(driverName, &facoreCHG007BarrierDriver{
		Driver: stdlib.GetDefaultDriver(),
		hooks:  barrier,
	})

	dbA, err := sql.Open(driverName, dsn)
	require.NoError(t, err)
	dbA.SetMaxOpenConns(1)
	dbA.SetMaxIdleConns(1)
	t.Cleanup(func() { require.NoError(t, dbA.Close()) })
	dbB, err := sql.Open(driverName, dsn)
	require.NoError(t, err)
	dbB.SetMaxOpenConns(1)
	dbB.SetMaxIdleConns(1)
	t.Cleanup(func() { require.NoError(t, dbB.Close()) })
	require.NoError(t, dbA.PingContext(ctx))
	require.NoError(t, dbB.PingContext(ctx))

	repoA := NewHealthStateRepository(dbA, DialectPostgres)
	repoB := NewHealthStateRepository(dbB, DialectPostgres)
	endpointX := "concurrent-x-" + suffix + ".invalid"
	accountX := "account-x-" + suffix
	endpointY := "concurrent-y-" + suffix + ".invalid"
	accountY := "account-y-" + suffix
	displayAX := "caller-a-x-" + suffix
	displayAY := "caller-a-y-" + suffix
	displayBX := "caller-b-x-" + suffix
	displayBY := "caller-b-y-" + suffix
	identityX, err := ProviderIdentityFingerprint(endpointX, 563, accountX)
	require.NoError(t, err)
	identityY, err := ProviderIdentityFingerprint(endpointY, 563, accountY)
	require.NoError(t, err)
	specA := []ProviderSpec{{
		DisplayName: displayAX, Endpoint: endpointX, Port: 563, Account: accountX,
		Role: ProviderRolePrimary, Order: 0,
	}, {
		DisplayName: displayAY, Endpoint: endpointY, Port: 563, Account: accountY,
		Role: ProviderRoleBackup, Order: 1,
	}}
	specB := []ProviderSpec{{
		DisplayName: displayBY, Endpoint: endpointY, Port: 563, Account: accountY,
		Role: ProviderRolePrimary, Order: 0,
	}, {
		DisplayName: displayBX, Endpoint: endpointX, Port: 563, Account: accountX,
		Role: ProviderRoleBackup, Order: 1,
	}}
	wantA := []facoreCHG007ProviderConfiguration{
		{displayName: displayAX, role: ProviderRolePrimary, order: 0, identity: identityX},
		{displayName: displayAY, role: ProviderRoleBackup, order: 1, identity: identityY},
	}
	wantB := []facoreCHG007ProviderConfiguration{
		{displayName: displayBY, role: ProviderRolePrimary, order: 0, identity: identityY},
		{displayName: displayBX, role: ProviderRoleBackup, order: 1, identity: identityX},
	}

	barrier.arm(ctx.Done())
	ready := make(chan struct{}, 2)
	start := make(chan struct{})
	results := make(chan facoreCHG007ReconcileResult, 2)
	invoke := func(caller string, repo *HealthStateRepository, specs []ProviderSpec) {
		ready <- struct{}{}
		<-start
		providers, reconcileErr := repo.ReconcileProviders(ctx, specs)
		results <- facoreCHG007ReconcileResult{caller: caller, providers: providers, err: reconcileErr}
	}
	go invoke("a", repoA, specA)
	go invoke("b", repoB, specB)
	<-ready
	<-ready
	close(start)

	byCaller := make(map[string]facoreCHG007ReconcileResult, 2)
	for range 2 {
		select {
		case result := <-results:
			byCaller[result.caller] = result
		case <-ctx.Done():
			t.Fatalf("concurrent reconcile did not finish: %v", ctx.Err())
		}
	}

	for caller, want := range map[string][]facoreCHG007ProviderConfiguration{"a": wantA, "b": wantB} {
		result, ok := byCaller[caller]
		require.True(t, ok, "missing result for caller %s", caller)
		require.NoError(t, result.err, "caller %s", caller)
		if assert.Len(t, result.providers, len(want), "caller %s must observe only its complete transaction-local configuration", caller) {
			for i, provider := range result.providers {
				assert.Equal(t, want[i].displayName, provider.DisplayName)
				assert.Equal(t, want[i].role, provider.Role)
				assert.Equal(t, want[i].order, provider.Order)
				assert.True(t, provider.Active)
			}
		}
	}

	lockAttempts, identityResults := barrier.stats()
	assert.Equal(t, 2, lockAttempts, "every reconciliation must acquire the PostgreSQL serialization boundary")
	assert.GreaterOrEqual(t, identityResults, 4, "both reconciliations must resolve both durable identities")

	type activeProvider struct {
		providerID  string
		displayName string
		role        ProviderRole
		generation  int64
		order       int
		identity    string
	}
	rows, err := admin.Connection().QueryContext(ctx, `
		SELECT p.id, p.display_name, p.role, p.current_generation, p.configured_order,
		       g.identity_fingerprint
		FROM health_providers p
		JOIN health_provider_generations g
		  ON g.provider_id = p.id AND g.generation = p.current_generation
		WHERE p.active = TRUE
		ORDER BY p.configured_order, p.id
	`)
	require.NoError(t, err)
	defer rows.Close()
	var active []activeProvider
	for rows.Next() {
		var provider activeProvider
		require.NoError(t, rows.Scan(&provider.providerID, &provider.displayName,
			&provider.role, &provider.generation, &provider.order, &provider.identity))
		active = append(active, provider)
	}
	require.NoError(t, rows.Err())
	if assert.Len(t, active, 2, "concurrent reconciliation must publish one complete winner, never a hybrid or union") {
		actual := make([]facoreCHG007ProviderConfiguration, len(active))
		for i, provider := range active {
			actual[i] = facoreCHG007ProviderConfiguration{
				displayName: provider.displayName,
				role:        provider.role,
				order:       provider.order,
				identity:    provider.identity,
			}
		}
		switch actual[0].displayName {
		case displayAX:
			assert.Equal(t, wantA, actual, "the final registry must be all of caller A")
		case displayBY:
			assert.Equal(t, wantB, actual, "the final registry must be all of caller B")
		default:
			assert.Fail(t, "the final registry does not belong to either complete caller configuration", "providers: %#v", actual)
		}
	}

	snapshot, err := repoA.CaptureActiveProviderSnapshot(ctx, time.Now().UTC())
	require.NoError(t, err, "the final winner must form a valid frozen dispatch snapshot")
	t.Cleanup(func() {
		_, cleanupErr := admin.Connection().Exec(`DELETE FROM health_provider_snapshots WHERE id = $1`, snapshot.ID)
		assert.NoError(t, cleanupErr)
	})
	if assert.Len(t, snapshot.Entries, 2, "the frozen snapshot must contain exactly the complete final winner") && len(active) == 2 {
		for i, entry := range snapshot.Entries {
			assert.Equal(t, active[i].providerID, entry.ProviderID)
			assert.Equal(t, active[i].generation, entry.ProviderGeneration)
			assert.Equal(t, active[i].role, entry.Role)
			assert.Equal(t, active[i].order, entry.Order)
		}
	}
}

func TestFACORECHG007PostgresOrdinaryConcurrentTransactionsRemainCoherent(t *testing.T) {
	dsn := os.Getenv("ALTMOUNT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("ALTMOUNT_TEST_POSTGRES_DSN is not configured")
	}

	t.Run("CreateHealthRun", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		f := newFACORECHG007PostgresControlFixture(t, ctx, dsn,
			"SELECT SEGMENT_COUNT FROM HEALTH_FILE_REVISIONS", "WHERE ID =")
		specs := [2]HealthRunSpec{
			{
				ID: "control-run-a-" + f.suffix, FileRevisionID: f.revision.ID,
				ProviderSnapshotID: f.snapshot.ID, Trigger: "manual", Mode: "observation",
				TotalSegments: f.revision.SegmentCount, CreatedAt: f.now.Add(time.Minute),
			},
			{
				ID: "control-run-b-" + f.suffix, FileRevisionID: f.revision.ID,
				ProviderSnapshotID: f.snapshot.ID, Trigger: "scheduled", Mode: "observation",
				TotalSegments: f.revision.SegmentCount, CreatedAt: f.now.Add(time.Minute),
			},
		}
		values, errs := facoreCHG007RunConcurrentPair(t, ctx, [2]func() (*HealthRun, error){
			func() (*HealthRun, error) { return f.repoA.CreateHealthRun(ctx, specs[0]) },
			func() (*HealthRun, error) { return f.repoB.CreateHealthRun(ctx, specs[1]) },
		})
		for i := range errs {
			require.NoError(t, errs[i], "ordinary concurrent run creation %d", i)
			require.NotNil(t, values[i])
			assert.Equal(t, specs[i].ID, values[i].ID)
			assert.Equal(t, specs[i].Trigger, values[i].Trigger)
			assert.Equal(t, HealthRunPending, values[i].Status)
		}
		assert.Equal(t, 2, f.barrier.hitCount(), "both callers must reach the internal pre-write read barrier")
		var count int
		require.NoError(t, f.admin.Connection().QueryRowContext(ctx,
			`SELECT COUNT(*) FROM health_runs WHERE file_revision_id = $1`, f.revision.ID).Scan(&count))
		assert.Equal(t, 2, count)
		for i, spec := range specs {
			retained, err := f.repo.GetHealthRun(ctx, spec.ID)
			require.NoError(t, err)
			require.NotNil(t, retained)
			assert.Equal(t, values[i].ID, retained.ID)
			assert.Equal(t, values[i].Trigger, retained.Trigger)
			assert.Equal(t, values[i].TotalSegments, retained.TotalSegments)
			assert.Equal(t, HealthRunPending, retained.Status)
		}
	})

	t.Run("RecordSyntheticOutput", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		f := newFACORECHG007PostgresControlFixture(t, ctx, dsn,
			"SELECT G.FILE_REVISION_ID, R.VIRTUAL_SIZE", "FROM HEALTH_GAP_RANGES G")
		gap, err := f.repo.UpsertGapRange(ctx, GapRangeWrite{
			ID: "control-gap-" + f.suffix, FileRevisionID: f.revision.ID,
			Kind: GapKindProvisional, StartSegment: 0, SegmentCount: 2,
			Status: GapStatusActive, CreatedAt: f.now,
		})
		require.NoError(t, err)
		emittedAt := f.now.Add(time.Minute)
		writes := [2]SyntheticOutputWrite{
			{
				ID: "control-synthetic-a-" + f.suffix, GapID: gap.ID,
				FileRevisionID: f.revision.ID, ByteStart: 0, ByteEnd: 99, EmittedAt: emittedAt,
			},
			{
				ID: "control-synthetic-b-" + f.suffix, GapID: gap.ID,
				FileRevisionID: f.revision.ID, ByteStart: 100, ByteEnd: 199, EmittedAt: emittedAt,
			},
		}
		values, errs := facoreCHG007RunConcurrentPair(t, ctx, [2]func() (*CacheRecoveryState, error){
			func() (*CacheRecoveryState, error) { return f.repoA.RecordSyntheticOutput(ctx, writes[0]) },
			func() (*CacheRecoveryState, error) { return f.repoB.RecordSyntheticOutput(ctx, writes[1]) },
		})
		for i := range errs {
			require.NoError(t, errs[i], "ordinary concurrent synthetic output %d", i)
			require.NotNil(t, values[i])
			assert.Equal(t, CacheRecoverySynthetic, values[i].Status)
			assert.Equal(t, int64(0), values[i].ContentRevision)
			assert.True(t, values[i].UpdatedAt.Equal(emittedAt))
		}
		assert.Equal(t, 2, f.barrier.hitCount(), "both callers must reach the internal pre-write read barrier")
		var ranges int
		require.NoError(t, f.admin.Connection().QueryRowContext(ctx,
			`SELECT COUNT(*) FROM health_synthetic_ranges WHERE gap_id = $1`, gap.ID).Scan(&ranges))
		assert.Equal(t, 2, ranges)
		for _, write := range writes {
			var byteStart, byteEnd int64
			var retainedAt time.Time
			require.NoError(t, f.admin.Connection().QueryRowContext(ctx, `
				SELECT byte_start, byte_end, emitted_at
				FROM health_synthetic_ranges WHERE id = $1
			`, write.ID).Scan(&byteStart, &byteEnd, &retainedAt))
			assert.Equal(t, write.ByteStart, byteStart)
			assert.Equal(t, write.ByteEnd, byteEnd)
			assert.True(t, retainedAt.Equal(emittedAt))
		}
		state, err := f.repo.GetCacheRecoveryState(ctx, f.revision.ID)
		require.NoError(t, err)
		require.NotNil(t, state)
		assert.Equal(t, CacheRecoverySynthetic, state.Status)
		assert.Equal(t, int64(0), state.ContentRevision)
		assert.True(t, state.UpdatedAt.Equal(emittedAt))
	})

	t.Run("MarkSyntheticRangeRecovered", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		f := newFACORECHG007PostgresControlFixture(t, ctx, dsn,
			"SELECT FILE_REVISION_ID", "FROM HEALTH_SYNTHETIC_RANGES WHERE ID =")
		gap, err := f.repo.UpsertGapRange(ctx, GapRangeWrite{
			ID: "control-recovery-gap-" + f.suffix, FileRevisionID: f.revision.ID,
			Kind: GapKindConfirmedAbsent, StartSegment: 0, SegmentCount: 2,
			Status: GapStatusActive, CreatedAt: f.now,
		})
		require.NoError(t, err)
		emittedAt := f.now.Add(time.Minute)
		writes := [2]SyntheticOutputWrite{
			{
				ID: "control-recovery-a-" + f.suffix, GapID: gap.ID,
				FileRevisionID: f.revision.ID, ByteStart: 0, ByteEnd: 99, EmittedAt: emittedAt,
			},
			{
				ID: "control-recovery-b-" + f.suffix, GapID: gap.ID,
				FileRevisionID: f.revision.ID, ByteStart: 100, ByteEnd: 199, EmittedAt: emittedAt,
			},
		}
		for _, write := range writes {
			_, err := f.repo.RecordSyntheticOutput(ctx, write)
			require.NoError(t, err)
		}
		recoveredAt := f.now.Add(2 * time.Minute)
		values, errs := facoreCHG007RunConcurrentPair(t, ctx, [2]func() (*CacheRecoveryState, error){
			func() (*CacheRecoveryState, error) {
				return f.repoA.MarkSyntheticRangeRecovered(ctx, writes[0].ID, recoveredAt)
			},
			func() (*CacheRecoveryState, error) {
				return f.repoB.MarkSyntheticRangeRecovered(ctx, writes[1].ID, recoveredAt)
			},
		})
		for i := range errs {
			require.NoError(t, errs[i], "ordinary concurrent synthetic recovery %d", i)
			require.NotNil(t, values[i])
			assert.Equal(t, CacheRecoveryPending, values[i].Status)
			assert.Equal(t, int64(0), values[i].ContentRevision)
			assert.True(t, values[i].UpdatedAt.Equal(recoveredAt))
		}
		assert.Equal(t, 2, f.barrier.hitCount(), "both callers must reach the internal pre-write read barrier")
		for _, write := range writes {
			var retained time.Time
			require.NoError(t, f.admin.Connection().QueryRowContext(ctx,
				`SELECT recovered_at FROM health_synthetic_ranges WHERE id = $1`, write.ID).Scan(&retained))
			assert.True(t, retained.Equal(recoveredAt))
		}
		state, err := f.repo.GetCacheRecoveryState(ctx, f.revision.ID)
		require.NoError(t, err)
		require.NotNil(t, state)
		assert.Equal(t, CacheRecoveryPending, state.Status)
		assert.Equal(t, int64(0), state.ContentRevision)
		assert.True(t, state.UpdatedAt.Equal(recoveredAt))
	})
}

func facoreCHG007SeedTombstonedProvider(t *testing.T, ctx context.Context, db *sql.DB, suffix string) {
	t.Helper()
	now := time.Now().UTC()
	providerID := "tombstoned-" + suffix
	endpoint := "tombstoned-" + suffix + ".invalid"
	account := "legacy-" + suffix
	fingerprint, err := ProviderIdentityFingerprint(endpoint, 119, account)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `
		INSERT INTO health_providers
			(id, display_name, role, configured_order, active, current_generation,
			 tombstoned_at, created_at, updated_at)
		VALUES ($1, $2, 'primary', 9, FALSE, 1, $3, $3, $3)
	`, providerID, "Tombstoned legacy provider", now)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `
		INSERT INTO health_provider_generations
			(provider_id, generation, endpoint, port, account, identity_fingerprint, created_at)
		VALUES ($1, 1, $2, 119, $3, $4, $5)
	`, providerID, endpoint, account, fingerprint, now)
	require.NoError(t, err)
}
