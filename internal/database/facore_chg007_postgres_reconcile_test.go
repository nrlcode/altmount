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
// Before the correction there is no table lock, so both identity lookups are
// held after their result snapshots are established. Releasing them together
// deterministically exposes the duplicate-active-provider race.
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
	listsInTx       int
	listsOutsideTx  int

	secondLockAttempt  chan struct{}
	identityPairReady  chan struct{}
	secondLockClosed   bool
	identityPairClosed bool
}

func newFACORECHG007ReconcileBarrier() *facoreCHG007ReconcileBarrier {
	return &facoreCHG007ReconcileBarrier{
		secondLockAttempt: make(chan struct{}),
		identityPairReady: make(chan struct{}),
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
	if !lockAware && ordinal == 2 && !b.identityPairClosed {
		close(b.identityPairReady)
		b.identityPairClosed = true
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
	case <-b.identityPairReady:
	case <-done:
	}
}

func (b *facoreCHG007ReconcileBarrier) recordProviderList(inTx bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.armed {
		return
	}
	if inTx {
		b.listsInTx++
		return
	}
	b.listsOutsideTx++
}

func (b *facoreCHG007ReconcileBarrier) stats() (locks, identityResults, listsInTx, listsOutsideTx int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lockAttempts, b.identityResults, b.listsInTx, b.listsOutsideTx
}

type facoreCHG007BarrierDriver struct {
	driver.Driver
	barrier *facoreCHG007ReconcileBarrier
}

func (d *facoreCHG007BarrierDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.Driver.Open(name)
	if err != nil {
		return nil, err
	}
	return &facoreCHG007BarrierConn{Conn: conn, barrier: d.barrier}, nil
}

type facoreCHG007BarrierConn struct {
	driver.Conn
	barrier *facoreCHG007ReconcileBarrier

	mu   sync.Mutex
	inTx bool
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
	c.mu.Lock()
	c.inTx = true
	c.mu.Unlock()
	return &facoreCHG007BarrierTx{Tx: tx, conn: c}, nil
}

func (c *facoreCHG007BarrierConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	preparer, ok := c.Conn.(driver.ConnPrepareContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return preparer.PrepareContext(ctx, query)
}

func (c *facoreCHG007BarrierConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if facoreCHG007IsReconcileLock(query) {
		c.barrier.beforeLock()
	}
	execer, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return execer.ExecContext(ctx, query, args)
}

func (c *facoreCHG007BarrierConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if facoreCHG007IsProviderList(query) {
		c.barrier.recordProviderList(c.transactionActive())
	}
	queryer, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	rows, err := queryer.QueryContext(ctx, query, args)
	if err != nil {
		return nil, err
	}
	if facoreCHG007IsIdentityLookup(query) {
		return &facoreCHG007BarrierRows{Rows: rows, barrier: c.barrier}, nil
	}
	return rows, nil
}

func (c *facoreCHG007BarrierConn) transactionActive() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inTx
}

func (c *facoreCHG007BarrierConn) finishTransaction() {
	c.mu.Lock()
	c.inTx = false
	c.mu.Unlock()
}

type facoreCHG007BarrierTx struct {
	driver.Tx
	conn *facoreCHG007BarrierConn
	once sync.Once
}

func (tx *facoreCHG007BarrierTx) Commit() error {
	err := tx.Tx.Commit()
	tx.once.Do(tx.conn.finishTransaction)
	return err
}

func (tx *facoreCHG007BarrierTx) Rollback() error {
	err := tx.Tx.Rollback()
	tx.once.Do(tx.conn.finishTransaction)
	return err
}

type facoreCHG007BarrierRows struct {
	driver.Rows
	barrier *facoreCHG007ReconcileBarrier
	once    sync.Once
}

func (r *facoreCHG007BarrierRows) Next(dest []driver.Value) error {
	err := r.Rows.Next(dest)
	if err == nil || errors.Is(err, io.EOF) {
		r.once.Do(r.barrier.afterIdentityResult)
	}
	return err
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

func facoreCHG007IsProviderList(query string) bool {
	normalized := facoreCHG007NormalizedSQL(query)
	return strings.Contains(normalized, "SELECT ID, DISPLAY_NAME, ROLE, CONFIGURED_ORDER, ACTIVE, CURRENT_GENERATION") &&
		strings.Contains(normalized, "FROM HEALTH_PROVIDERS")
}

type facoreCHG007ReconcileResult struct {
	caller    string
	providers []HealthProvider
	err       error
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
		Driver:  stdlib.GetDefaultDriver(),
		barrier: barrier,
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
	endpoint := "concurrent-" + suffix + ".invalid"
	account := "account-" + suffix
	displayA := "caller-a-" + suffix
	displayB := "caller-b-" + suffix
	specA := []ProviderSpec{{
		DisplayName: displayA, Endpoint: endpoint, Port: 563, Account: account,
		Role: ProviderRolePrimary, Order: 0,
	}}
	specB := []ProviderSpec{{
		DisplayName: displayB, Endpoint: endpoint, Port: 563, Account: account,
		Role: ProviderRolePrimary, Order: 0,
	}}

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

	for caller, wantDisplay := range map[string]string{"a": displayA, "b": displayB} {
		result, ok := byCaller[caller]
		require.True(t, ok, "missing result for caller %s", caller)
		require.NoError(t, result.err, "caller %s", caller)
		if assert.Len(t, result.providers, 1, "caller %s must observe only its transaction-local configuration", caller) {
			assert.Equal(t, wantDisplay, result.providers[0].DisplayName)
			assert.Equal(t, 0, result.providers[0].Order)
			assert.True(t, result.providers[0].Active)
		}
	}

	lockAttempts, identityResults, listsInTx, listsOutsideTx := barrier.stats()
	assert.Equal(t, 2, lockAttempts, "every reconciliation must acquire the PostgreSQL serialization boundary")
	assert.GreaterOrEqual(t, identityResults, 2, "both reconciliations must resolve against durable identity state")
	assert.Equal(t, 2, listsInTx, "each reconciliation result must be read before its transaction commits")
	assert.Zero(t, listsOutsideTx, "post-commit provider reads can return another caller's configuration")

	type activeProvider struct {
		providerID  string
		displayName string
		generation  int64
		order       int
		identity    string
	}
	rows, err := admin.Connection().QueryContext(ctx, `
		SELECT p.id, p.display_name, p.current_generation, p.configured_order,
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
			&provider.generation, &provider.order, &provider.identity))
		active = append(active, provider)
	}
	require.NoError(t, rows.Err())
	if assert.Len(t, active, 1, "concurrent reconciliation must publish one complete winner, never a union") {
		assert.Contains(t, []string{displayA, displayB}, active[0].displayName)
		assert.Equal(t, 0, active[0].order)
		wantIdentity, fingerprintErr := ProviderIdentityFingerprint(endpoint, 563, account)
		require.NoError(t, fingerprintErr)
		assert.Equal(t, wantIdentity, active[0].identity)
	}

	snapshot, err := repoA.CaptureActiveProviderSnapshot(ctx, time.Now().UTC())
	require.NoError(t, err, "the final winner must form a valid frozen dispatch snapshot")
	t.Cleanup(func() {
		_, cleanupErr := admin.Connection().Exec(`DELETE FROM health_provider_snapshots WHERE id = $1`, snapshot.ID)
		assert.NoError(t, cleanupErr)
	})
	if assert.Len(t, snapshot.Entries, 1, "the frozen snapshot must contain exactly the final winner") && len(active) == 1 {
		entry := snapshot.Entries[0]
		assert.Equal(t, active[0].providerID, entry.ProviderID)
		assert.Equal(t, active[0].generation, entry.ProviderGeneration)
		assert.Equal(t, active[0].order, entry.Order)
	}
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
