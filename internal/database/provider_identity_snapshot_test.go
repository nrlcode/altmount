package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type identityRecordingDriver struct {
	driver.Driver
	mu       sync.Mutex
	armed    bool
	queries  []string
	execs    []string
	begins   int
	prepares int
}

type identityDriverRecords struct {
	queries, execs   []string
	begins, prepares int
}

func (d *identityRecordingDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.Driver.Open(name)
	if err != nil {
		return nil, err
	}
	return &identityRecordingConn{Conn: conn, owner: d}, nil
}

func (d *identityRecordingDriver) arm() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.queries, d.execs = nil, nil
	d.begins, d.prepares = 0, 0
	d.armed = true
}

func (d *identityRecordingDriver) record(query string, exec bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.armed {
		return
	}
	if exec {
		d.execs = append(d.execs, query)
	} else {
		d.queries = append(d.queries, query)
	}
}

func (d *identityRecordingDriver) recordBegin() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.armed {
		d.begins++
	}
}

func (d *identityRecordingDriver) recordPrepare() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.armed {
		d.prepares++
	}
}

func (d *identityRecordingDriver) records() identityDriverRecords {
	d.mu.Lock()
	defer d.mu.Unlock()
	return identityDriverRecords{
		queries: append([]string(nil), d.queries...), execs: append([]string(nil), d.execs...),
		begins: d.begins, prepares: d.prepares,
	}
}

type identityRecordingConn struct {
	driver.Conn
	owner *identityRecordingDriver
}

func (c *identityRecordingConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.owner.recordBegin()
	beginner, ok := c.Conn.(driver.ConnBeginTx)
	if !ok {
		return nil, driver.ErrSkip
	}
	return beginner.BeginTx(ctx, opts)
}

func (c *identityRecordingConn) Prepare(query string) (driver.Stmt, error) {
	c.owner.recordPrepare()
	stmt, err := c.Conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt, nil
}

func (c *identityRecordingConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	c.owner.recordPrepare()
	preparer, ok := c.Conn.(driver.ConnPrepareContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	stmt, err := preparer.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt, nil
}

func (c *identityRecordingConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.owner.record(query, false)
	queryer, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return queryer.QueryContext(ctx, query, args)
}

func (c *identityRecordingConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.owner.record(query, true)
	execer, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return execer.ExecContext(ctx, query, args)
}

func createIdentityRegistryTables(t *testing.T, db *sql.DB, temporary bool) {
	t.Helper()
	prefix := ""
	if temporary {
		prefix = "TEMP "
	}
	_, err := db.Exec(fmt.Sprintf(`
		CREATE %sTABLE health_providers (
			id TEXT PRIMARY KEY,
			display_name TEXT NOT NULL,
			role TEXT NOT NULL,
			configured_order INTEGER NOT NULL,
			active BOOLEAN NOT NULL,
			current_generation BIGINT NOT NULL,
			tombstoned_at TIMESTAMP NULL,
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL
		)`, prefix))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`
		CREATE %sTABLE health_provider_generations (
			provider_id TEXT NOT NULL,
			generation BIGINT NOT NULL,
			endpoint TEXT NOT NULL,
			port INTEGER NOT NULL,
			account TEXT NOT NULL,
			identity_fingerprint TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL,
			PRIMARY KEY(provider_id, generation)
		)`, prefix))
	require.NoError(t, err)
}

func seedIdentityRegistry(t *testing.T, db *sql.DB, dialect Dialect) {
	t.Helper()
	now := time.Date(2026, time.July, 18, 12, 0, 0, 0, time.UTC)
	tombstonedAt := now.Add(time.Hour)
	q := dialectHelper{d: dialect}.q
	for _, provider := range []struct {
		id, name string
		active   bool
		current  int64
		tomb     *time.Time
		order    int
	}{
		{id: "active", name: "Active", active: true, current: 2, order: 0},
		{id: "tombstoned", name: "Tombstoned", current: 1, tomb: &tombstonedAt, order: 1},
		{id: "parent-only", name: "Parent only", current: 1, tomb: &tombstonedAt, order: 2},
	} {
		_, err := db.Exec(q(`
			INSERT INTO health_providers
				(id, display_name, role, configured_order, active, current_generation,
				 tombstoned_at, created_at, updated_at)
			VALUES (?, ?, 'primary', ?, ?, ?, ?, ?, ?)
		`), provider.id, provider.name, provider.order, provider.active, provider.current, provider.tomb, now, now)
		require.NoError(t, err)
	}
	for _, generation := range []struct {
		provider          string
		generation        int64
		endpoint, account string
	}{
		{provider: "active", generation: 1, endpoint: "old.invalid", account: "Account"},
		{provider: "active", generation: 2, endpoint: "new.invalid", account: "Account"},
		{provider: "tombstoned", generation: 1, endpoint: "tomb.invalid", account: "CaseSensitive"},
	} {
		fingerprint, err := ProviderIdentityFingerprint(generation.endpoint, 119, generation.account)
		require.NoError(t, err)
		_, err = db.Exec(q(`
			INSERT INTO health_provider_generations
				(provider_id, generation, endpoint, port, account, identity_fingerprint, created_at)
			VALUES (?, ?, ?, 119, ?, ?, ?)
		`), generation.provider, generation.generation, generation.endpoint, generation.account, fingerprint, now)
		require.NoError(t, err)
	}
}

func assertIdentityRegistryFixture(t *testing.T, snapshot ProviderIdentityRegistrySnapshot) {
	t.Helper()
	require.Len(t, snapshot.Providers, 3)
	providers := make(map[string]ProviderIdentityRecord, len(snapshot.Providers))
	for _, provider := range snapshot.Providers {
		providers[provider.ID] = provider
	}
	active, ok := providers["active"]
	require.True(t, ok)
	assert.True(t, active.Active)
	assert.Nil(t, active.TombstonedAt)
	assert.Equal(t, int64(2), active.CurrentGeneration)
	tombstoned, ok := providers["tombstoned"]
	require.True(t, ok)
	assert.False(t, tombstoned.Active)
	assert.NotNil(t, tombstoned.TombstonedAt)
	assert.Equal(t, int64(1), tombstoned.CurrentGeneration)
	parentOnly, ok := providers["parent-only"]
	require.True(t, ok)
	assert.False(t, parentOnly.Active)
	assert.NotNil(t, parentOnly.TombstonedAt)
	assert.Equal(t, int64(1), parentOnly.CurrentGeneration)

	require.Len(t, snapshot.Generations, 3)
	generations := make(map[string]ProviderIdentityGeneration, len(snapshot.Generations))
	for _, generation := range snapshot.Generations {
		generations[fmt.Sprintf("%s/%d", generation.ProviderID, generation.Generation)] = generation
	}
	assert.Equal(t, "old.invalid", generations["active/1"].Endpoint)
	assert.Equal(t, "new.invalid", generations["active/2"].Endpoint)
	assert.Equal(t, "CaseSensitive", generations["tombstoned/1"].Account)
	for _, generation := range snapshot.Generations {
		assert.NotEqual(t, "parent-only", generation.ProviderID)
		fingerprint, err := ProviderIdentityFingerprint(generation.Endpoint, generation.Port, generation.Account)
		require.NoError(t, err)
		assert.Equal(t, fingerprint, generation.IdentityFingerprint)
	}
}

func TestReadProviderIdentityRegistrySnapshotSQLiteIsOneReadOnlyJoin(t *testing.T) {
	recorder := &identityRecordingDriver{Driver: &sqlite3.SQLiteDriver{}}
	driverName := fmt.Sprintf("provider-identity-%p", recorder)
	sql.Register(driverName, recorder)
	db, err := sql.Open(driverName, ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	createIdentityRegistryTables(t, db, false)
	seedIdentityRegistry(t, db, DialectSQLite)
	_, err = db.Exec("PRAGMA query_only = ON")
	require.NoError(t, err)

	recorder.arm()
	snapshot, err := NewHealthStateRepository(db, DialectSQLite).ReadProviderIdentityRegistrySnapshot(context.Background())
	require.NoError(t, err)
	assertIdentityRegistryFixture(t, snapshot)
	records := recorder.records()
	require.Len(t, records.queries, 1, "the registry must come from one coherent statement snapshot")
	assert.Empty(t, records.execs, "a registry read must not execute writes or setup statements")
	assert.Zero(t, records.begins, "one SELECT needs no transaction escape hatch")
	assert.Zero(t, records.prepares, "the coherent read must execute directly")
	query := strings.ToUpper(records.queries[0])
	assert.Contains(t, query, "LEFT JOIN")
	assert.Contains(t, query, "HEALTH_PROVIDERS")
	assert.Contains(t, query, "HEALTH_PROVIDER_GENERATIONS")
}

func TestReadProviderIdentityRegistrySnapshotPostgresParity(t *testing.T) {
	dsn := os.Getenv("ALTMOUNT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("ALTMOUNT_TEST_POSTGRES_DSN is not configured")
	}
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.NoError(t, db.PingContext(context.Background()))
	createIdentityRegistryTables(t, db, true)
	seedIdentityRegistry(t, db, DialectPostgres)

	snapshot, err := NewHealthStateRepository(db, DialectPostgres).ReadProviderIdentityRegistrySnapshot(context.Background())
	require.NoError(t, err)
	assertIdentityRegistryFixture(t, snapshot)
}

func TestProviderIdentityFingerprintNormalization(t *testing.T) {
	canonical, err := ProviderIdentityFingerprint("news.example.invalid", 563, "Account")
	require.NoError(t, err)
	assert.Equal(t, "sha256:0c43e425d3199f440209b1062da70428c27a96049998fd74fec2b7093aa3da63", canonical)
	normalized, err := ProviderIdentityFingerprint("  NEWS.Example.Invalid.  ", 563, "  Account  ")
	require.NoError(t, err)
	assert.Equal(t, canonical, normalized, "endpoint case/space/one trailing dot and account space are insignificant")

	doubleDot, err := ProviderIdentityFingerprint("news.example.invalid..", 563, "Account")
	require.NoError(t, err)
	assert.NotEqual(t, canonical, doubleDot, "normalization removes exactly one trailing dot")
	lowerAccount, err := ProviderIdentityFingerprint("news.example.invalid", 563, "account")
	require.NoError(t, err)
	assert.NotEqual(t, canonical, lowerAccount, "account identity remains case-sensitive")
	otherPort, err := ProviderIdentityFingerprint("news.example.invalid", 564, "Account")
	require.NoError(t, err)
	assert.NotEqual(t, canonical, otherPort, "port remains part of provider identity")

	for _, input := range []struct {
		endpoint string
		port     int
	}{{endpoint: "  ", port: 119}, {endpoint: "news.invalid", port: 0}, {endpoint: "news.invalid", port: 65536}} {
		_, err := ProviderIdentityFingerprint(input.endpoint, input.port, "Account")
		assert.Error(t, err)
	}
}
