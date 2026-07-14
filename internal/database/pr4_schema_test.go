package database

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var pr4Tables = []string{
	"health_attempt_evidence",
	"health_cache_recovery",
	"health_confirmation_events",
	"health_file_revisions",
	"health_gap_provider_causes",
	"health_gap_ranges",
	"health_provider_coverage",
	"health_provider_generations",
	"health_provider_snapshot_entries",
	"health_provider_snapshots",
	"health_providers",
	"health_retry_states",
	"health_run_chunks",
	"health_runs",
	"health_segment_exceptions",
	"health_synthetic_ranges",
}

func pr4TableExists(t *testing.T, db *sql.DB, dialect Dialect, table string) bool {
	t.Helper()
	if dialect == DialectPostgres {
		var exists bool
		require.NoError(t, db.QueryRow(`SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = current_schema() AND table_name = $1
		)`, table).Scan(&exists))
		return exists
	}
	var count int
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, table,
	).Scan(&count))
	return count == 1
}

func pr4Columns(t *testing.T, db *sql.DB, dialect Dialect, table string) []string {
	t.Helper()
	var rows *sql.Rows
	var err error
	if dialect == DialectPostgres {
		rows, err = db.Query(`SELECT column_name FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = $1`, table)
	} else {
		rows, err = db.Query(`PRAGMA table_info(` + table + `)`)
	}
	require.NoError(t, err)
	defer rows.Close()
	var columns []string
	for rows.Next() {
		if dialect == DialectPostgres {
			var name string
			require.NoError(t, rows.Scan(&name))
			columns = append(columns, name)
			continue
		}
		var cid, notNull, pk int
		var name, typ string
		var defaultValue any
		require.NoError(t, rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk))
		columns = append(columns, name)
	}
	require.NoError(t, rows.Err())
	sort.Strings(columns)
	return columns
}

func requirePR4Schema(t *testing.T, db *sql.DB, dialect Dialect) {
	t.Helper()
	for _, table := range pr4Tables {
		assert.Truef(t, pr4TableExists(t, db, dialect, table), "missing PR4 table %s", table)
	}
	requiredColumns := map[string][]string{
		"health_file_revisions":       {"id", "file_health_id", "layout_fingerprint", "virtual_size", "segment_count", "active"},
		"health_providers":            {"id", "display_name", "role", "configured_order", "active", "current_generation"},
		"health_provider_generations": {"provider_id", "generation", "endpoint", "port", "account", "identity_fingerprint"},
		"health_runs":                 {"id", "file_revision_id", "provider_snapshot_id", "trigger", "mode", "status", "lease_owner", "lease_expires_at", "fencing_token", "total_segments", "resolved_segments", "stage", "cursor_segment", "pause_requested", "cancel_requested"},
		"health_run_chunks":           {"id", "run_id", "provider_id", "provider_generation", "observation_kind", "segment_start", "segment_count", "tested_bitmap", "present_bitmap", "absent_bitmap", "corrupt_bitmap", "temporary_bitmap", "inconclusive_bitmap", "retry_state", "commit_digest", "resolved_delta", "provider_checks_delta", "missing_candidates_delta", "inconclusive_delta"},
		"health_provider_coverage":    {"id", "file_revision_id", "provider_id", "provider_generation", "observation_kind", "segment_start", "segment_count", "tested_bitmap", "present_bitmap", "source_chunk_id"},
		"health_gap_ranges":           {"id", "file_revision_id", "kind", "start_segment", "segment_count", "status"},
		"health_synthetic_ranges":     {"id", "gap_id", "byte_start", "byte_end", "emitted_at", "recovered_at"},
		"health_cache_recovery":       {"file_revision_id", "status", "retry_count", "next_retry_at", "content_revision"},
	}
	for table, want := range requiredColumns {
		got := pr4Columns(t, db, dialect, table)
		for _, column := range want {
			assert.Containsf(t, got, column, "%s missing column %s", table, column)
		}
	}
}

func requirePR4SQLiteColumnNotNull(t *testing.T, db *sql.DB, table, column string) {
	t.Helper()
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, typ string
		var defaultValue any
		require.NoError(t, rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &primaryKey))
		if name == column {
			assert.Equalf(t, 1, notNull, "%s.%s must reject NULL identities", table, column)
			return
		}
	}
	require.NoError(t, rows.Err())
	t.Fatalf("column %s.%s does not exist", table, column)
}

func TestPR4SQLiteMigrationCreatesDurableHealthSchema(t *testing.T) {
	db, err := NewDB(Config{Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "pr4.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	requirePR4Schema(t, db.Connection(), DialectSQLite)
}

func TestPR4SQLitePrimaryIdentitiesAreExplicitlyNotNull(t *testing.T) {
	db, err := NewDB(Config{Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "not-null-identities.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	identities := map[string]string{
		"health_file_revisions":      "id",
		"health_providers":           "id",
		"health_provider_snapshots":  "id",
		"health_runs":                "id",
		"health_run_chunks":          "id",
		"health_provider_coverage":   "id",
		"health_attempt_evidence":    "idempotency_key",
		"health_confirmation_events": "idempotency_key",
		"health_retry_states":        "retry_key",
		"health_gap_ranges":          "id",
		"health_synthetic_ranges":    "id",
		"health_cache_recovery":      "file_revision_id",
	}
	for table, column := range identities {
		requirePR4SQLiteColumnNotNull(t, db.Connection(), table, column)
	}
}

func TestPR4SQLiteEnablesForeignKeysOnEveryPooledConnection(t *testing.T) {
	db, err := NewDB(Config{Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "pooled-fks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	ctx := context.Background()
	connections := make([]*sql.Conn, 0, 8)
	t.Cleanup(func() {
		for _, conn := range connections {
			require.NoError(t, conn.Close())
		}
	})
	for i := 0; i < 8; i++ {
		conn, err := db.Connection().Conn(ctx)
		require.NoError(t, err)
		connections = append(connections, conn)
		var enabled int
		require.NoError(t, conn.QueryRowContext(ctx, `PRAGMA foreign_keys`).Scan(&enabled))
		assert.Equalf(t, 1, enabled, "pooled SQLite connection %d is not enforcing foreign keys", i)
	}
}

func TestPR4PostgresMigrationCreatesEquivalentHealthSchema(t *testing.T) {
	dsn := os.Getenv("ALTMOUNT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("ALTMOUNT_TEST_POSTGRES_DSN is not configured")
	}
	db, err := NewDB(Config{Type: "postgres", DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	requirePR4Schema(t, db.Connection(), DialectPostgres)
}

func TestPR4SQLiteMigrationRoundTripPreservesExistingHealthRows(t *testing.T) {
	db, err := NewDB(Config{Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "roundtrip.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	conn := db.Connection()
	_, err = conn.Exec(`INSERT INTO file_health (file_path, status) VALUES (?, ?)`, "library/example.mkv", "pending")
	require.NoError(t, err)

	goose.SetBaseFS(embedMigrations)
	require.NoError(t, goose.SetDialect("sqlite3"))
	require.NoError(t, goose.DownToContext(context.Background(), conn, "migrations/sqlite", 34))
	for _, table := range pr4Tables {
		assert.Falsef(t, pr4TableExists(t, conn, DialectSQLite, table), "%s survived PR4 rollback", table)
	}
	var status string
	require.NoError(t, conn.QueryRow(`SELECT status FROM file_health WHERE file_path = ?`, "library/example.mkv").Scan(&status))
	assert.Equal(t, "pending", status)

	require.NoError(t, goose.UpContext(context.Background(), conn, "migrations/sqlite"))
	requirePR4Schema(t, conn, DialectSQLite)
	require.NoError(t, conn.QueryRow(`SELECT status FROM file_health WHERE file_path = ?`, "library/example.mkv").Scan(&status))
	assert.Equal(t, "pending", status)
}

func TestPR4PostgresMigrationRoundTripPreservesExistingHealthRows(t *testing.T) {
	dsn := os.Getenv("ALTMOUNT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("ALTMOUNT_TEST_POSTGRES_DSN is not configured")
	}
	db, err := NewDB(Config{Type: "postgres", DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	conn := db.Connection()
	const path = "pr4-postgres-roundtrip/example.mkv"
	_, err = conn.Exec(`INSERT INTO file_health (file_path, status) VALUES ($1, $2) ON CONFLICT(file_path) DO UPDATE SET status = EXCLUDED.status`, path, "pending")
	require.NoError(t, err)

	goose.SetBaseFS(embedMigrations)
	require.NoError(t, goose.SetDialect("postgres"))
	require.NoError(t, goose.DownToContext(context.Background(), conn, "migrations/postgres", 34))
	for _, table := range pr4Tables {
		assert.Falsef(t, pr4TableExists(t, conn, DialectPostgres, table), "%s survived PR4 rollback", table)
	}
	var status string
	require.NoError(t, conn.QueryRow(`SELECT status FROM file_health WHERE file_path = $1`, path).Scan(&status))
	assert.Equal(t, "pending", status)

	require.NoError(t, goose.UpContext(context.Background(), conn, "migrations/postgres"))
	requirePR4Schema(t, conn, DialectPostgres)
	require.NoError(t, conn.QueryRow(`SELECT status FROM file_health WHERE file_path = $1`, path).Scan(&status))
	assert.Equal(t, "pending", status)
}
