package database

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var pr5Tables = []string{
	"health_import_validations",
	"health_import_activation_journal",
	"health_run_schedule",
	"nzb_store_ref_operations",
}

func requirePR5Schema(t *testing.T, db *sql.DB, dialect Dialect) {
	t.Helper()
	for _, table := range pr5Tables {
		assert.Truef(t, pr4TableExists(t, db, dialect, table), "missing PR5 table %s", table)
	}
	requiredColumns := map[string][]string{
		"health_run_chunks": {
			"fresh_transport",
		},
		"health_providers": {
			"activation_epoch", "activated_at",
		},
		"health_provider_snapshot_entries": {
			"provider_activation_epoch",
		},
		"health_gap_ranges": {
			"episode", "revalidation_step", "next_revalidation_at", "last_revalidation_at",
		},
		"health_run_schedule": {
			"run_id", "dedupe_key", "active", "target_provider_id",
			"target_provider_generation", "target_provider_activation_epoch",
			"target_gap_id", "priority", "not_before", "created_at", "updated_at",
		},
		"health_import_validations": {
			"id", "queue_item_id", "file_revision_id", "run_id", "phase",
			"damage_policy", "confirmation_due_at", "unresolved_segments",
			"coverage_reused_at", "health_pending_settled_at", "created_at", "updated_at",
		},
		"health_import_activation_journal": {
			"queue_item_id", "candidate_revision_id", "file_health_id",
			"prior_revision_id", "prior_status", "prior_scheduled_check_at",
			"prior_priority", "prior_retry_count", "prior_repair_retry_count",
			"candidate_scheduled_check_at", "candidate_priority", "state",
			"created_at", "updated_at", "resolved_at",
		},
		"nzb_store_ref_operations": {
			"operation_key", "store_path_hash", "delta", "resulting_ref_count", "applied_at",
		},
	}
	for table, want := range requiredColumns {
		got := pr4Columns(t, db, dialect, table)
		for _, column := range want {
			assert.Containsf(t, got, column, "%s missing column %s", table, column)
		}
	}
}

func TestPR5SQLiteMigrationCreatesSchedulerImportAndEpisodeSchema(t *testing.T) {
	db, err := NewDB(Config{Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "pr5.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	requirePR5Schema(t, db.Connection(), DialectSQLite)
}

func TestPR5PostgresMigrationCreatesEquivalentSchedulerImportAndEpisodeSchema(t *testing.T) {
	dsn := os.Getenv("ALTMOUNT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("ALTMOUNT_TEST_POSTGRES_DSN is not configured")
	}
	db, err := NewDB(Config{Type: "postgres", DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	requirePR5Schema(t, db.Connection(), DialectPostgres)
}

func TestPR5SQLiteMigration036RollbackPreservesPR4State(t *testing.T) {
	db, err := NewDB(Config{Type: "sqlite", DatabasePath: filepath.Join(t.TempDir(), "roundtrip.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	conn := db.Connection()
	ctx := context.Background()

	_, err = conn.ExecContext(ctx, `
		INSERT INTO file_health (file_path, status) VALUES (?, ?)
	`, "library/pr5-roundtrip.mkv", "pending")
	require.NoError(t, err)

	goose.SetBaseFS(embedMigrations)
	require.NoError(t, goose.SetDialect("sqlite3"))
	require.NoError(t, goose.DownToContext(ctx, conn, "migrations/sqlite", 35))
	for _, table := range pr5Tables {
		assert.Falsef(t, pr4TableExists(t, conn, DialectSQLite, table), "%s survived PR5 rollback", table)
	}
	assert.NotContains(t, pr4Columns(t, conn, DialectSQLite, "health_providers"), "activation_epoch")
	assert.NotContains(t, pr4Columns(t, conn, DialectSQLite, "health_gap_ranges"), "episode")

	var status string
	require.NoError(t, conn.QueryRowContext(ctx, `
		SELECT status FROM file_health WHERE file_path = ?
	`, "library/pr5-roundtrip.mkv").Scan(&status))
	assert.Equal(t, "pending", status)
	requirePR4Schema(t, conn, DialectSQLite)

	require.NoError(t, goose.UpContext(ctx, conn, "migrations/sqlite"))
	requirePR5Schema(t, conn, DialectSQLite)
	require.NoError(t, conn.QueryRowContext(ctx, `
		SELECT status FROM file_health WHERE file_path = ?
	`, "library/pr5-roundtrip.mkv").Scan(&status))
	assert.Equal(t, "pending", status)
}

func TestPR5PostgresMigration036RollbackPreservesPR4State(t *testing.T) {
	dsn := os.Getenv("ALTMOUNT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("ALTMOUNT_TEST_POSTGRES_DSN is not configured")
	}
	db, err := NewDB(Config{Type: "postgres", DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	conn := db.Connection()
	ctx := context.Background()
	const path = "library/pr5-postgres-roundtrip.mkv"

	_, err = conn.ExecContext(ctx, `
		INSERT INTO file_health (file_path, status)
		VALUES ($1, $2)
		ON CONFLICT(file_path) DO UPDATE SET status = EXCLUDED.status
	`, path, "pending")
	require.NoError(t, err)

	goose.SetBaseFS(embedMigrations)
	require.NoError(t, goose.SetDialect("postgres"))
	require.NoError(t, goose.DownToContext(ctx, conn, "migrations/postgres", 35))
	for _, table := range pr5Tables {
		assert.Falsef(t, pr4TableExists(t, conn, DialectPostgres, table), "%s survived PR5 rollback", table)
	}
	assert.NotContains(t, pr4Columns(t, conn, DialectPostgres, "health_providers"), "activation_epoch")
	assert.NotContains(t, pr4Columns(t, conn, DialectPostgres, "health_gap_ranges"), "episode")
	requirePR4Schema(t, conn, DialectPostgres)

	require.NoError(t, goose.UpContext(ctx, conn, "migrations/postgres"))
	requirePR5Schema(t, conn, DialectPostgres)
	var status string
	require.NoError(t, conn.QueryRowContext(ctx, `
		SELECT status FROM file_health WHERE file_path = $1
	`, path).Scan(&status))
	assert.Equal(t, "pending", status)
}
