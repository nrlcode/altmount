package database

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const facoreCHG015MigrationVersion int64 = 38

func TestFACORECHG015HistoricalMigrationsRemainImmutable(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		wantSHA256 string
	}{
		{"postgres_027", "migrations/postgres/027_add_metadata_to_file_health.sql", "5d03a6eb80a3c72eb1b81e1e24be646fbf48d5d994b8e007692ec1602cce8f42"},
		{"sqlite_027", "migrations/sqlite/027_add_metadata_to_file_health.sql", "843e0418e1b5867f6d50cd20cebd035ec876feb92be147bded99a1c679e82625"},
		{"postgres_037", "migrations/postgres/037_add_file_health_claim_generation.sql", "654cc6e237b1c2a43e0fc409fc24b12ed4cd044cb64281d964ba0c2b97b83bdd"},
		{"sqlite_037", "migrations/sqlite/037_add_file_health_claim_generation.sql", "aee90f540ef90ab7503f16018639bd2a4bbc2101769cde3b31261e09832c61ee"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			contents, err := embedMigrations.ReadFile(test.path)
			require.NoError(t, err)
			assert.Equal(t, test.wantSHA256, fmt.Sprintf("%x", sha256.Sum256(contents)))
		})
	}
}

func TestFACORECHG015PostgresMetadataRepairAfterCrossSchemaSkip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	backend := newFACORECHG009PostgresMigrationBackend(t, ctx)
	goose.SetBaseFS(embedMigrations)
	require.NoError(t, goose.SetDialect(backend.gooseDialect))
	require.NoError(t, goose.UpToContext(ctx, backend.db, backend.migrationsDir, 26))

	const (
		filePath   = "facore-chg015/cross-schema.mkv"
		lastError  = "preserve migration evidence"
		activeJSON = `{"owner":"active"}`
		decoyJSON  = `{"owner":"decoy"}`
	)
	_, err := backend.dialectAwareSQL.ExecContext(ctx, `
		INSERT INTO file_health (file_path, status, retry_count, max_retries, last_error)
		VALUES (?, ?, ?, ?, ?)
	`, filePath, HealthStatusChecking, 2, 4, lastError)
	require.NoError(t, err)

	var activeSchema string
	require.NoError(t, backend.db.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&activeSchema))
	decoySchema := activeSchema + "_decoy"
	quotedDecoy := pgx.Identifier{decoySchema}.Sanitize()
	_, err = backend.db.ExecContext(ctx, "CREATE SCHEMA "+quotedDecoy)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, dropErr := backend.db.Exec("DROP SCHEMA IF EXISTS " + quotedDecoy + " CASCADE")
		assert.NoError(t, dropErr)
	})
	_, err = backend.db.ExecContext(ctx,
		"CREATE TABLE "+quotedDecoy+".file_health (metadata JSONB)")
	require.NoError(t, err)
	_, err = backend.db.ExecContext(ctx,
		"INSERT INTO "+quotedDecoy+".file_health (metadata) VALUES ($1::jsonb)", decoyJSON)
	require.NoError(t, err)

	require.NoError(t, goose.UpToContext(
		ctx, backend.db, backend.migrationsDir, facoreCHG014MigrationVersion,
	))
	require.Equal(t, facoreCHG014MigrationVersion,
		facoreCHG009DatabaseVersion(t, ctx, backend))
	var migration027Applied bool
	require.NoError(t, backend.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM goose_db_version
			WHERE version_id = 27 AND is_applied = TRUE
		)
	`).Scan(&migration027Applied))
	assert.True(t, migration027Applied)
	assert.NotContains(t, pr4Columns(t, backend.db, DialectPostgres, "file_health"), "metadata")

	var (
		status     HealthStatus
		retryCount int
		gotError   string
	)
	require.NoError(t, backend.dialectAwareSQL.QueryRowContext(ctx, `
		SELECT status, retry_count, last_error FROM file_health WHERE file_path = ?
	`, filePath).Scan(&status, &retryCount, &gotError))
	assert.Equal(t, HealthStatusChecking, status)
	assert.Equal(t, 2, retryCount)
	assert.Equal(t, lastError, gotError)

	repo := NewHealthRepository(backend.db, DialectPostgres)
	_, err = repo.GetFileHealth(ctx, filePath)
	require.Error(t, err)
	var postgresError *pgconn.PgError
	require.True(t, errors.As(err, &postgresError))
	assert.Equal(t, "42703", postgresError.Code)
	assert.Contains(t, postgresError.Message, "metadata")

	require.NoError(t, runMigrations(backend.db, DialectPostgres))
	require.Equal(t, facoreCHG015MigrationVersion,
		facoreCHG009DatabaseVersion(t, ctx, backend))
	metadataColumn := facoreCHG009ReadColumnMetadata(t, backend, "file_health", "metadata")
	assert.Equal(t, "jsonb", metadataColumn.dataType)
	assert.False(t, metadataColumn.notNull)

	requireActiveRow := func(t *testing.T, wantMetadata string) {
		t.Helper()
		got, getErr := repo.GetFileHealth(ctx, filePath)
		require.NoError(t, getErr)
		require.NotNil(t, got)
		assert.Equal(t, HealthStatusChecking, got.Status)
		assert.Equal(t, 2, got.RetryCount)
		assert.Equal(t, 4, got.MaxRetries)
		require.NotNil(t, got.LastError)
		assert.Equal(t, lastError, *got.LastError)
		if wantMetadata == "" {
			assert.Nil(t, got.Metadata)
			return
		}
		require.NotNil(t, got.Metadata)
		assert.JSONEq(t, wantMetadata, *got.Metadata)
	}
	requireDecoy := func(t *testing.T) {
		t.Helper()
		var got string
		require.NoError(t, backend.db.QueryRowContext(ctx,
			"SELECT metadata::text FROM "+quotedDecoy+".file_health").Scan(&got))
		assert.JSONEq(t, decoyJSON, got)
	}
	requireActiveRow(t, "")
	requireDecoy(t)

	_, err = backend.dialectAwareSQL.ExecContext(ctx,
		`UPDATE file_health SET metadata = ? WHERE file_path = ?`, activeJSON, filePath)
	require.NoError(t, err)
	requireActiveRow(t, activeJSON)
	requireDecoy(t)

	require.NoError(t, goose.DownToContext(
		ctx, backend.db, backend.migrationsDir, facoreCHG014MigrationVersion,
	))
	require.Equal(t, facoreCHG014MigrationVersion,
		facoreCHG009DatabaseVersion(t, ctx, backend))
	requireActiveRow(t, activeJSON)
	requireDecoy(t)

	require.NoError(t, runMigrations(backend.db, DialectPostgres))
	require.Equal(t, facoreCHG015MigrationVersion,
		facoreCHG009DatabaseVersion(t, ctx, backend))
	requireActiveRow(t, activeJSON)
	requireDecoy(t)
}

func TestFACORECHG015SQLiteNoOpMigrationPreservesMetadata(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	backend := newFACORECHG009SQLiteMigrationBackend(t, ctx)
	goose.SetBaseFS(embedMigrations)
	require.NoError(t, goose.SetDialect(backend.gooseDialect))
	require.NoError(t, goose.UpToContext(
		ctx, backend.db, backend.migrationsDir, facoreCHG014MigrationVersion,
	))

	const filePath = "facore-chg015/sqlite-parity.mkv"
	metadata := `{"owner":"sqlite"}`
	lastError := "preserve sqlite migration evidence"
	_, err := backend.dialectAwareSQL.ExecContext(ctx, `
		INSERT INTO file_health
			(file_path, status, retry_count, max_retries, last_error, metadata)
		VALUES (?, ?, ?, ?, ?, ?)
	`, filePath, HealthStatusPending, 1, 5, lastError, metadata)
	require.NoError(t, err)
	repo := NewHealthRepository(backend.db, DialectSQLite)
	want, err := repo.GetFileHealth(ctx, filePath)
	require.NoError(t, err)
	require.NotNil(t, want)

	requireRowUnchanged := func(t *testing.T) {
		t.Helper()
		got, getErr := repo.GetFileHealth(ctx, filePath)
		require.NoError(t, getErr)
		assert.Equal(t, want, got)
	}
	require.NoError(t, runMigrations(backend.db, DialectSQLite))
	require.Equal(t, facoreCHG015MigrationVersion,
		facoreCHG009DatabaseVersion(t, ctx, backend))
	requireRowUnchanged(t)

	require.NoError(t, goose.DownToContext(
		ctx, backend.db, backend.migrationsDir, facoreCHG014MigrationVersion,
	))
	require.Equal(t, facoreCHG014MigrationVersion,
		facoreCHG009DatabaseVersion(t, ctx, backend))
	requireRowUnchanged(t)

	require.NoError(t, runMigrations(backend.db, DialectSQLite))
	require.Equal(t, facoreCHG015MigrationVersion,
		facoreCHG009DatabaseVersion(t, ctx, backend))
	requireRowUnchanged(t)
}
